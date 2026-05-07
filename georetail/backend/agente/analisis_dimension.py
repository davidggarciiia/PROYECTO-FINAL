"""
agente/analisis_dimension.py — Lectura interpretativa + decisiones prácticas
por dimensión, generadas con LLM a partir de los datos reales de la zona y
un catálogo de implicaciones operativas (templates por dim×sector).

Endpoint llamador: POST /api/dimension/{dim_key}/{zona_id}/narrativa.

Flujo:
  1. Toma los datos relevantes de la dimensión (no todos los campos de la
     zona — solo los que importan para esta dim).
  2. Toma `IMPLICACIONES_TEMPLATES[dim][sector]` (catálogo). El LLM elige 3
     plantillas, las reordena por relevancia y las concreta con los números
     reales de la zona. Esto evita alucinaciones — el LLM no inventa
     decisiones, solo las personaliza.
  3. Genera "lectura" interpretativa de 50-60 palabras: NO descriptiva (no
     repite los números) sino INTERPRETATIVA (qué significan esos números
     para el negocio del sector concreto).
  4. Devuelve JSON estricto {"lectura": str, "decisiones": [str, str, str]}.

Cache: el endpoint cachea por (zona_id, dim_key, sector, perfil_hash) en
narrativa_dimension_cache durante 30 días.
"""
from __future__ import annotations

import hashlib
import json
import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

_MAX_TOKENS = 700
_TIMEOUT_S  = 35.0

# Dimensiones soportadas (Competencia tiene su propio análisis vía
# /api/competencia/{zona_id}/analisis-profundo, así que NO entra aquí).
DIMS_SOPORTADAS = {
    "flujo_peatonal", "demografia", "transporte",
    "seguridad", "turismo", "dinamismo",
}

# ── Catálogo de implicaciones por (dim, sector) ──────────────────────────────
# El LLM elige 3 plantillas de aquí, reordena, y concreta con los números
# reales de la zona. La clave "_default" se usa si el sector no está mapeado.
IMPLICACIONES_TEMPLATES: Dict[str, Dict[str, List[str]]] = {
    "flujo_peatonal": {
        "_default": [
            "Adapta horarios y plantilla al pico real de la zona.",
            "Si el flujo es residente, prioriza fidelización; si es de paso, escaparate y conversión.",
            "Concentra promociones en los días/franjas con mayor flujo medido.",
        ],
        "restauracion": [
            "Si el pico es tarde/noche, refuerza plantilla esas franjas y sirve carta express.",
            "Si el flujo cae en domingo, valora cierre o brunch reducido.",
            "Diseña la barra para servir 80–100 cubiertos/h en hora punta sin colas.",
            "Adapta horarios de cocina al pico real medido, no al horario estándar del barrio.",
            "Si el flujo es de paso, prioriza visibilidad del escaparate y conversión rápida.",
        ],
        "moda": [
            "Concentra rebajas y novedades en los días de pico medido.",
            "Aprovecha el flujo de paso con escaparate dinámico — rota cada 10 días.",
            "Si el flujo es residente, fideliza con club de cliente y stock recurrente.",
            "Calibra el aforo del probador a la franja punta para no perder ventas.",
        ],
        "estetica": [
            "Si predomina el residente, abre agenda con 48h de margen de reserva.",
            "El flujo de paso ayuda al walk-in para servicios cortos (manicura, depilación).",
            "Adapta el horario al pico real medido, no al horario del barrio.",
            "Refuerza la franja con más flujo con personal extra o servicios express.",
        ],
        "tatuajes": [
            "Sesiones largas → cita previa, agenda compacta en franjas valle.",
            "Aprovecha el pico de paso para captar walk-in con consulta gratuita.",
        ],
        "alimentacion": [
            "Stocks ajustados al pico semanal medido — evita rotura los viernes/sábados.",
            "Si el flujo es residente, fideliza con tarjeta y oferta semanal repetida.",
        ],
    },
    "demografia": {
        "_default": [
            "Asegúrate de que el ticket medio encaja con la renta media de la zona.",
            "Comunica en el idioma dominante del barrio.",
            "Adapta la oferta al rango de edad mediana de la zona.",
        ],
        "restauracion": [
            "El ticket medio debe encajar con la renta hogar de la zona.",
            "Si hay alta presencia universitaria, valora menú del día y after-work.",
            "Comunicación bilingüe ES/EN si la zona tiene >15% extranjeros.",
            "Renta alta soporta carta premium y maridajes; renta media pide menú diario.",
        ],
        "moda": [
            "Calibra precio y producto a la franja de edad dominante de la zona.",
            "Renta alta sostiene producto premium y exclusividad — usa una cápsula limitada.",
            "Si la zona es joven (<35 mediana), refuerza presencia en redes locales.",
            "Estudios altos predicen mejor conversión en marca-concepto que en chollo.",
        ],
        "estetica": [
            "El público objetivo del servicio debe coincidir con la edad mediana del barrio.",
            "Renta alta soporta tratamientos premium y aparatología cara.",
            "Si la zona envejece, prioriza servicios anti-edad y bienestar.",
        ],
        "tatuajes": [
            "Edad mediana joven (<35) eleva la demanda de proyectos completos vs flash.",
            "Renta media-alta soporta sesiones largas con artista de referencia.",
        ],
        "alimentacion": [
            "Renta media-alta soporta producto fresco premium y eco; renta media pide DPH.",
            "Población envejecida → reparto a domicilio y formatos individuales.",
        ],
    },
    "transporte": {
        "_default": [
            "Comunica los accesos en tu Google Business Profile y RRSS.",
            "La buena conectividad amplía tu zona de captación más allá del barrio.",
            "Si la conectividad es baja, refuerza visibilidad local y fidelización.",
        ],
        "restauracion": [
            "Comunica accesibilidad: «a 4 min de la parada X» en Google y RRSS.",
            "Si hay metro a < 5 min, dimensiona para clientela de fuera del barrio.",
            "Aliarse con hoteles cercanos para captar cliente de paso.",
            "Si el bicing es alto, ofrece happy-hour para captar after-work bici.",
        ],
        "moda": [
            "Buena conectividad amplía la zona de captación a toda la ciudad — invierte en ads.",
            "Si la zona es de metro principal, calcula cuántas líneas pasan y comunícalo.",
        ],
        "estetica": [
            "Cliente vendrá de su barrio si la oferta es nicho — la conectividad importa menos.",
            "Si el metro está cerca, capta cliente que cruza la ciudad por especialidad.",
        ],
    },
    "seguridad": {
        "_default": [
            "Adapta el cierre al pico nocturno de incidencias.",
            "Considera seguro RC ampliado si la zona supera la media de incidencias.",
            "Coordina con la policía local si la actividad nocturna es alta.",
        ],
        "restauracion": [
            "Cierre antes de las 23h reduce drásticamente el riesgo nocturno.",
            "Si hay actividad nocturna alta, contrata seguro RC ampliado y caja con doble llave.",
            "Coordina cierre con la ronda municipal si la franja 22-2h es complicada.",
            "Cámaras visibles en barra y puerta disuaden incidencias menores.",
        ],
        "moda": [
            "Persiana metálica reforzada y alarma 24/7 son obligadas si las incidencias superan la media.",
            "Etiqueta antirrobo en producto premium — el ratio dañina/visible importa.",
        ],
        "estetica": [
            "Negocio diurno → riesgo nocturno irrelevante; foco en visibilidad.",
            "Si trabajas con efectivo, ingresa cada 4h durante el día.",
        ],
        "tatuajes": [
            "Trabajo con efectivo + caja diaria → ingresa cada 4h y usa caja segura nocturna.",
        ],
    },
    "turismo": {
        "_default": [
            "Si dependes del turista, presupuesta caja para temporada baja.",
            "Si la clientela es local, fideliza con producto recurrente.",
            "Comunicación bilingüe básica si el lift turístico supera +20%.",
        ],
        "restauracion": [
            "Alta dependencia turística → presupuesta caja para cubrir el bajón de invierno.",
            "Carta bilingüe y QR en mesa son básicos en zonas con > 20% turista.",
            "Si la zona es local, fideliza al residente con menú semanal.",
            "Lift de verano alto → carta de temporada con producto fresco y horario extendido.",
        ],
        "moda": [
            "El turista suele buscar souvenir o producto local — diseña una cápsula específica.",
            "Producto premium funciona mejor en zonas hoteleras de 4-5★.",
            "Lift navideño alto → reserva stock para campaña de regalos diciembre.",
        ],
        "estetica": [
            "El turista no es tu público — el score turístico no debe penalizar la decisión.",
            "Si hay hoteles cercanos, ofrece servicios express para huésped.",
        ],
    },
    "dinamismo": {
        "_default": [
            "Tasa de supervivencia alta de la zona reduce la prima de riesgo.",
            "Si la zona crece en aperturas, el alquiler subirá en próxima revisión.",
            "Vigila la concentración sectorial — un HHI alto indica saturación.",
        ],
        "restauracion": [
            "Barrio en alza → posibilidad de revisión al alza del alquiler en próximo contrato.",
            "Llegada de operadores referente reduce el riesgo de abrir en solitario.",
            "Tasa de supervivencia 3a por encima de la media baja la prima de riesgo.",
            "Si el HHI está alto en restauración, posiciónate en un nicho aún no cubierto.",
        ],
        "moda": [
            "Aprovecha la llegada de marcas referente para co-marketing y ventas cruzadas.",
            "Concentración alta de moda en la zona → flujo cruzado, pero más exigencia diferencial.",
        ],
        "estetica": [
            "Aperturas crecientes pueden saturar el subsector — adelanta tu apertura si la idea está clara.",
            "Supervivencia baja en este subsector → estudia los cierres recientes antes de firmar.",
        ],
    },
}

# ── Subset de campos relevantes por dim (mantiene el prompt compacto) ────────
DIM_FIELDS: Dict[str, List[str]] = {
    "flujo_peatonal": [
        "vcity_flujo_peatonal", "flujo_peatonal_dia", "ratio_locales_comerciales",
        "score_flujo", "score_flujo_peatonal",
    ],
    "demografia": [
        "poblacion", "edad_media", "renta_media_hogar", "renta_media_uc",
        "pct_poblacio_25_44", "pct_extranjeros", "nivel_estudios_alto_pct",
        "score_demografia",
    ],
    "transporte": [
        "num_lineas_transporte", "num_paradas_transporte", "num_bicing_400m",
        "dist_metro_m", "dist_bus_m", "score_transporte",
    ],
    "seguridad": [
        "incidencias_por_1000hab", "incidencias_noche_pct", "comisarias_1km",
        "score_seguridad",
    ],
    "turismo": [
        "airbnb_density_500m", "booking_hoteles_500m", "dist_landmark_top3_m",
        "dist_playa_m", "summer_lift", "christmas_lift", "rebajas_lift",
        "score_turismo",
    ],
    "dinamismo": [
        "num_negocios_activos", "pct_locales_vacios", "tasa_supervivencia_3a",
        "ratio_apertura_cierre_1a", "negocios_historico_count", "hhi_sectorial",
        "tendencia", "score_dinamismo",
    ],
}


def hash_perfil(perfil: dict) -> str:
    """Hash determinista del perfil_refinado para clave de cache.

    Solo incluimos los campos que el LLM podría usar para personalizar la
    lectura — sector, subsector, matices, público, presupuesto. Cambios en
    otros campos (ubicación libre, m² preferidos…) no invalidan la cache.
    """
    relevant = {
        "sector":      perfil.get("sector"),
        "subsector":   perfil.get("subsector"),
        "matices":     sorted(perfil.get("matices") or []),
        "publico":     perfil.get("publico"),
        "presupuesto": perfil.get("presupuesto"),
    }
    blob = json.dumps(relevant, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:32]


def _zona_facts_for_dim(dim_key: str, zona: dict) -> dict:
    """Subset de la zona relevante para esta dim. Mantiene el prompt corto."""
    fields = DIM_FIELDS.get(dim_key, [])
    facts: Dict[str, Any] = {}
    for k in fields:
        v = zona.get(k)
        if v is None:
            continue
        # Convertir tipos no JSON-serializables (Decimal, asyncpg Records, etc.)
        if hasattr(v, "__float__"):
            try:
                v = float(v)
            except Exception:
                v = str(v)
        elif isinstance(v, (dict, list, str, int, float, bool)):
            pass
        else:
            v = str(v)
        facts[k] = v
    return facts


def _catalogo_para(dim_key: str, sector: str) -> List[str]:
    """Devuelve el catálogo de implicaciones para (dim, sector).

    Si el catálogo sectorial tiene <3 plantillas, se completa con las del
    `_default` (sin duplicar) para que el LLM siempre disponga de al menos 3
    opciones. Sin esto, sectores con catálogo escaso (moda, estética en
    transporte/seguridad/dinamismo) producen narrativas con 2 decisiones
    en vez de 3.
    """
    bucket = IMPLICACIONES_TEMPLATES.get(dim_key, {})
    sectorial: List[str] = list(bucket.get(sector) or [])
    default:   List[str] = list(bucket.get("_default") or [])

    if not sectorial and not default:
        return []

    if len(sectorial) < 3:
        for item in default:
            if item not in sectorial:
                sectorial.append(item)
            if len(sectorial) >= 3:
                break

    return sectorial or default


async def generar_narrativa_dimension(
    dim_key: str,
    zona: dict,
    sector: str,
    subsector: Optional[str],
    perfil: dict,
    session_id: Optional[str] = None,
) -> dict:
    """Genera {'lectura': str, 'decisiones': [str, ...]} con LLM.

    Lanza:
        ValueError — si dim_key no soportado o el LLM devuelve payload vacío.
        Exception — si el LLM falla (timeout, JSON inválido, etc.).
    """
    if dim_key not in DIMS_SOPORTADAS:
        raise ValueError(
            f"dim_key '{dim_key}' no soportado. Válidos: {sorted(DIMS_SOPORTADAS)}"
        )

    from routers.llm_router import completar

    hechos = _zona_facts_for_dim(dim_key, zona)
    catalogo = _catalogo_para(dim_key, sector)
    if not catalogo:
        # Defensa última: nunca llamar al LLM sin catálogo.
        catalogo = ["Adapta la operación a los datos reales de la zona."]

    # Nombre legible para que el LLM no se quede pegado al snake_case.
    dim_legible = {
        "flujo_peatonal": "Flujo peatonal",
        "demografia":     "Demografía",
        "transporte":     "Transporte",
        "seguridad":      "Seguridad",
        "turismo":        "Turismo",
        "dinamismo":      "Dinamismo comercial",
    }.get(dim_key, dim_key)

    sistema = (
        "Eres un consultor de retail senior especializado en Barcelona. "
        "Te dan los datos de UNA dimensión del análisis de viabilidad de una "
        "ubicación comercial y un catálogo de implicaciones operativas. Tu "
        "trabajo es producir, en JSON estricto, una lectura interpretativa "
        "(50-60 palabras, ESPAÑOL, NO descriptiva — debe interpretar qué "
        "significan los números para el negocio del sector concreto, no "
        "repetirlos) y exactamente 3 decisiones prácticas elegidas del "
        "catálogo, reordenadas por relevancia y reescritas concretando los "
        "números reales de la zona. NO añadas decisiones fuera del catálogo. "
        "Si una plantilla no aplica con los datos actuales, elige otra. "
        "Si no hay datos suficientes en una plantilla, omite el número y "
        "deja la decisión genérica. No inventes datos. Responde solo con "
        "JSON válido sin texto previo ni cierre."
    )

    user_payload = {
        "dimension":              dim_legible,
        "sector":                 sector,
        "subsector":              subsector,
        "perfil_matices":         perfil.get("matices") or [],
        "datos_zona":             hechos,
        "catalogo_implicaciones": catalogo,
        "instrucciones": {
            "lectura":    "50-60 palabras, en español, INTERPRETATIVO (qué significan los números para este sector concreto, qué oportunidad o riesgo abren, qué requeriría el negocio para aprovecharlos). Nada de listar cifras sin interpretar.",
            "decisiones": "Exactamente 3 ítems. Elegidos del catalogo_implicaciones, reordenados por relevancia para esta zona, reescritos en español concretando los números reales (ej. si la plantilla dice 'pico tarde/noche' y el dato dice 'noche=58%', escríbelo así). Si una plantilla no es aplicable a estos datos, elige otra del catálogo.",
            "formato":    '{"lectura": "...", "decisiones": ["...", "...", "..."]}',
        },
    }

    user_prompt = json.dumps(user_payload, ensure_ascii=False, indent=2)

    raw = await completar(
        mensajes=[{"role": "user", "content": user_prompt}],
        sistema=sistema,
        endpoint=f"narrativa_{dim_key}",
        session_id=session_id,
        max_tokens=_MAX_TOKENS,
        temperature=0.3,
        requiere_json=True,
        timeout=_TIMEOUT_S,
    )

    payload = _parse_json_strict(raw)

    lectura = (payload.get("lectura") or "").strip()
    decisiones_raw = payload.get("decisiones") or []
    decisiones = [
        str(d).strip()
        for d in decisiones_raw
        if isinstance(d, str) and d.strip()
    ]

    if not lectura:
        raise ValueError("LLM devolvió lectura vacía")
    if not decisiones:
        raise ValueError("LLM devolvió decisiones vacías")

    return {
        "lectura":    lectura,
        "decisiones": decisiones[:3],
    }


def _parse_json_strict(raw: str) -> dict:
    """Parser tolerante: intenta json.loads; si falla, recorta {...}."""
    if not raw:
        raise ValueError("LLM devolvió respuesta vacía")
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        i = raw.find("{")
        j = raw.rfind("}")
        if i < 0 or j < 0 or j <= i:
            raise
        return json.loads(raw[i:j + 1])
