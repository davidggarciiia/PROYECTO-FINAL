"""
api/buscar.py — POST /api/buscar

Endpoint principal. El usuario manda su descripción en lenguaje natural
y recibe zonas rankeadas, o una pregunta si falta información.

Flujo interno:
  1. Crear / recuperar sesión (Redis → PostgreSQL tabla `sesiones`)
  2. Validar descripción con LLM (¿es retail? ¿hay suficiente info?)
  3. Según el resultado del LLM:
       a. No es retail             → estado "error_tipo_negocio"
       b. Es retail, bloqueado leg → estado "inviable_legal"
       c. Falta info               → estado "cuestionario" (inicia cuestionario)
       d. Info suficiente          → filtrar zonas + scoring → estado "ok"
  4. Guardar búsqueda en `busquedas` para analytics
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
from typing import Literal, Optional
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from schemas.models import (
    ZonaResumen, ColorZona, EstadoBusqueda, PerfilRefinado,
    PerfilEstructurado, PublicoObjetivo, Operacion, UbicacionIdeal,
)
from api._utils import score_to_color
from agente.validador import validar_negocio
from scoring.concepto.taxonomy import compilar_concepto_negocio, lookup_canonical_tag
from scoring.nuances import nuances_resueltas
from agente.refinador import generar_pregunta_senal, refinar
from agente.traductor import traducir
from scoring.motor import calcular_scores_batch
from api._rareza import detectar_combinaciones_raras
from db.sesiones import crear_sesion, get_sesion, guardar_busqueda, actualizar_sesion
from db.zonas import filtrar_zonas_candidatas
from db.redis_client import get_redis

# Umbrales para el loop de preservación de señal (Fase 3).
_SIGNAL_THRESHOLD = 70
_SIGNAL_MAX_ROUNDS = 3

logger = logging.getLogger(__name__)
router = APIRouter(tags=["buscar"])


# ─── Request ──────────────────────────────────────────────────────────────────

class BuscarRequest(BaseModel):
    descripcion: str = Field(
        "",
        max_length=1000,
        description=(
            "Descripción del negocio en lenguaje natural. "
            "Obligatoria si `perfil_estructurado` no viene — en ese caso debe tener >=10 chars. "
            "Puede venir vacía cuando el frontend usa el formulario tipo test."
        ),
        examples=["Quiero abrir una cafetería de especialidad en un barrio de diseño"],
    )
    ciudad: Literal["Barcelona"] = Field(
        default="Barcelona",
        description="Ciudad donde buscar. Por ahora solo Barcelona.",
    )
    presupuesto_max: Optional[float] = Field(None, gt=0, description="Alquiler mensual máximo en €")
    m2_min: Optional[float] = Field(None, gt=0)
    m2_max: Optional[float] = Field(None, gt=0)
    distritos: Optional[list[str]] = Field(
        None,
        description="Lista de distritos a incluir. None = buscar en toda la ciudad.",
    )
    session_id: Optional[str] = Field(
        None,
        description="Pasar si el usuario ya tiene sesión (ej: vuelve del cuestionario).",
    )
    # ── Camino rápido: cuestionario tipo test ───────────────────────────────
    # Si viene, el endpoint se salta `validar_negocio` y `refinar` (ahorra
    # 1-3s y coste LLM) y construye el perfil directamente desde el form.
    # Solo se llama a `refinar()` si `perfil_estructurado.matices` no está vacío.
    perfil_estructurado: Optional[PerfilEstructurado] = Field(
        None,
        description=(
            "Payload del cuestionario estructurado. Si viene, se usa como fuente "
            "de verdad y se omite el LLM (excepto para el campo opcional `matices`)."
        ),
    )


# ─── Response ─────────────────────────────────────────────────────────────────

class BuscarResponse(BaseModel):
    session_id: str
    estado: EstadoBusqueda
    zonas: Optional[list[ZonaResumen]] = None
    total_zonas_analizadas: Optional[int] = Field(
        None,
        description="Total de zonas evaluadas antes de filtrar.",
    )
    pregunta: Optional[str] = Field(None, description="Pregunta del cuestionario si estado='cuestionario'.")
    progreso_cuestionario: Optional[int] = Field(None, ge=0, le=100)
    motivo: Optional[str] = Field(
        None,
        description="Mensaje para estados 'error_tipo_negocio' e 'inviable_legal'.",
    )
    warnings: list[str] = Field(
        default_factory=list,
        description=(
            "Avisos sobre combinaciones inusuales en el cuestionario "
            "(p. ej. ticket alto con presupuesto bajo). El frontend los muestra "
            "como nota antes del listado de zonas."
        ),
    )
    nuances_aplicadas: list[str] = Field(
        default_factory=list,
        description=(
            "Claves del catálogo `scoring/nuances.py` que el motor reconoció en "
            "`PerfilRefinado.nuances_detected` y aplicó al ranking. Un chip de la "
            "UI puede marcar «✓ aplicado» cuando el matiz aparezca aquí."
        ),
    )


# ─── Helpers ──────────────────────────────────────────────────────────────────

# ─── Capa de derivación: subsector → sector real interno ─────────────────────
# El cuestionario presenta `tatuajes` bajo `estetica` y `shisha_lounge` bajo
# `restauracion` (ver `api/opciones._SECTORES_OCULTOS`). Aquí restauramos el
# sector real para que la taxonomía conceptual aplique los pesos y perfiles
# específicos correctos. Sin esto, un tattoo studio caería en los pesos de
# estetica genérica y perdería su perfil propio (citas_previas=0.92,
# experiencial=0.72, etc.).

_SUBSECTORES_TATUAJES = frozenset({
    "tattoo_studio", "piercing_studio", "tattoo_gallery", "cosmetic_tattoo",
    "fine_line_studio", "street_tattoo", "body_art_collective",
})
_SUBSECTORES_SHISHA = frozenset({
    "classic_lounge", "premium_lounge", "terrace_lounge", "student_lounge",
    "tourist_lounge", "music_lounge", "food_lounge",
})


def _derivar_sector_interno(subsector: str | None, sector_form: str) -> str:
    """Devuelve el sector real interno desde el subsector.

    Si el subsector pertenece a un sector "oculto" (que el cuestionario
    presenta agrupado bajo otro), restauramos el sector original. Si no,
    devolvemos el sector tal cual lo envió el form.
    """
    if subsector in _SUBSECTORES_TATUAJES:
        return "tatuajes"
    if subsector in _SUBSECTORES_SHISHA:
        return "shisha_lounge"
    return sector_form


# Subsectores que típicamente operan con cita previa (alta `citas_previas`).
# Se usa solo dentro de _perfil_hint_desde_form para inyectar la dim numérica
# cuando el form indica uno de estos slugs canónicos.
_SUBSECTORES_CITAS_PREVIAS = frozenset({
    "tattoo_studio", "piercing_studio", "fine_line_studio", "cosmetic_tattoo",
    "hair_salon", "barber_shop", "nail_studio", "brow_lash_bar",
    "beauty_clinic", "skin_clinic", "day_spa", "tanning_studio",
    "physio_clinic", "dental_clinic", "psychology_center",
    "dermatology_clinic", "aesthetic_medicine", "nutritionist", "osteopathy",
    "veterinary_clinic", "pilates_reformer", "personal_training",
    "language_academy", "tutoring_center", "exam_prep_center",
    "massage_center", "interior_design",
})

# Subsectores con experiencia/destino fuerte (alta `experiencial`).
_SUBSECTORES_EXPERIENCIAL = frozenset({
    "fine_dining", "cocktail_bar", "premium_lounge", "music_lounge",
    "tourist_lounge", "day_spa", "concept_store", "tattoo_gallery",
    "art_gallery", "cultural_space", "vermut_bar", "tapas_bar",
})


def _perfil_hint_desde_form(pe: PerfilEstructurado) -> dict[str, float]:
    """Traduce los campos del cuestionario a las 8 dimensiones numéricas
    del perfil_negocio (PROFILE_KEYS) que consumen `zona_ideal_desde_perfil` y
    `score_bias_desde_perfil`.

    Devuelve un dict parcial con solo las claves que se pueden inferir; los huecos
    los rellena `_blend_profile` mezclando con el perfil del sector y los
    base_concepts/modifiers del subsector.
    """
    hint: dict[str, float] = {}
    op = pe.operacion or Operacion()
    pub = pe.publico_objetivo or PublicoObjetivo()
    ubi = pe.ubicacion_ideal or UbicacionIdeal()

    # nivel_precio: combinación ticket_tier + nivel_socioeconomico
    socio_to_price = {"bajo": 0.20, "medio": 0.40, "medio-alto": 0.65, "alto": 0.85}
    parts: list[float] = []
    if op.ticket_tier_p1_p5:
        parts.append((op.ticket_tier_p1_p5 - 1) / 4.0)  # 1→0.0, 5→1.0
    if pub.nivel_socioeconomico in socio_to_price:
        parts.append(socio_to_price[pub.nivel_socioeconomico])
    if parts:
        hint["nivel_precio"] = round(sum(parts) / len(parts), 3)

    # clientela_turismo + clientela_vecindario por flujo_tipo (con fallback a estilo_vida)
    flujo_to_turismo = {"turistas": 0.85, "residentes": 0.05,
                        "oficinas": 0.10, "estudiantes": 0.15, "mixto": 0.40}
    flujo_to_vecindario = {"turistas": 0.10, "residentes": 0.90,
                           "oficinas": 0.30, "estudiantes": 0.55, "mixto": 0.55}
    if ubi.flujo_tipo in flujo_to_turismo:
        hint["clientela_turismo"] = flujo_to_turismo[ubi.flujo_tipo]
        hint["clientela_vecindario"] = flujo_to_vecindario[ubi.flujo_tipo]
    elif pub.estilo_vida:
        if "turistas" in pub.estilo_vida:
            hint["clientela_turismo"] = 0.80
            hint["clientela_vecindario"] = 0.15
        elif "residentes" in pub.estilo_vida:
            hint["clientela_turismo"] = 0.10
            hint["clientela_vecindario"] = 0.85

    # dependencia_flujo: combina modelo_servicio + tipo_calle + densidad
    flujo_score = 0.50
    flujo_count = 0
    if op.modelo_servicio == "delivery_only":
        flujo_score += -0.40; flujo_count += 1
    elif op.modelo_servicio == "take_away":
        flujo_score += +0.20; flujo_count += 1
    if ubi.tipo_calle == "comercial_principal":
        flujo_score += +0.30; flujo_count += 1
    elif ubi.tipo_calle == "comercial_secundaria":
        flujo_score += +0.15; flujo_count += 1
    elif ubi.tipo_calle == "residencial":
        flujo_score += -0.25; flujo_count += 1
    elif ubi.tipo_calle == "peatonal":
        flujo_score += +0.25; flujo_count += 1
    if ubi.densidad_preferida == "alta":
        flujo_score += +0.20; flujo_count += 1
    elif ubi.densidad_preferida == "baja":
        flujo_score += -0.30; flujo_count += 1
    if flujo_count > 0:
        hint["dependencia_flujo"] = round(max(0.0, min(1.0, flujo_score)), 3)

    # horario_nocturno
    horarios = set((op.horarios_apertura or []) + (pub.horarios_pico or []))
    if "noche" in horarios:
        hint["horario_nocturno"] = 0.75
    elif "tarde" in horarios and "mañana" not in horarios and "manana" not in horarios:
        hint["horario_nocturno"] = 0.30

    # citas_previas: por subsector específico
    if pe.subsector in _SUBSECTORES_CITAS_PREVIAS:
        hint["citas_previas"] = 0.85

    # sensibilidad_alquiler: presupuesto + escala_operativa
    if pe.presupuesto_max:
        if pe.presupuesto_max < 1500:
            hint["sensibilidad_alquiler"] = 0.85
        elif pe.presupuesto_max < 2500:
            hint["sensibilidad_alquiler"] = 0.60
        elif pe.presupuesto_max < 4000:
            hint["sensibilidad_alquiler"] = 0.40
        else:
            hint["sensibilidad_alquiler"] = 0.25
    if op.escala_operativa == "solo":
        hint["sensibilidad_alquiler"] = max(hint.get("sensibilidad_alquiler", 0.50), 0.75)

    # experiencial: ticket alto o subsector específico
    if op.ticket_tier_p1_p5 and op.ticket_tier_p1_p5 >= 4:
        hint["experiencial"] = 0.65
    if pe.subsector in _SUBSECTORES_EXPERIENCIAL:
        hint["experiencial"] = max(hint.get("experiencial", 0.40), 0.75)

    return hint


def _validacion_desde_perfil_estructurado(pe: PerfilEstructurado) -> dict:
    """Construye el `dict validacion` desde el cuestionario estructurado.

    NO llama al LLM. El subsector se valida contra la taxonomía conceptual
    (`scoring/concepto/taxonomy.py`) que es la que el form usa como fuente
    de slugs (`specialty_coffee`, `coworking_office`...). La taxonomía
    `scoring/taxonomia.py` solo se usa en pipelines de scraping con slugs
    distintos (`cafeteria`, `pizzeria`...) — NO aquí.

    También compila un `concepto_negocio` rico inyectando un `perfil_hint`
    derivado del form (`_perfil_hint_desde_form`). Eso garantiza que la
    `zona_ideal` y los `pesos_scoring` sean específicos a la idea, no
    genéricos del sector.

    El cuestionario agrupa los subsectores de tatuajes bajo `estetica` y los
    de shisha_lounge bajo `restauracion` para no saturar el dropdown. Aquí
    `_derivar_sector_interno` restaura el sector real interno antes de
    compilar el concepto, para que se apliquen los pesos y perfiles
    específicos (citas_previas alto en tatuajes, horario_nocturno alto en
    shisha, etc.).
    """
    sector_form = pe.sector or "desconocido"
    sector = _derivar_sector_interno(pe.subsector, sector_form)

    # Validación: ¿el slug del subsector existe en la taxonomía conceptual?
    subsector_canonical = lookup_canonical_tag(pe.subsector) if pe.subsector else None
    subsector_validado = pe.subsector if subsector_canonical else None

    # idea_tags lleva el slug ORIGINAL del form — `compilar_concepto_negocio`
    # lo resuelve internamente vía `lookup_canonical_tag`.
    idea_tags: list[str] = []
    if subsector_validado:
        idea_tags.append(subsector_validado)

    # Compilar concepto_negocio con el perfil_hint derivado del form. Esto
    # activa una zona_ideal específica + score_bias modulados por la idea,
    # incluso cuando el form solo trae sector + subsector.
    perfil_hint = _perfil_hint_desde_form(pe)
    concepto_negocio = compilar_concepto_negocio(
        sector=sector,
        idea_tags=idea_tags,
        perfil_hint=perfil_hint,
    )
    perfil_negocio = concepto_negocio.get("perfil_negocio") or {}

    return {
        "es_retail":              True,
        "inviable_legal":         False,
        "motivo_legal":           None,
        "motivo":                 None,
        "informacion_suficiente": True,
        "sector_detectado":       sector,
        "subsector_detectado":    subsector_validado,
        "idea_tags":              idea_tags,
        "perfil_negocio":         perfil_negocio,
        "concepto_negocio":       concepto_negocio,
        "variables_conocidas":    {},
        "preguntas_necesarias":   [],
    }


def _perfil_refinado_desde_form(pe: PerfilEstructurado) -> dict:
    """
    Traduce el formulario estructurado a un `PerfilRefinado` serializable.
    Todo lo que el LLM normalmente extraería (público, operación, ubicación)
    viene ya tipado — se copia tal cual. `signal_preservation_score` = 100
    porque el usuario lo ha declarado explícitamente.
    """
    return PerfilRefinado(
        publico_objetivo=pe.publico_objetivo,
        operacion=pe.operacion,
        ubicacion_ideal=pe.ubicacion_ideal,
        nuances_detected=[],
        signal_preservation_score=100,
    ).model_dump()


def _build_zona_resumen(z: dict) -> ZonaResumen:
    """
    Convierte un dict fusionado (candidata + score) a ZonaResumen.
    Admite ambas variantes de nombre de campo para compatibilidad.
    """
    score = z.get("score_global")
    score = score if score is not None else 50.0
    _prob_3a = z.get("probabilidad_supervivencia_3a")
    prob = _prob_3a if _prob_3a is not None else z.get("probabilidad_supervivencia")
    return ZonaResumen(
        zona_id=z["zona_id"],
        nombre=z["nombre"],
        barrio=z["barrio"],
        distrito=z["distrito"],
        score_global=round(score, 1),
        probabilidad_supervivencia_3a=round(prob, 2) if prob is not None else None,
        alquiler_estimado=z.get("alquiler_estimado") or z.get("alquiler_mensual"),
        m2_disponibles=z.get("m2_disponibles") or z.get("m2"),
        color=score_to_color(score),
        lat=z["lat"],
        lng=z["lng"],
        resumen_ia=z.get("resumen_ia"),
    )


# ─── Endpoint ────────────────────────────────────────────────────────────────

@router.post(
    "/buscar",
    response_model=BuscarResponse,
    summary="Búsqueda principal de zonas por descripción en lenguaje natural",
)
async def buscar(body: BuscarRequest, request: Request) -> BuscarResponse:
    """
    El estado de la respuesta determina qué hace el frontend:
      - "ok"                → renderizar mapa con zonas
      - "cuestionario"      → mostrar la pregunta al usuario
      - "error_tipo_negocio"→ mostrar mensaje de error
      - "inviable_legal"    → mostrar advertencia legal + botón "Saber más"
    """
    # ── 0. Validar que hay cuestionario o descripción ────────────────────────
    if body.perfil_estructurado is None:
        if not body.descripcion or len(body.descripcion.strip()) < 10:
            raise HTTPException(
                status_code=422,
                detail=(
                    "Debes enviar `descripcion` (≥10 caracteres) o `perfil_estructurado`."
                ),
            )
    else:
        if not body.perfil_estructurado.sector:
            raise HTTPException(
                status_code=422,
                detail="`perfil_estructurado.sector` es obligatorio.",
            )

    # ── 1. Sesión ─────────────────────────────────────────────────────────────
    # Si viene el cuestionario estructurado, sus filtros prácticos tienen
    # precedencia sobre los `presupuesto_max/m2/distritos` del cuerpo raíz —
    # pero solo cuando el campo concreto del PE NO es None. Si el PE no setea
    # el campo, hacemos fallback al body raíz para no descartar filtros que el
    # frontend del cuestionario quick puede enviar a nivel raíz.
    pe = body.perfil_estructurado
    filtros_efectivos = {
        "presupuesto_max": (pe.presupuesto_max if (pe and pe.presupuesto_max is not None) else body.presupuesto_max),
        "m2_min":          (pe.m2_min          if (pe and pe.m2_min          is not None) else body.m2_min),
        "m2_max":          (pe.m2_max          if (pe and pe.m2_max          is not None) else body.m2_max),
        "distritos":       (pe.distritos       if (pe and pe.distritos)                    else body.distritos),
    }

    session_id = body.session_id or str(uuid4())
    sesion = await get_sesion(session_id)

    if sesion is None:
        ip_hash = _hash_ip(request.client.host if request.client else "")
        sesion = await crear_sesion(
            session_id=session_id,
            datos={
                "descripcion_original": body.descripcion,
                "ciudad": body.ciudad,
                "filtros": filtros_efectivos,
                "perfil": {},
            },
            ip_hash=ip_hash,
        )
    elif pe is not None:
        # Sesión existente + nuevo cuestionario → actualizar filtros en la sesión.
        sesion["filtros"] = filtros_efectivos
        try:
            await actualizar_sesion(session_id, {"filtros": filtros_efectivos})
        except Exception as exc:
            logger.warning("No se pudieron actualizar filtros desde el test: %s", exc)

    # ── 2. Validación: LLM o desde cuestionario estructurado ─────────────────
    if pe is not None:
        # Fast path: el formulario ya contiene toda la información necesaria.
        # Ni `validar_negocio` ni `refinar` se llaman (salvo `matices`, más abajo).
        logger.info("buscar: fast path (cuestionario estructurado) sector=%s", pe.sector)
        validacion = _validacion_desde_perfil_estructurado(pe)
    else:
        try:
            validacion = await validar_negocio(body.descripcion, session_id)
        except Exception as exc:
            logger.error("Error en validacion LLM: %s", exc, exc_info=True)
            validacion = {
                "es_retail":             True,
                "inviable_legal":        False,
                "motivo_legal":          None,
                "motivo":                None,
                "informacion_suficiente": False,
                "sector_detectado":      "desconocido",
                "variables_conocidas":   {},
                "preguntas_necesarias":  ["sector", "m2", "presupuesto", "cliente"],
            }

    # ── 3a. Negocio no apto ───────────────────────────────────────────────────
    if not validacion["es_retail"]:
        return BuscarResponse(
            session_id=session_id,
            estado=EstadoBusqueda.ERROR_NEGOCIO,
            motivo=(
                validacion.get("motivo")
                or "GeoRetail está diseñado para negocios que necesitan un local físico "
                   "con atención presencial (tienda, restaurante, estudio...)."
            ),
        )

    # ── 3b. Bloqueado legalmente ──────────────────────────────────────────────
    if validacion.get("inviable_legal"):
        return BuscarResponse(
            session_id=session_id,
            estado=EstadoBusqueda.INVIABLE_LEGAL,
            motivo=validacion.get("motivo_legal"),
        )

    # ── 3c/3d. Buscar y rankear zonas (sin cuestionario) ─────────────────────
    perfil = {
        **sesion.get("perfil", {}),
        "sector":         validacion["sector_detectado"],
        "idea_tags":      validacion.get("idea_tags") or [],
        "perfil_negocio": validacion.get("perfil_negocio") or {},
        "concepto_negocio": validacion.get("concepto_negocio") or {},
        "variables":      validacion["variables_conocidas"],
    }

    # Persistir flags legales + overrides financieros para que /api/legal y
    # /api/financiero puedan consumirlos sin volver a pedírselos al usuario.
    if pe is not None:
        if pe.flags_legales is not None:
            perfil["flags_legales"] = pe.flags_legales.model_dump(exclude_none=True)
        if pe.overrides_financieros is not None:
            perfil["overrides_financieros"] = pe.overrides_financieros.model_dump(exclude_none=True)
        # Usar el subsector ya validado contra taxonomia.py (puede ser None si era inválido)
        subsector_validado = validacion.get("subsector_detectado")
        if subsector_validado:
            perfil["subsector"] = subsector_validado

    # ── 3c-bis. Construir PerfilRefinado ────────────────────────────────────
    # Dos caminos posibles:
    #   (A) Fast path: el cuestionario ya contiene los campos estructurados.
    #       Se mapea directo y solo se invoca `refinar()` si hay `matices`.
    #   (B) Flujo clásico: el LLM refina la descripción y se cachea en Redis.

    if pe is not None:
        # ── (A) Fast path — sin LLM salvo `matices` ─────────────────────────
        perfil_refinado_dict = _perfil_refinado_desde_form(pe)
        if pe.matices and pe.matices.strip():
            # Cache Redis del refinador. Sin esto, el LLM (`refinar`) genera
            # `nuances_detected` ligeramente distintas en cada llamada (temp=0.2)
            # y eso desestabiliza el `perfil_hash` que usa el clasificador
            # de competencia LLM → cache miss en cada búsqueda → 25s repetidos.
            _TTL_MATICES = 3600
            matices_clave = (
                pe.sector + "|" + (pe.subsector or "") + "|" + pe.matices.strip()
            )
            matices_hash = hashlib.md5(matices_clave.encode("utf-8")).hexdigest()
            redis = get_redis()
            redis_key = f"matices_refinados:{matices_hash}"
            matices_dict: Optional[dict] = None
            try:
                cached_raw = await redis.get(redis_key)
                if cached_raw:
                    import json as _json
                    matices_dict = _json.loads(cached_raw)
            except Exception:
                matices_dict = None

            if matices_dict is None:
                try:
                    perfil_matices = await refinar(
                        descripcion=pe.matices.strip(),
                        sector_detectado=pe.sector,
                        tags_previos=[validacion["subsector_detectado"]] if validacion.get("subsector_detectado") else [],
                        session_id=session_id,
                    )
                    matices_dict = perfil_matices.model_dump()
                    try:
                        import json as _json
                        await redis.set(redis_key, _json.dumps(matices_dict), ex=_TTL_MATICES)
                    except Exception:
                        pass
                except Exception as exc:
                    logger.warning("refinar(matices) falló: %s — se ignora", exc)
                    matices_dict = None

            if matices_dict:
                perfil_refinado_dict["nuances_detected"] = (
                    matices_dict.get("nuances_detected") or []
                )
                # Si el form no especificó ciertos campos y el LLM los detecta
                # en los matices, los copiamos (no sobrescribimos lo ya dicho).
                for bloque in ("propuesta_valor",):
                    valor = matices_dict.get(bloque)
                    if valor:
                        perfil_refinado_dict[bloque] = valor

        perfil["perfil_refinado"] = perfil_refinado_dict
        # No hay loop de señal en fast path (signal_preservation_score=100).

    else:
        # ── (B) Flujo clásico con LLM + cache Redis ─────────────────────────
        # Se cachea por hash(descripción) para evitar repetir la llamada LLM si el
        # usuario pulsa "buscar" otra vez con el mismo texto dentro de la sesión.
        #
        # FIX BUG-007: Race condition — dos requests concurrentes con la misma
        # descripción pueden llegar aquí con cache_prev vacío y ambas llaman al LLM.
        # Solución: distributed lock en Redis (SET NX EX 30s) antes de llamar al LLM.
        # Solo el primer waiter que obtenga el lock llama al LLM y escribe en Redis.
        # El segundo waiter espera brevemente y lee el resultado ya calculado.
        _TTL_REFINADO = 3600  # 1 hora en segundos
        descripcion_hash = hashlib.md5(body.descripcion.strip().encode("utf-8")).hexdigest()
        cache_prev = (sesion.get("perfil") or {}).get("perfil_refinado_cache") or {}
        perfil_refinado_dict: dict = cache_prev.get(descripcion_hash) or {}

        if not perfil_refinado_dict:
            redis = get_redis()
            redis_cache_key = f"perfil_refinado:{descripcion_hash}"
            redis_lock_key = f"{redis_cache_key}:lock"

            # Check Redis cache first (covers cross-session deduplication).
            cached_raw = await redis.get(redis_cache_key)
            if cached_raw:
                import json as _json
                try:
                    perfil_refinado_dict = _json.loads(cached_raw)
                except Exception:
                    perfil_refinado_dict = {}

            if not perfil_refinado_dict:
                # Try to acquire distributed lock (NX = only if not exists, EX = 30s TTL).
                got_lock = await redis.set(redis_lock_key, "1", nx=True, ex=30)
                if got_lock:
                    try:
                        perfil_refinado_model = await refinar(
                            descripcion=body.descripcion,
                            sector_detectado=perfil["sector"],
                            tags_previos=perfil["idea_tags"],
                            session_id=session_id,
                        )
                        perfil_refinado_dict = perfil_refinado_model.model_dump()
                    except Exception as exc:
                        logger.warning("Refinador falló: %s — continuando con perfil vacío", exc)
                        perfil_refinado_dict = PerfilRefinado().model_dump()
                    finally:
                        # Write result to Redis before releasing lock so waiters can read it.
                        try:
                            import json as _json
                            await redis.set(redis_cache_key, _json.dumps(perfil_refinado_dict), ex=_TTL_REFINADO)
                        except Exception as exc:
                            logger.warning("No se pudo escribir perfil_refinado en Redis: %s", exc)
                        await redis.delete(redis_lock_key)
                else:
                    # Another request is computing the same profile — poll cache until the
                    # holder publishes the result. The holder's lock TTL is 30s y el LLM
                    # tarda normalmente 2-5s; esperamos hasta 30s en pasos de 0.25s para no
                    # duplicar la llamada al LLM en requests concurrentes de la misma query.
                    import json as _json
                    _POLL_INTERVAL_S = 0.25
                    _POLL_MAX_S = 30.0
                    waited = 0.0
                    while waited < _POLL_MAX_S:
                        await asyncio.sleep(_POLL_INTERVAL_S)
                        waited += _POLL_INTERVAL_S
                        cached_raw = await redis.get(redis_cache_key)
                        if cached_raw:
                            try:
                                perfil_refinado_dict = _json.loads(cached_raw)
                            except Exception:
                                perfil_refinado_dict = {}
                            if perfil_refinado_dict:
                                break
                        # Si el lock desapareció SIN haber publicado cache, el holder falló
                        # → salimos del polling y caemos al fallback LLM propio.
                        lock_still_held = await redis.get(redis_lock_key)
                        if not lock_still_held and not cached_raw:
                            break
                    if not perfil_refinado_dict:
                        # Fallback: lock holder may have failed; call LLM ourselves.
                        try:
                            perfil_refinado_model = await refinar(
                                descripcion=body.descripcion,
                                sector_detectado=perfil["sector"],
                                tags_previos=perfil["idea_tags"],
                                session_id=session_id,
                            )
                            perfil_refinado_dict = perfil_refinado_model.model_dump()
                        except Exception as exc:
                            logger.warning("Refinador (fallback) falló: %s — continuando con perfil vacío", exc)
                            perfil_refinado_dict = PerfilRefinado().model_dump()

        perfil["perfil_refinado"] = perfil_refinado_dict
        perfil["perfil_refinado_cache"] = {**cache_prev, descripcion_hash: perfil_refinado_dict}

        # ── 3c-ter. Loop de preservación de señal ────────────────────────────
        # Si el LLM declara score<70 y aún no hemos gastado los 3 rounds, pedimos
        # al usuario una aclaración específica antes de rankear zonas.
        # Solo aplica al flujo clásico — el fast path tiene score=100.
        signal_score = int(perfil_refinado_dict.get("signal_preservation_score") or 0)
        rounds_usados = int((sesion.get("perfil") or {}).get("signal_rounds", 0))
        if signal_score < _SIGNAL_THRESHOLD and rounds_usados < _SIGNAL_MAX_ROUNDS:
            perfil_mod = PerfilRefinado(**perfil_refinado_dict)
            pregunta_en = await generar_pregunta_senal(
                perfil_refinado=perfil_mod,
                descripcion=body.descripcion,
                session_id=session_id,
            )
            if pregunta_en:
                try:
                    pregunta_es = await traducir(pregunta_en, session_id)
                except Exception:
                    pregunta_es = pregunta_en
                # Persistir: incrementamos rounds y guardamos perfil_refinado actualizado
                perfil["signal_rounds"] = rounds_usados + 1
                try:
                    await actualizar_sesion(session_id, {"perfil": perfil})
                except Exception as exc:
                    logger.warning("No se pudo actualizar perfil en loop señal: %s", exc)
                progreso = int(min(90, 30 + rounds_usados * 20))
                return BuscarResponse(
                    session_id=session_id,
                    estado=EstadoBusqueda.CUESTIONARIO,
                    pregunta=pregunta_es,
                    progreso_cuestionario=progreso,
                )

    filtros = sesion["filtros"]
    zonas_candidatas = await filtrar_zonas_candidatas(filtros)

    if not zonas_candidatas:
        logger.warning("Sin zonas candidatas para session_id=%s con filtros=%s", session_id, filtros)
        filtros_amplios = {**filtros, "distritos": None}
        zonas_candidatas = await filtrar_zonas_candidatas(filtros_amplios)

    total_candidatas = len(zonas_candidatas)

    # calcular_scores_batch espera lista de IDs y el código de sector
    zona_ids      = [z["zona_id"] for z in zonas_candidatas]
    sector_codigo = perfil.get("sector", "desconocido")
    # Los idea_tags vienen del LLM directamente — capturan el concepto específico
    # (ej: dog_friendly + clientela_local + specialty_coffee) con mucha más
    # precisión que el sector genérico. La descripción es fallback si no hay tags.
    scores_list = await calcular_scores_batch(
        zona_ids,
        sector_codigo,
        idea_tags=validacion.get("idea_tags") or [],
        descripcion_negocio=body.descripcion,
        perfil_negocio=validacion.get("perfil_negocio") or {},
        concepto_negocio=validacion.get("concepto_negocio") or {},
        perfil_refinado=perfil_refinado_dict or None,
    )

    # Construir lookup de scores por zona_id
    scores_by_id = {s["zona_id"]: s for s in scores_list}

    # Fusionar datos de la candidata con el score calculado
    zonas_merged = []
    for c in zonas_candidatas:
        score_data = scores_by_id.get(c["zona_id"], {"score_global": 50.0})
        zonas_merged.append({**c, **score_data})

    # Ordenar por score descendente y construir respuesta
    zonas_merged.sort(key=lambda z: z.get("score_global", 0), reverse=True)
    zonas_response = [_build_zona_resumen(z) for z in zonas_merged]

    # ── 4. Persistir zonas en sesión + guardar búsqueda para analytics ────────
    try:
        await actualizar_sesion(session_id, {
            "zonas_actuales": zonas_merged,
            "perfil": perfil,
        })
    except Exception as exc:
        logger.warning("No se pudo actualizar zonas_actuales en sesión: %s", exc)

    try:
        await guardar_busqueda(
            session_id=session_id,
            descripcion=body.descripcion,
            filtros=filtros,
            perfil=perfil,
            num_resultados=len(zonas_response),
        )
    except Exception as exc:
        logger.warning("No se pudo guardar busqueda en analytics: %s", exc)

    # Warnings de combinaciones raras del cuestionario (vacío en flujo libre).
    warnings = detectar_combinaciones_raras(pe) if pe is not None else []

    # Nuances que el motor reconoció — se pasan al front para mostrar
    # «✓ aplicado al ranking» en cada chip del campo `matices`.
    nuances_in = list(perfil_refinado_dict.get("nuances_detected") or [])
    nuances_resueltas_set, nuances_no_resueltas = nuances_resueltas(nuances_in)
    nuances_input_aplicadas = [n for n in nuances_in if n not in nuances_no_resueltas]

    # Persistir en el perfil_refinado para que `/api/local` y el dossier puedan
    # marcar los chips como aplicados sin recalcular el matching.
    if perfil_refinado_dict and nuances_input_aplicadas:
        perfil_refinado_dict["nuances_input_aplicadas"] = nuances_input_aplicadas
        # Refrescar el perfil persistido en la sesión con esta info.
        perfil["perfil_refinado"] = perfil_refinado_dict
        try:
            await actualizar_sesion(session_id, {"perfil": perfil})
        except Exception as exc:
            logger.debug("No se pudo persistir nuances_input_aplicadas: %s", exc)

    return BuscarResponse(
        session_id=session_id,
        estado=EstadoBusqueda.OK,
        zonas=zonas_response,
        total_zonas_analizadas=total_candidatas,
        warnings=warnings,
        nuances_aplicadas=sorted(nuances_resueltas_set),
    )


# ─── Utilidades ───────────────────────────────────────────────────────────────

def _hash_ip(ip: str) -> str:
    """Hash SHA-256 de la IP para analytics anónimos."""
    return hashlib.sha256(ip.encode()).hexdigest()[:16]
