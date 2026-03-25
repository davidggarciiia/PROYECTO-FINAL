"""agente/analizador.py — Genera análisis IA de zona con LLM."""
from __future__ import annotations
import json, logging
from agente.prompts import ANALISIS_ZONA_SISTEMA
from agente.traductor import traducir_dict
from agente import extraer_json
from routers.llm_router import completar

logger = logging.getLogger(__name__)

# Campos de texto libre que el LLM genera en inglés y hay que traducir al español
_CAMPOS_TEXTO = [
    "resumen", "puntos_fuertes", "puntos_debiles",
    "oportunidad", "riesgos", "razon_recomendacion",
]


async def analizar_zona(zona_data: dict, perfil_negocio: dict,
                         session_id: str) -> dict:
    """
    Genera el análisis IA completo de una zona para un negocio específico.

    Args:
        zona_data: dict con todos los datos de la zona (scores, competidores, alertas, etc.)
        perfil_negocio: dict con el perfil del negocio del usuario
        session_id: para el log de LLM

    Returns:
        dict con resumen, puntos_fuertes, puntos_debiles, oportunidad, riesgos,
        recomendacion_final, razon_recomendacion — todo en español
    """
    prompt = _construir_prompt(zona_data, perfil_negocio)

    respuesta = await completar(
        mensajes=[{"role": "user", "content": prompt}],
        sistema=ANALISIS_ZONA_SISTEMA,
        endpoint="analisis_zona",
        session_id=session_id,
        max_tokens=900,
        temperature=0.3,
        requiere_json=True,
    )

    try:
        resultado = json.loads(extraer_json(respuesta))
    except json.JSONDecodeError as e:
        logger.error("JSON inválido en análisis zona=%s: %s", zona_data.get("zona_id"), e)
        return {
            "resumen": "No se pudo generar el análisis automático.",
            "puntos_fuertes": [],
            "puntos_debiles": [],
            "oportunidad": "",
            "riesgos": "",
            "recomendacion_final": "Con reservas",
            "razon_recomendacion": "Análisis no disponible temporalmente.",
        }
    try:
        # Traducir todos los campos de texto en una sola llamada LLM
        return await traducir_dict(resultado, _CAMPOS_TEXTO, session_id)
    except Exception as e:
        logger.error("Error traduciendo análisis zona=%s: %s", zona_data.get("zona_id"), e)
        return {
            "resumen": "No se pudo generar el análisis automático.",
            "puntos_fuertes": [],
            "puntos_debiles": [],
            "oportunidad": "",
            "riesgos": "",
            "recomendacion_final": "Con reservas",
            "razon_recomendacion": "Análisis no disponible temporalmente.",
        }


def _construir_prompt(z: dict, p: dict) -> str:
    """Construye el prompt con todos los datos relevantes de la zona."""
    flujo = z.get("flujo_peatonal_dia", {})
    competidores = z.get("competidores_cercanos", [])
    alertas = z.get("alertas", [])
    shap = z.get("shap_values") or {}

    shap_sorted = sorted(shap.items(), key=lambda x: x[1], reverse=True) if shap else []
    top_pos = [f"{k}: +{v:.1f}" for k, v in shap_sorted[:3] if v > 0]
    top_neg = [f"{k}: {v:.1f}" for k, v in reversed(shap_sorted) if v < 0][:3]

    comp_directo = [c["nombre"] for c in competidores if c.get("es_competencia_directa")]
    alerta_textos = [a["texto"] for a in alertas]

    # Formatear tasa de locales vacíos de forma segura
    pct_vacios_raw = z.get("pct_locales_vacios")
    pct_vacios_str = f"{pct_vacios_raw * 100:.0f}%" if pct_vacios_raw is not None else "N/A"

    # Scores por dimensión del modelo (0-100) — disponibles aunque falten datos crudos
    def _s(key: str) -> str:
        v = z.get(key)
        return f"{v:.0f}/100" if v is not None else "N/A"

    # Flujo peatonal: usar datos crudos si existen; si son 0, indicarlo y usar el score
    flujo_manana = flujo.get("manana", 0)
    flujo_tarde  = flujo.get("tarde",  0)
    flujo_noche  = flujo.get("noche",  0)
    hay_flujo_real = (flujo_manana + flujo_tarde + flujo_noche) > 0
    if hay_flujo_real:
        flujo_str = f"morning {flujo_manana} · afternoon {flujo_tarde} · evening {flujo_noche} pax/h"
    else:
        flujo_str = f"no sensor data — model score: {_s('score_flujo_peatonal')}"

    # Truncar campos de texto libre para evitar inyección de prompts y limitar tokens
    descripcion_safe    = str(p.get("descripcion", ""))[:300]
    perfil_cliente_safe = str(p.get("perfil_cliente", "not specified"))[:100]

    prompt = f"""
BUSINESS: {p.get("sector", "unknown")} — {descripcion_safe}
Target customer: {perfil_cliente_safe}
Price tier: {p.get("precio_objetivo", "medio")}

ZONE: {z.get("nombre", "")} — {z.get("barrio", "")} ({z.get("distrito", "")})
Global score: {z.get("score_global", "N/A")}/100
3-year survival probability: {z.get("probabilidad_supervivencia", "N/A")}

DIMENSION SCORES (model estimate, 0-100):
- Pedestrian flow: {_s("score_flujo_peatonal")} | Demographics: {_s("score_demografia")} | Competition (lower saturated = higher score): {_s("score_competencia")}
- Rent affordability: {_s("score_precio_alquiler")} | Transport access: {_s("score_transporte")} | Safety: {_s("score_seguridad")}
- Tourism: {_s("score_turismo_dim") if z.get("score_turismo_dim") is not None else _s("score_turismo")} | Commercial environment: {_s("score_entorno_comercial")}

RAW DATA (real-time sensors, may be unavailable):
- Pedestrian flow: {flujo_str}
- Average household income: {z.get("renta_media_hogar", "N/A")} €/year
- % foreigners: {z.get("pct_extranjeros", "N/A")}
- Vacant premises: {pct_vacios_str}
- Business turnover rate: {z.get("tasa_rotacion_anual", "N/A")}
- Transport lines within 500m: {z.get("num_lineas_transporte", "N/A")}
- Available premises: {z.get("alquiler_mensual", "N/A")} €/month · {z.get("m2", "N/A")} m²

DIRECT COMPETITION ({len(comp_directo)} competitors):
{", ".join(comp_directo[:5]) if comp_directo else "No direct competition detected"}

POSITIVE SHAP FACTORS: {", ".join(top_pos) if top_pos else "Not available"}
NEGATIVE SHAP FACTORS: {", ".join(top_neg) if top_neg else "Not available"}

ACTIVE ALERTS: {"; ".join(alerta_textos) if alerta_textos else "No alerts"}

Generate the analysis.
""".strip()

    return prompt
