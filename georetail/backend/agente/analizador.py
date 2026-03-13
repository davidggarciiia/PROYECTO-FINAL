"""agente/analizador.py — Genera análisis IA de zona con LLM."""
from __future__ import annotations
import json, logging
from agente.prompts import ANALISIS_ZONA_SISTEMA
from agente.traductor import traducir_dict
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
        limpio = respuesta.strip()
        if limpio.startswith("```"): limpio = "\n".join(limpio.split("\n")[1:-1])
        resultado = json.loads(limpio)
        # Traducir todos los campos de texto en una sola llamada LLM
        return await traducir_dict(resultado, _CAMPOS_TEXTO, session_id)
    except Exception as e:
        logger.error("Error JSON análisis zona=%s: %s", zona_data.get("zona_id"), e)
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
    top_pos = [f"{k}: +{v:.1f}" for k,v in shap_sorted[:3] if v > 0]
    top_neg = [f"{k}: {v:.1f}" for k,v in reversed(shap_sorted) if v < 0][:3]

    comp_directo = [c["nombre"] for c in competidores if c.get("es_competencia_directa")]
    alerta_textos = [a["texto"] for a in alertas]

    prompt = f"""
BUSINESS: {p.get("sector","unknown")} — {p.get("descripcion","")}
Target customer: {p.get("perfil_cliente","not specified")}
Price tier: {p.get("precio_objetivo","medio")}

ZONE: {z.get("nombre","")} — {z.get("barrio","")} ({z.get("distrito","")})
Global score: {z.get("score_global","N/A")}/100
3-year survival probability: {z.get("probabilidad_supervivencia","N/A")}

KEY DATA:
- Pedestrian flow: morning {flujo.get("manana",0)} · afternoon {flujo.get("tarde",0)} · evening {flujo.get("noche",0)} people/hour
- Average household income: {z.get("renta_media_hogar","N/A")} €/year
- % foreigners: {z.get("pct_extranjeros","N/A")}
- Tourism score: {z.get("score_turismo","N/A")}/100
- Vacant premises: {z.get("pct_locales_vacios","N/A") and f"{z.get('pct_locales_vacios',0)*100:.0f}%"}
- Business turnover rate: {z.get("tasa_rotacion_anual","N/A")}
- Transport lines within 500m: {z.get("num_lineas_transporte","N/A")}
- Available premises: {z.get("alquiler_mensual","N/A")} €/month · {z.get("m2","N/A")} m²

DIRECT COMPETITION ({len(comp_directo)} competitors):
{", ".join(comp_directo[:5]) if comp_directo else "No direct competition detected"}

POSITIVE SHAP FACTORS: {", ".join(top_pos) if top_pos else "Not available"}
NEGATIVE SHAP FACTORS: {", ".join(top_neg) if top_neg else "Not available"}

ACTIVE ALERTS: {"; ".join(alerta_textos) if alerta_textos else "No alerts"}

Generate the analysis.
""".strip()

    return prompt
