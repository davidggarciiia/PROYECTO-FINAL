"""agente/analizador.py — Genera análisis IA de zona con LLM."""
from __future__ import annotations
import json, logging
from agente.prompts import ANALISIS_ZONA_SISTEMA
from routers.llm_router import completar

logger = logging.getLogger(__name__)


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
        recomendacion_final, razon_recomendacion
    """
    prompt = _construir_prompt(zona_data, perfil_negocio)

    respuesta = await completar(
        mensajes=[{"role":"user","content":prompt}],
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
        return json.loads(limpio)
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

    # Top 3 factores SHAP positivos y negativos
    shap_sorted = sorted(shap.items(), key=lambda x: x[1], reverse=True) if shap else []
    top_pos = [f"{k}: +{v:.1f}" for k,v in shap_sorted[:3] if v > 0]
    top_neg = [f"{k}: {v:.1f}" for k,v in reversed(shap_sorted) if v < 0][:3]

    comp_directo = [c["nombre"] for c in competidores if c.get("es_competencia_directa")]
    alerta_textos = [a["texto"] for a in alertas]

    prompt = f"""
NEGOCIO: {p.get("sector","desconocido")} — {p.get("descripcion","")}
Perfil cliente: {p.get("perfil_cliente","no especificado")}
Precio objetivo: {p.get("precio_objetivo","medio")}

ZONA: {z.get("nombre","")} — {z.get("barrio","")} ({z.get("distrito","")})
Score global: {z.get("score_global","N/D")}/100
Probabilidad supervivencia 3 años: {z.get("probabilidad_supervivencia","N/D")}

DATOS CLAVE:
- Flujo peatonal: mañana {flujo.get("manana",0)} · tarde {flujo.get("tarde",0)} · noche {flujo.get("noche",0)} personas/hora
- Renta media hogar: {z.get("renta_media_hogar","N/D")} €/año
- % extranjeros: {z.get("pct_extranjeros","N/D")}
- Score turismo: {z.get("score_turismo","N/D")}/100
- Locales vacíos: {z.get("pct_locales_vacios","N/D") and f"{z.get('pct_locales_vacios',0)*100:.0f}%"}
- Tasa rotación negocios: {z.get("tasa_rotacion_anual","N/D")}
- Líneas transporte a 500m: {z.get("num_lineas_transporte","N/D")}
- Alquiler local disponible: {z.get("alquiler_mensual","N/D")} €/mes · {z.get("m2","N/D")} m²

COMPETENCIA DIRECTA ({len(comp_directo)} competidores):
{", ".join(comp_directo[:5]) if comp_directo else "Sin competencia directa detectada"}

FACTORES POSITIVOS (SHAP): {", ".join(top_pos) if top_pos else "No disponible"}
FACTORES NEGATIVOS (SHAP): {", ".join(top_neg) if top_neg else "No disponible"}

ALERTAS ACTIVAS: {"; ".join(alerta_textos) if alerta_textos else "Sin alertas"}

Genera el análisis.
""".strip()

    return prompt
