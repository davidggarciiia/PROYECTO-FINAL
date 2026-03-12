"""
agente/analisis.py — Análisis narrativo de una zona generado por LLM.

Genera un texto de 3-4 párrafos explicando por qué una zona es o no viable
para el tipo de negocio del usuario. Usa los datos reales de scores y
variables de la zona como contexto.

Usado por: POST /api/local (panel de detalle de zona)
Llama a:   routers/llm_router.py (Claude → GPT-4o → Groq → fallback)
"""
from __future__ import annotations

import json
import logging

from routers.llm_router import llamar_llm

logger = logging.getLogger(__name__)

_PROMPT_ANALISIS = """Eres un consultor experto en ubicaciones comerciales en Barcelona.
Analiza esta zona para el tipo de negocio indicado y redacta un análisis breve (3-4 párrafos)
en español, directo y útil para un emprendedor.

Tipo de negocio: {sector}
Zona: {zona_nombre} ({zona_id})
Barrio: {barrio}

Datos de la zona:
{datos_zona}

Instrucciones:
- Párrafo 1: fortalezas principales de la ubicación para este negocio
- Párrafo 2: riesgos o debilidades a tener en cuenta
- Párrafo 3: recomendación final (viable / con condiciones / no recomendable)
- No uses bullets ni headers. Solo párrafos de texto.
- Sé directo. No uses frases genéricas como "esta zona ofrece interesantes oportunidades".
- Basa el análisis en los datos numéricos proporcionados."""


async def generar_analisis_zona(
    zona_id: str,
    zona_nombre: str,
    barrio: str,
    sector_codigo: str,
    scores: dict,
    variables: dict,
) -> str:
    """
    Genera el análisis narrativo de una zona usando un LLM.

    Parámetros:
      zona_id       → ID de la zona (ejemplo: "bcn_eixample_01")
      zona_nombre   → nombre legible (ejemplo: "Eixample Nord")
      barrio        → nombre del barrio
      sector_codigo → tipo de negocio
      scores        → dict con score_global y scores por dimensión
      variables     → dict con flujo peatonal, renta, competidores, etc.

    Devuelve el texto del análisis (string).
    En caso de error devuelve un texto genérico para no romper la UI.
    """
    datos_zona = _formatear_datos(scores, variables)

    prompt = _PROMPT_ANALISIS.format(
        sector     = sector_codigo.replace("_", " ").title(),
        zona_id    = zona_id,
        zona_nombre = zona_nombre,
        barrio     = barrio,
        datos_zona = datos_zona,
    )

    try:
        respuesta = await llamar_llm(
            prompt   = prompt,
            endpoint = "analisis_zona",
            max_tokens = 600,
        )
        return respuesta.strip()

    except Exception as exc:
        logger.error("Error generando análisis para zona %s: %s", zona_id, exc)
        return _analisis_fallback(zona_nombre, sector_codigo, scores)


def _formatear_datos(scores: dict, variables: dict) -> str:
    """Convierte los datos numéricos en texto legible para el LLM."""
    lineas = []

    if scores:
        lineas.append(f"Score global: {scores.get('score_global', '?'):.0f}/100")
        lineas.append(f"  - Flujo peatonal:  {scores.get('score_flujo_peatonal', '?'):.0f}/100")
        lineas.append(f"  - Demografía:      {scores.get('score_demografia', '?'):.0f}/100")
        lineas.append(f"  - Competencia:     {scores.get('score_competencia', '?'):.0f}/100")
        lineas.append(f"  - Precio alquiler: {scores.get('score_precio_alquiler', '?'):.0f}/100")
        lineas.append(f"  - Transporte:      {scores.get('score_transporte', '?'):.0f}/100")
        lineas.append(f"  - Seguridad:       {scores.get('score_seguridad', '?'):.0f}/100")
        if scores.get("probabilidad_supervivencia"):
            prob = scores["probabilidad_supervivencia"] * 100
            lineas.append(f"Probabilidad supervivencia a 3 años (XGBoost): {prob:.0f}%")

    if variables:
        if variables.get("flujo_peatonal_total"):
            lineas.append(f"Flujo peatonal medio: {variables['flujo_peatonal_total']:.0f} personas/hora")
        if variables.get("renta_media_hogar"):
            lineas.append(f"Renta media del hogar: {variables['renta_media_hogar']:,.0f} €/año")
        if variables.get("num_negocios_activos"):
            lineas.append(f"Negocios activos en la zona: {variables['num_negocios_activos']}")
        if variables.get("pct_locales_vacios") is not None:
            lineas.append(f"Locales vacíos: {variables['pct_locales_vacios']*100:.0f}%")
        if variables.get("tasa_rotacion_anual") is not None:
            lineas.append(f"Tasa de rotación anual (negocios que cierran): {variables['tasa_rotacion_anual']*100:.0f}%")

    return "\n".join(lineas) if lineas else "Sin datos disponibles."


def _analisis_fallback(zona_nombre: str, sector_codigo: str, scores: dict) -> str:
    """Texto genérico si el LLM falla — mejor que un error en blanco."""
    score = scores.get("score_global", 50)
    if score >= 75:
        valoracion = "una ubicación con buenas perspectivas"
    elif score >= 50:
        valoracion = "una ubicación con potencial moderado"
    else:
        valoracion = "una ubicación con algunas limitaciones a considerar"

    return (
        f"{zona_nombre} presenta {valoracion} para un negocio de "
        f"{sector_codigo.replace('_', ' ')} (score {score:.0f}/100). "
        f"Revisa los scores por dimensión para identificar los puntos fuertes y débiles. "
        f"El análisis detallado no está disponible en este momento."
    )
