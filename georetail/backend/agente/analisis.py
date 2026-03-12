"""
agente/analisis.py — Adaptador público para el análisis IA de zona.

Envuelve agente/analizador.py y mapea las claves de retorno al formato
que espera api/local.py: { texto, pros, contras }.
"""
from __future__ import annotations
import logging
from agente.analizador import analizar_zona

logger = logging.getLogger(__name__)


async def generar_analisis_zona(zona: dict, perfil: dict, session_id: str = "detalle") -> dict:
    """
    Genera el análisis IA de una zona para un perfil de negocio.

    Llama a analizar_zona (que usa Claude Sonnet via llm_router) y adapta
    el resultado al formato { texto, pros, contras } que usa api/local.py.

    Args:
        zona:       dict completo de la zona (scores, competidores, alertas, etc.)
        perfil:     dict con el perfil del negocio del usuario
        session_id: para el log de LLM (opcional)

    Returns:
        dict con claves: texto (str), pros (list[str]), contras (list[str])
    """
    try:
        resultado = await analizar_zona(
            zona_data=zona,
            perfil_negocio=perfil,
            session_id=session_id,
        )
        return {
            "texto":    resultado.get("resumen", ""),
            "pros":     resultado.get("puntos_fuertes", []),
            "contras":  resultado.get("puntos_debiles", []),
        }
    except Exception as exc:
        logger.error("generar_analisis_zona zona=%s: %s", zona.get("zona_id"), exc)
        return {
            "texto":   "Análisis no disponible temporalmente.",
            "pros":    [],
            "contras": [],
        }
