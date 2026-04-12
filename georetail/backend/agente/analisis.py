"""
agente/analisis.py — Adaptador público para el análisis IA de zona.

Envuelve agente/analizador.py y expone una respuesta rica para el detalle.
"""
from __future__ import annotations
import logging
from agente.analizador import analizar_zona

logger = logging.getLogger(__name__)


async def generar_analisis_zona(zona: dict, perfil: dict, session_id: str = "detalle") -> dict:
    """
    Genera el análisis IA de una zona para un perfil de negocio.

    Llama a analizar_zona (que usa el router LLM) y devuelve tanto el formato
    legacy como la estructura rica por dimensión.

    Args:
        zona:       dict completo de la zona (scores, competidores, alertas, etc.)
        perfil:     dict con el perfil del negocio del usuario
        session_id: para el log de LLM (opcional)

    Returns:
        dict con claves legacy y nuevas:
          - texto / pros / contras
          - resumen_global / explicaciones_dimensiones / razon_recomendacion
    """
    try:
        resultado = await analizar_zona(
            zona_data=zona,
            perfil_negocio=perfil,
            session_id=session_id,
        )
        return {
            "texto": resultado.get("resumen_global") or resultado.get("resumen", ""),
            "pros": resultado.get("puntos_fuertes", []),
            "contras": resultado.get("puntos_debiles", []),
            **resultado,
        }
    except Exception as exc:
        logger.error("generar_analisis_zona zona=%s: %s", zona.get("zona_id"), exc)
        return {
            "texto":   "Análisis no disponible temporalmente.",
            "pros":    [],
            "contras": [],
            "resumen_global": "Análisis no disponible temporalmente.",
            "puntos_fuertes": [],
            "puntos_debiles": [],
            "razon_recomendacion": "No se pudo generar la explicación en este momento.",
            "recomendacion_final": "Con reservas",
            "explicaciones_dimensiones": {},
        }
