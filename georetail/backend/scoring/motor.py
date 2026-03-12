"""
scoring/motor.py — Interfaz pública del módulo de scoring (puntuación).

Los endpoints de la API importan desde aquí. Este módulo delega en:
  - scorer.py      → cálculo de scores con XGBoost o pesos manuales
  - features.py    → construcción de features (variables de entrada al modelo)

Funciones públicas:
  - calcular_scores_batch(zona_ids, perfil)  → usado por POST /api/buscar
  - get_scores_zona(zona_id, sector_codigo)  → usado por POST /api/local
"""
from __future__ import annotations

import logging
from typing import Optional

from scoring.features import construir_features, construir_features_batch
from scoring.scorer import puntuar_zona, puntuar_batch, guardar_scores

logger = logging.getLogger(__name__)


async def calcular_scores_batch(
    zona_ids: list[str],
    sector_codigo: str,
    m2: Optional[float] = None,
) -> list[dict]:
    """
    Calcula el score de viabilidad para una lista de zonas.

    Usado por POST /api/buscar después de filtrar candidatas en PostGIS.
    Devuelve lista de dicts con zona_id, score_global y scores por dimensión.

    Parámetros:
      zona_ids       → IDs de las zonas a puntuar
      sector_codigo  → tipo de negocio ('restauracion', 'moda', 'tatuajes'...)
      m2             → superficie del local (opcional, mejora la estimación financiera)
    """
    if not zona_ids:
        return []

    try:
        features_list = await construir_features_batch(zona_ids, sector_codigo)
        resultados    = await puntuar_batch(features_list, sector_codigo)
        await guardar_scores(resultados, sector_codigo)
        return resultados

    except Exception as exc:
        logger.error("calcular_scores_batch error: %s", exc, exc_info=True)
        # Fallback: devolver scores neutros para no romper la búsqueda
        return [
            {
                "zona_id":      z,
                "score_global": 50.0,
                "score_flujo_peatonal":    50.0,
                "score_demografia":        50.0,
                "score_competencia":       50.0,
                "score_precio_alquiler":   50.0,
                "score_transporte":        50.0,
                "score_seguridad":         50.0,
                "score_turismo":           50.0,
                "score_entorno_comercial": 50.0,
                "probabilidad_supervivencia": 0.50,
                "shap_values":             {},
                "modelo_version":          "fallback_error",
            }
            for z in zona_ids
        ]


async def get_scores_zona(
    zona_id: str,
    sector_codigo: str,
) -> dict:
    """
    Devuelve el score detallado de una zona concreta.

    Primero busca en caché (tabla scores_zona). Si no hay dato reciente
    (menos de 7 días), recalcula.

    Usado por POST /api/local (panel de detalle de zona).
    """
    from db.conexion import get_db

    async with get_db() as conn:
        # Buscar score reciente en caché (menos de 7 días)
        row = await conn.fetchrow(
            """
            SELECT sz.*, s.codigo AS sector_codigo
            FROM scores_zona sz
            JOIN sectores s ON s.id = sz.sector_id
            WHERE sz.zona_id = $1
              AND s.codigo   = $2
              AND sz.fecha_calculo >= NOW() - INTERVAL '7 days'
            ORDER BY sz.fecha_calculo DESC
            LIMIT 1
            """,
            zona_id, sector_codigo,
        )

    if row:
        return dict(row)

    # No hay caché — calcular ahora
    resultados = await calcular_scores_batch([zona_id], sector_codigo)
    return resultados[0] if resultados else _score_neutro(zona_id)


def _score_neutro(zona_id: str) -> dict:
    return {
        "zona_id":                    zona_id,
        "score_global":               50.0,
        "score_flujo_peatonal":       50.0,
        "score_demografia":           50.0,
        "score_competencia":          50.0,
        "score_precio_alquiler":      50.0,
        "score_transporte":           50.0,
        "score_seguridad":            50.0,
        "score_turismo":              50.0,
        "score_entorno_comercial":    50.0,
        "probabilidad_supervivencia": 0.50,
        "shap_values":                {},
        "modelo_version":             "sin_datos",
    }
