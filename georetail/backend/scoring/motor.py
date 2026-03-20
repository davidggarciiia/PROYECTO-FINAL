"""
scoring/motor.py — Interfaz pública del módulo de scoring.

Los endpoints de la API importan desde aquí. Este módulo delega en scorer.py.

Funciones públicas:
  - calcular_scores_batch(zona_ids, sector_codigo) → usado por POST /api/buscar
  - get_scores_zona(zona_id, sector_codigo)        → usado por POST /api/local
"""
from __future__ import annotations

import json
import logging
from typing import Optional

from scoring.scorer import calcular_scores_batch as _scorer_batch

logger = logging.getLogger(__name__)


async def calcular_scores_batch(
    zona_ids: list[str],
    sector_codigo: str,
    m2: Optional[float] = None,
) -> list[dict]:
    """
    Calcula el score de viabilidad para una lista de zonas.

    Devuelve lista de dicts con zona_id + score_global + scores por dimensión,
    ordenada por zona_id (el orden final lo hace api/buscar.py por score).

    Parámetros:
      zona_ids       → IDs de las zonas a puntuar
      sector_codigo  → tipo de negocio ('restauracion', 'moda', 'tatuajes'...)
      m2             → superficie del local (opcional, no afecta scoring actual)
    """
    if not zona_ids:
        return []

    try:
        # ── 1. Consultar scores precalculados en BD (cache) ────────────────────
        from db.conexion import get_db
        async with get_db() as conn:
            rows = await conn.fetch(
                """
                SELECT sz.zona_id,
                       sz.score_global,
                       sz.score_flujo_peatonal,
                       sz.score_demografia,
                       sz.score_competencia,
                       sz.score_precio_alquiler,
                       sz.score_transporte,
                       sz.score_seguridad,
                       sz.score_turismo,
                       sz.score_entorno_comercial,
                       sz.probabilidad_supervivencia,
                       sz.shap_values,
                       sz.modelo_version
                FROM scores_zona sz
                JOIN sectores s ON s.id = sz.sector_id
                WHERE sz.zona_id = ANY($1)
                  AND s.codigo   = $2
                ORDER BY sz.fecha_calculo DESC
                """,
                zona_ids, sector_codigo,
            )

        cached = {r["zona_id"]: dict(r) for r in rows}
        if len(cached) == len(zona_ids):
            return list(cached.values())

        # ── 2. Para zonas sin caché, calcular con scorer ──────────────────────
        sin_cache = [z for z in zona_ids if z not in cached]
        resultados = await _scorer_batch(sin_cache, sector_codigo)
        calculados = [{"zona_id": k, **v} for k, v in resultados.items()]

        return list(cached.values()) + calculados

    except Exception as exc:
        logger.error("calcular_scores_batch error: %s", exc, exc_info=True)
        # Fallback: devolver scores neutros para no romper la búsqueda
        return [
            {
                "zona_id":                     z,
                "score_global":                50.0,
                "score_flujo_peatonal":        50.0,
                "score_demografia":            50.0,
                "score_competencia":           50.0,
                "score_precio_alquiler":       50.0,
                "score_transporte":            50.0,
                "score_seguridad":             50.0,
                "score_turismo":               50.0,
                "score_entorno_comercial":     50.0,
                "probabilidad_supervivencia":  0.50,
                "shap_values":                 {},
                "modelo_version":              "fallback_error",
            }
            for z in zona_ids
        ]


async def get_scores_zona(
    zona_id: str,
    sector_codigo: str,
) -> dict:
    """
    Devuelve el score detallado de una zona en el formato que espera api/local.py:
      {
        "score_global": float,
        "probabilidad_supervivencia_3a": float | None,
        "scores_dimension": { "flujo_peatonal": float, ... },
        "explicaciones_shap": [ {"feature": str, "valor": float}, ... ],
      }

    Busca primero en caché (tabla scores_zona, < 7 días). Si no hay dato reciente,
    recalcula con XGBoost (o pesos manuales como fallback).
    """
    from db.conexion import get_db

    async with get_db() as conn:
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
        return _format_scores_for_api(dict(row))

    # No hay caché — calcular ahora
    resultados = await calcular_scores_batch([zona_id], sector_codigo)
    raw = resultados[0] if resultados else {"zona_id": zona_id, "score_global": 50.0}
    return _format_scores_for_api(raw)


# ─── Helpers privados ─────────────────────────────────────────────────────────

def _format_scores_for_api(raw: dict) -> dict:
    """
    Transforma el dict plano (columnas de scores_zona o resultado del scorer)
    al formato anidado que espera api/local.py.
    """
    # shap_values puede venir como str JSON (de la BD) o como dict (del scorer)
    shap_raw = raw.get("shap_values") or {}
    if isinstance(shap_raw, str):
        try:
            shap_raw = json.loads(shap_raw)
        except Exception:
            shap_raw = {}

    # Top 10 factores SHAP ordenados por valor absoluto
    explicaciones = [
        {"feature": k, "valor": round(float(v), 3)}
        for k, v in sorted(shap_raw.items(), key=lambda x: abs(x[1]), reverse=True)[:10]
    ] if shap_raw else []

    # La probabilidad puede llamarse con o sin el sufijo _3a según el origen
    prob = raw.get("probabilidad_supervivencia_3a") or raw.get("probabilidad_supervivencia")

    return {
        "score_global":                 raw.get("score_global", 50.0),
        "probabilidad_supervivencia_3a": round(prob, 3) if prob is not None else None,
        "scores_dimension": {
            "flujo_peatonal":    raw.get("score_flujo_peatonal"),
            "demografia":        raw.get("score_demografia"),
            "competencia":       raw.get("score_competencia"),
            "precio_alquiler":   raw.get("score_precio_alquiler"),
            "transporte":        raw.get("score_transporte"),
            "seguridad":         raw.get("score_seguridad"),
            "turismo":           raw.get("score_turismo"),
            "entorno_comercial": raw.get("score_entorno_comercial"),
        },
        "explicaciones_shap": explicaciones,
    }


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
