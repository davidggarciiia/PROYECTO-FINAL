"""
scoring/motor.py — Interfaz pública del módulo de scoring.

Los endpoints de la API importan desde aquí. Este módulo delega en scorer.py.

Funciones públicas:
  - calcular_scores_batch(zona_ids, sector_codigo) → usado por POST /api/buscar
  - get_scores_zona(zona_id, sector_codigo)        → usado por POST /api/local

Estrategia de caché:
  - Se incluyen scores de cualquier versión (seed_v1 incluido).
  - El score_global SIEMPRE se recalcula aplicando los pesos del sector sobre
    las dimensiones almacenadas. Así el seed aporta diferenciación por zona
    y los pesos del sector producen rankings distintos por tipo de negocio.
  - Cuando llegan datos reales (model_version='manual_v1') el recálculo usa
    esos datos en lugar del seed, sin cambiar ninguna lógica.
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
    """
    if not zona_ids:
        return []

    try:
        from db.conexion import get_db

        # Pesos del sector — necesarios para recalcular score_global
        pesos = await _get_pesos_sector(sector_codigo)

        # ── 1. Caché (incluye seed_v1) ─────────────────────────────────────────
        # Tomamos el registro más reciente por zona, preferendo datos reales
        # sobre seed. El ORDER BY garantiza que manual_v1 > seed_v1.
        async with get_db() as conn:
            rows = await conn.fetch(
                """
                SELECT DISTINCT ON (sz.zona_id)
                       sz.zona_id,
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
                ORDER BY sz.zona_id,
                         (sz.modelo_version != 'seed_v1') DESC,
                         sz.fecha_calculo DESC
                """,
                zona_ids, sector_codigo,
            )

        # Recalcular score_global con los pesos del sector pedido
        cached = {}
        for r in rows:
            d = dict(r)
            d["score_global"] = _recalcular_global(d, pesos)
            cached[d["zona_id"]] = d

        if len(cached) == len(zona_ids):
            return list(cached.values())

        # ── 2. Zonas sin caché → calcular desde cero ──────────────────────────
        sin_cache = [z for z in zona_ids if z not in cached]
        resultados = await _scorer_batch(sin_cache, sector_codigo)
        calculados = [{"zona_id": k, **v} for k, v in resultados.items()]

        return list(cached.values()) + calculados

    except Exception as exc:
        logger.error("calcular_scores_batch error: %s", exc, exc_info=True)
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
    Devuelve el score detallado de una zona en el formato que espera api/local.py.

    Busca en caché (prefiriendo datos reales sobre seed). Si no hay nada,
    recalcula con el scorer (XGBoost o pesos manuales).
    """
    from db.conexion import get_db

    pesos = await _get_pesos_sector(sector_codigo)

    async with get_db() as conn:
        row = await conn.fetchrow(
            """
            SELECT sz.*, s.codigo AS sector_codigo
            FROM scores_zona sz
            JOIN sectores s ON s.id = sz.sector_id
            WHERE sz.zona_id = $1
              AND s.codigo   = $2
            ORDER BY (sz.modelo_version != 'seed_v1') DESC,
                     sz.fecha_calculo DESC
            LIMIT 1
            """,
            zona_id, sector_codigo,
        )

    if row:
        d = dict(row)
        d["score_global"] = _recalcular_global(d, pesos)
        return _format_scores_for_api(d)

    # No hay caché — calcular ahora
    resultados = await calcular_scores_batch([zona_id], sector_codigo)
    raw = resultados[0] if resultados else {"zona_id": zona_id, "score_global": 50.0}
    return _format_scores_for_api(raw)


# ─── Helpers privados ─────────────────────────────────────────────────────────

async def _get_pesos_sector(sector_codigo: str) -> dict:
    """Lee los pesos del sector desde la BD."""
    from db.conexion import get_db
    async with get_db() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM sectores WHERE codigo = $1", sector_codigo
        )
    return dict(row) if row else {}


def _recalcular_global(scores: dict, pesos: dict) -> float:
    """
    Recalcula score_global aplicando los pesos del sector sobre las dimensiones.

    Esto garantiza que:
    - Zonas distintas puntúan distinto (las dimensiones varían por zona).
    - Sectores distintos producen rankings distintos (los pesos varían por sector).
    """
    dims = {
        "score_flujo_peatonal":    pesos.get("peso_flujo",        0.25),
        "score_demografia":        pesos.get("peso_demo",         0.20),
        "score_competencia":       pesos.get("peso_competencia",  0.15),
        "score_precio_alquiler":   pesos.get("peso_precio",       0.15),
        "score_transporte":        pesos.get("peso_transporte",   0.10),
        "score_seguridad":         pesos.get("peso_seguridad",    0.05),
        "score_turismo":           pesos.get("peso_turismo",      0.05),
        "score_entorno_comercial": pesos.get("peso_entorno",      0.05),
    }
    total = sum(
        (scores.get(dim) or 50.0) * peso
        for dim, peso in dims.items()
    )
    return round(total, 1)


def _format_scores_for_api(raw: dict) -> dict:
    """
    Transforma el dict plano (columnas de scores_zona o resultado del scorer)
    al formato anidado que espera api/local.py.
    """
    shap_raw = raw.get("shap_values") or {}
    if isinstance(shap_raw, str):
        try:
            shap_raw = json.loads(shap_raw)
        except Exception:
            shap_raw = {}

    explicaciones = [
        {"feature": k, "valor": round(float(v), 3)}
        for k, v in sorted(shap_raw.items(), key=lambda x: abs(x[1]), reverse=True)[:10]
    ] if shap_raw else []

    prob = raw.get("probabilidad_supervivencia_3a") or raw.get("probabilidad_supervivencia")

    return {
        "score_global":                  raw.get("score_global", 50.0),
        "probabilidad_supervivencia_3a":  round(prob, 3) if prob is not None else None,
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
