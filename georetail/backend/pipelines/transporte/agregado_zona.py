"""
pipelines/transporte/agregado_zona.py — Materializa num_lineas_transporte,
num_paradas_transporte y score_transporte_calculado en variables_zona por zona.

Se ejecuta después de `pipelines.transporte.transporte.ejecutar` (que pobla
lineas_transporte, paradas_transporte, paradas_lineas) para que el scorer pueda
consumir directamente desde variables_zona sin join espacial en runtime.

Tablas de entrada : paradas_transporte, paradas_lineas, lineas_transporte, zonas
Tabla de salida   : variables_zona (columnas añadidas en mig 034)
Frecuencia        : semanal (sábado 01:15, encadenado tras transporte)
"""
from __future__ import annotations

import logging
from datetime import date
from typing import Optional

from db.conexion import get_db

logger = logging.getLogger(__name__)

_RADIO_M = 500


async def ejecutar() -> dict:
    """Entry point del pipeline de agregación por zona."""
    eid = await _init()
    try:
        n = await _agregar_por_zona()
        await _fin(eid, n, "ok")
        logger.info("transporte_agregado OK — %d zonas actualizadas", n)
        return {"zonas_actualizadas": n, "radio_m": _RADIO_M}
    except Exception as exc:
        logger.error("Pipeline transporte_agregado ERROR: %s", exc, exc_info=True)
        await _fin(eid, 0, "error", str(exc))
        raise


async def _agregar_por_zona() -> int:
    """
    Por cada zona, calcula num_lineas, num_paradas y score_transporte en un
    radio de 500m y hace UPSERT en variables_zona del día actual.

    El score es una fórmula simple: min(100, num_lineas * 5.0), igual al
    fallback de scoring/scorer.py. Si en el futuro hay un modelo más rico,
    se puede delegar en `scoring.dimensiones.transporte.calcular_score_transporte`.
    """
    hoy = date.today()
    async with get_db() as conn:
        rows = await conn.fetch(
            """
            SELECT z.id AS zona_id,
                   COUNT(DISTINCT pl.linea_id)::int AS num_lineas,
                   COUNT(DISTINCT pt.id)::int       AS num_paradas
            FROM zonas z
            LEFT JOIN paradas_transporte pt
              ON ST_DWithin(pt.geometria::geography,
                            ST_Centroid(z.geometria)::geography,
                            $1)
            LEFT JOIN paradas_lineas pl ON pl.parada_id = pt.id
            GROUP BY z.id
            """,
            _RADIO_M,
        )

        n = 0
        for r in rows:
            zona_id = r["zona_id"]
            num_lineas = int(r["num_lineas"] or 0)
            num_paradas = int(r["num_paradas"] or 0)
            score = min(100.0, num_lineas * 5.0)

            # Anchor en variables_zona (crea fila del día si no existe)
            await conn.execute(
                """
                INSERT INTO variables_zona (zona_id, fecha, fuente,
                                            num_lineas_transporte,
                                            num_paradas_transporte,
                                            score_transporte_calculado)
                VALUES ($1, $2, 'transporte_agregado', $3, $4, $5)
                ON CONFLICT (zona_id, fecha) DO UPDATE
                SET num_lineas_transporte      = EXCLUDED.num_lineas_transporte,
                    num_paradas_transporte     = EXCLUDED.num_paradas_transporte,
                    score_transporte_calculado = EXCLUDED.score_transporte_calculado,
                    updated_at                 = NOW()
                """,
                zona_id, hoy, num_lineas, num_paradas, score,
            )
            n += 1

    return n


# ── pipeline_ejecuciones helpers ─────────────────────────────────────────────

async def _init() -> int:
    async with get_db() as conn:
        return await conn.fetchval(
            "INSERT INTO pipeline_ejecuciones (pipeline, estado) "
            "VALUES ('transporte_agregado', 'running') RETURNING id"
        )


async def _fin(eid: int, registros: int, estado: str, mensaje: Optional[str] = None) -> None:
    async with get_db() as conn:
        await conn.execute(
            "UPDATE pipeline_ejecuciones "
            "SET fecha_fin=NOW(), registros=$1, estado=$2, mensaje_error=$3 "
            "WHERE id=$4",
            registros, estado, mensaje, eid,
        )
