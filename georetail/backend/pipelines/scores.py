"""
pipelines/scores.py — Pipeline semanal de recálculo de scores.
Recalcula scores para todas las zonas × sectores activos.
"""
from __future__ import annotations
import asyncio, logging
from db.conexion import get_db

logger = logging.getLogger(__name__)
_SECTORES = ["restauracion","moda","estetica","tatuajes","shisha_lounge"]
_MAX_CONC = 10


async def ejecutar() -> dict:
    eid = await _init("scores")
    ok = 0
    try:
        from scoring.scorer import calcular_scores_batch, guardar_scores

        # Obtener todas las zonas activas
        async with get_db() as conn:
            zona_rows = await conn.fetch("""
                SELECT DISTINCT zona_id FROM variables_zona
                WHERE fecha >= CURRENT_DATE - INTERVAL '60 days'
            """)
            sector_ids = await conn.fetch("SELECT id, codigo FROM sectores")

        zona_ids = [r["zona_id"] for r in zona_rows]
        sector_map = {r["codigo"]: r["id"] for r in sector_ids}
        logger.info("Recalculando scores: %d zonas × %d sectores", len(zona_ids), len(_SECTORES))

        sem = asyncio.Semaphore(_MAX_CONC)

        for sector in _SECTORES:
            sid = sector_map.get(sector)
            if not sid:
                continue

            # Calcular en batch por sector
            resultados = await calcular_scores_batch(zona_ids, sector)
            for zona_id, scores in resultados.items():
                await guardar_scores(zona_id, sid, scores)
                ok += 1

        await _fin(eid, ok, "ok")
        return {"ok": ok}
    except Exception as e:
        logger.error("Pipeline scores error: %s", e)
        await _fin(eid, ok, "error", str(e))
        raise


async def _init(pipeline):
    async with get_db() as conn:
        return await conn.fetchval(
            "INSERT INTO pipeline_ejecuciones(pipeline,estado) VALUES($1,'running') RETURNING id", pipeline)

async def _fin(eid, reg, estado, msg=None):
    async with get_db() as conn:
        await conn.execute(
            "UPDATE pipeline_ejecuciones SET fecha_fin=NOW(),registros=$1,estado=$2,mensaje_error=$3 WHERE id=$4",
            reg, estado, msg, eid)
