"""
pipelines/scores.py — Pipeline semanal de recálculo de scores.
Recalcula scores para todas las zonas × sectores activos.
"""
from __future__ import annotations
import logging
from db.conexion import get_db

logger = logging.getLogger(__name__)
async def ejecutar() -> dict:
    eid = await _init("scores")
    ok = 0
    try:
        from scoring.scorer import calcular_scores_batch, guardar_scores

        # Obtener todas las zonas (fuente primaria: tabla zonas, no variables_zona)
        # Si se usara variables_zona como fuente, cuando está vacía (primera
        # ejecución o tras un reset) ninguna zona recibiría score.
        async with get_db() as conn:
            zona_rows = await conn.fetch("SELECT id FROM zonas ORDER BY id")
            sector_rows = await conn.fetch("SELECT id, codigo FROM sectores")

            # Advisory lock para evitar ejecuciones concurrentes de este pipeline
            locked = await conn.fetchval(
                "SELECT pg_try_advisory_lock(hashtext('pipeline_scores'))"
            )
            if not locked:
                logger.warning("Pipeline scores ya está en ejecución — saltando")
                await _fin(eid, 0, "ok", "skipped: lock not acquired")
                return {"ok": 0, "skipped": True}

        zona_ids = [r["id"] for r in zona_rows]
        # Usar sectores de BD en lugar de lista hardcoded
        sector_map = {r["codigo"]: r["id"] for r in sector_rows}
        sectores_activos = list(sector_map.keys())
        logger.info("Recalculando scores: %d zonas × %d sectores", len(zona_ids), len(sectores_activos))

        for sector in sectores_activos:
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
    finally:
        # Liberar advisory lock
        try:
            async with get_db() as conn:
                await conn.execute(
                    "SELECT pg_advisory_unlock(hashtext('pipeline_scores'))"
                )
        except Exception:
            pass


async def _init(pipeline):
    async with get_db() as conn:
        return await conn.fetchval(
            "INSERT INTO pipeline_ejecuciones(pipeline,estado) VALUES($1,'running') RETURNING id", pipeline)

async def _fin(eid, reg, estado, msg=None):
    async with get_db() as conn:
        await conn.execute(
            "UPDATE pipeline_ejecuciones SET fecha_fin=NOW(),registros=$1,estado=$2,mensaje_error=$3 WHERE id=$4",
            reg, estado, msg, eid)
