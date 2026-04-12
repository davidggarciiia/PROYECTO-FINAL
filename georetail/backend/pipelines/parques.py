"""Compatibilidad historica para ``pipelines.parques``.

El pipeline real vive en ``pipelines.entorno.parques``, pero parte del codigo
y varios tests antiguos siguen importando este modulo y esperan poder parchear
sus helpers internos directamente.
"""

from __future__ import annotations

from pipelines.entorno import parques as _real

logger = _real.logger
httpx = _real.httpx

_AMB_PARCS_URL = _real._AMB_PARCS_URL
_RADIO_M = _real._RADIO_M
_TIMEOUT = _real._TIMEOUT
_COL_ALIASES = _real._COL_ALIASES

_parse_float = _real._parse_float
_resolve_columns = _real._resolve_columns
_descargar_csv = _real._descargar_csv
_upsert_parques = _real._upsert_parques
_calcular_m2_zonas_verdes = _real._calcular_m2_zonas_verdes

try:
    from db.conexion import get_db
except Exception:  # pragma: no cover - compatibilidad en tests aislados
    get_db = None


async def _init_pipeline() -> int:
    """Registra inicio de ejecucion usando el conector disponible."""
    _db = get_db
    if _db is None:  # pragma: no cover - fallback defensivo
        from db.conexion import get_db as _db  # noqa: PLC0415
    async with _db() as conn:
        return await conn.fetchval(
            "INSERT INTO pipeline_ejecuciones(pipeline, estado) "
            "VALUES('parques', 'running') RETURNING id"
        )


async def _fin_pipeline(eid: int, registros: int, estado: str, msg: str = None) -> None:
    """Actualiza el tracking del pipeline usando el conector disponible."""
    _db = get_db
    if _db is None:  # pragma: no cover - fallback defensivo
        from db.conexion import get_db as _db  # noqa: PLC0415
    async with _db() as conn:
        await conn.execute(
            "UPDATE pipeline_ejecuciones "
            "SET fecha_fin = NOW(), registros = $1, estado = $2, mensaje_error = $3 "
            "WHERE id = $4",
            registros, estado, msg, eid,
        )


async def actualizar_parques_amb() -> dict:
    """Punto de entrada compatible y parcheable para codigo legado."""
    logger.info("Iniciando pipeline parques AMB")

    eid = await _init_pipeline()
    result = {"parques_descargados": 0, "parques_upserted": 0, "zonas_actualizadas": 0}

    try:
        parques = await _descargar_csv()
        if not parques:
            logger.warning("parques AMB: CSV vacio o sin filas validas - abortando")
            await _fin_pipeline(eid, 0, "ok")
            return result

        _db = get_db
        if _db is None:  # pragma: no cover - fallback defensivo
            from db.conexion import get_db as _db  # noqa: PLC0415

        async with _db() as conn:
            upserted = await _upsert_parques(conn, parques)
            zonas_upd = await _calcular_m2_zonas_verdes(conn)

        result = {
            "parques_descargados": len(parques),
            "parques_upserted": upserted,
            "zonas_actualizadas": zonas_upd,
        }
        await _fin_pipeline(eid, zonas_upd, "ok")
        logger.info("Pipeline parques AMB completado: %s", result)
        return result
    except Exception as exc:
        logger.error("Pipeline parques AMB error: %s", exc)
        await _fin_pipeline(eid, 0, "error", str(exc))
        raise


ejecutar = actualizar_parques_amb

