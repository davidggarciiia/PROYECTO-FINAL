from __future__ import annotations

from typing import Optional

from db.conexion import get_db
from scoring.infra.governance import get_model_feature_names_from_record


async def obtener_modelo_version(version: str) -> dict | None:
    async with get_db() as conn:
        row = await conn.fetchrow(
            """
            SELECT id, version, sector, metricas, demografia_feature_names, activo
            FROM modelos_versiones
            WHERE version = $1
            """,
            version,
        )
    return dict(row) if row else None


async def obtener_modelo_activo(sector: Optional[str] = None) -> dict | None:
    async with get_db() as conn:
        row = await conn.fetchrow(
            """
            SELECT id, version, sector, metricas, demografia_feature_names, activo
            FROM modelos_versiones
            WHERE activo = TRUE
              AND ($1::text IS NULL OR sector = $1)
            ORDER BY created_at DESC
            LIMIT 1
            """,
            sector,
        )
    return dict(row) if row else None


async def obtener_feature_names_modelo(
    *,
    version: Optional[str] = None,
    sector: Optional[str] = None,
    activo: bool = False,
) -> list[str]:
    record: dict | None
    if version:
        record = await obtener_modelo_version(version)
    elif activo:
        record = await obtener_modelo_activo(sector=sector)
    else:
        record = None
    return get_model_feature_names_from_record(record)
