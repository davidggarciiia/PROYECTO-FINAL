"""
pipelines/bicing.py — Carga de estaciones Bicing de Barcelona.

Fuente: API GBFS pública de BSMSA (Bicicletes de Barcelona, S.A.)
  https://api.bsmsa.eu/ext/api/bsm/gbfs/v2/en/station_information

No requiere autenticación. Datos: id, nombre, lat, lng, capacidad.

Tablas que rellena:
  - estaciones_bicing
"""
from __future__ import annotations

import logging
from typing import Optional

import httpx

from db.conexion import get_db

logger = logging.getLogger(__name__)

_BSMSA_STATION_INFO = "https://api.bsmsa.eu/ext/api/bsm/gbfs/v2/en/station_information"


async def ejecutar() -> dict:
    """Entry point del pipeline. Llamado por pipelines/scheduler.py y api/admin.py."""
    eid = await _init("bicing")
    try:
        n_estaciones = await _cargar_estaciones()
        await _asignar_zonas()

        await _fin(eid, n_estaciones, "ok")
        logger.info("Bicing OK — %d estaciones cargadas", n_estaciones)
        return {"estaciones": n_estaciones}

    except Exception as exc:
        logger.error("Pipeline bicing ERROR: %s", exc, exc_info=True)
        await _fin(eid, 0, "error", str(exc))
        raise


async def _cargar_estaciones() -> int:
    """Descarga todas las estaciones Bicing desde la API GBFS de BSMSA."""
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(_BSMSA_STATION_INFO)
        resp.raise_for_status()
        data = resp.json()

    stations = data.get("data", {}).get("stations", [])
    if not stations:
        logger.warning("Bicing: respuesta vacía de BSMSA — 0 estaciones")
        return 0

    async with get_db() as conn:
        n = 0
        for station in stations:
            station_id = str(station.get("station_id", ""))
            nombre     = str(station.get("name", ""))
            lat        = station.get("lat")
            lng        = station.get("lon")
            capacidad  = station.get("capacity")

            if not station_id or lat is None or lng is None:
                continue

            await conn.execute(
                """
                INSERT INTO estaciones_bicing
                    (id, nombre, lat, lng, geometria, capacidad, fuente)
                VALUES (
                    $1, $2, $3, $4,
                    ST_SetSRID(ST_MakePoint($4, $3), 4326),
                    $5, 'bsmsa'
                )
                ON CONFLICT (id) DO UPDATE
                SET nombre    = EXCLUDED.nombre,
                    lat       = EXCLUDED.lat,
                    lng       = EXCLUDED.lng,
                    geometria = EXCLUDED.geometria,
                    capacidad = EXCLUDED.capacidad,
                    fecha_carga = NOW()
                """,
                station_id,
                nombre,
                float(lat),
                float(lng),
                int(capacidad) if capacidad is not None else None,
            )
            n += 1

    logger.info("Estaciones Bicing cargadas: %d", n)
    return n


async def _asignar_zonas() -> None:
    """Asigna a cada estación su zona_id usando ST_Within."""
    async with get_db() as conn:
        await conn.execute(
            """
            UPDATE estaciones_bicing eb
            SET zona_id = z.id
            FROM zonas z
            WHERE ST_Within(eb.geometria, z.geometria)
              AND eb.zona_id IS NULL
            """
        )
    logger.info("Zonas asignadas a estaciones Bicing")


async def ejecutar_carriles() -> dict:
    """
    Pipeline de carriles bici desde Open Data BCN (CKAN).

    TODO: Implementar descarga desde el dataset de carrils-bici de Open Data BCN.
    La fuente es el Ajuntament de Barcelona a través de la API CKAN:
      https://opendata-ajuntament.barcelona.cat/data/ca/dataset/carrils-bici
    Requiere normalización de geometrías GeoJSON a PostGIS LINESTRING.
    """
    logger.info("Pipeline carriles_bici: pendiente de implementación")
    return {"carriles": 0}


async def _init(pipeline: str) -> int:
    async with get_db() as conn:
        return await conn.fetchval(
            "INSERT INTO pipeline_ejecuciones (pipeline, estado) "
            "VALUES ($1,'running') RETURNING id",
            pipeline,
        )


async def _fin(eid: int, registros: int, estado: str, mensaje: Optional[str] = None) -> None:
    async with get_db() as conn:
        await conn.execute(
            "UPDATE pipeline_ejecuciones "
            "SET fecha_fin=NOW(), registros=$1, estado=$2, mensaje_error=$3 "
            "WHERE id=$4",
            registros, estado, mensaje, eid,
        )
