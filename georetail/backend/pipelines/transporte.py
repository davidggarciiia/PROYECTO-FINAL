"""
pipelines/transporte.py — Carga de datos de transporte público de Barcelona.

Fuente: TMB API (Transports Metropolitans de Barcelona)
  - https://developer.tmb.cat
  - Endpoints usados: /lines (líneas), /stops (paradas)

Frecuencia: semanal (los datos de TMB cambian poco)

Tablas que rellena:
  - lineas_transporte
  - paradas_transporte
  - paradas_lineas
  - frecuencias_transporte

También asigna a cada parada su zona_id usando ST_Within (la parada
está dentro de la zona) para que el scoring pueda calcular cuántas
paradas hay cerca de cada zona.
"""
from __future__ import annotations

import logging
from typing import Optional

import httpx

from config import settings
from db.conexion import get_db

logger = logging.getLogger(__name__)

_TMB_BASE = "https://api.tmb.cat/v1/transit"

# Tipos de transporte y su color oficial
_TIPOS_COLOR = {
    "metro":    "#D03324",
    "bus":      "#E3000F",
    "tram":     "#007F3B",
    "fgc":      "#9B2743",
    "rodalies": "#9B2743",
}


async def ejecutar() -> dict:
    """Entry point del pipeline. Llamado por pipelines/scheduler.py."""
    eid = await _init("transporte")
    try:
        n_lineas  = await _cargar_lineas()
        n_paradas = await _cargar_paradas()
        await _asignar_zonas()

        total = n_lineas + n_paradas
        await _fin(eid, total, "ok")
        logger.info("Transporte OK — %d líneas, %d paradas", n_lineas, n_paradas)
        return {"lineas": n_lineas, "paradas": n_paradas}

    except Exception as exc:
        logger.error("Pipeline transporte ERROR: %s", exc, exc_info=True)
        await _fin(eid, 0, "error", str(exc))
        raise


async def _cargar_lineas() -> int:
    """Descarga y guarda todas las líneas de metro, bus, tram y FGC."""
    if not settings.TMB_APP_ID or not settings.TMB_APP_KEY:
        logger.warning("TMB_APP_ID / TMB_APP_KEY no configurados — saltando")
        return 0

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(
            f"{_TMB_BASE}/lines",
            params={"app_id": settings.TMB_APP_ID, "app_key": settings.TMB_APP_KEY},
        )
        resp.raise_for_status()
        lineas = resp.json().get("features", [])

    async with get_db() as conn:
        n = 0
        for linea in lineas:
            props = linea.get("properties", {})
            tipo  = _detectar_tipo(props)
            await conn.execute(
                """
                INSERT INTO lineas_transporte (id, codigo, nombre, tipo, color_hex, fuente)
                VALUES ($1, $2, $3, $4, $5, 'tmb')
                ON CONFLICT (id) DO UPDATE
                SET nombre = EXCLUDED.nombre,
                    tipo   = EXCLUDED.tipo
                """,
                str(props.get("ID_LINIA", "")),
                str(props.get("CODI_LINIA", "")),
                str(props.get("NOM_LINIA", "")),
                tipo,
                _TIPOS_COLOR.get(tipo, "#666666"),
            )
            n += 1

    return n


async def _cargar_paradas() -> int:
    """Descarga y guarda todas las paradas."""
    if not settings.TMB_APP_ID or not settings.TMB_APP_KEY:
        return 0

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(
            f"{_TMB_BASE}/stops",
            params={"app_id": settings.TMB_APP_ID, "app_key": settings.TMB_APP_KEY},
        )
        resp.raise_for_status()
        paradas = resp.json().get("features", [])

    async with get_db() as conn:
        n = 0
        for parada in paradas:
            props = parada.get("properties", {})
            geom  = parada.get("geometry", {})
            coords = geom.get("coordinates", [None, None])

            if not coords[0] or not coords[1]:
                continue

            lng, lat = float(coords[0]), float(coords[1])

            await conn.execute(
                """
                INSERT INTO paradas_transporte
                    (id, nombre, lat, lng, geometria, accesible_pmr, fuente)
                VALUES (
                    $1, $2, $3, $4,
                    ST_SetSRID(ST_MakePoint($4, $3), 4326),
                    $5, 'tmb'
                )
                ON CONFLICT (id) DO UPDATE
                SET nombre = EXCLUDED.nombre,
                    lat    = EXCLUDED.lat,
                    lng    = EXCLUDED.lng
                """,
                str(props.get("CODI_PARADA", "")),
                str(props.get("NOM_PARADA", "")),
                lat, lng,
                bool(props.get("ACCESSIBLE_PMR", False)),
            )

            # Registrar las líneas que pasan por esta parada
            for linea_id in _extraer_lineas(props):
                await conn.execute(
                    """
                    INSERT INTO paradas_lineas (parada_id, linea_id)
                    VALUES ($1, $2)
                    ON CONFLICT DO NOTHING
                    """,
                    str(props.get("CODI_PARADA", "")),
                    linea_id,
                )
            n += 1

    return n


async def _asignar_zonas() -> None:
    """
    Asigna a cada parada su zona_id usando ST_Within
    (la parada cae dentro del polígono de la zona).
    """
    async with get_db() as conn:
        await conn.execute(
            """
            UPDATE paradas_transporte pt
            SET zona_id = z.id
            FROM zonas z
            WHERE ST_Within(pt.geometria, z.geometria)
              AND pt.zona_id IS NULL
            """
        )
    logger.info("Zonas asignadas a paradas de transporte")


def _detectar_tipo(props: dict) -> str:
    nombre = str(props.get("NOM_LINIA", "")).lower()
    codigo = str(props.get("CODI_LINIA", "")).lower()
    if "metro" in nombre or codigo.startswith("l"):
        return "metro"
    if "tram" in nombre or codigo.startswith("t"):
        return "tram"
    if "fgc" in nombre or "ferrocarrils" in nombre:
        return "fgc"
    if "rodalies" in nombre or "cercanias" in nombre:
        return "rodalies"
    return "bus"


def _extraer_lineas(props: dict) -> list[str]:
    """Extrae los IDs de líneas de las propiedades de una parada."""
    lineas_str = str(props.get("LINIES", "") or "")
    return [l.strip() for l in lineas_str.split(",") if l.strip()]


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
