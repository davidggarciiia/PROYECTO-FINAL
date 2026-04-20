"""
pipelines/transporte.py — Carga de datos de transporte público de Barcelona.

Fuente: TMB API (Transports Metropolitans de Barcelona)
  https://developer.tmb.cat

Endpoints usados (rutas correctas verificadas):
  GET /v1/transit/linies          → todas las líneas (metro + bus + tram + FGC)
  GET /v1/transit/parades         → todas las paradas con coordenadas
  GET /v1/transit/linies/{t}/{c}/parades → paradas de una línea concreta

Tablas que rellena:
  - lineas_transporte
  - paradas_transporte
  - paradas_lineas
"""
from __future__ import annotations

import logging
from typing import Optional

import httpx

from config import settings
from db.conexion import get_db

logger = logging.getLogger(__name__)

_TMB_BASE  = "https://api.tmb.cat/v1/transit"

_TIPOS_COLOR = {
    "metro":    "#D03324",
    "bus":      "#E3000F",
    "tram":     "#007F3B",
    "fgc":      "#9B2743",
    "rodalies": "#9B2743",
}

# Tipo TMB → subfamilia para el endpoint de paradas por línea
_TIPO_FAMILIA = {
    "metro":    "metro",
    "bus":      "bus",
    "tram":     "tram",
    "fgc":      "fgc",
    "rodalies": "rodalies",
}


async def ejecutar() -> dict:
    """Entry point del pipeline. Llamado por pipelines/scheduler.py y api/admin.py."""
    eid = await _init("transporte")
    try:
        n_lineas  = await _cargar_lineas()
        n_paradas = await _cargar_paradas()
        await _cargar_paradas_por_linea()
        await _asignar_zonas()

        # Agregación por zona (materializa num_lineas/paradas en variables_zona)
        zonas_actualizadas = 0
        try:
            from pipelines.transporte.agregado_zona import ejecutar as ejecutar_agregado
            res_agg = await ejecutar_agregado()
            zonas_actualizadas = int(res_agg.get("zonas_actualizadas") or 0)
        except Exception as exc_agg:
            logger.warning("Agregado por zona falló (no bloqueante): %s", exc_agg)

        total = n_lineas + n_paradas
        await _fin(eid, total, "ok")
        logger.info(
            "Transporte OK — %d líneas, %d paradas, %d zonas agregadas",
            n_lineas, n_paradas, zonas_actualizadas,
        )
        return {
            "lineas": n_lineas,
            "paradas": n_paradas,
            "zonas_agregadas": zonas_actualizadas,
        }

    except Exception as exc:
        logger.error("Pipeline transporte ERROR: %s", exc, exc_info=True)
        await _fin(eid, 0, "error", str(exc))
        raise


async def _cargar_lineas() -> int:
    """Descarga y guarda todas las líneas desde /linies."""
    if not settings.TMB_APP_ID or not settings.TMB_APP_KEY:
        logger.warning("TMB_APP_ID / TMB_APP_KEY no configurados — saltando")
        return 0

    params = {"app_id": settings.TMB_APP_ID, "app_key": settings.TMB_APP_KEY}
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(f"{_TMB_BASE}/linies", params=params)
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

    logger.info("Líneas cargadas: %d", n)
    return n


async def _cargar_paradas() -> int:
    """Descarga y guarda todas las paradas desde /parades."""
    if not settings.TMB_APP_ID or not settings.TMB_APP_KEY:
        return 0

    params = {"app_id": settings.TMB_APP_ID, "app_key": settings.TMB_APP_KEY}
    async with httpx.AsyncClient(timeout=60.0) as client:
        resp = await client.get(f"{_TMB_BASE}/parades", params=params)
        resp.raise_for_status()
        paradas = resp.json().get("features", [])

    async with get_db() as conn:
        n = 0
        for parada in paradas:
            props = parada.get("properties", {})
            geom  = parada.get("geometry", {})
            coords = geom.get("coordinates", [None, None])

            if not coords or coords[0] is None or coords[1] is None:
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
                    lng    = EXCLUDED.lng,
                    geometria = EXCLUDED.geometria
                """,
                str(props.get("CODI_PARADA", "")),
                str(props.get("NOM_PARADA", "")),
                lat, lng,
                False,  # ACCESIBLE_PMR no disponible en este endpoint
            )
            n += 1

    logger.info("Paradas cargadas: %d", n)
    return n


async def _cargar_paradas_por_linea() -> None:
    """
    Para cada línea, descarga sus paradas y registra la relación en paradas_lineas.

    Endpoint: /linies/{tipo}/{CODI_LINIA}/parades
    """
    if not settings.TMB_APP_ID or not settings.TMB_APP_KEY:
        return

    params = {"app_id": settings.TMB_APP_ID, "app_key": settings.TMB_APP_KEY}

    async with get_db() as conn:
        lineas = await conn.fetch("SELECT id, codigo, tipo FROM lineas_transporte")

    total_rel = 0
    async with httpx.AsyncClient(timeout=30.0) as client:
        for linea in lineas:
            linea_id  = linea["id"]
            codi      = linea["codigo"]
            tipo      = linea["tipo"]
            familia   = _TIPO_FAMILIA.get(tipo, "bus")

            try:
                resp = await client.get(
                    f"{_TMB_BASE}/linies/{familia}/{codi}/parades",
                    params=params,
                )
                if resp.status_code != 200:
                    continue

                parades = resp.json().get("features", [])
                async with get_db() as conn:
                    for p in parades:
                        codi_parada = str(p.get("properties", {}).get("CODI_PARADA", "") or "")
                        if not codi_parada:
                            continue
                        await conn.execute(
                            """
                            INSERT INTO paradas_lineas (parada_id, linea_id)
                            VALUES ($1, $2)
                            ON CONFLICT DO NOTHING
                            """,
                            codi_parada, linea_id,
                        )
                        total_rel += 1

            except Exception as exc:
                logger.debug("Error parades linia %s/%s: %s", familia, codi, exc)

    logger.info("Relaciones parada-línea: %d", total_rel)


async def _asignar_zonas() -> None:
    """Asigna a cada parada su zona_id usando ST_Within."""
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


def _es_codigo_ortogonal(codigo: str) -> bool:
    """True si el código de línea pertenece a la red ortogonal (H/V/D/X)."""
    return bool(codigo) and codigo.upper()[0] in ('H', 'V', 'D', 'X')


def _detectar_tipo(props: dict) -> str:
    nombre  = str(props.get("NOM_LINIA",           "")).lower()
    operador= str(props.get("NOM_OPERADOR",         "")).lower()
    familia = str(props.get("NOM_TIPUS_TRANSPORT",  "")).lower()

    if "metro" in familia or "funicular" in familia:
        return "metro"
    if "tram" in familia or "tram" in nombre:
        return "tram"
    if "fgc" in operador or "ferrocarrils" in operador:
        return "fgc"
    if "rodalies" in familia or "cercanias" in nombre:
        return "rodalies"
    # Detectar bus ortogonal (red H/V/D/X de Barcelona) — subtipo gestionado en transporte_score.py
    codi = str(props.get("CODI_LINIA", ""))
    if _es_codigo_ortogonal(codi):
        return "bus"  # tipo='bus' en tabla, subtipo se detecta dinámicamente por código
    return "bus"


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
