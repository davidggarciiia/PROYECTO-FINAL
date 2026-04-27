"""
gtfs_atm.py — Carga de paradas/líneas de Tram, FGC y Rodalies desde GTFS ATM.

Fuente: ATM Autoritat del Transport Metropolità de Barcelona
URL: https://www.atm.cat/files/horaris/gtfs_tm_all.zip

Agencias importadas: TRAM, FGC, RODALIES/RENFE
Tablas: lineas_transporte, paradas_transporte, paradas_lineas
"""
from __future__ import annotations

import csv
import io
import logging
import zipfile
from typing import Optional

import httpx

from db.conexion import get_db

logger = logging.getLogger(__name__)

_GTFS_URL = "https://www.atm.cat/files/horaris/gtfs_tm_all.zip"
_DOWNLOAD_TIMEOUT = 120.0
_MAX_STOP_TIMES_ROWS = 2_000_000

# Bounding box del área metropolitana de Barcelona
BCN_BBOX = {"lat_min": 41.20, "lat_max": 41.55, "lng_min": 1.90, "lng_max": 2.40}

# Colores por operador
_COLOR_TRAM = "#83B040"
_COLOR_FGC = "#9B2743"
_COLOR_RODALIES = "#FF6C00"

# Colores individuales por código de línea tram (mismos para Trambaix y Trambesòs)
TRAM_COLORS: dict[str, str] = {
    "T1": _COLOR_TRAM,
    "T2": _COLOR_TRAM,
    "T3": _COLOR_TRAM,
    "T4": _COLOR_TRAM,
    "T5": _COLOR_TRAM,
    "T6": _COLOR_TRAM,
}

# Palabras clave para identificar agencias en agency.txt
_KEYWORDS_TRAM = ("tram",)
_KEYWORDS_FGC = ("fgc", "ferrocarrils")
_KEYWORDS_RODALIES = ("rodalies", "renfe", "cercanias")

# route_type GTFS → tipo interno
_ROUTE_TYPE_TRAM = 0       # tram/light rail
_ROUTE_TYPE_RAIL = (1, 2)  # metro/rail (FGC y Rodalies)


# ── Helpers internos ──────────────────────────────────────────────────────────

def _in_bbox(lat: float, lng: float) -> bool:
    return (
        BCN_BBOX["lat_min"] <= lat <= BCN_BBOX["lat_max"]
        and BCN_BBOX["lng_min"] <= lng <= BCN_BBOX["lng_max"]
    )


def _read_csv(zf: zipfile.ZipFile, filename: str) -> list[dict]:
    """Lee un archivo txt (CSV) del ZIP y devuelve lista de dicts."""
    try:
        with zf.open(filename) as f:
            # GTFS usa UTF-8 con posible BOM
            content = f.read().decode("utf-8-sig")
            reader = csv.DictReader(io.StringIO(content))
            return list(reader)
    except KeyError:
        logger.warning("GTFS: archivo '%s' no encontrado en el ZIP", filename)
        return []


def _read_csv_limited(zf: zipfile.ZipFile, filename: str, max_rows: int) -> list[dict]:
    """Lee hasta max_rows filas de un archivo CSV del ZIP."""
    try:
        with zf.open(filename) as f:
            content = f.read().decode("utf-8-sig")
            reader = csv.DictReader(io.StringIO(content))
            rows: list[dict] = []
            for i, row in enumerate(reader):
                if i >= max_rows:
                    logger.warning(
                        "GTFS: '%s' truncado a %d filas (archivo muy grande)",
                        filename, max_rows,
                    )
                    break
                rows.append(row)
            return rows
    except KeyError:
        logger.warning("GTFS: archivo '%s' no encontrado en el ZIP", filename)
        return []


def _detect_operator(agency_name: str, agency_id: str) -> Optional[str]:
    """Retorna 'tram', 'fgc', 'rodalies' o None según el nombre/id de la agencia."""
    name_low = agency_name.lower()
    id_low = agency_id.lower()
    combined = name_low + " " + id_low
    if any(k in combined for k in _KEYWORDS_TRAM):
        return "tram"
    if any(k in combined for k in _KEYWORDS_FGC):
        return "fgc"
    if any(k in combined for k in _KEYWORDS_RODALIES):
        return "rodalies"
    return None


def _color_for_route(operador: str, short_name: str) -> str:
    if operador == "tram":
        return TRAM_COLORS.get(short_name.upper(), _COLOR_TRAM)
    if operador == "fgc":
        return _COLOR_FGC
    return _COLOR_RODALIES


def _route_matches_operator(route_type_str: str, operador: str) -> bool:
    """Comprueba si el route_type GTFS es coherente con el operador."""
    try:
        rt = int(route_type_str)
    except (ValueError, TypeError):
        return True  # si no hay tipo, no filtrar
    if operador == "tram":
        return rt == _ROUTE_TYPE_TRAM
    # FGC y Rodalies usan rail/metro (1 o 2)
    return rt in _ROUTE_TYPE_RAIL


# ── Parseo GTFS ───────────────────────────────────────────────────────────────

def _parse_agencies(zf: zipfile.ZipFile) -> dict[str, str]:
    """
    Parsea agency.txt y devuelve {agency_id → operador}.
    operador es 'tram', 'fgc' o 'rodalies'.
    agency_id puede ser string vacío en feeds de una sola agencia.
    """
    rows = _read_csv(zf, "agency.txt")
    result: dict[str, str] = {}
    for row in rows:
        agency_id = (row.get("agency_id") or "").strip()
        agency_name = (row.get("agency_name") or "").strip()
        operador = _detect_operator(agency_name, agency_id)
        if operador:
            result[agency_id] = operador
            logger.info("GTFS agency: '%s' (id='%s') → %s", agency_name, agency_id, operador)
    return result


def _parse_routes(
    zf: zipfile.ZipFile,
    agency_operador: dict[str, str],
) -> dict[str, dict]:
    """
    Parsea routes.txt y devuelve {route_id → info}.
    info contiene: operador, codigo, nombre, tipo, color_hex.
    Solo incluye rutas de las agencias de interés.
    """
    rows = _read_csv(zf, "routes.txt")
    result: dict[str, dict] = {}
    for row in rows:
        route_id = (row.get("route_id") or "").strip()
        agency_id = (row.get("agency_id") or "").strip()

        # agency_id vacío → puede ser feed de una sola agencia
        operador = agency_operador.get(agency_id)
        if operador is None and "" in agency_operador:
            operador = agency_operador[""]
        if operador is None:
            continue

        route_type = (row.get("route_type") or "").strip()
        if not _route_matches_operator(route_type, operador):
            continue

        short_name = (row.get("route_short_name") or "").strip()
        long_name = (row.get("route_long_name") or "").strip()
        color_hex = _color_for_route(operador, short_name)

        result[route_id] = {
            "operador": operador,
            "codigo": short_name,
            "nombre": long_name or short_name,
            "tipo": operador,
            "color_hex": color_hex,
        }

    logger.info(
        "GTFS routes filtradas: %d (tram=%d, fgc=%d, rodalies=%d)",
        len(result),
        sum(1 for v in result.values() if v["operador"] == "tram"),
        sum(1 for v in result.values() if v["operador"] == "fgc"),
        sum(1 for v in result.values() if v["operador"] == "rodalies"),
    )
    return result


def _parse_stops(zf: zipfile.ZipFile) -> dict[str, dict]:
    """
    Parsea stops.txt y devuelve {stop_id → info} filtrando por BCN_BBOX.
    """
    rows = _read_csv(zf, "stops.txt")
    result: dict[str, dict] = {}
    for row in rows:
        stop_id = (row.get("stop_id") or "").strip()
        if not stop_id:
            continue
        try:
            lat = float(row.get("stop_lat") or 0)
            lng = float(row.get("stop_lon") or 0)
        except ValueError:
            continue
        if not _in_bbox(lat, lng):
            continue
        result[stop_id] = {
            "nombre": (row.get("stop_name") or "").strip(),
            "lat": lat,
            "lng": lng,
        }
    logger.info("GTFS paradas en bbox BCN: %d", len(result))
    return result


def _parse_route_stops(
    zf: zipfile.ZipFile,
    route_ids: set[str],
    valid_stop_ids: set[str],
) -> dict[str, set[str]]:
    """
    Parsea trips.txt + stop_times.txt para construir {route_id → {stop_id, ...}}.
    Solo incluye rutas y paradas de interés.
    """
    # trips.txt → {trip_id → route_id}
    trip_rows = _read_csv(zf, "trips.txt")
    trip_to_route: dict[str, str] = {}
    for row in trip_rows:
        trip_id = (row.get("trip_id") or "").strip()
        route_id = (row.get("route_id") or "").strip()
        if route_id in route_ids and trip_id:
            trip_to_route[trip_id] = route_id

    if not trip_to_route:
        logger.warning("GTFS: no se encontraron trips para las rutas de interés")
        return {}

    # Verificar tamaño de stop_times.txt antes de parsear
    try:
        info = zf.getinfo("stop_times.txt")
        size_mb = info.file_size / (1024 * 1024)
        logger.info("GTFS stop_times.txt: %.1f MB", size_mb)
    except KeyError:
        logger.warning("GTFS: stop_times.txt no encontrado")
        return {}

    use_limit = size_mb > 50
    if use_limit:
        logger.warning(
            "GTFS: stop_times.txt grande (%.1f MB) — limitando a %d filas",
            size_mb, _MAX_STOP_TIMES_ROWS,
        )
        st_rows = _read_csv_limited(zf, "stop_times.txt", _MAX_STOP_TIMES_ROWS)
    else:
        st_rows = _read_csv(zf, "stop_times.txt")

    route_stops: dict[str, set[str]] = {rid: set() for rid in route_ids}
    for row in st_rows:
        trip_id = (row.get("trip_id") or "").strip()
        stop_id = (row.get("stop_id") or "").strip()
        route_id = trip_to_route.get(trip_id)
        if route_id and stop_id in valid_stop_ids:
            route_stops[route_id].add(stop_id)

    return route_stops


# ── Persistencia ──────────────────────────────────────────────────────────────

async def _upsert_lineas(routes: dict[str, dict]) -> int:
    """Upsert de líneas en lineas_transporte. Retorna número de filas procesadas."""
    async with get_db() as conn:
        n = 0
        for route_id, info in routes.items():
            linea_id = f"gtfs_{route_id}"
            await conn.execute(
                """
                INSERT INTO lineas_transporte (id, codigo, nombre, tipo, color_hex, fuente)
                VALUES ($1, $2, $3, $4, $5, 'gtfs_atm')
                ON CONFLICT (id) DO UPDATE
                SET codigo    = EXCLUDED.codigo,
                    nombre    = EXCLUDED.nombre,
                    tipo      = EXCLUDED.tipo,
                    color_hex = EXCLUDED.color_hex,
                    fuente    = EXCLUDED.fuente
                """,
                linea_id,
                info["codigo"],
                info["nombre"],
                info["tipo"],
                info["color_hex"],
            )
            n += 1
    return n


async def _upsert_paradas(stops: dict[str, dict]) -> int:
    """Upsert de paradas en paradas_transporte. Retorna número de filas procesadas."""
    async with get_db() as conn:
        n = 0
        for stop_id, info in stops.items():
            parada_id = f"gtfs_{stop_id}"
            await conn.execute(
                """
                INSERT INTO paradas_transporte
                    (id, nombre, lat, lng, geometria, es_accesible_pmr, fuente)
                VALUES ($1, $2, $3, $4,
                        ST_SetSRID(ST_MakePoint($4, $3), 4326),
                        FALSE, 'gtfs_atm')
                ON CONFLICT (id) DO UPDATE
                SET nombre    = EXCLUDED.nombre,
                    lat       = EXCLUDED.lat,
                    lng       = EXCLUDED.lng,
                    geometria = EXCLUDED.geometria,
                    fuente    = EXCLUDED.fuente
                """,
                parada_id,
                info["nombre"],
                info["lat"],
                info["lng"],
            )
            n += 1
    return n


async def _upsert_paradas_lineas(route_stops: dict[str, set[str]]) -> int:
    """Inserta relaciones parada-línea en paradas_lineas. Retorna número de relaciones."""
    async with get_db() as conn:
        n = 0
        for route_id, stop_ids in route_stops.items():
            linea_id = f"gtfs_{route_id}"
            for stop_id in stop_ids:
                parada_id = f"gtfs_{stop_id}"
                await conn.execute(
                    """
                    INSERT INTO paradas_lineas (parada_id, linea_id)
                    VALUES ($1, $2)
                    ON CONFLICT DO NOTHING
                    """,
                    parada_id,
                    linea_id,
                )
                n += 1
    return n


# ── Registro pipeline ─────────────────────────────────────────────────────────

async def _init(pipeline: str) -> int:
    async with get_db() as conn:
        return await conn.fetchval(
            "INSERT INTO pipeline_ejecuciones (pipeline, estado) "
            "VALUES ($1, 'running') RETURNING id",
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


# ── Entry point ───────────────────────────────────────────────────────────────

async def ejecutar() -> dict:
    """
    Entry point del pipeline. Descarga el feed GTFS completo de ATM,
    filtra agencias TRAM/FGC/Rodalies e importa líneas y paradas a la BD.

    Retorna:
        {"tram": n, "fgc": n, "rodalies": n, "paradas": n}
    """
    eid = await _init("gtfs_atm")
    resultado: dict = {"tram": 0, "fgc": 0, "rodalies": 0, "paradas": 0}

    try:
        # 1. Descarga
        logger.info("GTFS ATM: descargando desde %s", _GTFS_URL)
        try:
            async with httpx.AsyncClient(timeout=_DOWNLOAD_TIMEOUT) as client:
                resp = await client.get(_GTFS_URL, follow_redirects=True)
                resp.raise_for_status()
                content = resp.content
            logger.info("GTFS ATM: ZIP descargado (%.1f MB)", len(content) / (1024 * 1024))
        except (httpx.TimeoutException, httpx.HTTPStatusError) as exc:
            logger.warning("GTFS ATM: descarga fallida — %s. Abortando.", exc)
            await _fin(eid, 0, "ok", f"descarga fallida: {exc}")
            return resultado

        # 2. Abrir ZIP en memoria
        try:
            zf = zipfile.ZipFile(io.BytesIO(content))
        except zipfile.BadZipFile as exc:
            logger.warning("GTFS ATM: ZIP inválido — %s", exc)
            await _fin(eid, 0, "ok", f"zip invalido: {exc}")
            return resultado

        # 3. Identificar agencias
        agency_operador = _parse_agencies(zf)
        if not agency_operador:
            logger.warning("GTFS ATM: no se encontraron agencias TRAM/FGC/Rodalies")
            await _fin(eid, 0, "ok", "sin agencias relevantes")
            return resultado

        operadores_encontrados = set(agency_operador.values())
        for op in ("tram", "fgc", "rodalies"):
            if op not in operadores_encontrados:
                logger.warning("GTFS ATM: agencia '%s' no encontrada en agency.txt", op)

        # 4. Parsear rutas
        routes = _parse_routes(zf, agency_operador)
        if not routes:
            logger.warning("GTFS ATM: no se encontraron rutas para las agencias seleccionadas")
            await _fin(eid, 0, "ok", "sin rutas relevantes")
            return resultado

        # 5. Parsear paradas (filtradas por bbox)
        stops = _parse_stops(zf)

        # 6. Mapear rutas → paradas via trips + stop_times
        route_ids = set(routes.keys())
        valid_stop_ids = set(stops.keys())
        route_stops = _parse_route_stops(zf, route_ids, valid_stop_ids)

        zf.close()

        # 7. Upsert en BD
        n_lineas = await _upsert_lineas(routes)
        n_paradas = await _upsert_paradas(stops)
        n_rels = await _upsert_paradas_lineas(route_stops)

        # 8. Contar por operador
        n_tram = sum(1 for v in routes.values() if v["operador"] == "tram")
        n_fgc = sum(1 for v in routes.values() if v["operador"] == "fgc")
        n_rodalies = sum(1 for v in routes.values() if v["operador"] == "rodalies")

        resultado = {
            "tram": n_tram,
            "fgc": n_fgc,
            "rodalies": n_rodalies,
            "paradas": n_paradas,
        }

        total = n_lineas + n_paradas
        await _fin(eid, total, "ok")
        logger.info(
            "GTFS ATM OK — tram=%d, fgc=%d, rodalies=%d, paradas=%d, relaciones=%d",
            n_tram, n_fgc, n_rodalies, n_paradas, n_rels,
        )
        return resultado

    except Exception as exc:
        logger.error("Pipeline gtfs_atm ERROR: %s", exc, exc_info=True)
        await _fin(eid, 0, "error", str(exc))
        raise
