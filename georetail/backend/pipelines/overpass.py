"""
pipelines/overpass.py — Carga de negocios activos desde OpenStreetMap Overpass API.

Reemplaza Google Places para poblar negocios_activos y calcular competencia
por zona. No requiere API key — usa la API pública de Overpass.

Estrategia de consulta:
  - Divide Barcelona en una malla de tiles de ~1km² (aprox. 9×7 tiles)
  - Consulta cada tile por separado para evitar timeouts y rate limits
  - Espera 1.5s entre tiles para respetar rate limits de la API pública
  - Solo consulta amenity/shop tags relevantes para nuestros sectores

Mapeado OSM → sector_codigo:
  amenity=restaurant/fast_food/bar/pub → restauracion
  shop=clothes/shoes/fashion           → moda
  amenity=pharmacy/dentist             → salud
  amenity=beauty/hairdresser           → estetica
  shop=supermarket/bakery              → alimentacion
  amenity=gym/leisure=fitness_centre   → deporte
  amenity=school/university            → educacion
  shop=* (general)                     → servicios

Tabla destino: negocios_activos (fuente='overpass')
"""
from __future__ import annotations

import asyncio
import logging
import os
from datetime import date
from hashlib import md5
from typing import Optional

import httpx

from db.conexion import get_db

logger = logging.getLogger(__name__)

# ── Configuración ──────────────────────────────────────────────────────────────
_OVERPASS_URLS = [
    "https://overpass-api.de/api/interpreter",
    "https://lz4.overpass-api.de/api/interpreter",
    "https://z.overpass-api.de/api/interpreter",
]
_TIMEOUT_S   = 90
_SLEEP_TILE  = 1.5    # segundos entre tiles (rate limit)
_SLEEP_RETRY = 10.0   # segundos al recibir 429 o 504

# Bounding box de Barcelona
_BCN_MIN_LAT, _BCN_MAX_LAT = 41.32, 41.47
_BCN_MIN_LNG, _BCN_MAX_LNG = 2.07, 2.23

# Tamaño de cada tile (en grados)
_TILE_LAT = 0.018   # ~2km
_TILE_LNG = 0.022   # ~2km

# ── Mapeado OSM tags → sector_codigo ───────────────────────────────────────────
_AMENITY_MAP: dict[str, str] = {
    "restaurant":    "restauracion",
    "fast_food":     "restauracion",
    "bar":           "restauracion",
    "pub":           "restauracion",
    "cafe":          "restauracion",
    "food_court":    "restauracion",
    "pharmacy":      "salud",
    "dentist":       "salud",
    "clinic":        "salud",
    "doctors":       "salud",
    "beauty":        "estetica",
    "hairdresser":   "estetica",
    "gym":           "deporte",
    "school":        "educacion",
    "university":    "educacion",
    "college":       "educacion",
    "kindergarten":  "educacion",
}
_SHOP_MAP: dict[str, str] = {
    "clothes":       "moda",
    "shoes":         "moda",
    "fashion":       "moda",
    "boutique":      "moda",
    "jewelry":       "moda",
    "accessories":   "moda",
    "supermarket":   "alimentacion",
    "bakery":        "alimentacion",
    "butcher":       "alimentacion",
    "greengrocer":   "alimentacion",
    "deli":          "alimentacion",
    "fishmonger":    "alimentacion",
    "convenience":   "alimentacion",
    "alcohol":       "alimentacion",
    "tattoo":        "tatuajes",
    "cosmetics":     "estetica",
    "beauty":        "estetica",
    "optician":      "salud",
    "sports":        "deporte",
    "outdoor":       "deporte",
    "fitness":       "deporte",
    "stationery":    "servicios",
    "electronics":   "servicios",
    "hardware":      "servicios",
    "furniture":     "servicios",
    "florist":       "servicios",
    "laundry":       "servicios",
    "dry_cleaning":  "servicios",
    "travel_agency": "servicios",
}
_LEISURE_MAP: dict[str, str] = {
    "fitness_centre": "deporte",
    "sports_centre":  "deporte",
    "swimming_pool":  "deporte",
}


# ═══════════════════════════════════════════════════════════════════════════════
# Punto de entrada
# ═══════════════════════════════════════════════════════════════════════════════

async def ejecutar() -> dict:
    eid = await _init("overpass")
    try:
        # Borrar datos previos de esta fuente
        async with get_db() as conn:
            res = await conn.execute(
                "DELETE FROM negocios_activos WHERE fuente='overpass'"
            )
            logger.info("Registros previos eliminados: %s", res)

        total_insertados = 0
        tiles = _generar_tiles()
        logger.info("Tiles a procesar: %d", len(tiles))

        url_idx = 0
        for i, (min_lat, min_lng, max_lat, max_lng) in enumerate(tiles):
            try:
                elementos = await _consultar_tile(
                    min_lat, min_lng, max_lat, max_lng,
                    _OVERPASS_URLS[url_idx % len(_OVERPASS_URLS)],
                )
                if elementos:
                    n = await _procesar_e_insertar(elementos)
                    total_insertados += n
                    if n > 0:
                        logger.debug(
                            "Tile %d/%d [%.3f,%.3f]: %d negocios",
                            i + 1, len(tiles), min_lat, min_lng, n,
                        )
            except RateLimitError:
                url_idx += 1
                logger.warning("Rate limit — cambiando a mirror %d, esperando %.0fs",
                               url_idx % len(_OVERPASS_URLS), _SLEEP_RETRY)
                await asyncio.sleep(_SLEEP_RETRY)
                # Reintentar el tile
                try:
                    elementos = await _consultar_tile(
                        min_lat, min_lng, max_lat, max_lng,
                        _OVERPASS_URLS[url_idx % len(_OVERPASS_URLS)],
                    )
                    if elementos:
                        total_insertados += await _procesar_e_insertar(elementos)
                except Exception as e2:
                    logger.warning("Tile reintento fallido: %s", e2)
            except Exception as exc:
                logger.debug("Tile %d error: %s", i + 1, exc)

            await asyncio.sleep(_SLEEP_TILE)

        await _fin(eid, total_insertados, "ok")
        logger.info("Overpass OK — %d negocios insertados", total_insertados)
        return {"insertados": total_insertados}

    except Exception as exc:
        logger.error("Pipeline overpass ERROR: %s", exc, exc_info=True)
        await _fin(eid, 0, "error", str(exc))
        raise


# ═══════════════════════════════════════════════════════════════════════════════
# Generación de tiles
# ═══════════════════════════════════════════════════════════════════════════════

def _generar_tiles() -> list[tuple[float, float, float, float]]:
    """Genera la malla de tiles para cubrir Barcelona completa."""
    tiles = []
    lat = _BCN_MIN_LAT
    while lat < _BCN_MAX_LAT:
        lng = _BCN_MIN_LNG
        while lng < _BCN_MAX_LNG:
            tiles.append((
                round(lat, 6),
                round(lng, 6),
                round(min(lat + _TILE_LAT, _BCN_MAX_LAT), 6),
                round(min(lng + _TILE_LNG, _BCN_MAX_LNG), 6),
            ))
            lng += _TILE_LNG
        lat += _TILE_LAT
    return tiles


# ═══════════════════════════════════════════════════════════════════════════════
# Consulta a Overpass
# ═══════════════════════════════════════════════════════════════════════════════

class RateLimitError(Exception):
    pass


async def _consultar_tile(
    min_lat: float, min_lng: float, max_lat: float, max_lng: float,
    url: str,
) -> list[dict]:
    """Consulta los negocios de un tile de la malla."""
    bbox = f"{min_lat},{min_lng},{max_lat},{max_lng}"
    # Consultar amenity y shop en un solo request
    query = f"""[out:json][timeout:{_TIMEOUT_S - 10}];
(
  node["amenity"~"restaurant|fast_food|bar|pub|cafe|pharmacy|dentist|clinic|doctors|beauty|hairdresser|gym|school|university|college|kindergarten"]({bbox});
  node["shop"~"clothes|shoes|fashion|jewelry|accessories|supermarket|bakery|butcher|greengrocer|deli|fishmonger|convenience|alcohol|tattoo|cosmetics|optician|sports|outdoor|florist|laundry|dry_cleaning|travel_agency"]({bbox});
  node["leisure"~"fitness_centre|sports_centre"]({bbox});
);
out center;"""

    async with httpx.AsyncClient(timeout=_TIMEOUT_S) as c:
        try:
            r = await c.post(url, data={"data": query})
        except httpx.TimeoutException:
            return []

    if r.status_code == 429:
        raise RateLimitError("429 Too Many Requests")
    if r.status_code == 504:
        return []  # Tile timeout — skip silently
    if r.status_code != 200:
        logger.debug("Overpass HTTP %d para bbox %s", r.status_code, bbox)
        return []

    try:
        return r.json().get("elements", [])
    except Exception:
        return []


# ═══════════════════════════════════════════════════════════════════════════════
# Procesado e inserción
# ═══════════════════════════════════════════════════════════════════════════════

def _sector_desde_tags(tags: dict) -> Optional[str]:
    amenity = tags.get("amenity", "")
    shop    = tags.get("shop", "")
    leisure = tags.get("leisure", "")

    if amenity in _AMENITY_MAP:
        return _AMENITY_MAP[amenity]
    if shop in _SHOP_MAP:
        return _SHOP_MAP[shop]
    if leisure in _LEISURE_MAP:
        return _LEISURE_MAP[leisure]
    return None


async def _procesar_e_insertar(elementos: list[dict]) -> int:
    """
    Procesa elementos OSM, los geocodifica a zona_id y los inserta en negocios_activos.
    """
    # Filtrar y preparar registros
    registros = []
    for el in elementos:
        tags   = el.get("tags", {})
        sector = _sector_desde_tags(tags)
        if not sector:
            continue

        lat = el.get("lat") or (el.get("center") or {}).get("lat")
        lng = el.get("lon") or (el.get("center") or {}).get("lon")
        if not lat or not lng:
            continue

        osm_id = str(el.get("id", ""))
        osm_type = el.get("type", "node")[0]  # n/w/r
        negocio_id = f"osm_{osm_type}{osm_id}"

        nombre = (
            tags.get("name")
            or tags.get("name:es")
            or tags.get("name:ca")
            or f"Negocio OSM {osm_id}"
        )

        registros.append({
            "id":     negocio_id,
            "nombre": nombre[:200],
            "sector": sector,
            "lat":    lat,
            "lng":    lng,
        })

    if not registros:
        return 0

    # Geocodificar a zona_id en batch
    registros = await _geocodificar_lote(registros)

    # Insertar
    n = 0
    async with get_db() as conn:
        for r in registros:
            if not r.get("zona_id"):
                continue
            try:
                await conn.execute(
                    """
                    INSERT INTO negocios_activos
                        (id, nombre, sector_codigo, lat, lng, geometria,
                         zona_id, activo, fuente, updated_at)
                    VALUES (
                        $1, $2, $3, $4, $5,
                        ST_SetSRID(ST_MakePoint($5, $4), 4326),
                        $6, true, 'overpass', NOW()
                    )
                    ON CONFLICT (id) DO UPDATE
                    SET nombre=EXCLUDED.nombre, sector_codigo=EXCLUDED.sector_codigo,
                        zona_id=EXCLUDED.zona_id, updated_at=NOW()
                    """,
                    r["id"], r["nombre"], r["sector"],
                    r["lat"], r["lng"], r["zona_id"],
                )
                n += 1
            except Exception as exc:
                logger.debug("Insert error %s: %s", r["id"], exc)
    return n


async def _geocodificar_lote(registros: list[dict]) -> list[dict]:
    """
    Asigna zona_id a cada negocio con una query batch (ST_Within).
    Fallback: ST_DWithin 50m para los que no caen dentro de ninguna zona.
    """
    if not registros:
        return registros

    _CHUNK = 200

    async with get_db() as conn:
        # Paso 1: ST_Within
        for i in range(0, len(registros), _CHUNK):
            chunk = registros[i : i + _CHUNK]
            values_sql = ", ".join(
                f"(${j*2+1}::float8, ${j*2+2}::float8, {j})"
                for j in range(len(chunk))
            )
            params = []
            for r in chunk:
                params.extend([r["lng"], r["lat"]])

            rows = await conn.fetch(
                f"""
                SELECT pts.idx, z.id AS zona_id
                FROM (VALUES {values_sql}) AS pts(lng, lat, idx)
                JOIN zonas z ON ST_Within(
                    ST_SetSRID(ST_MakePoint(pts.lng, pts.lat), 4326),
                    z.geometria
                )
                """,
                *params,
            )
            for row in rows:
                chunk[row["idx"]]["zona_id"] = row["zona_id"]

        # Paso 2: fallback ST_DWithin 50m
        pendientes_idx = [i for i, r in enumerate(registros) if not r.get("zona_id")]
        if pendientes_idx:
            pendientes = [registros[i] for i in pendientes_idx]
            for i in range(0, len(pendientes), _CHUNK):
                chunk = pendientes[i : i + _CHUNK]
                values_sql = ", ".join(
                    f"(${j*2+1}::float8, ${j*2+2}::float8, {j})"
                    for j in range(len(chunk))
                )
                params = []
                for r in chunk:
                    params.extend([r["lng"], r["lat"]])

                rows = await conn.fetch(
                    f"""
                    SELECT DISTINCT ON (pts.idx) pts.idx, z.id AS zona_id
                    FROM (VALUES {values_sql}) AS pts(lng, lat, idx)
                    JOIN zonas z ON ST_DWithin(
                        z.geometria::geography,
                        ST_SetSRID(ST_MakePoint(pts.lng, pts.lat), 4326)::geography,
                        50
                    )
                    ORDER BY pts.idx,
                        ST_Distance(
                            z.geometria::geography,
                            ST_SetSRID(ST_MakePoint(pts.lng, pts.lat), 4326)::geography
                        )
                    """,
                    *params,
                )
                for row in rows:
                    chunk[row["idx"]]["zona_id"] = row["zona_id"]

    return registros


# ═══════════════════════════════════════════════════════════════════════════════
# Helpers BD
# ═══════════════════════════════════════════════════════════════════════════════

async def _init(pipeline: str) -> int:
    async with get_db() as conn:
        return await conn.fetchval(
            "INSERT INTO pipeline_ejecuciones (pipeline, estado) "
            "VALUES ($1,'running') RETURNING id",
            pipeline,
        )


async def _fin(
    eid: int, registros: int, estado: str, mensaje: Optional[str] = None
) -> None:
    async with get_db() as conn:
        await conn.execute(
            "UPDATE pipeline_ejecuciones "
            "SET fecha_fin=NOW(), registros=$1, estado=$2, mensaje_error=$3 "
            "WHERE id=$4",
            registros, estado, mensaje, eid,
        )
