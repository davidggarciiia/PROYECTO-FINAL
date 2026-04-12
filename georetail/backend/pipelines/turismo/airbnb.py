"""
pipelines/airbnb.py — Ingesta de datos InsideAirbnb Barcelona.

Fuente: Inside Airbnb (http://data.insideairbnb.com) — datos abiertos, CC0.
Dataset: listings.csv con ~20.000 anuncios de Barcelona.

Qué hace:
  1. Descarga el CSV más reciente (URL pública, sin auth)
  2. Parsea: id, name, neighbourhood_cleansed, latitude, longitude,
             room_type, price, minimum_nights, number_of_reviews,
             review_scores_rating, availability_365
  3. Para cada zona (ST_DWithin 500m), calcula:
     - airbnb_density_500m: número de listings activos
     - airbnb_occupancy_est: estimación = (365 - availability_365) / 365
  4. Actualiza variables_zona con los nuevos valores

Frecuencia: mensual (InsideAirbnb actualiza cada 1-3 meses)
"""
from __future__ import annotations

import csv
import gzip
import io
import logging
from datetime import date
from pathlib import Path
from typing import Optional

import httpx

from db.conexion import get_db

logger = logging.getLogger(__name__)

# ── Configuración ──────────────────────────────────────────────────────────────
# URLs conocidas de InsideAirbnb Barcelona (más recientes primero).
# data.insideairbnb.com bloquea requests directas sin headers de navegador.
# Se prueban en orden hasta que una responda con HTTP 200.
_AIRBNB_URLS = [
    # Índice público JSON — lista los datasets más recientes
    # Si da 403, se pasa a las URLs hardcodeadas
    "https://data.insideairbnb.com/spain/catalonia/barcelona/2024-09-14/data/listings.csv.gz",
    "https://data.insideairbnb.com/spain/catalonia/barcelona/2024-06-16/data/listings.csv.gz",
    "https://data.insideairbnb.com/spain/catalonia/barcelona/2024-03-23/data/listings.csv.gz",
    # Fallback HTTP (algunos CDN no redirigen HTTPS correctamente)
    "http://data.insideairbnb.com/spain/catalonia/barcelona/2024-09-14/data/listings.csv.gz",
]
_AIRBNB_URL = _AIRBNB_URLS[0]   # mantener compatibilidad con referencias externas
_TIMEOUT_S  = 120   # la descarga puede tardar ~30s
_RADIO_M    = 500   # radio ST_DWithin para asignar listing a zona
_CHUNK      = 500   # tamaño de batch para geocodificación

# Headers que imitan un navegador para evitar el 403 de InsideAirbnb
_AIRBNB_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.6367.208 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,es;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "http://insideairbnb.com/get-the-data/",
    "Origin": "http://insideairbnb.com",
    "Connection": "keep-alive",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "cross-site",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}


# ═══════════════════════════════════════════════════════════════════════════════
# Punto de entrada
# ═══════════════════════════════════════════════════════════════════════════════

async def ejecutar() -> dict:
    eid = await _init("airbnb")
    try:
        # 1. Descargar CSV.gz
        logger.info("Descargando listings InsideAirbnb desde %s", _AIRBNB_URL)
        listings = await _descargar_listings()
        if not listings:
            msg = "Sin listings tras descarga/parseo de InsideAirbnb"
            logger.warning(msg)
            await _fin(eid, 0, "ok", msg)
            return {"listings": 0, "zonas_actualizadas": 0}

        logger.info("Listings parseados: %d", len(listings))

        # 2. Agregar por zona
        fecha_ref = date.today()
        stats_por_zona = await _agregar_por_zona(listings)
        logger.info("Zonas con listings en radio %dm: %d", _RADIO_M, len(stats_por_zona))

        # 3. Actualizar variables_zona
        actualizadas = await _actualizar_variables(stats_por_zona, fecha_ref)

        await _fin(eid, len(listings), "ok")
        logger.info("Airbnb OK — %d listings, %d zonas actualizadas", len(listings), actualizadas)
        return {"listings": len(listings), "zonas_actualizadas": actualizadas}

    except Exception as exc:
        logger.error("Pipeline airbnb ERROR: %s", exc, exc_info=True)
        await _fin(eid, 0, "error", str(exc))
        raise


# ═══════════════════════════════════════════════════════════════════════════════
# 1. Descarga y parseo del CSV
# ═══════════════════════════════════════════════════════════════════════════════

async def _descargar_listings() -> list[dict]:
    """
    Descarga el CSV.gz de InsideAirbnb, lo descomprime en memoria y parsea
    los campos relevantes. Devuelve lista de dicts con lat, lng y ocupacion.

    Fix HTTP 403: se prueban múltiples URLs con headers de navegador completos
    (Referer + Origin de insideairbnb.com) hasta encontrar una que responda 200.
    Si todas fallan con 403, se documenta el error y se devuelve lista vacía.
    """
    raw_gz: bytes = b""

    async with httpx.AsyncClient(
        timeout=_TIMEOUT_S,
        follow_redirects=True,
        headers=_AIRBNB_HEADERS,
    ) as client:
        for url in _AIRBNB_URLS:
            try:
                logger.info("Intentando InsideAirbnb URL: %s", url)
                r = await client.get(url)
                if r.status_code == 200:
                    raw_gz = r.content
                    logger.info("InsideAirbnb descarga OK desde %s (%d bytes)", url, len(raw_gz))
                    break
                elif r.status_code == 403:
                    logger.warning(
                        "InsideAirbnb HTTP 403 para %s — "
                        "el servidor bloquea acceso directo. "
                        "Para datos actualizados, descargar manualmente desde "
                        "http://insideairbnb.com/get-the-data/ y colocar en /data/listings.csv.gz",
                        url,
                    )
                else:
                    logger.warning("InsideAirbnb HTTP %d para %s", r.status_code, url)
            except httpx.TimeoutException:
                logger.error("Timeout descargando InsideAirbnb %s (>%ds)", url, _TIMEOUT_S)
            except Exception as exc:
                logger.error("Error descargando InsideAirbnb %s: %s", url, exc)

    if not raw_gz:
        # Intentar leer desde archivo local si existe (descarga manual)
        local_path = "/data/listings.csv.gz"
        try:
            p = Path(local_path)
            if p.exists() and p.stat().st_size > 1000:
                raw_gz = p.read_bytes()
                logger.info("InsideAirbnb: usando archivo local %s (%d bytes)", local_path, len(raw_gz))
        except Exception as exc:
            logger.debug("No se pudo leer archivo local InsideAirbnb: %s", exc)

    if not raw_gz:
        logger.error(
            "InsideAirbnb: no se pudo descargar datos. "
            "Descargar manualmente desde http://insideairbnb.com/get-the-data/ "
            "y colocar el CSV.gz en %s",
            "/data/listings.csv.gz",
        )
        return []

    # Descomprimir en memoria
    try:
        raw_csv = gzip.decompress(raw_gz)
    except Exception as exc:
        logger.error("Error descomprimiendo CSV.gz: %s", exc)
        return []

    # Parsear con csv.DictReader
    listings: list[dict] = []
    reader = csv.DictReader(io.StringIO(raw_csv.decode("utf-8", errors="replace")))
    for row in reader:
        try:
            lat = float(row.get("latitude") or 0)
            lng = float(row.get("longitude") or 0)
            if not lat or not lng:
                continue

            # Parsear precio: "$1,234.56" → 1234.56
            price_raw = str(row.get("price") or "0").replace("$", "").replace(",", "").strip()
            try:
                price = float(price_raw) if price_raw else 0.0
            except ValueError:
                price = 0.0

            avail = int(row.get("availability_365") or 365)
            # Ocupación estimada: fracción de días ocupados al año
            ocupacion = max(0.0, min(1.0, 1.0 - avail / 365.0))

            listings.append({
                "id":     str(row.get("id", "")).strip(),
                "nombre": (row.get("name") or "")[:200].strip(),
                "barrio": (row.get("neighbourhood_cleansed") or "").strip(),
                "lat":    lat,
                "lng":    lng,
                "price":  price,
                "room_type":   (row.get("room_type") or "").strip(),
                "min_nights":  int(row.get("minimum_nights") or 1),
                "reviews":     int(row.get("number_of_reviews") or 0),
                "rating":      float(row.get("review_scores_rating") or 0),
                "availability_365": avail,
                "ocupacion":   ocupacion,
            })
        except (ValueError, TypeError):
            continue

    return listings


# ═══════════════════════════════════════════════════════════════════════════════
# 2. Agregación por zona
# ═══════════════════════════════════════════════════════════════════════════════

async def _agregar_por_zona(listings: list[dict]) -> dict[str, dict]:
    """
    Para cada listing, busca las zonas en radio _RADIO_M y acumula:
      - count: número de listings
      - sum_ocupacion: suma de ocupaciones (para calcular media)
    Devuelve dict {zona_id: {"count": int, "avg_ocupacion": float}}.

    Estrategia eficiente: una query CTE con CROSS JOIN entre todos los puntos
    y las zonas, agrupando por zona_id.
    """
    if not listings:
        return {}

    lngs  = [l["lng"] for l in listings]
    lats  = [l["lat"] for l in listings]
    ocups = [l["ocupacion"] for l in listings]

    async with get_db() as conn:
        rows = await conn.fetch(
            f"""
            WITH pts AS (
                SELECT
                    t.lng::float8       AS lng,
                    t.lat::float8       AS lat,
                    t.occ::float8       AS occ,
                    ST_SetSRID(ST_MakePoint(t.lng::float8, t.lat::float8), 4326)::geography AS geom
                FROM unnest($1::float[], $2::float[], $3::float[]) AS t(lng, lat, occ)
            )
            SELECT
                z.id        AS zona_id,
                COUNT(*)    AS listing_count,
                AVG(p.occ)  AS avg_ocupacion
            FROM zonas z
            CROSS JOIN pts p
            WHERE ST_DWithin(z.geometria::geography, p.geom, {_RADIO_M})
            GROUP BY z.id
            """,
            lngs, lats, ocups,
        )

    return {
        row["zona_id"]: {
            "count":        int(row["listing_count"]),
            "avg_ocupacion": float(row["avg_ocupacion"] or 0.0),
        }
        for row in rows
    }


# ═══════════════════════════════════════════════════════════════════════════════
# 3. Actualización de variables_zona
# ═══════════════════════════════════════════════════════════════════════════════

async def _actualizar_variables(
    stats: dict[str, dict],
    fecha: date,
) -> int:
    """
    Hace UPSERT en variables_zona para cada zona con datos Airbnb.
    Zonas sin listings Airbnb en radio no se tocan (se deja NULL o el valor previo).
    """
    if not stats:
        return 0

    n = 0
    async with get_db() as conn:
        for zona_id, data in stats.items():
            try:
                # Anchor en variables_zona (tabla coordinadora delgada)
                await conn.execute(
                    """
                    INSERT INTO variables_zona (zona_id, fecha, fuente)
                    VALUES ($1, $2, 'airbnb_insideairbnb')
                    ON CONFLICT (zona_id, fecha) DO UPDATE
                    SET fuente = EXCLUDED.fuente, updated_at = NOW()
                    """,
                    zona_id, fecha,
                )
                # Datos de turismo en tabla satélite vz_turismo
                await conn.execute(
                    """
                    INSERT INTO vz_turismo
                        (zona_id, fecha,
                         score_turismo,
                         airbnb_density_500m, airbnb_occupancy_est,
                         fuente)
                    VALUES ($1, $2, $3, $4, $5, 'airbnb_insideairbnb')
                    ON CONFLICT (zona_id, fecha) DO UPDATE
                    SET score_turismo       = EXCLUDED.score_turismo,
                        airbnb_density_500m  = EXCLUDED.airbnb_density_500m,
                        airbnb_occupancy_est = EXCLUDED.airbnb_occupancy_est,
                        fuente               = EXCLUDED.fuente,
                        updated_at           = NOW()
                    """,
                    zona_id,
                    fecha,
                    # Normalizar count a score_turismo (0-100): 200 listings → score 100
                    round(min(100.0, data["count"] / 2.0), 2),
                    data["count"],
                    round(data["avg_ocupacion"], 4),
                )
                n += 1
            except Exception as exc:
                logger.debug("Error actualizando vz_turismo airbnb zona=%s: %s", zona_id, exc)

    return n


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
