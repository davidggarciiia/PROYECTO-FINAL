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
     - airbnb_listings_500m: número de listings activos
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
from typing import Optional

import httpx

from db.conexion import get_db

logger = logging.getLogger(__name__)

# ── Configuración ──────────────────────────────────────────────────────────────
_AIRBNB_URL = (
    "http://data.insideairbnb.com/spain/catalonia/barcelona/"
    "2024-09-14/data/listings.csv.gz"
)
_TIMEOUT_S  = 120   # la descarga puede tardar ~30s
_RADIO_M    = 500   # radio ST_DWithin para asignar listing a zona
_CHUNK      = 500   # tamaño de batch para geocodificación


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
    """
    try:
        async with httpx.AsyncClient(timeout=_TIMEOUT_S, follow_redirects=True) as client:
            r = await client.get(_AIRBNB_URL)
            if r.status_code != 200:
                logger.error("InsideAirbnb HTTP %d", r.status_code)
                return []
            raw_gz = r.content
    except httpx.TimeoutException:
        logger.error("Timeout descargando InsideAirbnb (>%ds)", _TIMEOUT_S)
        return []
    except Exception as exc:
        logger.error("Error descargando InsideAirbnb: %s", exc)
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
            await conn.execute(
                """
                INSERT INTO variables_zona
                    (zona_id, fecha, score_turismo, fuente)
                VALUES ($1, $2, $3, 'airbnb_insideairbnb')
                ON CONFLICT (zona_id, fecha) DO UPDATE
                SET score_turismo = EXCLUDED.score_turismo,
                    fuente = EXCLUDED.fuente
                """,
                zona_id,
                fecha,
                # Normalizar count a score_turismo (0-100) como proxy turístico
                # también guardamos airbnb_listings_500m y airbnb_occupancy_est
                # en columnas adicionales si existen, o lo dejamos en score_turismo
                round(min(100.0, data["count"] / 2.0), 2),  # 200 listings → score 100
            )
            # Actualizar columnas específicas de airbnb si existen en el schema
            try:
                await conn.execute(
                    """
                    UPDATE variables_zona
                    SET airbnb_listings_500m  = $1,
                        airbnb_occupancy_est  = $2
                    WHERE zona_id = $3 AND fecha = $4
                    """,
                    data["count"],
                    round(data["avg_ocupacion"], 4),
                    zona_id,
                    fecha,
                )
            except Exception:
                # Columnas no existen aún en el schema — silenciar y continuar
                pass
            n += 1

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
