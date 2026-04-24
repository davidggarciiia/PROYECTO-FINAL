"""
pipelines/booking.py — Ingesta de alojamientos turísticos de Barcelona.

Fuentes (en orden de prioridad):
  1. HUT (Habitatges d'Ús Turístic) — Generalitat de Catalunya vía CKAN.
     Dataset público, sin autenticación, actualización mensual.
  2. OSM Overpass API — hotels, hostels, guest_houses en el bbox de BCN.
  3. Booking.com Affiliate Partner API (opcional) — si BOOKING_API_KEY está
     configurada. Enriquece con precio medio, rating y número de reseñas.

Qué actualiza:
  - Tabla: alojamientos_turisticos (INSERT … ON CONFLICT DO UPDATE)
  - Variables zona: booking_hoteles_500m, booking_rating_medio

Frecuencia: semanal jueves 03:00 (scheduler.py)
"""
from __future__ import annotations

import asyncio
import logging
from datetime import date
from typing import Optional

import httpx

from config import settings
from db.conexion import get_db

logger = logging.getLogger(__name__)

# ── Overpass ──────────────────────────────────────────────────────────────────
_OVERPASS_URLS = [
    "https://overpass-api.de/api/interpreter",
    "https://lz4.overpass-api.de/api/interpreter",
    "https://z.overpass-api.de/api/interpreter",
]
_BCN_BBOX  = "41.32,2.07,41.47,2.23"
_TIMEOUT_S = 45
_RADIO_M   = 500

# ── HUT Open Data BCN ─────────────────────────────────────────────────────────
_CKAN_BASE      = "https://opendata-ajuntament.barcelona.cat/data/api/action"
_RID_HUT        = "9a83a093-f88c-4e59-b11e-5b24b9f0d8a2"   # dataset HUT Ajuntament
_CKAN_LIMIT     = 1000
_CKAN_SLEEP     = 0.3


# ═══════════════════════════════════════════════════════════════════════════════
# Punto de entrada
# ═══════════════════════════════════════════════════════════════════════════════

async def ejecutar() -> dict:
    eid = await _init("booking")
    try:
        alojamientos: list[dict] = []

        # 1. OSM: hoteles, hostels, guest_houses
        osm_items = await _osm_alojamientos()
        alojamientos.extend(osm_items)
        logger.info("OSM: %d alojamientos", len(osm_items))

        # 2. HUT: apartamentos turísticos Generalitat
        hut_items = await _hut_alojamientos()
        alojamientos.extend(hut_items)
        logger.info("HUT: %d alojamientos", len(hut_items))

        # 3. Booking.com API (opcional)
        if _has_booking_key():
            booking_items = await _booking_api_alojamientos()
            alojamientos.extend(booking_items)
            logger.info("Booking API: %d alojamientos", len(booking_items))

        if not alojamientos:
            await _fin(eid, 0, "ok", "Sin alojamientos tras consulta de fuentes")
            return {"alojamientos": 0, "zonas_actualizadas": 0}

        # 4. Persistir en BD
        insertados = await _persistir(alojamientos)
        logger.info("Alojamientos persistidos: %d", insertados)

        # 5. Actualizar variables_zona
        zonas_actualizadas = await _actualizar_variables()
        logger.info("Zonas actualizadas con booking_hoteles_500m: %d", zonas_actualizadas)

        await _fin(eid, insertados, "ok")
        return {"alojamientos": insertados, "zonas_actualizadas": zonas_actualizadas}

    except Exception as exc:
        logger.error("Pipeline booking ERROR: %s", exc, exc_info=True)
        await _fin(eid, 0, "error", str(exc))
        raise


# ═══════════════════════════════════════════════════════════════════════════════
# 1. OSM Overpass — hoteles / hostels / pensiones
# ═══════════════════════════════════════════════════════════════════════════════

async def _osm_alojamientos() -> list[dict]:
    """Extrae alojamientos turísticos de OpenStreetMap vía Overpass."""
    query = f"""
    [out:json][timeout:{_TIMEOUT_S}];
    (
      node["tourism"="hotel"]({_BCN_BBOX});
      way["tourism"="hotel"]({_BCN_BBOX});
      node["tourism"="hostel"]({_BCN_BBOX});
      way["tourism"="hostel"]({_BCN_BBOX});
      node["tourism"="guest_house"]({_BCN_BBOX});
      way["tourism"="guest_house"]({_BCN_BBOX});
      node["tourism"="motel"]({_BCN_BBOX});
    );
    out center tags;
    """
    resultados: list[dict] = []

    # Overpass rechaza POST sin User-Agent con HTTP 406 Not Acceptable.
    headers = {"User-Agent": "GeoRetail/1.0 (barcelona; booking lodging)"}
    async with httpx.AsyncClient(timeout=_TIMEOUT_S, headers=headers) as client:
        for url in _OVERPASS_URLS:
            try:
                resp = await client.post(url, data={"data": query})
                if resp.status_code != 200:
                    continue
                data = resp.json()
                break
            except Exception as exc:
                logger.warning("Overpass %s falló: %s", url, exc)
                continue
        else:
            logger.error("Todos los servidores Overpass fallaron")
            return []

    for elem in data.get("elements", []):
        tags = elem.get("tags", {})
        # Coordenadas: nodes tienen lat/lon; ways tienen center
        if elem.get("type") == "node":
            lat = float(elem.get("lat", 0) or 0)
            lng = float(elem.get("lon", 0) or 0)
        else:
            center = elem.get("center", {})
            lat = float(center.get("lat", 0) or 0)
            lng = float(center.get("lon", 0) or 0)

        if not lat or not lng:
            continue

        tourism = tags.get("tourism", "hotel")
        tipo_map = {
            "hotel":       "hotel",
            "hostel":      "hostel",
            "guest_house": "hotel",
            "motel":       "hotel",
        }
        tipo = tipo_map.get(tourism, "hotel")

        estrellas_raw = tags.get("stars") or tags.get("tourism:stars")
        try:
            estrellas = int(estrellas_raw) if estrellas_raw else None
        except (ValueError, TypeError):
            estrellas = None

        nombre = tags.get("name") or tags.get("name:es") or tags.get("name:ca") or ""
        if not nombre:
            continue

        resultados.append({
            "nombre":      nombre[:300],
            "tipo":        tipo,
            "estrellas":   estrellas,
            "lat":         lat,
            "lng":         lng,
            "num_habitaciones": None,
            "precio_noche_medio": None,
            "rating":      None,
            "num_reviews": None,
            "booking_id":  None,
            "fuente":      "osm",
        })

    return resultados


# ═══════════════════════════════════════════════════════════════════════════════
# 2. HUT — Habitatges d'Ús Turístic (Generalitat / Open Data BCN)
# ═══════════════════════════════════════════════════════════════════════════════

async def _hut_alojamientos() -> list[dict]:
    """Descarga HUT del Open Data BCN (dataset apartamentos turísticos)."""
    resultados: list[dict] = []
    offset = 0

    async with httpx.AsyncClient(timeout=30) as client:
        while True:
            try:
                resp = await client.get(
                    f"{_CKAN_BASE}/datastore_search",
                    params={
                        "resource_id": _RID_HUT,
                        "limit":  _CKAN_LIMIT,
                        "offset": offset,
                    },
                )
                if resp.status_code != 200:
                    break
                body = resp.json()
            except Exception as exc:
                logger.warning("HUT CKAN error offset=%d: %s", offset, exc)
                break

            if not body.get("success"):
                break

            records = body.get("result", {}).get("records", [])
            if not records:
                break

            for rec in records:
                lat_raw = rec.get("latitud") or rec.get("lat") or rec.get("LATITUD")
                lng_raw = rec.get("longitud") or rec.get("lon") or rec.get("LONGITUD")
                try:
                    lat = float(str(lat_raw).replace(",", "."))
                    lng = float(str(lng_raw).replace(",", "."))
                except (TypeError, ValueError):
                    continue

                if not (-90 <= lat <= 90) or not (-180 <= lng <= 180):
                    continue

                nombre = (
                    rec.get("nom_local") or rec.get("nom") or
                    rec.get("NOM_LOCAL")  or "HUT"
                )[:300]

                resultados.append({
                    "nombre":      nombre,
                    "tipo":        "apartamento",
                    "estrellas":   None,
                    "lat":         lat,
                    "lng":         lng,
                    "num_habitaciones": None,
                    "precio_noche_medio": None,
                    "rating":      None,
                    "num_reviews": None,
                    "booking_id":  None,
                    "fuente":      "hut",
                })

            offset += _CKAN_LIMIT
            if len(records) < _CKAN_LIMIT:
                break
            await asyncio.sleep(_CKAN_SLEEP)

    return resultados


# ═══════════════════════════════════════════════════════════════════════════════
# 3. Booking.com Affiliate Partner API (opcional)
# ═══════════════════════════════════════════════════════════════════════════════

def _has_booking_key() -> bool:
    key = getattr(settings, "BOOKING_API_KEY", None)
    return bool(key and key.strip() and key.strip() not in {"...", "sk-..."})


async def _booking_api_alojamientos() -> list[dict]:
    """
    Booking.com Affiliate Partner API v3 — búsqueda de hoteles en Barcelona.
    Doc: https://developers.booking.com/affiliatepartner/docs

    Requiere BOOKING_API_KEY en .env (obtener en affiliate.booking.com).
    Si la key no está configurada, esta función no se llama (ver _has_booking_key).

    Fix HTTP 202 (JavaScript challenge): si el scraper curl_cffi devuelve vacío,
    intenta BookingScraper.scrape_barcelona_playwright() como fallback.
    """
    key = getattr(settings, "BOOKING_API_KEY", "")
    resultados: list[dict] = []

    # Intentar primero el BookingScraper (curl_cffi) y si falla, Playwright
    try:
        from pipelines.inmobiliario.scraping.booking_scraper import BookingScraper
        async with BookingScraper() as scraper:
            hoteles = await scraper.scrape_barcelona(max_pages=5)
            if not hoteles:
                logger.info(
                    "Booking curl_cffi sin resultados (posible JS challenge HTTP 202) "
                    "— intentando Playwright"
                )
                hoteles = await scraper.scrape_barcelona_playwright(max_pages=5)

            for h in hoteles:
                resultados.append({
                    "nombre":      h.nombre[:300],
                    "tipo":        h.tipo,
                    "estrellas":   h.estrellas,
                    "lat":         h.lat,
                    "lng":         h.lng,
                    "num_habitaciones": None,
                    "precio_noche_medio": h.precio_noche,
                    "rating":      h.rating,
                    "num_reviews": h.num_reviews,
                    "booking_id":  h.booking_id,
                    "fuente":      "booking_scraper",
                })
            if resultados:
                logger.info("BookingScraper: %d hoteles obtenidos", len(resultados))
                return resultados
    except Exception as exc:
        logger.warning("BookingScraper falló, continuando con API: %s", exc)

    # city_id de Barcelona en Booking.com: -372490
    params = {
        "city_ids": "-372490",
        "rows":     100,
        "offset":   0,
        "extras":   "hotel_info,hotel_facilities",
    }
    headers = {
        "Authorization": f"Basic {key}",
        "Content-Type":  "application/json",
    }

    async with httpx.AsyncClient(timeout=30) as client:
        while True:
            try:
                resp = await client.get(
                    "https://distribution-xml.booking.com/3.0/json/hotels",
                    params=params,
                    headers=headers,
                )
                if resp.status_code != 200:
                    logger.warning("Booking API HTTP %d", resp.status_code)
                    break
                data = resp.json()
            except Exception as exc:
                logger.warning("Booking API error: %s", exc)
                break

            hotels = data.get("result", [])
            if not hotels:
                break

            for h in hotels:
                location = h.get("location", {})
                try:
                    lat = float(location.get("latitude")  or 0)
                    lng = float(location.get("longitude") or 0)
                except (TypeError, ValueError):
                    continue
                if not lat or not lng:
                    continue

                estrellas_raw = h.get("class") or h.get("star_rating")
                try:
                    estrellas = int(float(str(estrellas_raw))) if estrellas_raw else None
                except (TypeError, ValueError):
                    estrellas = None

                resultados.append({
                    "nombre":      (h.get("name") or "")[:300],
                    "tipo":        "hotel",
                    "estrellas":   estrellas,
                    "lat":         lat,
                    "lng":         lng,
                    "num_habitaciones": h.get("number_of_rooms"),
                    "precio_noche_medio": None,
                    "rating":      h.get("review_score"),
                    "num_reviews": h.get("number_of_reviews"),
                    "booking_id":  str(h.get("hotel_id") or ""),
                    "fuente":      "booking_api",
                })

            if len(hotels) < params["rows"]:
                break
            params["offset"] += params["rows"]
            await asyncio.sleep(0.5)

    return resultados


# ═══════════════════════════════════════════════════════════════════════════════
# 4. Persistencia en BD
# ═══════════════════════════════════════════════════════════════════════════════

async def _persistir(alojamientos: list[dict]) -> int:
    """UPSERT en alojamientos_turisticos. Deduplica por (nombre, lat, lng)."""
    if not alojamientos:
        return 0

    n = 0
    async with get_db() as conn:
        for a in alojamientos:
            try:
                await conn.execute(
                    """
                    INSERT INTO alojamientos_turisticos
                        (nombre, tipo, estrellas, lat, lng, geometria,
                         num_habitaciones, precio_noche_medio, rating,
                         num_reviews, booking_id, fuente)
                    VALUES
                        ($1, $2, $3, $4, $5,
                         ST_SetSRID(ST_MakePoint($5, $4), 4326),
                         $6, $7, $8, $9, $10, $11)
                    ON CONFLICT DO NOTHING
                    """,
                    a["nombre"], a["tipo"], a["estrellas"],
                    a["lat"], a["lng"],
                    a["num_habitaciones"], a["precio_noche_medio"],
                    a["rating"], a["num_reviews"], a["booking_id"], a["fuente"],
                )
                n += 1
            except Exception as exc:
                logger.debug("Error insertando alojamiento '%s': %s", a["nombre"], exc)

    return n


# ═══════════════════════════════════════════════════════════════════════════════
# 5. Actualización de variables_zona
# ═══════════════════════════════════════════════════════════════════════════════

async def _actualizar_variables() -> int:
    """
    Para cada zona, cuenta hoteles en radio 500m y calcula rating medio.
    Actualiza booking_hoteles_500m y booking_rating_medio en variables_zona.
    """
    hoy = date.today()
    async with get_db() as conn:
        rows = await conn.fetch(
            f"""
            SELECT
                z.id AS zona_id,
                COUNT(a.id)::int      AS hoteles,
                AVG(a.rating)         AS rating_medio
            FROM zonas z
            LEFT JOIN alojamientos_turisticos a
                ON ST_DWithin(z.geometria::geography, a.geometria::geography, {_RADIO_M})
                AND a.es_activo = TRUE
            GROUP BY z.id
            """
        )

        n = 0
        for row in rows:
            if row["hoteles"] == 0:
                continue
            try:
                # Anchor en variables_zona (tabla coordinadora delgada)
                await conn.execute(
                    """
                    INSERT INTO variables_zona (zona_id, fecha, fuente)
                    VALUES ($1, $2, 'booking')
                    ON CONFLICT (zona_id, fecha) DO UPDATE
                    SET fuente = EXCLUDED.fuente, updated_at = NOW()
                    """,
                    row["zona_id"], hoy,
                )
                # Datos de turismo en tabla satélite vz_turismo
                await conn.execute(
                    """
                    INSERT INTO vz_turismo
                        (zona_id, fecha, booking_hoteles_500m, booking_rating_medio, fuente)
                    VALUES ($1, $2, $3, $4, 'booking')
                    ON CONFLICT (zona_id, fecha) DO UPDATE
                    SET booking_hoteles_500m = EXCLUDED.booking_hoteles_500m,
                        booking_rating_medio = EXCLUDED.booking_rating_medio,
                        fuente               = EXCLUDED.fuente,
                        updated_at           = NOW()
                    """,
                    row["zona_id"], hoy,
                    row["hoteles"],
                    round(float(row["rating_medio"]), 2) if row["rating_medio"] else None,
                )
                n += 1
            except Exception as exc:
                logger.debug("Error actualizando variables zona %s: %s", row["zona_id"], exc)

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
