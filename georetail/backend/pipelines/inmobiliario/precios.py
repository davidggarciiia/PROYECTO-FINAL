"""
pipelines/precios.py — Pipeline semanal de precios de alquiler comercial.

Fuentes (en orden de prioridad):
  1. Open Data BCN  (oficial)   — precio/m² mensual por barrio (sin API key)
  2. Idealista      (scraping)  — reemplaza la API oficial (curl_cffi anti-bot)
  3. Fotocasa       (scraping)  — locales en alquiler Barcelona/Madrid/Valencia
  4. Habitaclia     (scraping)  — locales en alquiler Barcelona/Madrid/Valencia
  5. Milanuncios    (scraping)  — anuncios particulares
  6. Pisos.com      (scraping)  — listados adicionales

Anti-bot para scrapers: ver pipelines/scraping/base_scraper.py
Configurar en .env:
  SCRAPING_SERVICE=none|scrapingbee|zenrows
  SCRAPINGBEE_API_KEY=...
  ZENROWS_API_KEY=...
  SCRAPING_PROXIES=http://user:pass@proxy:port,...

Tabla destino: precios_alquiler_zona + locales
"""
from __future__ import annotations

import logging
import os
import httpx
from db.conexion import get_db

logger = logging.getLogger(__name__)

# ── Open Data Barcelona ───────────────────────────────────────────────────────
# API key opcional — sin key funciona con límite ~1000 req/día por IP.
# Configurar OPEN_DATA_BCN_API_KEY en el .env para límite ~10.000 req/día.
# Registro gratuito: https://opendata-ajuntament.barcelona.cat → Mi cuenta → API Key
_CKAN_HEADERS: dict = (
    {"Authorization": os.environ.get("OPEN_DATA_BCN_API_KEY", "")}
    if os.environ.get("OPEN_DATA_BCN_API_KEY")
    else {}
)

# Ciudades a escanear con los scrapers
_CIUDADES_SCRAPING = ["barcelona"]  # Ampliar cuando haya datos de más ciudades en BD


async def ejecutar() -> dict:
    eid = await _init()
    ok = 0
    try:
        # 1. Open Data BCN (siempre disponible, sin API key)
        ok += await _precios_open_data()

        # 2. Scrapers de todos los portales (Idealista + Fotocasa + Habitaclia + ...)
        ok += await _locales_scrapers()

        await _fin(eid, ok, "ok")
        return {"registros": ok}
    except Exception as e:
        logger.error("Pipeline precios error: %s", e)
        await _fin(eid, ok, "error", str(e))
        raise


# ── 1. Open Data BCN ───────────────────────────────────────────────────────────

async def _precios_open_data() -> int:
    """
    Descarga precios de alquiler de locales comerciales del CKAN de BCN.
    Dataset: locllo-evolucio (evolución de precios de alquiler por barrio)
    """
    _CKAN_BASE = "https://opendata-ajuntament.barcelona.cat/data/api/action"
    ok = 0
    try:
        async with httpx.AsyncClient(timeout=30.0, headers=_CKAN_HEADERS) as c:
            sql = "SELECT * FROM \"locllo-evolucio\" ORDER BY \"Any\" DESC, \"Trimestre\" DESC LIMIT 1000"
            r = await c.get(f"{_CKAN_BASE}/datastore_search_sql", params={"sql": sql})
            r.raise_for_status()
            rows = r.json().get("result", {}).get("records", [])

        async with get_db() as conn:
            for row in rows:
                barrio_nom = row.get("Nom_Barri", "")
                precio_m2 = float(row.get("Index_Preu_Unitari", "0").replace(",", ".") or 0)
                any_ = int(row.get("Any", 2024))
                trim = int(row.get("Trimestre", "1") or 1)
                mes = (trim - 1) * 3 + 2
                fecha = f"{any_}-{mes:02d}-01"

                if not precio_m2:
                    continue

                zona_id = await conn.fetchval("""
                    SELECT z.id FROM zonas z
                    JOIN barrios b ON b.id = z.barrio_id
                    WHERE b.nombre ILIKE $1
                    LIMIT 1
                """, f"%{barrio_nom}%")

                if not zona_id:
                    continue

                await conn.execute("""
                    INSERT INTO precios_alquiler_zona (zona_id, fecha, precio_m2, fuente)
                    VALUES ($1, $2, $3, 'open_data_bcn')
                    ON CONFLICT (zona_id, fecha, fuente)
                    DO UPDATE SET precio_m2 = EXCLUDED.precio_m2
                """, zona_id, fecha, precio_m2)
                ok += 1

    except Exception as e:
        logger.warning("_precios_open_data error: %s", e)

    return ok


# ── 2. Scrapers (Idealista + todos los portales) ───────────────────────────────

async def _locales_scrapers() -> int:
    """
    Ejecuta los scrapers de Idealista, Fotocasa, Habitaclia, Milanuncios y Pisos.com.
    Idealista usa scraping en lugar de la API oficial (requería aprobación + pago).
    Los scrapers se ejecutan secuencialmente para no sobrecargar los portales.
    """
    from pipelines.inmobiliario.scraping import (
        IdealistaScraper,
        FotocasaScraper,
        HabitacliaScraper,
        MilanunciosScraper,
        PisosScraper,
        ScrapingConfig,
    )

    cfg = ScrapingConfig()
    ok = 0

    scrapers = [
        ("idealista",   IdealistaScraper(cfg)),
        ("fotocasa",    FotocasaScraper(cfg)),
        ("habitaclia",  HabitacliaScraper(cfg)),
        ("milanuncios", MilanunciosScraper(cfg)),
        ("pisos",       PisosScraper(cfg)),
    ]

    for nombre, scraper in scrapers:
        for ciudad in _CIUDADES_SCRAPING:
            try:
                logger.info("Iniciando scraper %s ciudad=%s", nombre, ciudad)
                locales = await scraper.scrape(ciudad=ciudad, max_paginas=3)
                if locales:
                    n = await _upsert_locales(locales, nombre)
                    ok += n
                    logger.info("Scraper %s ciudad=%s: %d locales insertados", nombre, ciudad, n)
            except Exception as e:
                logger.warning("Scraper %s ciudad=%s error: %s", nombre, ciudad, e)

    return ok


# ── Upsert de locales ──────────────────────────────────────────────────────────

async def _upsert_locales(items: list[dict], fuente: str) -> int:
    """Inserta o actualiza locales y actualiza precio/m² de la zona."""
    ok = 0
    async with get_db() as conn:
        for item in items:
            # Normalizar campos según si viene de Idealista o de un scraper
            if fuente == "idealista":
                lat = item.get("latitude")
                lng = item.get("longitude")
                precio = item.get("price")
                m2 = item.get("size")
                direccion = item.get("address", "")
                prop_id = f"idealista_{item.get('propertyCode', '')}"
            else:
                lat = item.get("lat")
                lng = item.get("lng")
                precio = item.get("precio")
                m2 = item.get("m2")
                direccion = item.get("direccion", "")
                prop_id = item.get("id") or f"{fuente}_unknown"

            if not lat or not lng:
                continue

            zona_id = await conn.fetchval("""
                SELECT id FROM zonas
                WHERE ST_Within(ST_SetSRID(ST_MakePoint($1, $2), 4326), geometria)
                LIMIT 1
            """, float(lng), float(lat))

            await conn.execute("""
                INSERT INTO locales (
                    id, zona_id, direccion, lat, lng,
                    geometria, m2, alquiler_mensual, esta_disponible, planta, fuente
                )
                VALUES (
                    $1, $2, $3, $4, $5,
                    ST_SetSRID(ST_MakePoint($5, $4), 4326),
                    $6, $7, TRUE, 'PB', $8
                )
                ON CONFLICT (id) DO UPDATE SET
                    alquiler_mensual  = EXCLUDED.alquiler_mensual,
                    esta_disponible   = TRUE,
                    updated_at        = NOW()
            """, prop_id, zona_id, direccion, float(lat), float(lng),
                float(m2) if m2 else None,
                float(precio) if precio else None,
                fuente)
            ok += 1

            # Actualizar precio/m² de la zona
            if zona_id and precio and m2 and float(m2) > 0:
                precio_m2 = round(float(precio) / float(m2), 2)
                await conn.execute("""
                    INSERT INTO precios_alquiler_zona (zona_id, fecha, precio_m2, num_muestras, fuente)
                    VALUES ($1, CURRENT_DATE, $2, 1, $3)
                    ON CONFLICT (zona_id, fecha, fuente) DO UPDATE SET
                        precio_m2 = (
                            precios_alquiler_zona.precio_m2 * precios_alquiler_zona.num_muestras
                            + EXCLUDED.precio_m2
                        ) / (precios_alquiler_zona.num_muestras + 1),
                        num_muestras = precios_alquiler_zona.num_muestras + 1
                """, zona_id, precio_m2, fuente)

    return ok


# ── Control de ejecución ───────────────────────────────────────────────────────

async def _init():
    async with get_db() as conn:
        return await conn.fetchval(
            "INSERT INTO pipeline_ejecuciones(pipeline, estado) VALUES('precios', 'running') RETURNING id"
        )


async def _fin(eid, reg, estado, msg=None):
    async with get_db() as conn:
        await conn.execute(
            "UPDATE pipeline_ejecuciones SET fecha_fin=NOW(), registros=$1, estado=$2, mensaje_error=$3 WHERE id=$4",
            reg, estado, msg, eid,
        )
