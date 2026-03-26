"""
pipelines/mercado_inmobiliario.py — Pipeline de locales en alquiler desde Habitaclia.

FLUJO:
  1. HabitacliaScraper.scrape()         → list[dict] con precio, m2, barrio, url
  2. _persistir_en_inmuebles_portales() → INSERT en inmuebles_portales (staging)
  3. _sincronizar_a_locales()           → Lee staging, asigna zona_id por barrio,
                                          escribe en tabla locales (lo ve el frontend)
  4. _limpiar_seed()                    → Oculta locales hardcodeados en zonas cubiertas
  5. _actualizar_precios_zona()         → Recalcula medianas en precios_alquiler_zona

Puntos de entrada:
  ejecutar_habitaclia(max_paginas)  — lanzado por el script poblar_locales_habitaclia.py
  ejecutar(modo, ...)               — compatibilidad con el scheduler y api/admin.py
"""
from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime, timezone
from typing import Optional

from config import get_settings
from db.conexion import get_db
from pipelines.scraping.habitaclia_scraper import HabitacliaScraper
from pipelines.scraping.base_scraper import ScrapingConfig
from pipelines.scraping.models import InmueblePortal, desde_dict_scraper

logger = logging.getLogger(__name__)


# ── Punto de entrada principal ────────────────────────────────────────────────

async def ejecutar_habitaclia(max_paginas: int = 107) -> dict:
    """
    Scraping completo de Habitaclia + sincronización a la tabla locales.

    Args:
        max_paginas: páginas del listado (107 = cobertura total BCN ~1.600 locales).
                     Para prueba rápida usa 5 (~75 locales, ~10 min).
    """
    cfg          = get_settings()
    scraping_cfg = _build_scraping_config(cfg)

    logger.info("Pipeline Habitaclia — max_paginas=%d", max_paginas)
    stats = {
        "scrapeados": 0, "guardados_portales": 0,
        "sincronizados_locales": 0, "seed_ocultados": 0, "errores": 0,
    }

    # PASO 1: Scraping ─────────────────────────────────────────────────────────
    try:
        scraper      = HabitacliaScraper(config=scraping_cfg)
        anuncios_raw = await scraper.scrape(ciudad="barcelona", max_paginas=max_paginas)
        stats["scrapeados"] = len(anuncios_raw)
        logger.info("Habitaclia: %d anuncios scrapeados", len(anuncios_raw))
    except Exception as exc:
        logger.error("Error en HabitacliaScraper: %s", exc, exc_info=True)
        stats["errores"] += 1
        return stats

    if not anuncios_raw:
        logger.warning("Habitaclia: sin resultados")
        return stats

    # PASO 2: Staging en inmuebles_portales ────────────────────────────────────
    ids_conocidos = await _cargar_ids_conocidos()
    nuevos        = await _persistir_en_inmuebles_portales(anuncios_raw, ids_conocidos)
    stats["guardados_portales"] = nuevos
    logger.info("Habitaclia: %d nuevos en inmuebles_portales", nuevos)

    # PASO 3: Sincronizar staging → locales (tabla que usa el frontend) ────────
    sincronizados = await _sincronizar_a_locales()
    stats["sincronizados_locales"] = sincronizados
    logger.info("Habitaclia: %d locales sincronizados en tabla locales", sincronizados)

    # PASO 4: Ocultar seed en zonas ya cubiertas ───────────────────────────────
    ocultos = await _limpiar_seed()
    stats["seed_ocultados"] = ocultos

    # PASO 5: Recalcular precios por zona ──────────────────────────────────────
    if sincronizados > 0:
        try:
            await _actualizar_precios_zona()
        except Exception as exc:
            logger.error("Error en _actualizar_precios_zona: %s", exc)

    logger.info("Pipeline Habitaclia completado: %s", stats)
    return stats


async def ejecutar(
    modo: str = "locales_alquiler",
    zonas: Optional[list[str]] = None,
    max_paginas: Optional[int] = None,
    portales: Optional[list[str]] = None,
) -> dict:
    """Compatibilidad con el scheduler y api/admin.py."""
    if modo in ("locales_alquiler", "locales_venta", "viviendas"):
        return await ejecutar_habitaclia(max_paginas=max_paginas or 107)
    return {"scrapeados": 0, "guardados_portales": 0, "sincronizados_locales": 0}


# ── PASO 2: Persistencia en staging ──────────────────────────────────────────

async def _cargar_ids_conocidos() -> set[str]:
    async with get_db() as conn:
        rows = await conn.fetch("SELECT portal_id FROM inmuebles_portales")
    return {r["portal_id"] for r in rows}


async def _persistir_en_inmuebles_portales(
    anuncios_raw: list[dict],
    ids_conocidos: set[str],
) -> int:
    nuevos = 0
    for raw in anuncios_raw:
        raw.setdefault("tipo_operacion", "alquiler-locales")
        inm = desde_dict_scraper(raw)
        if not inm or inm.portal_id in ids_conocidos:
            continue
        try:
            await _upsert_inmueble(inm)
            ids_conocidos.add(inm.portal_id)
            nuevos += 1
        except Exception as exc:
            logger.debug("Error guardando %s: %s", inm.portal_id, exc)
    return nuevos


async def _upsert_inmueble(inm: InmueblePortal) -> None:
    async with get_db() as conn:
        await conn.execute("""
            INSERT INTO inmuebles_portales (
                portal_id, fuente, url, tipo_operacion, tipo_inmueble,
                precio, precio_m2, precio_anterior,
                superficie_util, superficie_construida,
                titulo, direccion, barrio, distrito, codigo_postal,
                lat, lon,
                anyo_construccion, estado_conservacion, certificado_energia,
                escaparate, esquina, sotano, altura_libre_m, potencia_kw,
                dias_publicado, es_particular, agente_nombre,
                num_fotos, etiquetas, fecha_scraping
            ) VALUES (
                $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,
                $11,$12,$13,$14,$15,$16,$17,
                $18,$19,$20,$21,$22,$23,$24,$25,
                $26,$27,$28,$29,$30,$31
            )
            ON CONFLICT (portal_id) DO UPDATE SET
                precio         = EXCLUDED.precio,
                precio_m2      = EXCLUDED.precio_m2,
                fecha_scraping = EXCLUDED.fecha_scraping
        """,
            inm.portal_id, inm.fuente, inm.url, inm.tipo_operacion, inm.tipo_inmueble,
            inm.precio, inm.precio_m2, inm.precio_anterior,
            inm.superficie_util, inm.superficie_construida,
            inm.titulo, inm.direccion, inm.barrio, inm.distrito, inm.codigo_postal,
            inm.lat, inm.lon,
            inm.anyo_construccion, inm.estado_conservacion, inm.certificado_energia,
            inm.escaparate, inm.esquina, inm.sotano, inm.altura_libre_m, inm.potencia_kw,
            inm.dias_publicado, inm.es_particular, inm.agente_nombre,
            inm.num_fotos, inm.etiquetas, datetime.now(timezone.utc),
        )


# ── PASO 3: Sincronización a la tabla locales ─────────────────────────────────

async def _sincronizar_a_locales() -> int:
    """
    Lee inmuebles_portales (Habitaclia) y escribe en la tabla locales.

    La tabla locales tiene estas columnas según 001_schema_inicial.sql:
      id, zona_id, direccion, lat, lng, geometria,
      m2, planta, escaparate_ml, referencia_catastral,
      alquiler_mensual, disponible, fuente, created_at, updated_at

    Habitaclia NO devuelve lat/lng exactos, solo el nombre del barrio.
    Usamos el centroide de la zona asignada como coordenadas del local.
    Esto es suficiente — el frontend usa las coords de la zona para el mapa.
    """
    mapa_barrios = await _cargar_mapa_barrios()
    logger.info("Mapa barrios: %d entradas disponibles", len(mapa_barrios))

    async with get_db() as conn:
        rows = await conn.fetch("""
            SELECT portal_id, precio, precio_m2, superficie_util AS m2,
                   barrio, distrito, escaparate
            FROM inmuebles_portales
            WHERE fuente = 'habitaclia'
              AND tipo_operacion = 'alquiler-locales'
              AND precio IS NOT NULL
              AND precio BETWEEN 100 AND 100000
            ORDER BY fecha_scraping DESC
        """)

    logger.info("Staging Habitaclia: %d anuncios a sincronizar", len(rows))

    sincronizados = 0
    sin_zona      = 0

    async with get_db() as conn:
        for row in rows:
            barrio_texto = (row["barrio"] or row["distrito"] or "").strip()
            zona_info    = _resolver_zona(barrio_texto, mapa_barrios)

            if not zona_info:
                sin_zona += 1
                continue

            zona_id, lat, lng = zona_info

            # ID del local: portal_id truncado a 30 chars (tamaño de la columna)
            local_id = row["portal_id"][:30]

            try:
                await conn.execute("""
                    INSERT INTO locales (
                        id, zona_id, direccion,
                        lat, lng, geometria,
                        m2, planta, escaparate_ml,
                        alquiler_mensual, disponible, fuente
                    ) VALUES (
                        $1, $2, $3,
                        $4, $5, ST_SetSRID(ST_MakePoint($5, $4), 4326),
                        $6, 'PB', $7,
                        $8, TRUE, 'habitaclia'
                    )
                    ON CONFLICT (id) DO UPDATE SET
                        alquiler_mensual = EXCLUDED.alquiler_mensual,
                        m2               = COALESCE(EXCLUDED.m2, locales.m2),
                        disponible       = TRUE,
                        fuente           = 'habitaclia',
                        updated_at       = NOW()
                """,
                    local_id,
                    zona_id,
                    barrio_texto,
                    lat, lng,
                    float(row["m2"]) if row["m2"] else None,
                    1.0 if row["escaparate"] else None,
                    float(row["precio"]),
                )
                sincronizados += 1
            except Exception as exc:
                logger.debug("Error insertando local %s: %s", local_id, exc)

    logger.info(
        "Sincronización: %d escritos, %d sin zona asignada",
        sincronizados, sin_zona,
    )
    return sincronizados


async def _cargar_mapa_barrios() -> dict[str, tuple[str, float, float]]:
    """
    Devuelve: barrio_nombre_lower → (zona_id, lat_centroide, lng_centroide)
    """
    async with get_db() as conn:
        rows = await conn.fetch("""
            SELECT DISTINCT ON (b.nombre)
                b.nombre                       AS barrio_nombre,
                z.id                           AS zona_id,
                ST_Y(ST_Centroid(z.geometria)) AS lat,
                ST_X(ST_Centroid(z.geometria)) AS lng
            FROM barrios b
            JOIN zonas z ON z.barrio_id = b.id
            ORDER BY b.nombre, z.id
        """)

    return {
        row["barrio_nombre"].lower().strip(): (row["zona_id"], row["lat"], row["lng"])
        for row in rows
    }


def _resolver_zona(
    barrio_texto: str,
    mapa_barrios: dict[str, tuple[str, float, float]],
) -> Optional[tuple[str, float, float]]:
    """
    Convierte el texto libre de barrio de Habitaclia a (zona_id, lat, lng).

    Ejemplos de texto de Habitaclia:
      "Sant Gervasi - Galvany"        → busca "sant gervasi"
      "La Nova Esquerra de l'Eixample" → busca "eixample"
      "El Raval"                       → busca "raval"
    """
    if not barrio_texto:
        return None

    texto = barrio_texto.lower().strip()

    # 1. Match exacto
    if texto in mapa_barrios:
        return mapa_barrios[texto]

    # 2. Match parcial — el texto de Habitaclia está en el nombre del barrio o viceversa
    for key, val in mapa_barrios.items():
        if texto in key or key in texto:
            return val

    # 3. Match por palabras significativas (≥4 chars)
    palabras = [p for p in re.split(r"[\s\-']+", texto) if len(p) >= 4]
    for key, val in mapa_barrios.items():
        if any(p in key for p in palabras):
            return val

    # 4. Fallback por términos conocidos de Habitaclia → nombre de barrio en BD
    _FALLBACK = {
        "eixample":      "eixample",     "nova esquerra":  "eixample",
        "dreta":         "eixample",     "esquerra":       "eixample",
        "sagrada":       "eixample",     "fort pienc":     "eixample",
        "gracia":        "gràcia",       "gràcia":         "gràcia",
        "vila de":       "gràcia",
        "sant gervasi":  "sant gervasi", "gervasi":        "sant gervasi",
        "sarria":        "sarrià",       "sarrià":         "sarrià",
        "pedralbes":     "sarrià",       "tres torres":    "sarrià",
        "raval":         "raval",        "gotic":          "barri gòtic",
        "gothic":        "barri gòtic",  "gòtic":          "barri gòtic",
        "born":          "sant pere",    "barceloneta":    "barceloneta",
        "sant pere":     "sant pere",    "santa caterina": "sant pere",
        "poblenou":      "poblenou",     "clot":           "clot",
        "vila olimpica": "poblenou",     "provencals":     "poblenou",
        "sants":         "sants",        "hostafrancs":    "hostafrancs",
        "poble sec":     "sants",        "montjuic":       "sants",
        "les corts":     "les corts",    "maternitat":     "les corts",
        "horta":         "horta",        "guinardo":       "guinardó",
        "carmel":        "carmel",
        "nou barris":    "nou barris",   "prosperitat":    "prosperitat",
        "sant andreu":   "sant andreu",  "sagrera":        "sagrera",
        "navas":         "sant andreu",
    }
    for keyword, barrio_bd in _FALLBACK.items():
        if keyword in texto:
            if barrio_bd in mapa_barrios:
                return mapa_barrios[barrio_bd]
            # Búsqueda parcial del fallback en el mapa
            for key, val in mapa_barrios.items():
                if barrio_bd in key:
                    return val

    return None


# ── PASO 4 y 5: Limpieza y precios ───────────────────────────────────────────

async def _limpiar_seed() -> int:
    """Oculta los locales del seed hardcodeado en zonas ya cubiertas por Habitaclia."""
    async with get_db() as conn:
        result = await conn.execute("""
            UPDATE locales seed_loc
            SET disponible = FALSE
            WHERE seed_loc.id LIKE 'loc_%'
              AND EXISTS (
                  SELECT 1 FROM locales real_loc
                  WHERE real_loc.zona_id  = seed_loc.zona_id
                    AND real_loc.fuente   = 'habitaclia'
                    AND real_loc.disponible = TRUE
              )
        """)
    try:
        n = int(result.split()[-1])
        if n > 0:
            logger.info("Seed: %d locales hardcodeados ocultados", n)
        return n
    except (IndexError, ValueError):
        return 0


async def _actualizar_precios_zona() -> int:
    """Recalcula medianas de precio/m² por zona en precios_alquiler_zona."""
    async with get_db() as conn:
        resultado = await conn.execute("""
            INSERT INTO precios_alquiler_zona (zona_id, precio_m2, tipo, fecha, fuente, n_muestras)
            SELECT
                l.zona_id,
                PERCENTILE_CONT(0.5) WITHIN GROUP
                    (ORDER BY l.alquiler_mensual / NULLIF(l.m2, 0))::numeric(10,2),
                'alquiler',
                NOW(),
                'habitaclia',
                COUNT(*)::int
            FROM locales l
            WHERE l.fuente     = 'habitaclia'
              AND l.disponible = TRUE
              AND l.alquiler_mensual IS NOT NULL
              AND l.m2 IS NOT NULL AND l.m2 > 0
              AND l.zona_id IS NOT NULL
            GROUP BY l.zona_id
            HAVING COUNT(*) >= 2
            ON CONFLICT (zona_id, tipo, (DATE_TRUNC('day', fecha))) DO UPDATE SET
                precio_m2  = EXCLUDED.precio_m2,
                fecha      = EXCLUDED.fecha,
                n_muestras = EXCLUDED.n_muestras
        """)
    try:
        return int(resultado.split()[-1])
    except (IndexError, ValueError):
        return 0


def _build_scraping_config(cfg) -> ScrapingConfig:
    return ScrapingConfig(
        service        = getattr(cfg, "SCRAPING_SERVICE", "none"),
        scrapingbee_key= getattr(cfg, "SCRAPINGBEE_API_KEY", ""),
        zenrows_key    = getattr(cfg, "ZENROWS_API_KEY", ""),
        proxies_raw    = getattr(cfg, "SCRAPING_PROXIES", ""),
        delay_min      = float(getattr(cfg, "SCRAPING_DELAY_MIN", 1.5)),
        delay_max      = float(getattr(cfg, "SCRAPING_DELAY_MAX", 4.0)),
        timeout        = int(getattr(cfg, "SCRAPING_TIMEOUT", 25)),
        max_retries    = int(getattr(cfg, "SCRAPING_MAX_RETRIES", 3)),
    )
