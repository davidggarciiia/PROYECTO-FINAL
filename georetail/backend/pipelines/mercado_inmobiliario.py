"""
pipelines/mercado_inmobiliario.py — Pipeline de locales en alquiler desde Habitaclia.

FLUJO:
  1. HabitacliaScraper.scrape()           → list[dict] del listado (sin páginas de detalle)
  2. _persistir_en_inmuebles_portales()   → INSERT en inmuebles_portales (staging)
  3. _sincronizar_a_locales()             → staging → tabla locales (lo ve el frontend)
  4. _limpiar_seed()                      → oculta locales hardcodeados en zonas cubiertas
  5. _actualizar_precios_zona()           → recalcula medianas en precios_alquiler_zona

SCHEMAS REALES (verificados contra las migraciones SQL):

  locales (001_schema_inicial.sql + 005_habitaclia_locales.sql):
    id VARCHAR(50) PK, zona_id VARCHAR(20), direccion TEXT,
    lat FLOAT NOT NULL, lng FLOAT NOT NULL,
    geometria GEOMETRY(POINT, 4326),
    m2 FLOAT, planta VARCHAR(10), escaparate_ml FLOAT,
    referencia_catastral VARCHAR(30), alquiler_mensual FLOAT,
    disponible BOOLEAN, fuente VARCHAR(30),
    url TEXT, titulo TEXT, descripcion TEXT,   ← añadidas por migración 005
    created_at TIMESTAMPTZ, updated_at TIMESTAMPTZ

  inmuebles_portales (004_inmuebles_portales.sql):
    portal_id TEXT UNIQUE, fuente TEXT, url TEXT, tipo_operacion TEXT,
    precio NUMERIC(12,2), precio_m2 NUMERIC(10,2), superficie_util NUMERIC(10,2),
    titulo TEXT, direccion TEXT, barrio TEXT, distrito TEXT,
    lat DOUBLE PRECISION, lon DOUBLE PRECISION, escaparate BOOLEAN, ...

  precios_alquiler_zona (001_schema_inicial.sql):
    zona_id, fecha, precio_m2, precio_min, precio_max, num_muestras, fuente
    UNIQUE (zona_id, fecha, fuente)
    — SIN columna 'tipo' —

  zonas (001_schema_inicial.sql):
    id VARCHAR(20) PK, nombre TEXT, barrio_id INT → barrios(id),
    geometria GEOMETRY(POLYGON, 4326)

  barrios (001_schema_inicial.sql):
    id SERIAL PK, codigo VARCHAR(6), nombre TEXT, distrito_id INT,
    geometria GEOMETRY(MULTIPOLYGON, 4326)
"""
from __future__ import annotations

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
    Scraping de Habitaclia (solo listados) + sincronización a tabla locales.

    Args:
        max_paginas: 107 = cobertura total BCN (~1.600 locales).
                     Para prueba rápida usa 5 (~75 locales, ~2 min).
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
        logger.warning("Habitaclia: sin resultados — ¿bloqueado?")
        return stats

    # PASO 2: Persistencia en staging ──────────────────────────────────────────
    ids_conocidos = await _cargar_ids_conocidos()
    nuevos        = await _persistir_en_inmuebles_portales(anuncios_raw, ids_conocidos)
    stats["guardados_portales"] = nuevos
    logger.info("Habitaclia: %d nuevos en inmuebles_portales", nuevos)

    # PASO 3: Sincronizar staging → tabla locales ──────────────────────────────
    sincronizados = await _sincronizar_a_locales()
    stats["sincronizados_locales"] = sincronizados
    logger.info("Habitaclia: %d locales sincronizados en tabla locales", sincronizados)

    # PASO 4: Ocultar seed en zonas cubiertas ──────────────────────────────────
    ocultos = await _limpiar_seed()
    stats["seed_ocultados"] = ocultos

    # PASO 5: Recalcular precios por zona ──────────────────────────────────────
    if sincronizados > 0:
        try:
            n_precios = await _actualizar_precios_zona()
            logger.info("Precios actualizados en %d zonas", n_precios)
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
    return await ejecutar_habitaclia(max_paginas=max_paginas or 107)


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


# ── PASO 3: Sincronización a tabla locales ────────────────────────────────────

async def _sincronizar_a_locales() -> int:
    """
    Lee inmuebles_portales (Habitaclia) y escribe en tabla locales.

    Puntos importantes del schema real:
      - locales.lat y locales.lng son FLOAT NOT NULL → usamos centroide de la zona
      - locales.id es VARCHAR(50) tras migración 005 → truncamos a 50 chars
      - locales.url y locales.titulo existen tras migración 005
      - ST_MakePoint(lng, lat) — PostGIS usa (X=lng, Y=lat)
    """
    mapa_zonas = await _cargar_mapa_zonas()
    logger.info("Mapa zonas: %d entradas disponibles", len(mapa_zonas))

    if not mapa_zonas:
        logger.error("Mapa de zonas vacío — ¿están cargados los datos de barrios?")
        return 0

    async with get_db() as conn:
        rows = await conn.fetch("""
            SELECT portal_id, precio, precio_m2, superficie_util AS m2,
                   barrio, distrito, escaparate, url, titulo
            FROM inmuebles_portales
            WHERE fuente          = 'habitaclia'
              AND tipo_operacion  = 'alquiler-locales'
              AND precio IS NOT NULL
              AND precio BETWEEN 100 AND 100000
            ORDER BY fecha_scraping DESC
        """)

    logger.info("Staging Habitaclia: %d anuncios candidatos", len(rows))

    sincronizados = 0
    sin_zona = 0

    async with get_db() as conn:
        for row in rows:
            barrio_texto = (row["barrio"] or row["distrito"] or "").strip()
            zona_info    = _resolver_zona(barrio_texto, mapa_zonas)

            if not zona_info:
                sin_zona += 1
                logger.debug("Sin zona para barrio: '%s'", barrio_texto)
                continue

            zona_id, lat, lng = zona_info

            # ID máx. 50 chars (schema tras migración 005)
            local_id = row["portal_id"][:50]

            try:
                await conn.execute("""
                    INSERT INTO locales (
                        id, zona_id, direccion,
                        lat, lng, geometria,
                        m2, planta, escaparate_ml,
                        alquiler_mensual, disponible, fuente,
                        url, titulo
                    ) VALUES (
                        $1, $2, $3,
                        $4, $5, ST_SetSRID(ST_MakePoint($5, $4), 4326),
                        $6, 'PB', $7,
                        $8, TRUE, 'habitaclia',
                        $9, $10
                    )
                    ON CONFLICT (id) DO UPDATE SET
                        alquiler_mensual = EXCLUDED.alquiler_mensual,
                        m2               = COALESCE(EXCLUDED.m2, locales.m2),
                        disponible       = TRUE,
                        fuente           = 'habitaclia',
                        url              = COALESCE(EXCLUDED.url, locales.url),
                        titulo           = COALESCE(EXCLUDED.titulo, locales.titulo),
                        updated_at       = NOW()
                """,
                    local_id,
                    zona_id,
                    barrio_texto,
                    lat,                                           # lat (Y)
                    lng,                                           # lng (X)
                    float(row["m2"]) if row["m2"] else None,
                    1.0 if row["escaparate"] else None,            # escaparate_ml
                    float(row["precio"]),
                    row["url"] or "",
                    row["titulo"] or "",
                )
                sincronizados += 1
            except Exception as exc:
                logger.debug("Error insertando local %s: %s", local_id, exc)

    logger.info(
        "Sincronización: %d escritos, %d sin zona asignada",
        sincronizados, sin_zona,
    )
    if sin_zona > 0:
        logger.warning(
            "%d anuncios sin zona — revisa el mapa de barrios o el parser de URLs",
            sin_zona,
        )
    return sincronizados


async def _cargar_mapa_zonas() -> dict[str, tuple[str, float, float]]:
    """
    Devuelve: barrio_nombre_lower → (zona_id, lat_centroide, lng_centroide)

    Hace JOIN barrios → zonas usando los polígonos de geometría para obtener
    el centroide de cada zona. Esto da coordenadas reales para locales.lat/lng,
    que son NOT NULL en el schema.
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
            WHERE z.geometria IS NOT NULL
            ORDER BY b.nombre, z.id
        """)

    return {
        row["barrio_nombre"].lower().strip(): (
            row["zona_id"],
            float(row["lat"]),
            float(row["lng"]),
        )
        for row in rows
        if row["lat"] is not None and row["lng"] is not None
    }


def _resolver_zona(
    barrio_texto: str,
    mapa_zonas: dict[str, tuple[str, float, float]],
) -> Optional[tuple[str, float, float]]:
    """
    Convierte texto libre de barrio (Habitaclia) a (zona_id, lat, lng).

    El mapa_zonas tiene los nombres EXACTOS del seed_demo.sql, que son:
      'el raval', 'el gòtic', 'la barceloneta', 'el born',
      'esquerra eixample', 'dreta eixample', 'sagrada família', 'fort pienc',
      'sants', 'hostafrancs', 'la bordeta',
      'les corts', 'la maternitat',
      'sant gervasi', 'sarrià',
      'vila de gràcia', 'el camp de gràcia', 'la salut',
      'el guinardó', 'la vall d'hebron',
      'nou barris nord', 'prosperitat',
      'sant andreu', 'la sagrera',
      'el poblenou', 'el clot', 'el parc', 'la vila olímpica'

    Estrategia en 4 capas:
      1. Match exacto
      2. Match por contenido (texto ⊆ clave o clave ⊆ texto)
      3. Match por palabras clave significativas (≥4 chars)
      4. Fallback explícito: términos de Habitaclia → nombre exacto en el mapa
    """
    if not barrio_texto:
        return None

    texto = barrio_texto.lower().strip()

    # 1. Match exacto
    if texto in mapa_zonas:
        return mapa_zonas[texto]

    # 2. Contenido bidireccional
    for key, val in mapa_zonas.items():
        if texto in key or key in texto:
            return val

    # 3. Match por palabras significativas (≥4 chars)
    palabras = [p for p in re.split(r"[\s\-'·]+", texto) if len(p) >= 4]
    for key, val in mapa_zonas.items():
        if any(p in key for p in palabras):
            return val

    # 4. Fallback calibrado contra los nombres exactos del seed_demo.sql.
    #    Clave: término que puede venir de Habitaclia (slug de URL o texto libre)
    #    Valor: nombre exacto del barrio en la BD (lower, sin artículos iniciales)
    #
    #    Nombres en BD (28 barrios):
    #      el raval · el gòtic · la barceloneta · el born
    #      esquerra eixample · dreta eixample · sagrada família · fort pienc
    #      sants · hostafrancs · la bordeta
    #      les corts · la maternitat
    #      sant gervasi · sarrià
    #      vila de gràcia · el camp de gràcia · la salut
    #      el guinardó · la vall d'hebron
    #      nou barris nord · prosperitat
    #      sant andreu · la sagrera
    #      el poblenou · el clot · el parc · la vila olímpica
    _FALLBACK: dict[str, str] = {
        # ── Eixample ─────────────────────────────────────────────────────────
        "nova esquerra":         "esquerra eixample",
        "antiga esquerra":       "esquerra eixample",
        "esquerra de l eixample": "esquerra eixample",
        "esquerra eixample":     "esquerra eixample",
        "esquerra":              "esquerra eixample",
        "dreta de l eixample":   "dreta eixample",
        "dreta eixample":        "dreta eixample",
        "dreta":                 "dreta eixample",
        "sagrada familia":       "sagrada família",
        "sagrada família":       "sagrada família",
        "sagrada":               "sagrada família",
        "fort pienc":            "fort pienc",
        "sant antoni":           "esquerra eixample",  # Sant Antoni pertenece a Esquerra
        "eixample":              "esquerra eixample",  # genérico → Esquerra (más grande)
        # ── Gràcia ───────────────────────────────────────────────────────────
        "vila de gracia":        "vila de gràcia",
        "vila de gràcia":        "vila de gràcia",
        "vila de":               "vila de gràcia",
        "camp de gracia":        "el camp de gràcia",
        "camp de gràcia":        "el camp de gràcia",
        "la salut":              "la salut",
        "salut":                 "la salut",
        "gracia":                "vila de gràcia",
        "gràcia":                "vila de gràcia",
        # ── Sarrià-Sant Gervasi ───────────────────────────────────────────────
        "sant gervasi galvany":  "sant gervasi",
        "sant gervasi bonanova": "sant gervasi",
        "sant gervasi":          "sant gervasi",
        "gervasi":               "sant gervasi",
        "sarria":                "sarrià",
        "sarrià":                "sarrià",
        "pedralbes":             "sarrià",
        "tres torres":           "sarrià",
        "vallvidrera":           "sarrià",
        # ── Ciutat Vella ──────────────────────────────────────────────────────
        "el raval":              "el raval",
        "raval":                 "el raval",
        "barri gotic":           "el gòtic",
        "barri gòtic":           "el gòtic",
        "barrio gotico":         "el gòtic",
        "gotic":                 "el gòtic",
        "gòtic":                 "el gòtic",
        "gothic":                "el gòtic",
        "barceloneta":           "la barceloneta",
        "el born":               "el born",
        "born":                  "el born",
        "sant pere":             "el born",           # Sant Pere → zona Born (más cercana)
        "santa caterina":        "el born",
        "sant pere santa caterina el born": "el born",
        # ── Sants-Montjuïc ────────────────────────────────────────────────────
        "sants":                 "sants",
        "hostafrancs":           "hostafrancs",
        "la bordeta":            "la bordeta",
        "bordeta":               "la bordeta",
        "poble sec":             "sants",             # Poble Sec no tiene zona propia → Sants
        "montjuic":              "sants",
        # ── Les Corts ─────────────────────────────────────────────────────────
        "les corts":             "les corts",
        "maternitat":            "la maternitat",
        "la maternitat":         "la maternitat",
        # ── Horta-Guinardó ────────────────────────────────────────────────────
        "el guinardo":           "el guinardó",
        "guinardo":              "el guinardó",
        "el guinardó":           "el guinardó",
        "horta":                 "el guinardó",       # Horta → zona Guinardó (más próxima)
        "carmel":                "el guinardó",
        "el carmel":             "el guinardó",
        # ── Nou Barris ────────────────────────────────────────────────────────
        "nou barris":            "nou barris nord",
        "prosperitat":           "prosperitat",
        "trinitat":              "nou barris nord",
        "roquetes":              "nou barris nord",
        "verdun":                "nou barris nord",
        "porta":                 "nou barris nord",
        # ── Sant Andreu ───────────────────────────────────────────────────────
        "sant andreu":           "sant andreu",
        "la sagrera":            "la sagrera",
        "sagrera":               "la sagrera",
        "bon pastor":            "sant andreu",
        "navas":                 "sant andreu",
        # ── Sant Martí ────────────────────────────────────────────────────────
        "el poblenou":           "el poblenou",
        "poblenou":              "el poblenou",
        "el clot":               "el clot",
        "clot":                  "el clot",
        "vila olimpica":         "la vila olímpica",
        "vila olímpica":         "la vila olímpica",
        "la vila olimpica":      "la vila olímpica",
        "el parc":               "el parc",
        "parc":                  "el parc",
        "diagonal mar":          "el poblenou",
        "rambla del poblenou":   "el poblenou",
    }

    for keyword, barrio_bd in _FALLBACK.items():
        if keyword in texto:
            # Búsqueda exacta primero
            if barrio_bd in mapa_zonas:
                return mapa_zonas[barrio_bd]
            # Búsqueda parcial (el nombre en BD puede tener artículo: "el raval")
            for key, val in mapa_zonas.items():
                if barrio_bd in key or key in barrio_bd:
                    return val

    return None


# ── PASO 4: Limpiar seed ──────────────────────────────────────────────────────

async def _limpiar_seed() -> int:
    """
    Oculta locales del seed hardcodeado en zonas ya cubiertas por Habitaclia.
    Los IDs de seed empiezan por 'loc_' (según seed_demo.sql).
    """
    async with get_db() as conn:
        result = await conn.execute("""
            UPDATE locales seed_loc
            SET disponible = FALSE
            WHERE seed_loc.id LIKE 'loc_%'
              AND EXISTS (
                  SELECT 1 FROM locales real_loc
                  WHERE real_loc.zona_id   = seed_loc.zona_id
                    AND real_loc.fuente    = 'habitaclia'
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


# ── PASO 5: Actualizar precios por zona ───────────────────────────────────────

async def _actualizar_precios_zona() -> int:
    """
    Recalcula medianas de precio/m² por zona en precios_alquiler_zona.

    Schema verificado (001_schema_inicial.sql):
      zona_id, fecha, precio_m2, precio_min, precio_max, num_muestras, fuente
      UNIQUE (zona_id, fecha, fuente)
      — NO tiene columna 'tipo' —

    Solo incluye locales con m2 > 0 para evitar divisiones por cero.
    Mínimo 2 muestras por zona para que la mediana sea significativa.
    """
    async with get_db() as conn:
        resultado = await conn.execute("""
            INSERT INTO precios_alquiler_zona
                (zona_id, fecha, precio_m2, precio_min, precio_max, num_muestras, fuente)
            SELECT
                l.zona_id,
                CURRENT_DATE AS fecha,
                PERCENTILE_CONT(0.5) WITHIN GROUP
                    (ORDER BY l.alquiler_mensual / l.m2)::float  AS precio_m2,
                MIN(l.alquiler_mensual / l.m2)::float            AS precio_min,
                MAX(l.alquiler_mensual / l.m2)::float            AS precio_max,
                COUNT(*)::int                                     AS num_muestras,
                'habitaclia'                                      AS fuente
            FROM locales l
            WHERE l.fuente          = 'habitaclia'
              AND l.disponible      = TRUE
              AND l.alquiler_mensual IS NOT NULL
              AND l.m2 IS NOT NULL
              AND l.m2 > 0
              AND l.zona_id IS NOT NULL
            GROUP BY l.zona_id
            HAVING COUNT(*) >= 2
            ON CONFLICT (zona_id, fecha, fuente) DO UPDATE SET
                precio_m2    = EXCLUDED.precio_m2,
                precio_min   = EXCLUDED.precio_min,
                precio_max   = EXCLUDED.precio_max,
                num_muestras = EXCLUDED.num_muestras
        """)
    try:
        return int(resultado.split()[-1])
    except (IndexError, ValueError):
        return 0


# ── Configuración ─────────────────────────────────────────────────────────────

def _build_scraping_config(cfg) -> ScrapingConfig:
    return ScrapingConfig(
        service         = getattr(cfg, "SCRAPING_SERVICE", "none"),
        scrapingbee_key = getattr(cfg, "SCRAPINGBEE_API_KEY", ""),
        zenrows_key     = getattr(cfg, "ZENROWS_API_KEY", ""),
        proxies_raw     = getattr(cfg, "SCRAPING_PROXIES", ""),
        delay_min       = float(getattr(cfg, "SCRAPING_DELAY_MIN", 1.5)),
        delay_max       = float(getattr(cfg, "SCRAPING_DELAY_MAX", 4.0)),
        timeout         = int(getattr(cfg, "SCRAPING_TIMEOUT", 25)),
        max_retries     = 1,   # fijo a 1: fallar rápido es mejor que esperar 75s
    )
