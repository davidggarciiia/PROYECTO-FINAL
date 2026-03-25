"""
pipelines/mercado_inmobiliario.py — Pipeline unificado de datos de mercado inmobiliario.

FLUJO COMPLETO:
  1. HabitacliaScraper.scrape()        → lista de dicts (precio, m2, barrio, url...)
  2. _persistir_anuncios()             → INSERT en inmuebles_portales (staging)
  3. _sincronizar_locales_desde_portales() → Lee inmuebles_portales donde fuente='habitaclia'
                                             Asigna zona_id por fuzzy match de barrio
                                             INSERT/UPDATE en locales (lo que ve el frontend)
  4. limpiar_seed_si_hay_real()        → Elimina locales del seed en zonas ya cubiertas

Función pública principal:  ejecutar(modo, ...)     → pipeline completo
Función de Habitaclia:       ejecutar_habitaclia()   → solo Habitaclia + sincronización
"""
from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime, timezone
from typing import Optional

from config import get_settings
from db.conexion import get_db
from pipelines.scraping import (
    HabitacliaScraper,
    ScrapingConfig,
    desde_dict_scraper,
)
from pipelines.scraping.models import InmueblePortal

logger = logging.getLogger(__name__)


# ── Punto de entrada: solo Habitaclia (el que funciona) ───────────────────────

async def ejecutar_habitaclia(max_paginas: int = 107) -> dict:
    """
    Lanza el scraping de Habitaclia y sincroniza los resultados en la tabla locales.

    Args:
        max_paginas: páginas del listado de Habitaclia (107 = cobertura total BCN).
                     Cada página tiene ~15 URLs → ~1.600 locales en total.
                     Para prueba rápida usar max_paginas=5 (~75 locales).

    Returns:
        dict con estadísticas del proceso.
    """
    cfg = get_settings()
    scraping_cfg = _build_scraping_config(cfg)

    logger.info("Pipeline Habitaclia — max_paginas=%d", max_paginas)
    stats: dict = {
        "scrapeados": 0,
        "guardados_portales": 0,
        "sincronizados_locales": 0,
        "seed_eliminados": 0,
        "errores": 0,
    }

    # ── PASO 1: Scraping ──────────────────────────────────────────────────────
    try:
        scraper = HabitacliaScraper(config=scraping_cfg)
        anuncios_raw = await scraper.scrape(ciudad="barcelona", max_paginas=max_paginas)
        stats["scrapeados"] = len(anuncios_raw)
        logger.info("Habitaclia: %d anuncios scrapeados", len(anuncios_raw))
    except Exception as exc:
        logger.error("Error en HabitacliaScraper: %s", exc, exc_info=True)
        stats["errores"] += 1
        return stats

    if not anuncios_raw:
        logger.warning("Habitaclia: sin resultados — ¿está bloqueado?")
        return stats

    # ── PASO 2: Persistir en inmuebles_portales (staging) ────────────────────
    ids_conocidos = await _cargar_ids_conocidos()
    nuevos = await _persistir_anuncios(
        anuncios_raw=anuncios_raw,
        ids_conocidos=ids_conocidos,
        tipo_operacion="alquiler-locales",
    )
    stats["guardados_portales"] = nuevos
    logger.info("Habitaclia: %d nuevos guardados en inmuebles_portales", nuevos)

    # ── PASO 3: Sincronizar → tabla locales (lo que ve el frontend) ───────────
    sincronizados = await _sincronizar_locales_desde_portales(fuente="habitaclia")
    stats["sincronizados_locales"] = sincronizados
    logger.info("Habitaclia: %d locales sincronizados en tabla locales", sincronizados)

    # ── PASO 4: Limpiar seed donde ya hay datos reales ────────────────────────
    eliminados = await _limpiar_seed()
    stats["seed_eliminados"] = eliminados
    logger.info("Habitaclia: %d locales de seed eliminados (sustituidos por reales)", eliminados)

    # ── PASO 5: Actualizar medianas de precio por zona ────────────────────────
    if sincronizados > 0:
        try:
            await _actualizar_precios_zona()
        except Exception as exc:
            logger.error("Error actualizando precios_alquiler_zona: %s", exc)

    logger.info("Pipeline Habitaclia completado: %s", stats)
    return stats


# ── Punto de entrada genérico (mantiene compatibilidad con scheduler) ─────────

async def ejecutar(
    modo: str = "locales_alquiler",
    zonas: Optional[list[str]] = None,
    max_paginas: Optional[int] = None,
    portales: Optional[list[str]] = None,
) -> dict:
    """
    Pipeline genérico. Actualmente Habitaclia es la fuente activa.
    Idealista desactivado (bloquea). Fotocasa/Pisos inestables.
    """
    max_pag = max_paginas or 107
    if modo == "locales_alquiler":
        return await ejecutar_habitaclia(max_paginas=max_pag)
    # Para otros modos, devolver vacío (añadir fuentes según se activen)
    logger.info("Modo '%s' sin fuentes activas actualmente", modo)
    return {"scrapeados": 0, "guardados_portales": 0, "sincronizados_locales": 0}


# ── Sincronización inmuebles_portales → locales ───────────────────────────────

async def _sincronizar_locales_desde_portales(fuente: str = "habitaclia") -> int:
    """
    Lee inmuebles_portales donde fuente='{fuente}' y los escribe en la tabla locales,
    que es de donde el frontend lee precio, m² y dirección.

    Estrategia de asignación de zona_id:
      - Habitaclia devuelve el nombre del barrio en texto libre (ej: "Sant Gervasi - Galvany")
      - Hacemos fuzzy match contra la tabla barrios con ILIKE y palabras clave
      - Si encontramos el barrio, tomamos el centroide de la primera zona de ese barrio
        como coordenadas del local (aproximación razonable — no tenemos lat/lng exacto)
      - Si no encontramos barrio, saltamos el anuncio (no podemos asignarlo al mapa)

    Coordenadas: Habitaclia NO devuelve lat/lng exactos (solo barrio en texto).
    Usamos el centroide de la zona asignada como aproximación.
    El frontend usa las coordenadas de la zona para el mapa, no las del local,
    así que esto no afecta a la visualización.
    """
    # Cargar mapa barrio_texto → (zona_id, lat_centroide, lng_centroide)
    mapa_barrios = await _cargar_mapa_barrios()
    logger.info("Mapa barrios cargado: %d entradas", len(mapa_barrios))

    # Leer todos los inmuebles de Habitaclia desde el staging
    async with get_db() as conn:
        rows = await conn.fetch("""
            SELECT
                portal_id, url, titulo, precio, precio_m2,
                superficie_util AS m2,
                barrio, distrito,
                escaparate,
                fecha_scraping
            FROM inmuebles_portales
            WHERE fuente = $1
              AND tipo_operacion = 'alquiler-locales'
              AND precio IS NOT NULL
              AND precio BETWEEN 100 AND 100000
            ORDER BY fecha_scraping DESC
        """, fuente)

    logger.info("Habitaclia: %d anuncios en staging para sincronizar", len(rows))

    sincronizados = 0
    sin_zona = 0

    async with get_db() as conn:
        for row in rows:
            barrio_texto = row["barrio"] or row["distrito"] or ""
            zona_info = _resolver_zona(barrio_texto, mapa_barrios)

            if not zona_info:
                sin_zona += 1
                logger.debug("Sin zona para barrio '%s'", barrio_texto)
                continue

            zona_id, lat, lng = zona_info

            # ID del local: usar portal_id directamente (ya tiene prefijo habitaclia_)
            local_id = row["portal_id"]
            if len(local_id) > 50:
                local_id = local_id[:50]

            # Inferir planta desde título/descripción (Habitaclia suele mencionarlo)
            planta = _inferir_planta(row["titulo"] or "")

            # Metros lineales de escaparate — Habitaclia a veces lo indica
            escaparate_ml = 1.0 if row["escaparate"] else None

            try:
                await conn.execute("""
                    INSERT INTO locales (
                        id, zona_id, direccion, lat, lng, geometria,
                        m2, planta, escaparate_ml, alquiler_mensual,
                        disponible, fuente, url, titulo, updated_at
                    ) VALUES (
                        $1, $2, $3, $4, $5,
                        ST_SetSRID(ST_MakePoint($5, $4), 4326),
                        $6, $7, $8, $9,
                        TRUE, 'habitaclia', $10, $11, NOW()
                    )
                    ON CONFLICT (id) DO UPDATE SET
                        alquiler_mensual = EXCLUDED.alquiler_mensual,
                        m2               = EXCLUDED.m2,
                        disponible       = TRUE,
                        url              = EXCLUDED.url,
                        updated_at       = NOW()
                """,
                    local_id,
                    zona_id,
                    barrio_texto,   # dirección = barrio (lo mejor que tenemos)
                    lat,
                    lng,
                    row["m2"],
                    planta,
                    escaparate_ml,
                    row["precio"],
                    row["url"] or "",
                    row["titulo"] or "",
                )
                sincronizados += 1
            except Exception as exc:
                logger.debug("Error insertando local %s: %s", local_id, exc)

    logger.info(
        "Sincronización: %d locales escritos en tabla locales (%d sin zona asignada)",
        sincronizados, sin_zona,
    )
    return sincronizados


async def _cargar_mapa_barrios() -> dict[str, tuple[str, float, float]]:
    """
    Construye un mapa barrio_nombre_lower → (zona_id, lat, lng).

    Para cada barrio cargamos el centroide de su primera zona como coordenada
    representativa. Esto nos permite asignar lat/lng a los locales de Habitaclia
    que solo tienen nombre de barrio.
    """
    async with get_db() as conn:
        rows = await conn.fetch("""
            SELECT DISTINCT ON (b.nombre)
                b.nombre                            AS barrio_nombre,
                z.id                                AS zona_id,
                ST_Y(ST_Centroid(z.geometria))      AS lat,
                ST_X(ST_Centroid(z.geometria))      AS lng
            FROM barrios b
            JOIN zonas z ON z.barrio_id = b.id
            ORDER BY b.nombre, z.id
        """)

    mapa: dict[str, tuple[str, float, float]] = {}
    for row in rows:
        key = row["barrio_nombre"].lower().strip()
        mapa[key] = (row["zona_id"], row["lat"], row["lng"])

    return mapa


def _resolver_zona(
    barrio_texto: str,
    mapa_barrios: dict[str, tuple[str, float, float]],
) -> Optional[tuple[str, float, float]]:
    """
    Resuelve el texto libre de barrio de Habitaclia a una (zona_id, lat, lng).

    Estrategia en 3 capas:
      1. Match exacto con el nombre del barrio
      2. Match parcial: alguna palabra clave del barrio está en el texto
      3. Match por distrito: si el texto contiene el nombre del distrito

    Ejemplos de texto de Habitaclia:
      "Sant Gervasi - Galvany"   → "sant gervasi"
      "La Nova Esquerra de l'Eixample" → "esquerra eixample"
      "El Raval"                 → "el raval"
    """
    if not barrio_texto:
        return None

    texto = barrio_texto.lower().strip()
    # Normalizar: quitar apóstrofes, guiones, artículos
    texto_norm = re.sub(r"['\-]", " ", texto)
    texto_norm = re.sub(r"\b(el|la|els|les|de|del|l|l'|d')\b", "", texto_norm)
    texto_norm = re.sub(r"\s+", " ", texto_norm).strip()

    # 1. Match exacto
    if texto in mapa_barrios:
        return mapa_barrios[texto]

    # 2. Match parcial — buscar palabras significativas del texto en las claves
    palabras = [p for p in texto_norm.split() if len(p) >= 4]
    mejor_match = None
    mejor_score = 0

    for key, val in mapa_barrios.items():
        key_norm = re.sub(r"['\-]", " ", key)
        key_norm = re.sub(r"\b(el|la|els|les|de|del|l)\b", "", key_norm)
        key_norm = re.sub(r"\s+", " ", key_norm).strip()

        coincidencias = sum(1 for p in palabras if p in key_norm)
        if coincidencias > mejor_score:
            mejor_score = coincidencias
            mejor_match = val

    if mejor_score >= 1 and mejor_match:
        return mejor_match

    # 3. Fallback por distrito — si el texto menciona el distrito, usar primera zona
    _DISTRITO_FALLBACK = {
        "eixample":         "esquerra eixample",
        "gracia":           "vila de gràcia",
        "gràcia":           "vila de gràcia",
        "sants":            "sants",
        "ciutat vella":     "el raval",
        "sant marti":       "el poblenou",
        "sant martí":       "el poblenou",
        "sant andreu":      "sant andreu",
        "nou barris":       "nou barris nord",
        "horta":            "el guinardó",
        "les corts":        "les corts",
        "sarria":           "sant gervasi",
        "sarrià":           "sant gervasi",
        "montjuic":         "sants",
        "montjuïc":         "sants",
        "poblenou":         "el poblenou",
        "raval":            "el raval",
        "gothic":           "el gòtic",
        "gotic":            "el gòtic",
        "born":             "el born",
        "barceloneta":      "la barceloneta",
        "sant gervasi":     "sant gervasi",
        "sant pere":        "el born",
        "poble sec":        "sants",
        "hostafrancs":      "hostafrancs",
        "clot":             "el clot",
        "sagrera":          "la sagrera",
        "navas":            "sant andreu",
    }

    for keyword, barrio_fallback in _DISTRITO_FALLBACK.items():
        if keyword in texto:
            if barrio_fallback in mapa_barrios:
                return mapa_barrios[barrio_fallback]

    return None


def _inferir_planta(titulo: str) -> str:
    """Infiere la planta desde el título del anuncio de Habitaclia."""
    t = titulo.lower()
    if any(k in t for k in ("sótano", "sotano", "semi", "subterráneo")):
        return "-1"
    if any(k in t for k in ("planta 1", "primer pis", "1a planta", "primera")):
        return "1"
    return "PB"  # Default: planta baja (lo más común en locales comerciales)


# ── Helpers de BD ─────────────────────────────────────────────────────────────

async def _cargar_ids_conocidos() -> set[str]:
    """Carga los portal_id ya en inmuebles_portales para deduplicación."""
    async with get_db() as conn:
        rows = await conn.fetch("SELECT portal_id FROM inmuebles_portales")
    return {r["portal_id"] for r in rows}


async def _persistir_anuncios(
    anuncios_raw: list[dict],
    ids_conocidos: set[str],
    tipo_operacion: str,
) -> int:
    """Persiste los anuncios crudos en inmuebles_portales."""
    nuevos = 0
    for raw in anuncios_raw:
        if "tipo_operacion" not in raw:
            raw["tipo_operacion"] = tipo_operacion
        inm = desde_dict_scraper(raw)
        if not inm:
            continue
        if inm.portal_id in ids_conocidos:
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
                num_fotos, etiquetas,
                fecha_scraping
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
            inm.num_fotos, inm.etiquetas,
            datetime.now(timezone.utc),
        )


async def _limpiar_seed() -> int:
    """Elimina locales del seed donde ya hay datos reales de Habitaclia."""
    async with get_db() as conn:
        try:
            result = await conn.fetchval("SELECT limpiar_seed_si_hay_real()")
            return int(result or 0)
        except Exception as exc:
            # La función puede no existir si no se aplicó la migración 005
            logger.debug("limpiar_seed_si_hay_real no disponible: %s", exc)
            return 0


async def _actualizar_precios_zona() -> int:
    """Recalcula medianas de precio/m² por barrio en precios_alquiler_zona."""
    async with get_db() as conn:
        resultado = await conn.execute("""
            INSERT INTO precios_alquiler_zona (zona_id, precio_m2, tipo, fecha, fuente, n_muestras)
            SELECT
                l.zona_id,
                PERCENTILE_CONT(0.5) WITHIN GROUP
                    (ORDER BY l.alquiler_mensual / NULLIF(l.m2, 0))::numeric(10,2) AS precio_m2,
                'alquiler'          AS tipo,
                NOW()               AS fecha,
                'habitaclia'        AS fuente,
                COUNT(*)::int       AS n_muestras
            FROM locales l
            WHERE l.fuente      = 'habitaclia'
              AND l.disponible  = TRUE
              AND l.alquiler_mensual IS NOT NULL
              AND l.m2 IS NOT NULL
              AND l.m2 > 0
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
        service=getattr(cfg, "SCRAPING_SERVICE", "none"),
        scrapingbee_key=getattr(cfg, "SCRAPINGBEE_API_KEY", ""),
        zenrows_key=getattr(cfg, "ZENROWS_API_KEY", ""),
        proxies_raw=getattr(cfg, "SCRAPING_PROXIES", ""),
        delay_min=float(getattr(cfg, "SCRAPING_DELAY_MIN", 1.5)),
        delay_max=float(getattr(cfg, "SCRAPING_DELAY_MAX", 4.0)),
        timeout=int(getattr(cfg, "SCRAPING_TIMEOUT", 25)),
        max_retries=int(getattr(cfg, "SCRAPING_MAX_RETRIES", 3)),
    )
