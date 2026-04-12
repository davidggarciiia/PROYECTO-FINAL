"""
pipelines/mercado_inmobiliario.py — Pipeline unificado de datos de mercado inmobiliario.

Orquesta TODOS los scrapers de portales (Idealista, Fotocasa, Habitaclia,
Milanuncios, Pisos.com) y persiste los resultados en una única tabla
`inmuebles_portales`, diferenciada por el campo `fuente`.

Función pública: ejecutar()
Llamado desde:
  - pipelines/scheduler.py  (automático, por cron)
  - workers/tasks.py        (Celery, bajo demanda)

Modos de ejecución:
  "locales_alquiler"  → locales en alquiler (fuente principal de precios para scoring)
  "locales_venta"     → locales en venta (análisis de valor de activo)
  "viviendas"         → viviendas en alquiler (proxy socioeconómico por zona)

Portales por modo:
  locales_alquiler: Idealista (por zona BCN) + Fotocasa + Habitaclia + Milanuncios + Pisos
  locales_venta:    Idealista (por zona BCN) + Fotocasa
  viviendas:        Idealista (por zona BCN) + Fotocasa + Habitaclia
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

from config import get_settings
from db.conexion import get_db
from pipelines.inmobiliario.scraping import (
    FotocasaScraper,
    HabitacliaScraper,
    IdealistaScraper,
    MilanunciosScraper,
    PisosScraper,
    ScrapingConfig,
    desde_dict_scraper,
)
from pipelines.inmobiliario.scraping.models import InmueblePortal
from pipelines.inmobiliario.scraping.urls import urls_idealista_por_zona

logger = logging.getLogger(__name__)


# ── Punto de entrada público ──────────────────────────────────────────────────

async def ejecutar(
    modo: str = "locales_alquiler",
    zonas: Optional[list[str]] = None,
    max_paginas: Optional[int] = None,
    portales: Optional[list[str]] = None,
) -> dict:
    """
    Ejecuta el pipeline de mercado inmobiliario.

    Args:
        modo:       "locales_alquiler" | "locales_venta" | "viviendas"
        zonas:      zonas de BCN para Idealista. None = todos los distritos.
        max_paginas: límite de páginas por portal/zona. None = sin límite.
        portales:   lista de portales a usar. None = todos los aplicables al modo.

    Returns:
        dict con estadísticas: total_nuevos, total_actualizados, errores, por_fuente
    """
    cfg = get_settings()
    scraping_cfg = _build_scraping_config(cfg)
    max_pag = max_paginas or int(getattr(cfg, "SCRAPING_MAX_PAGINAS", 0)) or None

    logger.info(
        "Pipeline mercado_inmobiliario — modo=%s zonas=%s max_paginas=%s portales=%s",
        modo, zonas, max_pag, portales,
    )

    ids_conocidos = await _cargar_ids_conocidos()
    logger.info("%d anuncios ya en BD", len(ids_conocidos))

    stats: dict = {"total_nuevos": 0, "total_actualizados": 0, "errores": 0, "por_fuente": {}}

    # Lanzar todos los scrapers aplicables en paralelo (con semáforo de cortesía)
    tareas = _build_tareas(modo, zonas, max_pag, portales, scraping_cfg)
    sem = asyncio.Semaphore(3)  # máximo 3 portales en paralelo

    async def _ejecutar_con_sem(nombre, coro):
        async with sem:
            return nombre, await coro

    resultados = await asyncio.gather(
        *[_ejecutar_con_sem(n, c) for n, c in tareas],
        return_exceptions=True,
    )

    # Persistir resultados
    for resultado in resultados:
        if isinstance(resultado, Exception):
            logger.error("Error en tarea de scraping: %s", resultado)
            stats["errores"] += 1
            continue

        nombre_portal, anuncios_raw = resultado
        if not anuncios_raw:
            logger.info("%s: sin resultados", nombre_portal)
            continue

        nuevos = await _persistir_anuncios(
            anuncios_raw=anuncios_raw,
            ids_conocidos=ids_conocidos,
            tipo_operacion=_modo_a_tipo_operacion(modo),
        )
        stats["por_fuente"][nombre_portal] = nuevos
        stats["total_nuevos"] += nuevos
        logger.info("%s: %d nuevos anuncios guardados", nombre_portal, nuevos)

    # Actualizar medianas de precio/m² en precios_alquiler_zona
    if stats["total_nuevos"] > 0 and modo == "locales_alquiler":
        try:
            actualizados = await _actualizar_precios_zona()
            stats["total_actualizados"] = actualizados
            logger.info("precios_alquiler_zona: %d zonas actualizadas", actualizados)
        except Exception as exc:
            logger.error("Error actualizando precios_alquiler_zona: %s", exc)

    logger.info(
        "Pipeline mercado_inmobiliario completado — nuevos=%d actualizados=%d errores=%d",
        stats["total_nuevos"], stats["total_actualizados"], stats["errores"],
    )
    return stats


# ── Constructor de tareas por modo ────────────────────────────────────────────

def _build_tareas(
    modo: str,
    zonas: Optional[list[str]],
    max_paginas: Optional[int],
    portales: Optional[list[str]],
    cfg: ScrapingConfig,
) -> list[tuple[str, asyncio.coroutine]]:
    """
    Devuelve lista de (nombre, coroutine) según modo y portales activos.
    """
    max_pag = max_paginas or 10  # default razonable por portal

    # Idealista: scrape por zona de BCN usando IdealistaScraper + urls.py
    def tareas_idealista(tipo: str) -> list[tuple[str, asyncio.coroutine]]:
        tipo_op = _modo_a_tipo_operacion(tipo) if tipo != "viviendas" else "alquiler-viviendas"
        url_tipo = {
            "locales_alquiler": "alquiler-locales",
            "locales_venta":    "venta-locales",
            "viviendas":        "alquiler-viviendas",
        }.get(modo, "alquiler-locales")
        zona_urls = urls_idealista_por_zona(tipo=url_tipo, zonas=zonas)
        tareas = []
        for nombre_zona, url_base in zona_urls:
            nombre_clave = f"idealista:{nombre_zona}"
            coro = _scrape_idealista_zona(
                url_base=url_base,
                ciudad="barcelona",
                max_paginas=max_pag,
                cfg=cfg,
            )
            tareas.append((nombre_clave, coro))
        return tareas

    # Definición por modo
    mapa: dict[str, list[tuple[str, asyncio.coroutine]]] = {
        "locales_alquiler": (
            tareas_idealista("locales_alquiler")
            + [
                ("fotocasa",    _scrape_generico(FotocasaScraper(cfg),    "barcelona", max_pag)),
                ("habitaclia",  _scrape_generico(HabitacliaScraper(cfg),  "barcelona", max_pag)),
                ("milanuncios", _scrape_generico(MilanunciosScraper(cfg), "barcelona", max_pag)),
                ("pisos",       _scrape_generico(PisosScraper(cfg),       "barcelona", max_pag)),
            ]
        ),
        "locales_venta": (
            tareas_idealista("locales_venta")
            + [
                ("fotocasa", _scrape_generico(FotocasaScraper(cfg), "barcelona", max_pag)),
            ]
        ),
        "viviendas": (
            tareas_idealista("viviendas")
            + [
                ("fotocasa",   _scrape_generico(FotocasaScraper(cfg),   "barcelona", max_pag)),
                ("habitaclia", _scrape_generico(HabitacliaScraper(cfg), "barcelona", max_pag)),
            ]
        ),
    }

    tareas_modo = mapa.get(modo, [])

    # Filtrar por portales si se especificaron
    if portales:
        tareas_modo = [
            (n, c) for n, c in tareas_modo
            if any(n == p or n.startswith(f"{p}:") for p in portales)
        ]

    return tareas_modo


async def _scrape_generico(scraper, ciudad: str, max_paginas: int) -> list[dict]:
    """Wrapper para scrapers con interfaz scrape(ciudad, max_paginas)."""
    try:
        return await scraper.scrape(ciudad=ciudad, max_paginas=max_paginas)
    except Exception as exc:
        logger.error("Error en scraper %s: %s", type(scraper).__name__, exc, exc_info=True)
        return []


async def _scrape_idealista_zona(
    url_base: str,
    ciudad: str,
    max_paginas: int,
    cfg: ScrapingConfig,
) -> list[dict]:
    """
    Scrape de Idealista para una URL de zona concreta.
    IdealistaScraper acepta una URL base con paginación automática.
    """
    try:
        # IdealistaScraper.scrape() acepta una ciudad genérica; para BCN por zona
        # pasamos la URL directamente a través de una subclase ligera
        scraper = _IdealistaZonaScraper(url_base, cfg)
        return await scraper.scrape(max_paginas=max_paginas)
    except Exception as exc:
        logger.error("Error scrapeando zona Idealista %s: %s", url_base, exc, exc_info=True)
        return []


class _IdealistaZonaScraper:
    """
    Adaptador mínimo que hace que IdealistaScraper use una URL de zona BCN
    en lugar de las URLs genéricas por ciudad.
    """
    def __init__(self, url_base: str, cfg: ScrapingConfig):
        self._url_base = url_base
        self._cfg = cfg

    async def scrape(self, max_paginas: int) -> list[dict]:
        from pipelines.inmobiliario.scraping.base_scraper import BaseScraper
        from pipelines.inmobiliario.scraping.idealista_scraper import _parse_idealista, _es_challenge

        resultados = []
        async with BaseScraper(self._cfg) as scraper:
            for pagina in range(1, max_paginas + 1):
                url = self._url_base if pagina == 1 else f"{self._url_base.rstrip('/')}/pagina-{pagina}.htm"
                referer = self._url_base if pagina > 1 else "https://www.idealista.com"

                html = await scraper.get(url, referer=referer)
                if not html:
                    html = await scraper.get_with_playwright(url, wait_selector=".item-list")
                if not html or _es_challenge(html):
                    logger.warning("Idealista zona bloqueada o sin respuesta: %s pág %d", self._url_base, pagina)
                    break

                items = _parse_idealista(html)
                if not items:
                    break

                resultados.extend(items)
        return resultados


# ── Persistencia ──────────────────────────────────────────────────────────────

async def _cargar_ids_conocidos() -> set[str]:
    """Carga todos los portal_id ya en BD para deduplicación rápida en memoria."""
    async with get_db() as conn:
        rows = await conn.fetch("SELECT portal_id FROM inmuebles_portales")
    return {r["portal_id"] for r in rows}


async def _persistir_anuncios(
    anuncios_raw: list[dict],
    ids_conocidos: set[str],
    tipo_operacion: str,
) -> int:
    """
    Convierte los dicts crudos a InmueblePortal y hace upsert en BD.
    Devuelve el número de anuncios nuevos insertados.
    """
    nuevos = 0
    import json as _json

    for raw in anuncios_raw:
        # Añadir tipo_operacion si el scraper no lo incluye
        if "tipo_operacion" not in raw:
            raw["tipo_operacion"] = tipo_operacion

        inm = desde_dict_scraper(raw)
        if not inm:
            continue
        if inm.portal_id in ids_conocidos:
            continue  # ya existe — el ON CONFLICT actualizará precio/fecha

        try:
            await _upsert_inmueble(inm)
            ids_conocidos.add(inm.portal_id)
            nuevos += 1
        except Exception as exc:
            logger.warning("Error guardando anuncio %s: %s", inm.portal_id, exc)

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
                $1,$2,$3,$4,$5,
                $6,$7,$8,
                $9,$10,
                $11,$12,$13,$14,$15,
                $16,$17,
                $18,$19,$20,
                $21,$22,$23,$24,$25,
                $26,$27,$28,
                $29,$30,
                $31
            )
            ON CONFLICT (portal_id) DO UPDATE SET
                precio          = EXCLUDED.precio,
                precio_m2       = EXCLUDED.precio_m2,
                precio_anterior = EXCLUDED.precio_anterior,
                dias_publicado  = EXCLUDED.dias_publicado,
                fecha_scraping  = EXCLUDED.fecha_scraping
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


async def _actualizar_precios_zona() -> int:
    """
    Recalcula medianas de precio/m² por barrio desde inmuebles_portales
    y hace upsert en precios_alquiler_zona.
    Consumido por scoring/features.py → _precio() / _precios().
    """
    async with get_db() as conn:
        resultado = await conn.execute("""
            INSERT INTO precios_alquiler_zona (zona_id, precio_m2, tipo, fecha, fuente, n_muestras)
            SELECT
                b.id                                                              AS zona_id,
                PERCENTILE_CONT(0.5) WITHIN GROUP
                    (ORDER BY ip.precio_m2)::numeric(10,2)                        AS precio_m2,
                'alquiler'                                                        AS tipo,
                NOW()                                                             AS fecha,
                'scraping_portales'                                               AS fuente,
                COUNT(*)::int                                                     AS n_muestras
            FROM inmuebles_portales ip
            JOIN barrios b ON LOWER(b.nombre) = LOWER(ip.barrio)
            WHERE ip.tipo_operacion = 'alquiler-locales'
              AND ip.precio_m2 IS NOT NULL
              AND ip.precio_m2 BETWEEN 5 AND 200
              AND ip.fecha_scraping > NOW() - INTERVAL '60 days'
            GROUP BY b.id
            HAVING COUNT(*) >= 3
            ON CONFLICT (zona_id, tipo, (DATE_TRUNC('day', fecha))) DO UPDATE SET
                precio_m2  = EXCLUDED.precio_m2,
                fecha      = EXCLUDED.fecha,
                n_muestras = EXCLUDED.n_muestras
        """)
    try:
        return int(resultado.split()[-1])
    except (IndexError, ValueError):
        return 0


# ── Helpers ───────────────────────────────────────────────────────────────────

def _modo_a_tipo_operacion(modo: str) -> str:
    return {
        "locales_alquiler": "alquiler-locales",
        "locales_venta":    "venta-locales",
        "viviendas":        "alquiler-viviendas",
    }.get(modo, "alquiler-locales")


def _build_scraping_config(cfg) -> ScrapingConfig:
    """Construye ScrapingConfig a partir de los settings de la app."""
    return ScrapingConfig(
        service=getattr(cfg, "SCRAPING_SERVICE", "none"),
        scrapingbee_key=getattr(cfg, "SCRAPINGBEE_API_KEY", ""),
        zenrows_key=getattr(cfg, "ZENROWS_API_KEY", ""),
        proxies_raw=getattr(cfg, "SCRAPING_PROXIES", ""),
        delay_min=float(getattr(cfg, "SCRAPING_DELAY_MIN", 1.5)),
        delay_max=float(getattr(cfg, "SCRAPING_DELAY_MAX", 4.5)),
        timeout=int(getattr(cfg, "SCRAPING_TIMEOUT", 20)),
        max_retries=int(getattr(cfg, "SCRAPING_MAX_RETRIES", 3)),
    )
