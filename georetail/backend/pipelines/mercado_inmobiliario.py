"""
pipelines/mercado_inmobiliario.py — Pipeline unificado de datos de mercado inmobiliario.

FUENTES Y VOLUMEN ESTIMADO (locales en alquiler, Barcelona ciudad):

  ┌─────────────────┬──────────┬──────────────────┬──────────────────────┐
  │ Portal          │ ~Anuncios│ Anti-bot         │ Método               │
  ├─────────────────┼──────────┼──────────────────┼──────────────────────┤
  │ Wallapop        │  200-400 │ Ninguno          │ API JSON directa ✅  │
  │ Pisos.com       │ 1.000+   │ Ninguno          │ HTML puro, bs4 ✅    │
  │ Habitaclia      │ 1.500+   │ Cloudflare básico│ curl_cffi ✅         │
  │ Fotocasa        │ 2.000+   │ DataDome         │ curl_cffi + Playwright│
  │ Milanuncios     │  300-500 │ Mínimo           │ curl_cffi ✅         │
  │ Idealista       │ 3.000+   │ DataDome pesado  │ ❌ Sin proxy de pago │
  └─────────────────┴──────────┴──────────────────┴──────────────────────┘

  TOTAL ALCANZABLE SIN PAGAR: ~3.000-4.500 locales únicos en Barcelona.

Estrategia de ejecución:
  - Wallapop primero (API JSON, instantáneo, ~200 locales)
  - Pisos.com segundo (sin anti-bot, fácil, ~1.000+ locales)
  - Habitaclia tercero (Cloudflare básico, ~1.500 locales)
  - Fotocasa cuarto (DataDome, puede fallar)
  - Milanuncios quinto (complemento)

Función pública: ejecutar()
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

from config import get_settings
from db.conexion import get_db
from pipelines.scraping import (
    FotocasaScraper,
    HabitacliaScraper,
    MilanunciosScraper,
    PisosScraper,
    WallapopScraper,
    ScrapingConfig,
    desde_dict_scraper,
)
from pipelines.scraping.models import InmueblePortal

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
        zonas:      ignorado (los portales usan radio geográfico o ciudad)
        max_paginas: límite de páginas por portal.
                     None = valores por defecto razonables por portal.
        portales:   lista de portales a usar. None = todos los aplicables.
                    Valores: "wallapop" | "pisos" | "habitaclia" | "fotocasa" | "milanuncios"

    Returns:
        dict: { total_nuevos, total_actualizados, errores, por_fuente }
    """
    cfg = get_settings()
    scraping_cfg = _build_scraping_config(cfg)

    logger.info(
        "Pipeline mercado_inmobiliario — modo=%s portales=%s",
        modo, portales or "todos",
    )

    ids_conocidos = await _cargar_ids_conocidos()
    logger.info("%d anuncios ya en BD", len(ids_conocidos))

    stats: dict = {
        "total_nuevos": 0,
        "total_actualizados": 0,
        "errores": 0,
        "por_fuente": {},
    }

    tareas = _build_tareas(modo, max_paginas, portales, scraping_cfg)

    # Máximo 2 portales en paralelo para no saturar nuestra IP
    # (Wallapop es API, los demás son scraping HTTP)
    sem = asyncio.Semaphore(2)

    async def _ejecutar_con_sem(nombre, coro):
        async with sem:
            logger.info("Iniciando scraping de: %s", nombre)
            return nombre, await coro

    resultados = await asyncio.gather(
        *[_ejecutar_con_sem(n, c) for n, c in tareas],
        return_exceptions=True,
    )

    for resultado in resultados:
        if isinstance(resultado, Exception):
            logger.error("Error en tarea de scraping: %s", resultado)
            stats["errores"] += 1
            continue

        nombre_portal, anuncios_raw = resultado
        if not anuncios_raw:
            logger.info("%s: sin resultados", nombre_portal)
            stats["por_fuente"][nombre_portal] = 0
            continue

        nuevos = await _persistir_anuncios(
            anuncios_raw=anuncios_raw,
            ids_conocidos=ids_conocidos,
            tipo_operacion=_modo_a_tipo_operacion(modo),
        )
        stats["por_fuente"][nombre_portal] = nuevos
        stats["total_nuevos"] += nuevos
        logger.info("%s: %d nuevos anuncios guardados", nombre_portal, nuevos)

    if stats["total_nuevos"] > 0 and modo == "locales_alquiler":
        try:
            actualizados = await _actualizar_precios_zona()
            stats["total_actualizados"] = actualizados
            logger.info("precios_alquiler_zona: %d zonas actualizadas", actualizados)
        except Exception as exc:
            logger.error("Error actualizando precios_alquiler_zona: %s", exc)

    logger.info(
        "Pipeline completado — nuevos=%d actualizados=%d errores=%d por_fuente=%s",
        stats["total_nuevos"], stats["total_actualizados"],
        stats["errores"], stats["por_fuente"],
    )
    return stats


# ── Constructor de tareas ─────────────────────────────────────────────────────

def _build_tareas(
    modo: str,
    max_paginas: Optional[int],
    portales: Optional[list[str]],
    cfg: ScrapingConfig,
) -> list[tuple[str, asyncio.coroutine]]:
    """
    Construye la lista de tareas de scraping para el modo dado.

    Orden de prioridad para locales_alquiler:
      1. wallapop   — API JSON, sin bloqueos (~200-400 locales)
      2. pisos      — HTML puro, sin anti-bot (~1.000+ locales)
      3. habitaclia — curl_cffi funciona (~1.500 locales)
      4. fotocasa   — DataDome, puede fallar a veces (~2.000 si funciona)
      5. milanuncios — complemento (~300-500 locales)

    max_paginas por defecto (equilibrio velocidad/cobertura):
      - wallapop:   5 páginas  → ~200 locales  (API, rápido)
      - pisos:     30 páginas  → ~600 locales  (sin anti-bot, seguro)
      - habitaclia: 30 páginas → ~900 locales  (Cloudflare básico)
      - fotocasa:   10 páginas → ~200 locales  (DataDome, precaución)
      - milanuncios: 8 páginas → ~200 locales  (complemento)
    """
    # Páginas por defecto si no se especifican
    pag_wallapop    = max_paginas or 5
    pag_pisos       = max_paginas or 30
    pag_habitaclia  = max_paginas or 30
    pag_fotocasa    = max_paginas or 10
    pag_milanuncios = max_paginas or 8

    mapa: dict[str, list[tuple]] = {
        "locales_alquiler": [
            ("wallapop",    _scrape_wallapop(pag_wallapop)),
            ("pisos",       _scrape_generico(PisosScraper(cfg),       "barcelona", pag_pisos)),
            ("habitaclia",  _scrape_generico(HabitacliaScraper(cfg),  "barcelona", pag_habitaclia)),
            ("fotocasa",    _scrape_generico(FotocasaScraper(cfg),    "barcelona", pag_fotocasa)),
            ("milanuncios", _scrape_generico(MilanunciosScraper(cfg), "barcelona", pag_milanuncios)),
            # Idealista: DESACTIVADO — DataDome pesado, bloquea sin proxy de pago
            # Para activar: añadir SCRAPING_SERVICE=scrapingbee o zenrows en .env
            # ("idealista", _scrape_generico(IdealistaScraper(cfg), "barcelona", 10)),
        ],
        "locales_venta": [
            ("wallapop",  _scrape_wallapop_venta(pag_wallapop)),
            ("pisos",     _scrape_generico(PisosScraper(cfg),   "barcelona", pag_pisos)),
            ("fotocasa",  _scrape_generico(FotocasaScraper(cfg), "barcelona", pag_fotocasa)),
        ],
        "viviendas": [
            ("habitaclia",  _scrape_generico(HabitacliaScraper(cfg),  "barcelona", pag_habitaclia)),
            ("fotocasa",    _scrape_generico(FotocasaScraper(cfg),    "barcelona", pag_fotocasa)),
            ("milanuncios", _scrape_generico(MilanunciosScraper(cfg), "barcelona", pag_milanuncios)),
        ],
    }

    tareas_modo = mapa.get(modo, [])

    if portales:
        tareas_modo = [(n, c) for n, c in tareas_modo if n in portales]

    return tareas_modo


async def _scrape_wallapop(max_paginas: int) -> list[dict]:
    """Scrape de Wallapop — locales en alquiler (API JSON)."""
    try:
        scraper = WallapopScraper(delay_entre_paginas=1.0)
        return await scraper.scrape(max_paginas=max_paginas)
    except Exception as exc:
        logger.error("Error en WallapopScraper: %s", exc, exc_info=True)
        return []


async def _scrape_wallapop_venta(max_paginas: int) -> list[dict]:
    """Scrape de Wallapop — locales en venta (API JSON)."""
    try:
        import httpx
        from pipelines.scraping.wallapop_scraper import (
            _API_BASE, _BCN_LAT, _BCN_LON, _BCN_RADIUS_M,
            _CATEGORY_LOCALES, _HEADERS, _STEP,
            _extraer_items, _extraer_next_start, _parsear_item,
        )
        resultados: list[dict] = []
        offset = 0
        async with httpx.AsyncClient(timeout=20.0, follow_redirects=True, http2=True) as client:
            for pagina in range(1, max_paginas + 1):
                params = {
                    "category_ids":     _CATEGORY_LOCALES,
                    "transaction_type": "sell",
                    "latitude":         _BCN_LAT, "longitude": _BCN_LON,
                    "distance":         _BCN_RADIUS_M,
                    "start": offset, "step": _STEP,
                    "order_by": "most_recent", "source": "search_box",
                }
                r = await client.get(_API_BASE, params=params, headers=_HEADERS)
                if r.status_code != 200:
                    break
                data = r.json()
                for item in _extraer_items(data):
                    parsed = _parsear_item(item)
                    if parsed:
                        parsed["tipo_operacion"] = "venta-locales"
                        resultados.append(parsed)
                next_start = _extraer_next_start(data)
                if next_start is None:
                    break
                offset = next_start
                await asyncio.sleep(1.0)
        return resultados
    except Exception as exc:
        logger.error("Error WallapopScraper venta: %s", exc, exc_info=True)
        return []


async def _scrape_generico(scraper, ciudad: str, max_paginas: int) -> list[dict]:
    """Wrapper genérico para scrapers HTML."""
    try:
        return await scraper.scrape(ciudad=ciudad, max_paginas=max_paginas)
    except Exception as exc:
        logger.error(
            "Error en scraper %s: %s", type(scraper).__name__, exc, exc_info=True
        )
        return []


# ── Persistencia ──────────────────────────────────────────────────────────────

async def _cargar_ids_conocidos() -> set[str]:
    async with get_db() as conn:
        rows = await conn.fetch("SELECT portal_id FROM inmuebles_portales")
    return {r["portal_id"] for r in rows}


async def _persistir_anuncios(
    anuncios_raw: list[dict],
    ids_conocidos: set[str],
    tipo_operacion: str,
) -> int:
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
                $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,
                $11,$12,$13,$14,$15,$16,$17,
                $18,$19,$20,$21,$22,$23,$24,$25,
                $26,$27,$28,$29,$30,$31
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
