"""
pipelines/scraping_portales.py — Pipeline de scraping de portales inmobiliarios.

Raspa diariamente los principales portales de búsqueda de locales comerciales
en Barcelona y persiste los datos en las tablas `locales` y `precios_alquiler_zona`.

Portales:
  - Fotocasa    (Cloudflare → Playwright + stealth)
  - Habitaclia  (JS rendering → Playwright + stealth)
  - Milanuncios (API JSON interna → httpx; fallback Playwright)
  - pisos.com   (SSR parcial → httpx + JSON-LD; fallback Playwright)

Técnicas anti-detección por portal:
  ┌─────────────────┬─────────────────────────────────────────────────────────┐
  │ Portal          │ Protección           → Solución                         │
  ├─────────────────┼─────────────────────────────────────────────────────────┤
  │ Fotocasa        │ Cloudflare CF+BM     → Playwright + playwright-stealth   │
  │                 │ TLS fingerprint      → Chromium real (no httpx)          │
  │                 │ Bot score            → Scroll humano + delays gaussianos │
  │                 │                      → Proxy residencial (opcional)      │
  ├─────────────────┼─────────────────────────────────────────────────────────┤
  │ Habitaclia      │ ThreatMetrix básico  → UA real + headers completos       │
  │                 │ Rate limit ~60/h     → Delays 3-7s entre páginas         │
  │                 │ JS rendering         → Playwright si httpx falla         │
  ├─────────────────┼─────────────────────────────────────────────────────────┤
  │ Milanuncios     │ HUMAN Security       → API JSON interna (sin JS needed)  │
  │                 │ Rate limit ~100/h    → Delays 3-8s                       │
  │                 │ UA check             → UA real + headers X-Requested-With│
  ├─────────────────┼─────────────────────────────────────────────────────────┤
  │ pisos.com       │ UA check básico      → UA real de Chrome                 │
  │                 │ Rate limit ~30-60/h  → Delays 3.5-8s                     │
  │                 │ SSR con JSON-LD      → httpx + JSON-LD (sin JS)          │
  └─────────────────┴─────────────────────────────────────────────────────────┘

Variables de entorno:
  SCRAPER_PROXY_URL   URL del proxy residencial (socks5://user:pass@host:port)
                      Recomendado para Fotocasa. Gratis: no hay.
                      Opciones de pago: Oxylabs, Bright Data, Smartproxy.
  SCRAPERAPI_KEY      Clave de ScraperAPI (último recurso si proxy no disponible)
                      Plan básico: 1000 req/mes gratis.
                      Precio: ~$29/mes para 250K req.

Tabla destino: locales (con fuente='fotocasa'/'habitaclia'/etc.)
               precios_alquiler_zona (precio/m² por zona)
               pipeline_ejecuciones (log de estado)

Scheduler: semanal miércoles 02:00 (entre aforaments y precios)
"""
from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

from db.conexion import get_db
from scrapers.base import LocalRaw

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)

# Portales habilitados. Desactivar individualmente si hay bloqueo persistente.
_PORTALES_ACTIVOS = {
    "fotocasa": True,
    "habitaclia": True,
    "milanuncios": True,
    "pisos_com": True,
}


async def ejecutar() -> dict:
    """
    Punto de entrada del pipeline.
    Ejecuta todos los scrapers en paralelo con asyncio.gather().
    """
    eid = await _init()
    total = 0
    errores: list[str] = []

    try:
        resultados = await _scrape_todos_portales()

        async with get_db() as conn:
            for local in resultados:
                try:
                    guardado = await _persistir_local(conn, local)
                    if guardado:
                        total += 1
                except Exception as exc:
                    logger.debug("Error persistiendo %s: %s", local.portal_id, exc)

        logger.info("Pipeline scraping_portales: %d locales guardados", total)
        await _fin(eid, total, "ok")
        return {"registros": total, "errores": errores}

    except Exception as exc:
        logger.error("Pipeline scraping_portales error: %s", exc)
        await _fin(eid, total, "error", str(exc))
        raise


async def _scrape_todos_portales() -> list[LocalRaw]:
    """
    Lanza todos los scrapers en paralelo.

    NOTA IMPORTANTE sobre paralelismo y anti-bot:
    Ejecutar portales en paralelo (asyncio.gather) es seguro porque cada
    portal tiene su propio pool de conexiones y delays independientes.
    Sin embargo, si se usa el mismo proxy para todos, el proveedor de proxy
    podría detectar el patrón. En ese caso, usar delays escalonados o
    proxies distintos por portal.
    """
    tareas = []

    if _PORTALES_ACTIVOS.get("fotocasa"):
        tareas.append(_scrape_portal("fotocasa"))
    if _PORTALES_ACTIVOS.get("habitaclia"):
        tareas.append(_scrape_portal("habitaclia"))
    if _PORTALES_ACTIVOS.get("milanuncios"):
        tareas.append(_scrape_portal("milanuncios"))
    if _PORTALES_ACTIVOS.get("pisos_com"):
        tareas.append(_scrape_portal("pisos_com"))

    if not tareas:
        return []

    # Escalonar inicio de scrapers (evita patrones de IP sospechosos)
    resultados_por_portal = await asyncio.gather(*tareas, return_exceptions=True)

    todos: list[LocalRaw] = []
    for res in resultados_por_portal:
        if isinstance(res, Exception):
            logger.warning("Error en scraper: %s", res)
        elif isinstance(res, list):
            todos.extend(res)

    logger.info("Total bruto de todos los portales: %d locales", len(todos))
    return todos


async def _scrape_portal(nombre: str) -> list[LocalRaw]:
    """Ejecuta el scraper de un portal con manejo de errores."""
    # Importaciones lazy para evitar problemas si playwright no está instalado
    if nombre == "fotocasa":
        from scrapers.fotocasa import FotocasaScraper
        scraper = FotocasaScraper()
    elif nombre == "habitaclia":
        from scrapers.habitaclia import HabitacliaScraper
        scraper = HabitacliaScraper()
    elif nombre == "milanuncios":
        from scrapers.milanuncios import MilanunciosScraper
        scraper = MilanunciosScraper()
    elif nombre == "pisos_com":
        from scrapers.pisos_com import PisosComScraper
        scraper = PisosComScraper()
    else:
        raise ValueError(f"Portal desconocido: {nombre}")

    try:
        locales = await scraper.scrape()
        logger.info("[%s] Scraping completado: %d locales", nombre, len(locales))
        return locales
    except Exception as exc:
        logger.error("[%s] Scraping fallido: %s", nombre, exc)
        return []


async def _persistir_local(conn, local: LocalRaw) -> bool:
    """
    Guarda un local en la tabla `locales` y actualiza `precios_alquiler_zona`.

    Lógica:
      - Precio: solo se actualiza si el nuevo precio es distinto (evita ruido)
      - Zona: se infiere por coordenadas (ST_Within) si disponibles,
        o por barrio (ILIKE) como fallback
      - Precio/m²: se agrega al promedio de la zona (media móvil)
    """
    # Mínimo: necesitamos precio para que el local sea útil para el análisis
    if not local.precio:
        return False

    # Inferir zona desde coordenadas
    zona_id = None
    if local.lat and local.lng:
        zona_id = await conn.fetchval("""
            SELECT id FROM zonas
            WHERE ST_Within(ST_SetSRID(ST_MakePoint($1, $2), 4326), geometria)
            LIMIT 1
        """, local.lng, local.lat)

    # Fallback: inferir zona desde barrio
    if not zona_id and local.barrio:
        zona_id = await conn.fetchval("""
            SELECT z.id FROM zonas z
            JOIN barrios b ON b.id = z.barrio_id
            WHERE b.nombre ILIKE $1
            LIMIT 1
        """, f"%{local.barrio}%")

    # Fallback: inferir zona desde dirección (buscar nombre de barrio en dirección)
    if not zona_id and local.direccion:
        zona_id = await conn.fetchval("""
            SELECT z.id FROM zonas z
            JOIN barrios b ON b.id = z.barrio_id
            WHERE $1 ILIKE '%' || b.nombre || '%'
            LIMIT 1
        """, local.direccion)

    await conn.execute("""
        INSERT INTO locales (
            id, zona_id, direccion, lat, lng,
            geometria, m2, alquiler_mensual, disponible, planta, fuente
        ) VALUES (
            $1, $2, $3, $4, $5,
            CASE WHEN $4 IS NOT NULL AND $5 IS NOT NULL
                 THEN ST_SetSRID(ST_MakePoint($5, $4), 4326)
                 ELSE NULL
            END,
            $6, $7, TRUE, $8, $9
        )
        ON CONFLICT (id) DO UPDATE SET
            alquiler_mensual = EXCLUDED.alquiler_mensual,
            m2               = COALESCE(EXCLUDED.m2, locales.m2),
            disponible       = TRUE,
            zona_id          = COALESCE(EXCLUDED.zona_id, locales.zona_id),
            updated_at       = NOW()
    """,
        local.portal_id,          # $1 id
        zona_id,                  # $2 zona_id
        local.direccion or "",    # $3 direccion
        local.lat,                # $4 lat
        local.lng,                # $5 lng
        local.m2,                 # $6 m2
        local.precio,             # $7 alquiler_mensual
        local.planta,             # $8 planta
        local.portal,             # $9 fuente
    )

    # Actualizar precio/m² de la zona (media móvil incremental)
    if zona_id and local.precio_m2:
        await conn.execute("""
            INSERT INTO precios_alquiler_zona (zona_id, fecha, precio_m2, num_muestras, fuente)
            VALUES ($1, CURRENT_DATE, $2, 1, $3)
            ON CONFLICT (zona_id, fecha, fuente) DO UPDATE SET
                precio_m2   = (
                    precios_alquiler_zona.precio_m2 * precios_alquiler_zona.num_muestras
                    + EXCLUDED.precio_m2
                ) / (precios_alquiler_zona.num_muestras + 1),
                num_muestras = precios_alquiler_zona.num_muestras + 1
        """, zona_id, local.precio_m2, local.portal)

    return True


# ─── Tracking de ejecución ────────────────────────────────────────────────────

async def _init() -> int:
    async with get_db() as conn:
        return await conn.fetchval(
            "INSERT INTO pipeline_ejecuciones(pipeline, estado) VALUES('scraping_portales', 'running') RETURNING id"
        )


async def _fin(eid: int, reg: int, estado: str, msg: str | None = None) -> None:
    async with get_db() as conn:
        await conn.execute(
            "UPDATE pipeline_ejecuciones SET fecha_fin=NOW(), registros=$1, estado=$2, mensaje_error=$3 WHERE id=$4",
            reg, estado, msg, eid,
        )


# ─── CLI para pruebas manuales ────────────────────────────────────────────────

if __name__ == "__main__":
    import asyncio

    async def _test():
        """Ejecutar un portal específico en modo prueba."""
        import sys
        portal = sys.argv[1] if len(sys.argv) > 1 else "milanuncios"
        print(f"Probando scraper: {portal}")
        locales = await _scrape_portal(portal)
        for l in locales[:5]:
            print(f"  {l.portal_id}: {l.titulo[:50]} | {l.precio}€/mes | {l.m2}m²")
        print(f"Total: {len(locales)} locales")

    asyncio.run(_test())
