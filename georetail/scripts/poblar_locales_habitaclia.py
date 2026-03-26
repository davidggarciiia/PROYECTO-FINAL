"""
poblar_locales_habitaclia.py — Script único para poblar la BD con datos de Habitaclia.

Ubicación: /app/poblar_locales_habitaclia.py  (raíz del backend, donde Python lo encuentra)

Uso desde el contenedor Docker:
    docker exec georetail_backend python poblar_locales_habitaclia.py
    docker exec georetail_backend python poblar_locales_habitaclia.py --max-paginas 5

Argumentos:
    --max-paginas N   Páginas del listado de Habitaclia (default: 107 = todo BCN)
                      Cada página tiene ~15 URLs → ~15 locales con detalle.
                      Para test rápido usa --max-paginas 5 (~75 locales, ~10 min).

Qué hace:
  1. Aplica la migración 005 si no está aplicada (amplía VARCHAR id de 30→50)
  2. Lanza el scraper de Habitaclia
  3. Guarda en inmuebles_portales (staging)
  4. Sincroniza a tabla locales (lo que ve el frontend)
  5. Oculta locales del seed donde ya hay datos reales
  6. Muestra resumen final
"""
import argparse
import asyncio
import logging
import os
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("poblar_habitaclia")


async def main(max_paginas: int) -> None:
    logger.info("=" * 60)
    logger.info("POBLAR LOCALES DESDE HABITACLIA")
    logger.info("=" * 60)
    logger.info("max_paginas: %d (~%d locales)", max_paginas, max_paginas * 15)
    logger.info("")

    # Inicializar pool de BD y Redis ANTES de cualquier import que use get_db()
    logger.info("Paso 1/5: Inicializando conexiones...")
    from db.conexion import init_db_pool, close_db_pool
    from db.redis_client import init_redis, close_redis

    await init_db_pool()
    await init_redis()
    logger.info("✅ Pool BD y Redis inicializados")

    try:
        # Migración 005: ampliar VARCHAR(30)→VARCHAR(50) para IDs de Habitaclia
        logger.info("Paso 2/5: Aplicando migración 005...")
        await _aplicar_migracion_005()

        # Scraping + sincronización
        logger.info("Paso 3/5: Iniciando pipeline Habitaclia...")
        from pipelines.mercado_inmobiliario import ejecutar_habitaclia
        stats = await ejecutar_habitaclia(max_paginas=max_paginas)
        logger.info("Pipeline completado: %s", stats)

        # Resumen
        logger.info("Paso 4/5: Resumen de resultados...")
        await _mostrar_resumen()

        logger.info("Paso 5/5: ✅ Completado.")
        logger.info("")
        logger.info("Los datos de Habitaclia están en la BD.")
        logger.info("Recarga el frontend para ver los precios actualizados.")

    finally:
        await close_db_pool()
        await close_redis()


async def _aplicar_migracion_005() -> None:
    """
    Amplía la columna id de locales de VARCHAR(30) a VARCHAR(50).
    Los IDs de Habitaclia son del tipo 'habitaclia_47595000000952' (26 chars)
    truncados a 30 — necesitamos 50 por seguridad.
    """
    from db.conexion import get_db
    try:
        async with get_db() as conn:
            row = await conn.fetchrow("""
                SELECT character_maximum_length
                FROM information_schema.columns
                WHERE table_name = 'locales' AND column_name = 'id'
            """)
            current_len = row["character_maximum_length"] if row else 30
            if current_len and current_len < 50:
                await conn.execute("ALTER TABLE locales ALTER COLUMN id TYPE VARCHAR(50)")
                # También la FK en locales_historico_precios
                await conn.execute(
                    "ALTER TABLE locales_historico_precios "
                    "ALTER COLUMN local_id TYPE VARCHAR(50)"
                )
                logger.info("✅ Migración 005: id ampliado de VARCHAR(%d) a VARCHAR(50)", current_len)
            else:
                logger.info("Migración 005 ya aplicada (VARCHAR(%s))", current_len or "?")
    except Exception as e:
        logger.warning("Migración 005: %s (puede que ya esté aplicada)", e)


async def _mostrar_resumen() -> None:
    from db.conexion import get_db
    try:
        async with get_db() as conn:
            # Locales por fuente
            rows = await conn.fetch("""
                SELECT fuente, COUNT(*) AS total,
                       ROUND(AVG(alquiler_mensual)::numeric, 0) AS precio_medio,
                       ROUND(AVG(m2)::numeric, 0) AS m2_medio
                FROM locales
                WHERE disponible = TRUE
                GROUP BY fuente ORDER BY total DESC
            """)
            logger.info("\nLocales disponibles por fuente:")
            for r in rows:
                logger.info(
                    "  %-15s %4d locales | precio medio %s€/mes | m² medio %sm²",
                    r["fuente"], r["total"],
                    int(r["precio_medio"] or 0), int(r["m2_medio"] or 0),
                )

            # Top zonas con datos de Habitaclia
            rows2 = await conn.fetch("""
                SELECT z.nombre, d.nombre AS distrito, COUNT(*) AS total,
                       MIN(l.alquiler_mensual)::int AS precio_min,
                       MAX(l.alquiler_mensual)::int AS precio_max
                FROM locales l
                JOIN zonas z    ON z.id = l.zona_id
                JOIN barrios b  ON b.id = z.barrio_id
                JOIN distritos d ON d.id = b.distrito_id
                WHERE l.fuente = 'habitaclia' AND l.disponible = TRUE
                GROUP BY z.nombre, d.nombre
                ORDER BY total DESC LIMIT 10
            """)
            if rows2:
                logger.info("\nTop 10 zonas con más locales de Habitaclia:")
                for r in rows2:
                    logger.info(
                        "  %-30s (%s): %d locales | %d€ - %d€/mes",
                        r["nombre"], r["distrito"], r["total"],
                        r["precio_min"] or 0, r["precio_max"] or 0,
                    )

            # Total en staging
            n = await conn.fetchval(
                "SELECT COUNT(*) FROM inmuebles_portales WHERE fuente='habitaclia'"
            )
            logger.info("\nTotal en inmuebles_portales (staging): %d", n or 0)

    except Exception as e:
        logger.warning("Error en resumen: %s", e)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Poblar locales desde Habitaclia")
    parser.add_argument(
        "--max-paginas", type=int, default=107,
        help="Páginas del listado (default: 107 = todo BCN, ~1.600 locales)",
    )
    args = parser.parse_args()
    asyncio.run(main(max_paginas=args.max_paginas))
