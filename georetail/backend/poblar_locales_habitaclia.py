#!/usr/bin/env python3
"""
poblar_locales_habitaclia.py — Carga inicial de locales reales desde Habitaclia.

Ejecutar DENTRO del contenedor backend:

  # Prueba rápida (5 páginas, ~75 locales, ~2 min):
  docker exec -it georetail_backend python /app/poblar_locales_habitaclia.py --max-paginas 5

  # Cobertura completa (~107 páginas, ~1.600 locales, ~20 min):
  docker exec -it georetail_backend python /app/poblar_locales_habitaclia.py

  # Solo sincronizar staging → locales (si ya se scrapeó antes):
  docker exec -it georetail_backend python /app/poblar_locales_habitaclia.py --solo-sync

Qué hace:
  1. Aplica migración 005 (amplía locales.id a VARCHAR(50), añade url/titulo/descripcion)
  2. Scraping de Habitaclia (SOLO páginas de listado, sin páginas de detalle)
  3. Persiste en inmuebles_portales (staging)
  4. Sincroniza en tabla locales (lo que ve el frontend)
  5. Oculta locales del seed en zonas ya cubiertas
  6. Actualiza medianas de precio/m² en precios_alquiler_zona
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys

# Añadir el directorio backend al path para imports absolutos
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("poblar_habitaclia")


async def aplicar_migracion(conn):
    """
    Aplica la migración 005 usando una conexión ya abierta.
    El path es relativo al propio script (/app/db/migraciones/).
    """
    sql_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "db", "migraciones", "005_habitaclia_locales.sql",
    )
    if not os.path.exists(sql_path):
        logger.warning(
            "Migración 005 no encontrada en %s — aplícala manualmente con:\n"
            "  docker exec georetail_postgres psql -U postgres -d georetail "
            "-f /docker-entrypoint-initdb.d/005_habitaclia_locales.sql",
            sql_path,
        )
        return

    try:
        with open(sql_path) as f:
            sql = f.read()
        await conn.execute(sql)
        logger.info("Migración 005 aplicada correctamente")
    except Exception as exc:
        # Si ya está aplicada (ALTER TABLE falla sobre columna existente) es OK
        logger.info("Migración 005: %s (probablemente ya aplicada)", exc)


async def main():
    parser = argparse.ArgumentParser(
        description="Poblar tabla locales con datos reales de Habitaclia"
    )
    parser.add_argument(
        "--max-paginas",
        type=int,
        default=107,
        help="Páginas del listado (107 = cobertura total BCN, 5 = prueba rápida)",
    )
    parser.add_argument(
        "--solo-sync",
        action="store_true",
        help="Solo sincronizar inmuebles_portales → locales (sin scrapear de nuevo)",
    )
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("POBLAR LOCALES DESDE HABITACLIA")
    logger.info("=" * 60)
    logger.info("max_paginas: %d (~%d locales estimados)", args.max_paginas, args.max_paginas * 15)
    logger.info("Estrategia: UN SOLO PASO (solo listados, sin páginas de detalle)")
    logger.info("")

    # Inicializar pool de BD antes de cualquier operación con la base de datos.
    # Sin esto, get_db() lanza RuntimeError("Pool BD no inicializado").
    from db.conexion import init_db_pool, close_db_pool, get_db
    logger.info("Conectando a la base de datos...")
    await init_db_pool()

    try:
        # Aplicar migración 005 (requiere pool ya inicializado)
        logger.info("Paso 1/5: Aplicando migración 005...")
        async with get_db() as conn:
            await aplicar_migracion(conn)

        if args.solo_sync:
            logger.info("Modo --solo-sync: sincronizando sin scrapear...")
            from pipelines.mercado_inmobiliario import _sincronizar_a_locales, _limpiar_seed
            sync      = await _sincronizar_a_locales()
            ocultados = await _limpiar_seed()
            logger.info("Sincronizados: %d | Seed ocultados: %d", sync, ocultados)
            return

        # Pipeline completo
        logger.info("Paso 2/5: Iniciando pipeline Habitaclia...")
        from pipelines.mercado_inmobiliario import ejecutar_habitaclia
        stats = await ejecutar_habitaclia(max_paginas=args.max_paginas)

        logger.info("")
        logger.info("=" * 60)
        logger.info("RESULTADO FINAL")
        logger.info("=" * 60)
        logger.info("  Anuncios scrapeados:            %d", stats.get("scrapeados", 0))
        logger.info("  Guardados en staging:           %d", stats.get("guardados_portales", 0))
        logger.info("  Sincronizados en tabla locales: %d", stats.get("sincronizados_locales", 0))
        logger.info("  Locales seed ocultados:         %d", stats.get("seed_ocultados", 0))
        logger.info("  Errores:                        %d", stats.get("errores", 0))
        logger.info("")

        if stats.get("sincronizados_locales", 0) > 0:
            logger.info("✅ El frontend mostrará datos reales de Habitaclia.")
            logger.info("   Refresca el mapa para ver los precios actualizados.")
        elif stats.get("scrapeados", 0) > 0:
            logger.warning(
                "⚠️  Se scrapearon %d anuncios pero 0 se sincronizaron.\n"
                "   Posible causa: ningún barrio de Habitaclia coincide con los de la BD.\n"
                "   Comprueba que las tablas 'barrios' y 'zonas' tienen datos:\n"
                "     docker exec georetail_postgres psql -U postgres -d georetail "
                "-c 'SELECT count(*) FROM barrios; SELECT count(*) FROM zonas;'",
                stats.get("scrapeados", 0),
            )
        else:
            logger.warning(
                "⚠️  0 anuncios scrapeados. Posibles causas:\n"
                "   - Habitaclia bloqueó la IP (espera 10-15 min y reintenta)\n"
                "   - curl_cffi no está instalado: pip install curl_cffi\n"
                "   - Cambiaron los selectores del listado"
            )

    finally:
        # Cerrar el pool siempre, incluso si hay excepción
        await close_db_pool()


if __name__ == "__main__":
    asyncio.run(main())
