#!/usr/bin/env python3
"""
scripts/poblar_locales_habitaclia.py — Carga inicial de locales reales desde Habitaclia.

Ejecutar DENTRO del contenedor backend (una sola vez, o cuando quieras refrescar):

  docker exec -it georetail_backend \
    python scripts/poblar_locales_habitaclia.py

  # Prueba rápida (5 páginas, ~75 locales, ~5 min):
  docker exec -it georetail_backend \
    python scripts/poblar_locales_habitaclia.py --max-paginas 5

  # Cobertura completa (107 páginas, ~1.600 locales, ~25 min):
  docker exec -it georetail_backend \
    python scripts/poblar_locales_habitaclia.py --max-paginas 107

Qué hace:
  1. Aplica la migración 005 (amplía locales.id a VARCHAR(50), añade columna url)
  2. Lanza HabitacliaScraper con la técnica de 2 pasos (listado + detalle)
  3. Persiste en inmuebles_portales (staging)
  4. Sincroniza en tabla locales (lo que ve el frontend)
  5. Elimina locales del seed donde ya hay datos reales
  6. Actualiza medianas de precio/m² en precios_alquiler_zona

Al terminar, el frontend mostrará precios y m² reales de Habitaclia.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys

# Añadir el directorio backend al path para imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("poblar_habitaclia")


async def aplicar_migracion():
    """Aplica la migración 005 si no se ha aplicado ya."""
    from db.conexion import get_db
    import os

    sql_path = os.path.join(
        os.path.dirname(__file__),
        "..", "db", "migraciones", "005_habitaclia_locales.sql",
    )
    if not os.path.exists(sql_path):
        logger.warning("Migración 005 no encontrada en %s — asegúrate de aplicarla manualmente", sql_path)
        return

    try:
        with open(sql_path) as f:
            sql = f.read()
        async with get_db() as conn:
            await conn.execute(sql)
        logger.info("Migración 005 aplicada correctamente")
    except Exception as exc:
        # Si ya está aplicada, el ALTER TABLE puede fallar — es OK
        logger.info("Migración 005: %s (puede ya estar aplicada)", exc)


async def main():
    parser = argparse.ArgumentParser(
        description="Poblar tabla locales con datos reales de Habitaclia"
    )
    parser.add_argument(
        "--max-paginas",
        type=int,
        default=107,
        help="Páginas del listado de Habitaclia (107 = todas, 5 = prueba rápida)",
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
    logger.info("max_paginas: %d (~%d locales)", args.max_paginas, args.max_paginas * 15)
    logger.info("")

    # 1. Migración
    logger.info("Paso 1/5: Aplicando migración 005...")
    await aplicar_migracion()

    if args.solo_sync:
        # Solo sincronizar staging → locales (si ya se scrapeó antes)
        logger.info("Modo --solo-sync: sincronizando sin scrapear...")
        from pipelines.mercado_inmobiliario import _sincronizar_locales_desde_portales, _limpiar_seed
        sync = await _sincronizar_locales_desde_portales(fuente="habitaclia")
        eliminados = await _limpiar_seed()
        logger.info("Sincronizados: %d | Seed eliminados: %d", sync, eliminados)
        return

    # 2. Pipeline completo
    logger.info("Paso 2/5: Iniciando pipeline Habitaclia...")
    from pipelines.mercado_inmobiliario import ejecutar_habitaclia

    stats = await ejecutar_habitaclia(max_paginas=args.max_paginas)

    logger.info("")
    logger.info("=" * 60)
    logger.info("RESULTADO FINAL")
    logger.info("=" * 60)
    logger.info("  Anuncios scrapeados:           %d", stats.get("scrapeados", 0))
    logger.info("  Guardados en staging:          %d", stats.get("guardados_portales", 0))
    logger.info("  Sincronizados en tabla locales:%d", stats.get("sincronizados_locales", 0))
    logger.info("  Locales de seed eliminados:    %d", stats.get("seed_eliminados", 0))
    logger.info("  Errores:                       %d", stats.get("errores", 0))
    logger.info("")

    if stats.get("sincronizados_locales", 0) > 0:
        logger.info("✅ El frontend ahora mostrará datos reales de Habitaclia.")
        logger.info("   Refresca el mapa para ver los precios actualizados.")
    else:
        logger.warning("⚠️  No se sincronizaron locales. Posibles causas:")
        logger.warning("   - El scraping fue bloqueado (revisar logs de HabitacliaScraper)")
        logger.warning("   - La migración 005 no se aplicó (locales.id demasiado corto)")
        logger.warning("   - Ningún barrio de Habitaclia coincidió con los de la BD")
        logger.warning("   Prueba con: python scripts/poblar_locales_habitaclia.py --solo-sync")


if __name__ == "__main__":
    asyncio.run(main())
