"""
scripts/run_pipeline.py — Runner manual de pipelines con pool reconfigurado.

El pool por defecto (`db.conexion.init_db_pool`) usa `command_timeout=10`,
pensado para el camino interactivo del API. Varios pipelines (HUT con 10k
puntos × 86 zonas, airbnb con 20k listings, competencia batch) necesitan
consultas de >30s. Este runner reinicializa el pool global con un timeout
más generoso antes de despachar al pipeline elegido.

Uso:
    python -m scripts.run_pipeline hut
    python -m scripts.run_pipeline landmarks
    python -m scripts.run_pipeline airbnb
    python -m scripts.run_pipeline booking
    python -m scripts.run_pipeline venues_ocio
    python -m scripts.run_pipeline scores
    python -m scripts.run_pipeline dinamismo
    python -m scripts.run_pipeline resenas_scrape --max-negocios 5 --batch-size 5
"""
from __future__ import annotations

import asyncio
import importlib
import logging
import sys
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("run_pipeline")


# Mapeo nombre → módulo con función `ejecutar()`. Se mantiene sincronizado
# con el whitelist de `api/admin.py` para que el comportamiento sea idéntico.
_PIPELINES: dict[str, str] = {
    "hut":                  "pipelines.turismo.hut",
    "landmarks":            "pipelines.turismo.landmarks",
    "airbnb":               "pipelines.turismo.airbnb",
    "booking":              "pipelines.turismo.booking",
    "venues_ocio":          "pipelines.entorno.venues_ocio",
    "resenas":              "pipelines.entorno.resenas",
    "resenas_scrape":       "pipelines.entorno.resenas_scrape",
    "scores":               "pipelines.scores",
    "dinamismo":            "pipelines.comercio.dinamismo",
    "competencia":          "pipelines.comercio.competencia",
    "seguridad":            "pipelines.entorno.seguridad",
    "transporte":           "pipelines.transporte.transporte",
    "bicing":               "pipelines.transporte.bicing",
    "aforaments":           "pipelines.peatonal.aforaments",
    "demografia":           "pipelines.demografia.demografia",
}


async def _init_pool_slow() -> None:
    """Recrea el pool con `command_timeout=300` (5 min) para análisis batch."""
    import asyncpg
    from config import get_settings
    from db import conexion as _conexion

    if _conexion._pool is not None:
        await _conexion.close_db_pool()

    dsn = get_settings().DATABASE_URL
    _conexion._pool = await asyncpg.create_pool(
        dsn=dsn,
        min_size=2,
        max_size=8,
        command_timeout=300,
        max_inactive_connection_lifetime=300,
        init=_conexion._init_connection,
    )
    logger.info("Pool (batch) inicializado con command_timeout=300s")


def _parse_value(value: str):
    if value.lower() in {"true", "false"}:
        return value.lower() == "true"
    try:
        return int(value)
    except ValueError:
        try:
            return float(value)
        except ValueError:
            return value


def _parse_kwargs(args: list[str]) -> dict:
    kwargs: dict = {}
    idx = 0
    while idx < len(args):
        item = args[idx]
        if not item.startswith("--"):
            idx += 1
            continue
        key = item[2:].replace("-", "_")
        if "=" in key:
            key, raw_value = key.split("=", 1)
            kwargs[key] = _parse_value(raw_value)
            idx += 1
            continue
        if idx + 1 < len(args) and not args[idx + 1].startswith("--"):
            kwargs[key] = _parse_value(args[idx + 1])
            idx += 2
        else:
            kwargs[key] = True
            idx += 1
    return kwargs


async def _main(nombre: str, kwargs: Optional[dict] = None) -> int:
    if nombre not in _PIPELINES:
        logger.error("Pipeline desconocido: %s. Disponibles: %s",
                     nombre, ", ".join(sorted(_PIPELINES)))
        return 2

    modulo_path = _PIPELINES[nombre]
    try:
        mod = importlib.import_module(modulo_path)
    except ImportError as exc:
        logger.error("No se pudo importar %s: %s", modulo_path, exc)
        return 1

    if not hasattr(mod, "ejecutar"):
        logger.error("%s no tiene función ejecutar()", modulo_path)
        return 1

    await _init_pool_slow()
    try:
        res = await mod.ejecutar(**(kwargs or {}))
        logger.info("Pipeline '%s' OK → %s", nombre, res)
    except Exception as exc:
        logger.exception("Pipeline '%s' falló: %s", nombre, exc)
        return 1
    finally:
        from db.conexion import close_db_pool
        await close_db_pool()

    return 0


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        print("Disponibles:", ", ".join(sorted(_PIPELINES)))
        sys.exit(2)
    sys.exit(asyncio.run(_main(sys.argv[1], _parse_kwargs(sys.argv[2:]))))
