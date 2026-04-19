"""
pipelines/turismo/intensitat_oficial.py — Ingesta del ground truth oficial de
intensidad turística de Open Data BCN.

Fuente:
  3 GeoPackage (1 por capa) descargados manualmente a CSV/opendata_bcn/turisme/:
    - 2019_turisme_allotjament.gpkg  (alojamiento turístico)
    - 2017_turisme_atractius.gpkg    (atractivos turísticos)
    - 2016_turisme_oci.gpkg          (ocio)
  Cada uno contiene polígonos (raster vectorizado ~100m) con campo DN (0-100)
  que representa la intensidad turística normalizada en ese punto.

Qué hace:
  Para cada GPKG:
    1. Carga con geopandas (CRS origen: EPSG:25831 / ETRS89 UTM 31N).
    2. Reproyecta a EPSG:4326 (mismo CRS que zonas en nuestra BD).
    3. Inserta en tabla intensitat_turismo_oficial (mig 030) en bloque.

Uso:
  python -m pipelines.turismo.intensitat_oficial
"""
from __future__ import annotations

import logging
import os
from pathlib import Path

import geopandas as gpd
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

# ── Configuración ──────────────────────────────────────────────────────────────
_REPO_ROOT = Path(__file__).resolve().parents[3].parent
_GPKG_DIR  = _REPO_ROOT / "CSV" / "opendata_bcn" / "turisme"

_CAPAS = [
    ("allotjament", 2019, "2019_turisme_allotjament.gpkg"),
    ("atractius",   2017, "2017_turisme_atractius.gpkg"),
    ("oci",         2016, "2016_turisme_oci.gpkg"),
]

_DEFAULT_DB_URL = "postgresql://postgres:password@localhost:5432/georetail"


def cargar_capa(capa: str, anio: int, fname: str, engine) -> int:
    """Lee 1 GPKG, reproyecta a 4326, escribe a la tabla. Devuelve filas insertadas."""
    path = _GPKG_DIR / fname
    if not path.exists():
        raise FileNotFoundError(f"GPKG no encontrado: {path}")

    logger.info("Cargando %s (%s)…", path.name, capa)
    gdf = gpd.read_file(path)

    if gdf.crs is None:
        raise ValueError(f"{path.name}: CRS no definido")
    gdf = gdf.to_crs("EPSG:4326")

    # Adaptar columnas al esquema destino (nuestra tabla usa 'geom', no 'geometry')
    out = gdf[["DN", "geometry"]].copy()
    out.rename(columns={"DN": "intensidad"}, inplace=True)
    out = out.rename_geometry("geom")
    out.insert(0, "anio", anio)
    out.insert(0, "capa", capa)

    # Borrar previo de la misma capa para que la operación sea idempotente
    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM intensitat_turismo_oficial WHERE capa = :c"),
            {"c": capa},
        )

    out.to_postgis(
        "intensitat_turismo_oficial",
        engine,
        if_exists="append",
        index=False,
    )
    return len(out)


def main() -> dict:
    """Carga las 3 capas. Devuelve resumen {capa: rows_inserted}."""
    db_url = os.environ.get("DATABASE_URL", _DEFAULT_DB_URL)
    # geopandas/SQLAlchemy: psycopg2 driver (síncrono)
    engine = create_engine(db_url.replace("postgresql://", "postgresql+psycopg2://"))

    resumen: dict = {}
    for capa, anio, fname in _CAPAS:
        try:
            n = cargar_capa(capa, anio, fname, engine)
            resumen[capa] = n
            logger.info("  OK %s: %d polígonos insertados", capa, n)
        except Exception as exc:
            logger.error("  FAIL %s: %s", capa, exc)
            resumen[capa] = 0

    return resumen


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    res = main()
    print()
    print("Resumen ingesta intensitat_turismo_oficial:")
    for capa, n in res.items():
        print(f"  {capa:15s} -> {n:>6d} polígonos")
