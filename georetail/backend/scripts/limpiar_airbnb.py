"""
scripts/limpiar_airbnb.py — Limpieza del CSV summary de InsideAirbnb.

El volcado público de InsideAirbnb ("summary" listings.csv) trae filas con
lat/lng vacíos, duplicados por id y precios en varios formatos. Este script
lo normaliza con DuckDB (queries SQL sobre el CSV sin cargarlo todo a RAM)
y lo deja gzipped en `$CSV_DIR/turisme/listings.csv.gz`, que es la ruta que
consume `pipelines/turismo/airbnb.py` como fallback local.

Uso (dentro del contenedor backend):

    python -m scripts.limpiar_airbnb /data/csv/_cleaned/turisme/listings_raw.csv
"""
from __future__ import annotations

import gzip
import logging
import os
import sys
from pathlib import Path

import duckdb

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("limpiar_airbnb")


def limpiar(input_csv: Path, output_gz: Path) -> dict:
    """Lee el CSV crudo, aplica filtros de calidad y escribe gzip."""
    if not input_csv.exists():
        raise FileNotFoundError(f"CSV de entrada no existe: {input_csv}")

    output_gz.parent.mkdir(parents=True, exist_ok=True)

    # DuckDB infiere tipos y maneja bien campos con comas dentro de strings
    # citados. Usamos read_csv_auto con all_varchar para preservar los campos
    # textuales (name, license) sin romper por heurísticas de tipo.
    con = duckdb.connect(":memory:")
    con.execute(
        f"""
        CREATE OR REPLACE TABLE raw AS
        SELECT * FROM read_csv_auto(?, all_varchar=TRUE, ignore_errors=TRUE)
        """,
        [str(input_csv)],
    )

    total_raw = con.execute("SELECT COUNT(*) FROM raw").fetchone()[0]

    # Normaliza y filtra:
    # - lat/lng parseables y dentro del bbox razonable de Barcelona
    # - id no vacío
    # - deduplicación por id (nos quedamos con la última aparición)
    con.execute("""
        CREATE OR REPLACE TABLE clean AS
        WITH parsed AS (
            SELECT
                id,
                name,
                host_id,
                neighbourhood,
                TRY_CAST(latitude  AS DOUBLE) AS lat,
                TRY_CAST(longitude AS DOUBLE) AS lng,
                room_type,
                TRY_CAST(
                    REPLACE(REPLACE(REPLACE(COALESCE(price, ''), '$', ''), ',', ''), ' ', '')
                    AS DOUBLE
                ) AS price,
                TRY_CAST(minimum_nights AS INTEGER)   AS minimum_nights,
                TRY_CAST(number_of_reviews AS INTEGER) AS number_of_reviews,
                last_review,
                TRY_CAST(reviews_per_month AS DOUBLE)  AS reviews_per_month,
                TRY_CAST(availability_365 AS INTEGER)  AS availability_365,
                license,
                ROW_NUMBER() OVER (PARTITION BY id ORDER BY last_review DESC NULLS LAST) AS rn
            FROM raw
            WHERE id IS NOT NULL
              AND id <> ''
              AND latitude IS NOT NULL
              AND longitude IS NOT NULL
        )
        SELECT id, name, host_id, neighbourhood,
               -- el pipeline lee "neighbourhood_cleansed" — duplicamos para compat.
               neighbourhood AS neighbourhood_cleansed,
               lat AS latitude, lng AS longitude,
               room_type, price, minimum_nights, number_of_reviews,
               last_review, reviews_per_month, availability_365, license
        FROM parsed
        WHERE rn = 1
          AND lat BETWEEN 41.30 AND 41.50
          AND lng BETWEEN 2.05  AND 2.25
    """)

    stats = con.execute(
        "SELECT COUNT(*) AS n, COUNT(DISTINCT neighbourhood) AS barrios FROM clean"
    ).fetchone()
    total_clean, n_barrios = int(stats[0]), int(stats[1])

    # Export a CSV temporal y comprimimos con gzip (asset que espera el pipeline).
    tmp_csv = output_gz.with_suffix(".csv.tmp")
    con.execute(
        "COPY clean TO ? (FORMAT CSV, HEADER TRUE, QUOTE '\"')",
        [str(tmp_csv)],
    )
    con.close()

    raw_bytes = tmp_csv.read_bytes()
    with gzip.open(output_gz, "wb", compresslevel=6) as gz:
        gz.write(raw_bytes)
    tmp_csv.unlink()

    return {
        "input":        str(input_csv),
        "output":       str(output_gz),
        "total_raw":    total_raw,
        "total_clean":  total_clean,
        "filtradas":    total_raw - total_clean,
        "barrios":      n_barrios,
        "output_bytes": output_gz.stat().st_size,
    }


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print(__doc__)
        return 2

    input_csv = Path(argv[1])
    csv_root = Path(os.environ.get("CSV_DIR", "/data/csv/_cleaned"))
    output_gz = csv_root / "turisme" / "listings.csv.gz"

    try:
        stats = limpiar(input_csv, output_gz)
    except Exception as exc:
        logger.exception("Fallo limpiando Airbnb: %s", exc)
        return 1

    logger.info("Limpieza OK — %s", stats)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
