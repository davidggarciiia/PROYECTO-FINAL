"""
scripts/backfill_cerrados.py — Alimenta `negocios_historico` con los cierres
que gosom detecta sin depender del Registre Mercantil.

Fuente:
    `negocios_activos.fecha_cierre_detectada IS NOT NULL`

Destino:
    `negocios_historico` — tabla que alimenta el label del XGBoost (vivió ≥3a).

Criterio:
    - `fecha_apertura` = `created_at` (primera vez que el scraper lo vio).
      Proxy conservador cuando no disponemos de fecha oficial.
    - `fecha_cierre`   = `fecha_cierre_detectada`.
    - `fuente`         = 'google_scrape'.
    - Sólo se inserta si NO existe ya una fila para ese `id` (evita duplicar
      con filas del Registre Mercantil que puedan tener fecha exacta).

Dry-run por defecto (imprime lo que haría sin escribir):
    python -m scripts.backfill_cerrados
    python -m scripts.backfill_cerrados --apply
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from collections import Counter

from db.conexion import get_db

logger = logging.getLogger(__name__)


async def backfill(apply: bool) -> dict:
    stats = {"candidatos": 0, "aplicados": 0, "por_sector": Counter()}

    async with get_db() as conn:
        rows = await conn.fetch("""
            SELECT na.id, na.zona_id, na.sector_codigo,
                   na.created_at::date AS fecha_apertura,
                   na.fecha_cierre_detectada AS fecha_cierre
            FROM negocios_activos na
            WHERE na.fecha_cierre_detectada IS NOT NULL
              AND na.activo = FALSE
              AND NOT EXISTS (
                  SELECT 1 FROM negocios_historico nh WHERE nh.id = na.id
              )
              AND na.created_at IS NOT NULL
              AND na.fecha_cierre_detectada >= na.created_at::date
        """)
        stats["candidatos"] = len(rows)

        if not apply:
            for r in rows[:5]:
                logger.info("[dry-run] %s  %s → %s  sector=%s",
                            r["id"], r["fecha_apertura"], r["fecha_cierre"],
                            r["sector_codigo"])
            for r in rows:
                stats["por_sector"][r["sector_codigo"]] += 1
            return {**stats, "por_sector": dict(stats["por_sector"])}

        async with conn.transaction():
            for r in rows:
                try:
                    await conn.execute("""
                        INSERT INTO negocios_historico
                            (id, zona_id, sector_codigo, fecha_apertura,
                             fecha_cierre, fuente)
                        VALUES ($1, $2, $3, $4, $5, 'google_scrape')
                        ON CONFLICT (id) DO NOTHING
                    """, r["id"], r["zona_id"], r["sector_codigo"],
                        r["fecha_apertura"], r["fecha_cierre"])
                    stats["aplicados"] += 1
                    stats["por_sector"][r["sector_codigo"]] += 1
                except Exception as e:
                    logger.warning("backfill falló id=%s: %s", r["id"], e)

    return {**stats, "por_sector": dict(stats["por_sector"])}


async def _main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true",
                        help="Ejecuta los INSERTs (default: dry-run)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    reporte = await backfill(apply=args.apply)
    modo = "APLICADO" if args.apply else "DRY-RUN"
    print(f"[{modo}]")
    print(f"  candidatos: {reporte['candidatos']}")
    print(f"  aplicados:  {reporte['aplicados']}")
    for sec, n in sorted(reporte["por_sector"].items(), key=lambda x: -x[1]):
        print(f"    {sec:20}  {n}")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(_main()))
