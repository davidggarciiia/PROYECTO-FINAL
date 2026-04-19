"""
scripts/backfill_subsectores.py — Rellena `negocios_activos.subsector_codigo`
en filas antiguas insertadas antes de la migración 007.

Estrategia:
  - `nombre` de un negocio es una pista débil pero útil ("Pizzería Luigi" →
    subsector=pizzeria aunque no tengamos la categoría textual original).
  - Probamos clasificar con `nombre` + `sector_codigo` como pista y sólo
    escribimos el subsector si:
      1. `clasificar(nombre)` devuelve el MISMO sector ya guardado.
      2. `subsector` no es None.
    Así evitamos sobrescribir con falsos positivos cuando el nombre no da
    señal clara.

Uso:
    python -m scripts.backfill_subsectores              # dry-run (sólo cuenta)
    python -m scripts.backfill_subsectores --apply      # ejecuta UPDATEs
    python -m scripts.backfill_subsectores --apply --batch 1000
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from collections import Counter

from db.conexion import get_db
from scoring.taxonomia import clasificar, SUBSECTORES

logger = logging.getLogger(__name__)


async def backfill(batch: int, apply: bool) -> dict:
    stats = {"escaneadas": 0, "propuestas": 0, "aplicadas": 0,
             "por_subsector": Counter()}
    offset = 0

    async with get_db() as conn:
        while True:
            rows = await conn.fetch("""
                SELECT id, nombre, sector_codigo
                FROM negocios_activos
                WHERE subsector_codigo IS NULL
                  AND sector_codigo IS NOT NULL
                ORDER BY id
                LIMIT $1 OFFSET $2
            """, batch, offset)
            if not rows:
                break

            propuestas: list[tuple[str, str]] = []
            for r in rows:
                stats["escaneadas"] += 1
                sector_infer, subsector_infer = clasificar(r["nombre"])
                if (subsector_infer is not None
                        and sector_infer == r["sector_codigo"]
                        and subsector_infer in SUBSECTORES.get(r["sector_codigo"], ())):
                    propuestas.append((r["id"], subsector_infer))
                    stats["propuestas"] += 1
                    stats["por_subsector"][subsector_infer] += 1

            if apply and propuestas:
                async with conn.transaction():
                    for negocio_id, sub in propuestas:
                        await conn.execute(
                            "UPDATE negocios_activos "
                            "SET subsector_codigo = $1 "
                            "WHERE id = $2 AND subsector_codigo IS NULL",
                            sub, negocio_id)
                        stats["aplicadas"] += 1

            if len(rows) < batch:
                break
            offset += batch

    return {**stats, "por_subsector": dict(stats["por_subsector"])}


async def _main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true",
                        help="Ejecuta los UPDATEs (default: dry-run, sólo imprime)")
    parser.add_argument("--batch", type=int, default=500,
                        help="Filas por lote (default 500)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    reporte = await backfill(batch=args.batch, apply=args.apply)

    modo = "APLICADO" if args.apply else "DRY-RUN"
    print(f"[{modo}]")
    print(f"  escaneadas:   {reporte['escaneadas']}")
    print(f"  propuestas:   {reporte['propuestas']}")
    print(f"  aplicadas:    {reporte['aplicadas']}")
    if reporte["por_subsector"]:
        print("  por subsector:")
        for sub, n in sorted(reporte["por_subsector"].items(), key=lambda x: -x[1]):
            print(f"    {sub:20}  {n}")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(_main()))
