"""
scripts/audit_competencia.py — Auditoría interna de la dimensión Competencia.

Muestra, para una muestra aleatoria de zonas, qué fracción tiene datos de
competencia frescos (`competencia_detalle_zona` actualizada en los últimos N
días) y qué fracción se queda sin filas — proxy de "rincones de BCN donde el
scraper gosom no llega todavía".

Exit code 1 si más del `--umbral-pct` (default 20 %) de zonas no tienen datos
recientes — apto para cron semanal en CI.

Uso:
    python -m scripts.audit_competencia --muestra 200
    python -m scripts.audit_competencia --muestra 200 --dias 7 --umbral-pct 20 --json
    python -m scripts.audit_competencia --sector restauracion
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from collections import Counter
from typing import Optional

from db.conexion import get_db


async def auditar(muestra: int, sector: Optional[str], dias: int) -> dict:
    async with get_db() as conn:
        zonas = await conn.fetch(
            """
            SELECT z.id, b.nombre AS barrio, d.nombre AS distrito
            FROM zonas z
            JOIN barrios b   ON b.id = z.barrio_id
            JOIN distritos d ON d.id = b.distrito_id
            ORDER BY random()
            LIMIT $1
            """,
            muestra,
        )
        zona_ids = [z["id"] for z in zonas]
        if not zona_ids:
            return {"error": "no hay zonas en BD"}

        rows = await conn.fetch(
            """
            SELECT cdz.zona_id, cdz.sector_codigo, cdz.subsector_codigo,
                   cdz.fecha,
                   (cdz.fecha >= CURRENT_DATE - ($3 || ' days')::interval) AS reciente,
                   cdz.num_directos, cdz.hhi_index
            FROM competencia_detalle_zona cdz
            WHERE cdz.zona_id = ANY($1)
              AND ($2::text IS NULL OR cdz.sector_codigo = $2)
            """,
            zona_ids, sector, str(dias),
        )

    total_filas = len(rows)
    zonas_con_datos_recientes = {r["zona_id"] for r in rows if r["reciente"]}
    zonas_sin_filas = len(zona_ids) - len({r["zona_id"] for r in rows})

    hhi_values = [float(r["hhi_index"]) for r in rows if r["hhi_index"] is not None]
    directos_values = [int(r["num_directos"]) for r in rows if r["num_directos"] is not None]

    # Histograma por subsector: cuántas filas son agregado por subsector (útil
    # para ver si la taxonomía fina está realmente llegando al pipeline).
    por_subsector: Counter = Counter(
        r["subsector_codigo"] or "(macro)" for r in rows
    )

    pct_recientes = (
        round(100.0 * len(zonas_con_datos_recientes) / len(zona_ids), 1)
        if zona_ids else 0.0
    )
    pct_sin_filas = round(100.0 * zonas_sin_filas / len(zona_ids), 1) if zona_ids else 0.0

    return {
        "zonas_muestreadas":         len(zona_ids),
        "zonas_con_datos_recientes": len(zonas_con_datos_recientes),
        "zonas_sin_filas":           zonas_sin_filas,
        "total_filas":               total_filas,
        "pct_recientes":             pct_recientes,
        "pct_sin_filas":             pct_sin_filas,
        "hhi_medio":     round(sum(hhi_values) / len(hhi_values), 3) if hhi_values else None,
        "directos_medio": round(sum(directos_values) / len(directos_values), 1) if directos_values else None,
        "por_subsector": dict(por_subsector),
    }


def _imprimir_reporte(r: dict) -> None:
    if r.get("error"):
        print(f"ERROR: {r['error']}", file=sys.stderr)
        return
    print(f"zonas muestreadas:           {r['zonas_muestreadas']}")
    print(f"con datos recientes:         {r['zonas_con_datos_recientes']} ({r['pct_recientes']}%)")
    print(f"sin filas:                   {r['zonas_sin_filas']} ({r['pct_sin_filas']}%)")
    print(f"total filas:                 {r['total_filas']}")
    if r.get("hhi_medio") is not None:
        print(f"HHI medio:                   {r['hhi_medio']}")
    if r.get("directos_medio") is not None:
        print(f"competidores directos medio: {r['directos_medio']}")
    if r.get("por_subsector"):
        print("filas por subsector:")
        for sub, n in sorted(r["por_subsector"].items(), key=lambda x: -x[1])[:15]:
            print(f"  {sub:25}  {n}")


async def _main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--muestra", type=int, default=200)
    parser.add_argument("--sector", default=None)
    parser.add_argument("--dias", type=int, default=7,
                        help="Umbral de 'recientes' (default 7 días)")
    parser.add_argument("--umbral-pct", type=float, default=20.0,
                        help="Exit code 1 si > este %% de zonas sin filas (default 20)")
    parser.add_argument("--json", action="store_true", help="Salida JSON para CI")
    args = parser.parse_args()

    logging.basicConfig(level=logging.WARNING)
    reporte = await auditar(args.muestra, args.sector, args.dias)

    if args.json:
        print(json.dumps(reporte, indent=2, ensure_ascii=False))
    else:
        _imprimir_reporte(reporte)

    return 1 if reporte.get("pct_sin_filas", 0) > args.umbral_pct else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(_main()))
