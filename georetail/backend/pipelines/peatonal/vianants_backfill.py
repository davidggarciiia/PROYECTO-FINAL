"""
pipelines/peatonal/vianants_backfill.py - Backfill histórico mensual de vianants.

Usa la misma fuente que el pipeline oficial de vianants, pero recorre años/meses
para dejar snapshots históricos utilizables por el dataset de entrenamiento.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import logging
from collections import defaultdict
from datetime import date
from pathlib import Path
from typing import Iterable, Optional

from db.conexion import get_db
from pipelines.peatonal import vianants
from pipelines.peatonal.temporalidad import materializar_historico

logger = logging.getLogger(__name__)


def _iter_local_files(from_year: int, to_year: int) -> list[Path]:
    if not vianants._CSV_VIANANTS.exists():
        return []
    files: list[Path] = []
    for path in sorted(vianants._CSV_VIANANTS.glob("*.csv")):
        try:
            year = int(path.name[:4])
        except ValueError:
            year = None
        if year is None or from_year <= year <= to_year:
            files.append(path)
    return files


def _load_local_trams(from_year: int, to_year: int) -> list[dict]:
    trams: list[dict] = []
    for path in _iter_local_files(from_year, to_year):
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                parsed = vianants._normalitzar_registre(row)
                if not parsed or parsed.get("mes") is None:
                    continue
                anyo = int(parsed["any"])
                if from_year <= anyo <= to_year:
                    trams.append(parsed)
    return trams


async def _reset_month(fecha_ref: date) -> None:
    async with get_db() as conn:
        await conn.execute(
            """
            DELETE FROM vz_flujo
            WHERE fecha = $1
              AND fuente = 'vianants_bcn'
            """,
            fecha_ref,
        )
        await conn.execute(
            """
            DELETE FROM variables_zona
            WHERE fecha = $1
              AND fuente = 'vianants_bcn'
            """,
            fecha_ref,
        )
        await conn.execute(
            """
            DELETE FROM vianants_trams
            WHERE any = $1
              AND mes = $2
              AND fuente = 'bcn_vianants'
            """,
            fecha_ref.year,
            fecha_ref.month,
        )


def _group_by_month(trams: Iterable[dict]) -> dict[tuple[int, int], list[dict]]:
    grouped: dict[tuple[int, int], list[dict]] = defaultdict(list)
    for tram in trams:
        month = tram.get("mes")
        if month is None:
            continue
        grouped[(int(tram["any"]), int(month))].append(tram)
    return dict(grouped)


async def ejecutar(
    *,
    from_year: int,
    to_year: int,
    use_ckan: bool = False,
) -> dict[str, int]:
    if from_year > to_year:
        raise ValueError("from_year no puede ser mayor que to_year")

    eid = await vianants._init("vianants_backfill")
    try:
        trams = _load_local_trams(from_year, to_year)
        if not trams and use_ckan:
            rid = await vianants._buscar_dataset_ckan()
            if rid:
                trams = [
                    tram
                    for tram in await vianants._descargar_datos(rid)
                    if tram.get("mes") is not None and from_year <= int(tram["any"]) <= to_year
                ]

        if not trams:
            msg = "Sin trams históricos locales ni descarga CKAN para el rango pedido"
            await vianants._fin(eid, 0, "ok", msg)
            logger.warning(msg)
            return {"trams": 0, "zonas_actualizadas": 0, "meses": 0}

        await vianants._crear_taula_si_no_existeix()

        total_trams = 0
        total_zonas = 0
        months = 0
        for (year, month), month_trams in sorted(_group_by_month(trams).items()):
            fecha_ref = date(year, month, 1)
            await _reset_month(fecha_ref)
            total_trams += await vianants._insertar_trams(month_trams, fecha_ref)
            total_zonas += await vianants._asignar_zonas(month_trams, fecha_ref)
            await vianants._recalcular_totales(fecha_ref)
            months += 1

        temporal_result = await materializar_historico(
            start_year=from_year,
            end_year=to_year,
        )

        await vianants._fin(eid, total_trams, "ok")
        return {
            "trams": total_trams,
            "zonas_actualizadas": total_zonas,
            "meses": months,
            "snapshots_temporales": int(temporal_result.get("snapshots_materializados", 0)),
        }
    except Exception as exc:
        logger.error("Backfill histórico de vianants falló: %s", exc, exc_info=True)
        await vianants._fin(eid, 0, "error", str(exc))
        raise


async def _main() -> None:
    parser = argparse.ArgumentParser(description="Backfill histórico mensual de vianants BCN")
    parser.add_argument("--from-year", type=int, required=True)
    parser.add_argument("--to-year", type=int, required=True)
    parser.add_argument("--use-ckan", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    result = await ejecutar(
        from_year=args.from_year,
        to_year=args.to_year,
        use_ckan=args.use_ckan,
    )
    print(result)


if __name__ == "__main__":
    asyncio.run(_main())
