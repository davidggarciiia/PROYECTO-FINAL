"""
clean_all.py — Limpieza y filtrado de CSVs del commit 949f0c3.

Operaciones por fuente:
  - aforaments/       : strip BOM, strip espacios en IDs, todos son Barcelona
  - barcelona_open_data/: strip BOM, ya son datos exclusivos de Barcelona
  - cens_comercial/   : strip BOM, strip whitespace, todos son Barcelona
  - turisme/          : fix coordenadas con coma decimal, todos son Barcelona
  - ine/              : strip BOM, sep ; → ,, filtrar solo 08019 Barcelona,
                        fix separador miles (16.682 → 16682),
                        fix decimal coma (24,7 → 24.7)
  - amb_opendata/     : strip BOM, parsear metadata + tabla, filtrar Barcelona,
                        fix decimal coma, sep ; → ,
  - _meta/            : copiar tal cual (manifiestos JSON, no son CSV)

Salida: CSV/_cleaned/ — misma estructura de directorios.
Originales intactos.
"""
from __future__ import annotations

import csv
import io
import json
import logging
import re
import shutil
from pathlib import Path
from typing import Any

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[2]
CSV_ROOT  = REPO_ROOT / "CSV"
OUT_ROOT  = CSV_ROOT / "_cleaned"

# Municipio Barcelona en ficheros INE
BCN_INE_CODE = "08019"

# ─── helpers ──────────────────────────────────────────────────────────────────

def _read_text(path: Path) -> str:
    """Lee fichero eliminando BOM si existe."""
    return path.read_text(encoding="utf-8-sig")


def _write_csv(dest: Path, rows: list[list[str]], header: list[str]) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    with dest.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)


def _fix_decimal_comma(val: str) -> str:
    """Convierte '24,7' → '24.7' si el campo parece número con coma decimal."""
    v = val.strip()
    # Patron: dígitos, coma, dígitos (sin punto de miles previo)
    if re.match(r'^-?\d+,\d+$', v):
        return v.replace(',', '.')
    return v


def _fix_ine_number(val: str) -> str:
    """
    Convierte notación española de miles en INE:
      '16.682'  → '16682'   (punto como sep de miles, sin decimales)
      '16.682,5'→ '16682.5' (punto miles + coma decimal)
      '24,7'    → '24.7'    (solo decimal con coma)
    """
    v = val.strip()
    if not v:
        return v
    # Caso: tiene punto Y coma → punto=miles, coma=decimal
    if '.' in v and ',' in v:
        v = v.replace('.', '').replace(',', '.')
    # Caso: tiene punto sin coma → punto=miles
    elif '.' in v and ',' not in v:
        # Verificar que es realmente miles (3 dígitos tras el punto)
        parts = v.split('.')
        if all(len(p) <= 3 for p in parts[1:]):
            v = v.replace('.', '')
    # Caso: solo coma → decimal
    elif ',' in v and '.' not in v:
        v = v.replace(',', '.')
    return v


def _strip_id(val: str) -> str:
    return val.strip().strip('"').strip()


# ─── aforaments ───────────────────────────────────────────────────────────────

def clean_aforaments() -> dict[str, Any]:
    src = CSV_ROOT / "aforaments"
    dst = OUT_ROOT / "aforaments"
    stats: dict[str, int] = {}

    for csv_path in sorted(src.glob("*.csv")):
        text = _read_text(csv_path)
        reader = csv.reader(io.StringIO(text))
        rows = list(reader)
        if not rows:
            continue

        header = [h.strip().strip('"') for h in rows[0]]
        cleaned = []
        for row in rows[1:]:
            if not any(row):
                continue
            # Strip espacios y comillas en todos los campos
            clean_row = [c.strip().strip('"').strip() for c in row]
            # Normalizar Id_aforament si existe como primera columna relevante
            try:
                id_idx = header.index("Id_aforament")
                clean_row[id_idx] = clean_row[id_idx].lstrip()
            except ValueError:
                pass
            cleaned.append(clean_row)

        dest = dst / csv_path.name
        dest.parent.mkdir(parents=True, exist_ok=True)
        with dest.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(header)
            w.writerows(cleaned)
        stats[csv_path.name] = len(cleaned)
        logger.info("aforaments/%s → %d filas", csv_path.name, len(cleaned))

    return stats


# ─── barcelona_open_data ──────────────────────────────────────────────────────

def clean_barcelona_open_data() -> dict[str, Any]:
    src = CSV_ROOT / "barcelona_open_data"
    dst = OUT_ROOT / "barcelona_open_data"
    stats: dict[str, int] = {}

    for csv_path in sorted(src.rglob("*.csv")):
        text = _read_text(csv_path)
        reader = csv.reader(io.StringIO(text))
        rows = list(reader)
        if not rows:
            continue

        header = rows[0]
        cleaned = []
        for row in rows[1:]:
            if not any(r.strip() for r in row):
                continue
            # Strip whitespace en cada celda
            clean_row = [c.strip() for c in row]
            cleaned.append(clean_row)

        rel = csv_path.relative_to(src)
        dest = dst / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        with dest.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow([h.strip() for h in header])
            w.writerows(cleaned)
        stats[str(rel)] = len(cleaned)
        logger.info("barcelona_open_data/%s → %d filas", rel, len(cleaned))

    return stats


# ─── cens_comercial ───────────────────────────────────────────────────────────

def clean_cens_comercial() -> dict[str, Any]:
    src = CSV_ROOT / "cens_comercial"
    dst = OUT_ROOT / "cens_comercial"
    stats: dict[str, int] = {}

    for csv_path in sorted(src.glob("*.csv")):
        text = _read_text(csv_path)
        reader = csv.reader(io.StringIO(text))
        rows = list(reader)
        if not rows:
            continue

        header = [h.strip() for h in rows[0]]
        seen: set[tuple] = set()
        cleaned = []
        dupes = 0

        for row in rows[1:]:
            if not any(r.strip() for r in row):
                continue
            clean_row = [c.strip() for c in row]

            # Deduplicacion por fila completa
            key = tuple(clean_row)
            if key in seen:
                dupes += 1
                continue
            seen.add(key)
            cleaned.append(clean_row)

        dest = dst / csv_path.name
        dest.parent.mkdir(parents=True, exist_ok=True)
        with dest.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(header)
            w.writerows(cleaned)
        stats[csv_path.name] = {"rows": len(cleaned), "dupes_removed": dupes}
        logger.info("cens_comercial/%s → %d filas (%d dupes eliminados)", csv_path.name, len(cleaned), dupes)

    return stats


# ─── turisme ──────────────────────────────────────────────────────────────────

def clean_turisme() -> dict[str, Any]:
    src = CSV_ROOT / "turisme"
    dst = OUT_ROOT / "turisme"
    stats: dict[str, int] = {}

    for csv_path in sorted(src.glob("*.csv")):
        text = _read_text(csv_path)
        reader = csv.reader(io.StringIO(text))
        rows = list(reader)
        if not rows:
            continue

        header = [h.strip() for h in rows[0]]

        # Detectar columnas de coordenadas
        coord_cols = {
            i for i, h in enumerate(header)
            if any(k in h.upper() for k in ["LONGITUD", "LATITUD", "COORD", "_X", "_Y"])
        }

        cleaned = []
        for row in rows[1:]:
            if not any(r.strip() for r in row):
                continue
            clean_row = [c.strip() for c in row]
            # Fix coma decimal en coordenadas: "2,17017" → "2.17017"
            for idx in coord_cols:
                if idx < len(clean_row):
                    clean_row[idx] = clean_row[idx].replace(',', '.')
            cleaned.append(clean_row)

        dest = dst / csv_path.name
        dest.parent.mkdir(parents=True, exist_ok=True)
        with dest.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(header)
            w.writerows(cleaned)
        stats[csv_path.name] = len(cleaned)
        logger.info("turisme/%s → %d filas", csv_path.name, len(cleaned))

    return stats


# ─── ine ──────────────────────────────────────────────────────────────────────

def clean_ine() -> dict[str, Any]:
    src = CSV_ROOT / "ine"
    dst = OUT_ROOT / "ine"
    stats: dict[str, Any] = {}

    for csv_path in sorted(src.rglob("*.csv")):
        text = _read_text(csv_path)
        # Separador es ;
        reader = csv.reader(io.StringIO(text), delimiter=";")
        rows = list(reader)
        if not rows:
            continue

        header = [h.strip() for h in rows[0]]
        # Columna municipio
        muni_col = next(
            (i for i, h in enumerate(header) if "municipio" in h.lower()),
            0,
        )

        cleaned = []
        skipped = 0
        for row in rows[1:]:
            if not any(r.strip() for r in row):
                continue
            muni_val = row[muni_col].strip() if muni_col < len(row) else ""
            # Filtrar: solo filas de Barcelona (08019)
            if not muni_val.startswith(BCN_INE_CODE):
                skipped += 1
                continue

            clean_row = []
            for i, val in enumerate(row):
                v = val.strip()
                # Ultima columna "Total" suele tener numeros con formato español
                if i == len(header) - 1 or header[i].lower() in ("total", "valor"):
                    v = _fix_ine_number(v)
                else:
                    v = _fix_decimal_comma(v)
                clean_row.append(v)
            cleaned.append(clean_row)

        rel = csv_path.relative_to(src)
        dest = dst / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        with dest.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(header)
            w.writerows(cleaned)
        stats[str(rel)] = {"rows_bcn": len(cleaned), "rows_skipped": skipped}
        logger.info(
            "ine/%s → %d filas Barcelona (%d otras municipios eliminadas)",
            rel, len(cleaned), skipped,
        )

    return stats


# ─── amb_opendata ─────────────────────────────────────────────────────────────

def _parse_amb_table(text: str) -> tuple[list[str], list[list[str]]]:
    """
    Parsea ficheros AMB que tienen cabecera de metadatos seguida de tabla.
    Detecta la fila de cabecera real (la que contiene ';' y 'Nom Àmbit' o similar).
    """
    lines = text.splitlines()
    header_idx = None
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith("Nom Àmbit") or (
            ";" in stripped and i > 0 and not stripped.startswith("Nom:") and
            not stripped.startswith("ID:") and not stripped.startswith("Desc") and
            not stripped.startswith("Font") and not stripped.startswith("Unitats") and
            not stripped.startswith("Variables") and not stripped.startswith("Última") and
            not stripped.startswith("Nota")
        ):
            header_idx = i
            break

    if header_idx is None:
        return [], []

    table_text = "\n".join(lines[header_idx:])
    reader = csv.reader(io.StringIO(table_text), delimiter=";")
    rows = list(reader)
    if not rows:
        return [], []

    header = [h.strip() for h in rows[0]]
    data = []
    for row in rows[1:]:
        if not any(r.strip() for r in row):
            continue
        data.append([c.strip() for c in row])
    return header, data


def clean_amb_opendata() -> dict[str, Any]:
    src = CSV_ROOT / "amb_opendata"
    dst = OUT_ROOT / "amb_opendata"
    stats: dict[str, Any] = {}

    for csv_path in sorted(src.rglob("*.csv")):
        text = _read_text(csv_path)

        # Detectar si tiene metadatos al inicio (ficheros AMB con cabecera especial)
        has_meta = any(
            text.lstrip().startswith(prefix)
            for prefix in ("Nom:", "﻿Nom:", "row;col")
        )

        if has_meta and csv_path.name != "idescat-aec-15392-1.csv":
            header, data = _parse_amb_table(text)
            if not header:
                logger.warning("amb_opendata/%s: no se pudo parsear tabla", csv_path.relative_to(src))
                continue

            # Filtrar a Barcelona
            bcn_col = 0  # primera columna es "Nom Àmbit"
            bcn_keywords = {"barcelona"}
            cleaned = []
            skipped = 0
            for row in data:
                nom = row[bcn_col].strip().lower() if row else ""
                if nom not in bcn_keywords:
                    skipped += 1
                    continue
                # Fix decimal coma
                clean_row = [_fix_decimal_comma(c) for c in row]
                cleaned.append(clean_row)

            rel = csv_path.relative_to(src)
            dest = dst / rel
            dest.parent.mkdir(parents=True, exist_ok=True)
            with dest.open("w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(header)
                w.writerows(cleaned)
            stats[str(rel)] = {"rows_bcn": len(cleaned), "rows_skipped": skipped}
            logger.info(
                "amb_opendata/%s → %d filas Barcelona (%d otras eliminadas)",
                rel, len(cleaned), skipped,
            )

        else:
            # idescat-aec-15392-1.csv u otros: copiar con fix de BOM y sep
            reader = csv.reader(io.StringIO(text), delimiter=";")
            rows = list(reader)
            if not rows:
                continue
            header = [h.strip() for h in rows[0]]
            cleaned = []
            for row in rows[1:]:
                if not any(r.strip() for r in row):
                    continue
                clean_row = [_fix_decimal_comma(c.strip()) for c in row]
                cleaned.append(clean_row)

            rel = csv_path.relative_to(src)
            dest = dst / rel
            dest.parent.mkdir(parents=True, exist_ok=True)
            with dest.open("w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(header)
                w.writerows(cleaned)
            stats[str(rel)] = len(cleaned)
            logger.info("amb_opendata/%s → %d filas", rel, len(cleaned))

    return stats


# ─── main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    logger.info("=== Iniciando limpieza de CSVs ===")
    logger.info("Fuente : %s", CSV_ROOT)
    logger.info("Destino: %s", OUT_ROOT)

    OUT_ROOT.mkdir(parents=True, exist_ok=True)

    report: dict[str, Any] = {}

    logger.info("--- aforaments ---")
    report["aforaments"] = clean_aforaments()

    logger.info("--- barcelona_open_data ---")
    report["barcelona_open_data"] = clean_barcelona_open_data()

    logger.info("--- cens_comercial ---")
    report["cens_comercial"] = clean_cens_comercial()

    logger.info("--- turisme ---")
    report["turisme"] = clean_turisme()

    logger.info("--- ine ---")
    report["ine"] = clean_ine()

    logger.info("--- amb_opendata ---")
    report["amb_opendata"] = clean_amb_opendata()

    # Totales
    total_files = sum(
        len(v) if isinstance(v, dict) else 1
        for v in report.values()
    )
    logger.info("=== Limpieza completada: %d grupos de ficheros procesados ===", total_files)

    # Guardar reporte de auditoria
    audit_path = OUT_ROOT / "audit_report.json"
    audit_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("Reporte guardado en %s", audit_path)


if __name__ == "__main__":
    main()
