from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import re
import unicodedata
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import pandas as pd

from db.conexion import get_db
from scoring.infra.governance import dataset_intended_demography_features

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[3]
CSV_ROOT = Path(os.environ.get("CSV_DIR", str(REPO_ROOT / "CSV")))
MANIFEST_PATH = CSV_ROOT / "_meta" / "public_data_manifest_latest.json"
BACKFILL_ROOT = CSV_ROOT / "_meta" / "demografia_backfill"
LATEST_ROOT = BACKFILL_ROOT / "latest"
BACKFILL_SOURCE = "demografia_local_manifest"

CORE_SPATIAL_DATASET_IDS: tuple[str, ...] = (
    "renda-disponible-llars-bcn",
    "atles-renda-index-gini",
    "atles-renda-p80-p20-distribucio",
    "pad_mdbas_edat-q",
    "pad_mdbas_niv-educa-esta_sexe",
    "pad_mdb_nacionalitat-g_edat-q_sexe",
    "pad_dom_mdbas_n-persones",
    "pad_dom_mdbas_edat-0018",
    "pad_sol_mdb_sexe_edat-q",
    "atles-renda-mitjana",
    "atles-renda-mediana",
)
LEGACY_BCN_BATCH_1_IDS: tuple[str, ...] = (
    "atles-renda-index-gini",
    "atles-renda-p80-p20-distribucio",
    "pad_dom_mdbas_n-persones",
    "pad_dom_mdbas_edat-0018",
    "pad_sol_mdb_sexe_edat-q",
)
SUPPORT_CITYWIDE_DATASET_IDS: tuple[str, ...] = (
    "adrh_barcelona_renta",
    "adrh_barcelona_desigualdad",
    "rfdbc",
    "afi",
    "irpf",
)

SECTION_POPULATION_DATASET_ID = "pad_mdbas_edat-q"
AGE_CODE_TO_MIDPOINT: dict[int, float] = {
    **{index: index * 5.0 + 2.5 for index in range(20)},
    20: 102.5,
}
AGE_25_44_CODES = {5, 6, 7, 8}
HIGH_EDUCATION_CODES = {5, 6}
BARCELONA_MATCH = "barcelona"
VALUE_COLUMNS = ("valor", "Valor", "Total")
CITY_COLUMNS = ("municipi", "municipio", "Municipios", "municipios")
YEAR_COLUMNS = ("any", "Any", "Periodo", "Data_Referencia", "mes")
MISSING_TOKENS = {"", "..", "...", "null", "nan", "na", "n/d"}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso_now() -> str:
    return _utc_now().isoformat().replace("+00:00", "Z")


def _normalize_text(value: Any) -> str:
    raw = str(value or "").strip().lower()
    normalized = unicodedata.normalize("NFKD", raw)
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def _resolve_local_path(raw_path: str | None) -> Path | None:
    if not raw_path:
        return None
    normalized = raw_path.replace("\\", "/")
    candidate = Path(normalized)
    if candidate.is_absolute():
        return candidate
    return REPO_ROOT / candidate


def _load_manifest(path: Path = MANIFEST_PATH) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _entry_by_dataset_id(manifest: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        entry["dataset_id"]: entry
        for entry in manifest.get("datasets") or []
        if isinstance(entry, Mapping) and entry.get("dataset_id")
    }


def _file_year(file_entry: Mapping[str, Any]) -> int | None:
    years = file_entry.get("years") or []
    for value in reversed(years):
        try:
            return int(str(value)[:4])
        except (TypeError, ValueError):
            continue
    local_path = str(file_entry.get("local_path") or "")
    match = re.search(r"(19|20)\d{2}", local_path)
    if match:
        return int(match.group(0))
    return None


def _dataset_cleaned_dir(dataset_id: str) -> Path | None:
    cleaned_root = CSV_ROOT / "_cleaned"
    candidates = [
        path
        for path in cleaned_root.rglob("*")
        if path.is_dir() and path.name == dataset_id
    ]
    if not candidates:
        return None
    candidates.sort(key=lambda path: len(path.parts))
    return candidates[0]


def _parse_year_value(value: Any) -> int | None:
    text = str(value or "").strip()
    if not text:
        return None
    match = re.search(r"(19|20)\d{2}", text)
    if match:
        return int(match.group(0))
    return None


def _extract_year(frame: pd.DataFrame, path: Path) -> int:
    for column in YEAR_COLUMNS:
        if column not in frame.columns or frame.empty:
            continue
        value = str(frame[column].iloc[0]).strip()
        year = _parse_year_value(value)
        if year is not None:
            return year
    match = re.search(r"(19|20)\d{2}", path.stem)
    if match:
        return int(match.group(0))
    raise ValueError(f"No se pudo inferir el año para {path}")


def _select_cleaned_dataset_files(
    dataset_id: str,
    *,
    years_mode: str,
    keep_history_years: int = 0,
) -> list[tuple[int, Path]]:
    dataset_dir = _dataset_cleaned_dir(dataset_id)
    if dataset_dir is None or not dataset_dir.exists():
        return []

    selected: list[tuple[int, Path]] = []
    for path in sorted(dataset_dir.glob("*.csv")):
        match = re.search(r"(19|20)\d{2}", path.stem)
        if not match:
            continue
        selected.append((int(match.group(0)), path))

    selected.sort(key=lambda item: item[0])
    if years_mode == "latest" and selected:
        keep = 1 + max(keep_history_years, 0)
        return selected[-keep:]
    return selected


def _select_manifest_dataset_files(
    entry: Mapping[str, Any],
    *,
    years_mode: str,
    allowed_years: set[int] | None = None,
) -> list[tuple[int, Path]]:
    preferred = [file_entry for file_entry in entry.get("files") or [] if file_entry.get("preferred")]
    candidates = preferred or list(entry.get("files") or [])
    selected: list[tuple[int, Path]] = []

    for file_entry in candidates:
        year = _file_year(file_entry)
        path = _resolve_local_path(file_entry.get("local_path"))
        if year is None or path is None or not path.exists():
            continue
        if allowed_years is not None and year not in allowed_years:
            continue
        selected.append((year, path))

    selected.sort(key=lambda item: item[0])
    if years_mode == "latest" and selected:
        return [selected[-1]]
    return selected


def _read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(
        path,
        dtype=str,
        encoding="utf-8",
        encoding_errors="ignore",
        keep_default_na=False,
    )


def _normalize_barri_code(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip().str.replace(".0", "", regex=False).str.zfill(6)


def _to_numeric(series: pd.Series) -> pd.Series:
    cleaned = series.astype(str).str.strip()
    cleaned = cleaned.mask(cleaned.str.lower().isin(MISSING_TOKENS))
    cleaned = cleaned.str.replace(",", ".", regex=False)
    return pd.to_numeric(cleaned, errors="coerce")


def _section_sequence(series: pd.Series) -> pd.Series:
    return _to_numeric(series).astype("Int64").mod(1000)


async def _load_barri_context() -> tuple[dict[str, list[str]], dict[str, float]]:
    async with get_db() as conn:
        rows = await conn.fetch(
            """
            SELECT
                z.id AS zona_id,
                LPAD(COALESCE(b.codigo::text, ''), 6, '0') AS barri_code,
                ST_Area(z.geometria::geography) / 1e6 AS area_km2
            FROM zonas z
            JOIN barrios b ON b.id = z.barrio_id
            JOIN distritos d ON d.id = b.distrito_id
            JOIN ciudades c ON c.id = d.ciudad_id
            WHERE LOWER(c.nombre) = 'barcelona'
            """
        )

    zone_map: dict[str, list[str]] = defaultdict(list)
    area_by_barri: dict[str, float] = defaultdict(float)
    for row in rows:
        barri_code = str(row["barri_code"]).zfill(6)
        zone_map[barri_code].append(str(row["zona_id"]))
        area_by_barri[barri_code] += float(row["area_km2"] or 0.0)
    return dict(zone_map), dict(area_by_barri)


def _build_records_from_metric_frame(
    metric_frame: pd.DataFrame,
    *,
    feature_name: str,
    metric_date: date,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in metric_frame.itertuples(index=False):
        rows.append(
            {
                "barri_code": str(row.barri_code),
                "fecha": metric_date,
                "values": {feature_name: round(float(row.metric), 6)},
            }
        )
    return rows


def _prepare_weighted_barri_frame(
    frame: pd.DataFrame,
    *,
    value_column: str,
    weights_by_section: Mapping[tuple[str, int], float] | None,
) -> tuple[pd.DataFrame, bool]:
    working = frame.loc[:, ["Codi_Barri", "Seccio_Censal", value_column]].copy()
    working["barri_code"] = _normalize_barri_code(working["Codi_Barri"])
    working["section_seq"] = _section_sequence(working["Seccio_Censal"])
    working["value_num"] = _to_numeric(working[value_column])
    working = working.dropna(subset=["section_seq", "value_num"])
    if working.empty:
        return pd.DataFrame(columns=["barri_code", "metric"]), True

    if not weights_by_section:
        grouped = working.groupby("barri_code", as_index=False)["value_num"].mean()
        grouped = grouped.rename(columns={"value_num": "metric"})
        return grouped, True

    working["weight"] = [
        weights_by_section.get((str(barri), int(section)))
        for barri, section in zip(working["barri_code"], working["section_seq"])
    ]

    fallback_used = False
    rows: list[dict[str, Any]] = []
    for barri_code, group in working.groupby("barri_code"):
        valid_weights = group["weight"].notna()
        if valid_weights.all() and float(group.loc[valid_weights, "weight"].sum()) > 0:
            weights = group["weight"].astype(float)
            metric = float((group["value_num"] * weights).sum() / weights.sum())
        else:
            fallback_used = True
            metric = float(group["value_num"].mean())
        rows.append({"barri_code": barri_code, "metric": metric})
    return pd.DataFrame(rows), fallback_used


def _prepare_tamano_hogar_frame(frame: pd.DataFrame) -> pd.DataFrame:
    working = frame.loc[:, ["Codi_Barri", "Valor", "N_PERSONES_AGG"]].copy()
    working["barri_code"] = _normalize_barri_code(working["Codi_Barri"])
    working["hogares"] = _to_numeric(working["Valor"])
    working["personas"] = _to_numeric(working["N_PERSONES_AGG"])
    working = working.dropna(subset=["hogares", "personas"])
    if working.empty:
        return pd.DataFrame(columns=["barri_code", "metric"])

    rows: list[dict[str, Any]] = []
    for barri_code, group in working.groupby("barri_code"):
        hogares_total = float(group["hogares"].sum())
        if hogares_total <= 0:
            continue
        metric = float((group["hogares"] * group["personas"]).sum() / hogares_total)
        rows.append({"barri_code": barri_code, "metric": metric})
    return pd.DataFrame(rows)


def _dom_00_18_convention_consistent(samples: Sequence[pd.DataFrame]) -> bool:
    normalized_sets: list[tuple[int, ...]] = []
    for frame in samples:
        values = sorted(
            {
                int(value)
                for value in _to_numeric(frame.get("DOM_00_18", pd.Series(dtype=str))).dropna().astype(int).tolist()
            }
        )
        if not values:
            continue
        normalized_sets.append(tuple(values))

    if not normalized_sets:
        return False

    first = normalized_sets[0]
    return (
        all(current == first for current in normalized_sets)
        and 1 in first
        and any(value > 1 for value in first)
    )


def _prepare_hogares_con_menores_frame(frame: pd.DataFrame) -> pd.DataFrame:
    working = frame.loc[:, ["Codi_Barri", "Valor", "DOM_00_18"]].copy()
    working["barri_code"] = _normalize_barri_code(working["Codi_Barri"])
    working["hogares"] = _to_numeric(working["Valor"])
    working["dom_code"] = _to_numeric(working["DOM_00_18"])
    working = working.dropna(subset=["hogares", "dom_code"])
    if working.empty:
        return pd.DataFrame(columns=["barri_code", "metric"])

    rows: list[dict[str, Any]] = []
    for barri_code, group in working.groupby("barri_code"):
        hogares_total = float(group["hogares"].sum())
        if hogares_total <= 0:
            continue
        sin_menores = float(group.loc[group["dom_code"] == 1, "hogares"].sum())
        metric = max(0.0, min(1.0, (hogares_total - sin_menores) / hogares_total))
        rows.append({"barri_code": barri_code, "metric": metric})
    return pd.DataFrame(rows)


def _prepare_personas_solas_frame(
    frame: pd.DataFrame,
    *,
    population_by_barri: Mapping[str, float],
) -> tuple[pd.DataFrame, int]:
    working = frame.loc[:, ["Codi_Barri", "Valor"]].copy()
    working["barri_code"] = _normalize_barri_code(working["Codi_Barri"])
    working["valor_num"] = _to_numeric(working["Valor"])
    working = working.dropna(subset=["valor_num"])
    if working.empty:
        return pd.DataFrame(columns=["barri_code", "metric"]), 0

    grouped = working.groupby("barri_code", as_index=False)["valor_num"].sum()
    rows: list[dict[str, Any]] = []
    missing_population = 0
    for row in grouped.itertuples(index=False):
        denom = float(population_by_barri.get(str(row.barri_code), 0.0))
        if denom <= 0:
            missing_population += 1
            continue
        metric = max(0.0, min(1.0, float(row.valor_num) / denom))
        rows.append({"barri_code": str(row.barri_code), "metric": metric})
    return pd.DataFrame(rows), 0 if not rows else missing_population


def _prepare_section_population_support(
    files: Sequence[tuple[int, Path]],
    *,
    area_by_barri: Mapping[str, float],
) -> tuple[
    list[dict[str, Any]],
    dict[int, dict[tuple[str, int], float]],
    dict[int, dict[str, float]],
]:
    section_population: dict[int, dict[tuple[str, int], float]] = {}
    barri_population: dict[int, dict[str, float]] = {}
    records: list[dict[str, Any]] = []

    for _, path in files:
        frame = _read_csv(path)
        year = _extract_year(frame, path)
        if not {"Codi_Barri", "Seccio_Censal", "Valor", "EDAT_Q"}.issubset(frame.columns):
            continue

        working = frame.loc[:, ["Codi_Barri", "Seccio_Censal", "Valor", "EDAT_Q"]].copy()
        working["barri_code"] = _normalize_barri_code(working["Codi_Barri"])
        working["section_seq"] = _section_sequence(working["Seccio_Censal"])
        working["valor_num"] = _to_numeric(working["Valor"])
        working["age_code"] = _to_numeric(working["EDAT_Q"]).astype("Int64")
        working["age_midpoint"] = working["age_code"].map(AGE_CODE_TO_MIDPOINT)
        working = working.dropna(subset=["section_seq", "valor_num"])
        if working.empty:
            continue

        grouped_section = (
            working.groupby(["barri_code", "section_seq"], as_index=False)["valor_num"]
            .sum()
        )
        section_population[year] = {
            (str(row.barri_code), int(row.section_seq)): float(row.valor_num)
            for row in grouped_section.itertuples(index=False)
        }

        grouped_barri = working.groupby("barri_code", as_index=False)["valor_num"].sum()
        barri_population[year] = {
            str(row.barri_code): float(row.valor_num)
            for row in grouped_barri.itertuples(index=False)
        }

        for barri_code, group in working.groupby("barri_code"):
            poblacion = float(group["valor_num"].sum())
            if poblacion <= 0:
                continue

            edad_media = None
            valid_age = group["age_midpoint"].notna()
            if valid_age.any():
                age_weights = group.loc[valid_age, "valor_num"].astype(float)
                edad_media = float(
                    (group.loc[valid_age, "age_midpoint"].astype(float) * age_weights).sum()
                    / age_weights.sum()
                )

            pct_25_44 = float(
                group.loc[group["age_code"].isin(list(AGE_25_44_CODES)), "valor_num"].sum() / poblacion
            )
            densidad = None
            area_km2 = float(area_by_barri.get(str(barri_code), 0.0))
            if area_km2 > 0:
                densidad = poblacion / area_km2

            values: dict[str, float] = {
                "poblacion": round(poblacion, 6),
                "pct_poblacio_25_44": round(pct_25_44, 6),
            }
            if edad_media is not None:
                values["edad_media"] = round(edad_media, 6)
            if densidad is not None:
                values["densidad_hab_km2"] = round(densidad, 6)

            records.append(
                {
                    "barri_code": str(barri_code),
                    "fecha": date(year, 1, 1),
                    "values": values,
                }
            )

    return records, section_population, barri_population


def _prepare_pct_extranjeros_frame(frame: pd.DataFrame) -> pd.DataFrame:
    working = frame.loc[:, ["Codi_Barri", "Valor", "NACIONALITAT_G"]].copy()
    working["barri_code"] = _normalize_barri_code(working["Codi_Barri"])
    working["valor_num"] = _to_numeric(working["Valor"])
    working["nat_code"] = _to_numeric(working["NACIONALITAT_G"]).astype("Int64")
    working = working.dropna(subset=["valor_num", "nat_code"])
    if working.empty:
        return pd.DataFrame(columns=["barri_code", "metric"])

    rows: list[dict[str, Any]] = []
    for barri_code, group in working.groupby("barri_code"):
        total = float(group["valor_num"].sum())
        if total <= 0:
            continue
        extranjeros = float(group.loc[group["nat_code"] != 1, "valor_num"].sum())
        rows.append({"barri_code": barri_code, "metric": max(0.0, min(1.0, extranjeros / total))})
    return pd.DataFrame(rows)


def _prepare_nivel_estudios_frame(frame: pd.DataFrame) -> pd.DataFrame:
    working = frame.loc[:, ["Codi_Barri", "Valor", "NIV_EDUCA_esta"]].copy()
    working["barri_code"] = _normalize_barri_code(working["Codi_Barri"])
    working["valor_num"] = _to_numeric(working["Valor"])
    working["edu_code"] = _to_numeric(working["NIV_EDUCA_esta"]).astype("Int64")
    working = working.dropna(subset=["valor_num", "edu_code"])
    if working.empty:
        return pd.DataFrame(columns=["barri_code", "metric"])

    rows: list[dict[str, Any]] = []
    for barri_code, group in working.groupby("barri_code"):
        total = float(group["valor_num"].sum())
        if total <= 0:
            continue
        estudios_altos = float(group.loc[group["edu_code"].isin(list(HIGH_EDUCATION_CODES)), "valor_num"].sum())
        rows.append({"barri_code": barri_code, "metric": max(0.0, min(1.0, estudios_altos / total))})
    return pd.DataFrame(rows)


def _metric_records_from_frame_per_year(
    files: Sequence[tuple[int, Path]],
    *,
    frame_builder,
    feature_name: str,
    metric_month: int = 1,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for _, path in files:
        frame = _read_csv(path)
        year = _extract_year(frame, path)
        metric_frame = frame_builder(frame)
        records.extend(
            _build_records_from_metric_frame(
                metric_frame,
                feature_name=feature_name,
                metric_date=date(year, metric_month, 1),
            )
        )
    return records


def _weighted_metric_records_and_map(
    files: Sequence[tuple[int, Path]],
    *,
    feature_name: str,
    value_column: str,
    metric_month: int,
    weights_by_section: Mapping[int, Mapping[tuple[str, int], float]],
    warning_counter: Counter[str],
) -> tuple[list[dict[str, Any]], dict[int, dict[str, float]]]:
    records: list[dict[str, Any]] = []
    metrics_by_year: dict[int, dict[str, float]] = {}

    for _, path in files:
        frame = _read_csv(path)
        year = _extract_year(frame, path)
        metric_frame, fallback_used = _prepare_weighted_barri_frame(
            frame,
            value_column=value_column,
            weights_by_section=weights_by_section.get(year),
        )
        if fallback_used:
            warning_counter[f"{feature_name}:fallback_simple_mean:{year}"] += 1

        metrics_by_year[year] = {
            str(row.barri_code): float(row.metric)
            for row in metric_frame.itertuples(index=False)
        }
        records.extend(
            _build_records_from_metric_frame(
                metric_frame,
                feature_name=feature_name,
                metric_date=date(year, metric_month, 1),
            )
        )

    return records, metrics_by_year


def _delta_renta_records(
    renta_hogar_by_year: Mapping[int, Mapping[str, float]],
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for year in sorted(renta_hogar_by_year):
        previous_year = year - 3
        if previous_year not in renta_hogar_by_year:
            continue
        current_values = renta_hogar_by_year[year]
        previous_values = renta_hogar_by_year[previous_year]
        for barri_code, current in current_values.items():
            previous = float(previous_values.get(barri_code, 0.0))
            if previous <= 0:
                continue
            delta = (float(current) - previous) / previous
            records.append(
                {
                    "barri_code": barri_code,
                    "fecha": date(year, 6, 1),
                    "values": {"delta_renta_3a": round(delta, 6)},
                }
            )
    return records


def _merge_records(records: Iterable[dict[str, Any]]) -> dict[tuple[str, date], dict[str, Any]]:
    merged: dict[tuple[str, date], dict[str, Any]] = {}
    for record in records:
        key = (record["barri_code"], record["fecha"])
        bucket = merged.setdefault(
            key,
            {"barri_code": record["barri_code"], "fecha": record["fecha"], "values": {}},
        )
        bucket["values"].update(record["values"])
    return merged


async def _persist_records(
    merged_records: Mapping[tuple[str, date], Mapping[str, Any]],
    *,
    zone_map: Mapping[str, Sequence[str]],
) -> tuple[int, dict[str, float]]:
    values_by_feature: dict[str, list[float]] = defaultdict(list)
    rows_written = 0

    async with get_db() as conn:
        async with conn.transaction():
            for record in merged_records.values():
                barri_code = str(record["barri_code"])
                zona_ids = list(zone_map.get(barri_code, ()))
                if not zona_ids:
                    continue

                values = record["values"]
                for zona_id in zona_ids:
                    await conn.execute(
                        """
                        INSERT INTO variables_zona (zona_id, fecha, fuente)
                        VALUES ($1, $2, $3)
                        ON CONFLICT (zona_id, fecha) DO UPDATE
                        SET fuente = EXCLUDED.fuente,
                            updated_at = NOW()
                        """,
                        zona_id,
                        record["fecha"],
                        BACKFILL_SOURCE,
                    )
                    await conn.execute(
                        """
                        INSERT INTO vz_demografia (
                            zona_id,
                            fecha,
                            poblacion,
                            edad_media,
                            renta_media_hogar,
                            renta_media_uc,
                            renta_mediana_uc,
                            pct_extranjeros,
                            densidad_hab_km2,
                            nivel_estudios_alto_pct,
                            pct_poblacio_25_44,
                            delta_renta_3a,
                            gini,
                            p80_p20,
                            tamano_hogar,
                            hogares_con_menores,
                            personas_solas,
                            fuente
                        )
                        VALUES (
                            $1, $2, $3, $4, $5, $6, $7, $8, $9,
                            $10, $11, $12, $13, $14, $15, $16, $17, $18
                        )
                        ON CONFLICT (zona_id, fecha) DO UPDATE
                        SET poblacion = COALESCE(EXCLUDED.poblacion, vz_demografia.poblacion),
                            edad_media = COALESCE(EXCLUDED.edad_media, vz_demografia.edad_media),
                            renta_media_hogar = COALESCE(EXCLUDED.renta_media_hogar, vz_demografia.renta_media_hogar),
                            renta_media_uc = COALESCE(EXCLUDED.renta_media_uc, vz_demografia.renta_media_uc),
                            renta_mediana_uc = COALESCE(EXCLUDED.renta_mediana_uc, vz_demografia.renta_mediana_uc),
                            pct_extranjeros = COALESCE(EXCLUDED.pct_extranjeros, vz_demografia.pct_extranjeros),
                            densidad_hab_km2 = COALESCE(EXCLUDED.densidad_hab_km2, vz_demografia.densidad_hab_km2),
                            nivel_estudios_alto_pct = COALESCE(EXCLUDED.nivel_estudios_alto_pct, vz_demografia.nivel_estudios_alto_pct),
                            pct_poblacio_25_44 = COALESCE(EXCLUDED.pct_poblacio_25_44, vz_demografia.pct_poblacio_25_44),
                            delta_renta_3a = COALESCE(EXCLUDED.delta_renta_3a, vz_demografia.delta_renta_3a),
                            gini = COALESCE(EXCLUDED.gini, vz_demografia.gini),
                            p80_p20 = COALESCE(EXCLUDED.p80_p20, vz_demografia.p80_p20),
                            tamano_hogar = COALESCE(EXCLUDED.tamano_hogar, vz_demografia.tamano_hogar),
                            hogares_con_menores = COALESCE(EXCLUDED.hogares_con_menores, vz_demografia.hogares_con_menores),
                            personas_solas = COALESCE(EXCLUDED.personas_solas, vz_demografia.personas_solas),
                            fuente = EXCLUDED.fuente,
                            updated_at = NOW()
                        """,
                        zona_id,
                        record["fecha"],
                        values.get("poblacion"),
                        values.get("edad_media"),
                        values.get("renta_media_hogar"),
                        values.get("renta_media_uc"),
                        values.get("renta_mediana_uc"),
                        values.get("pct_extranjeros"),
                        values.get("densidad_hab_km2"),
                        values.get("nivel_estudios_alto_pct"),
                        values.get("pct_poblacio_25_44"),
                        values.get("delta_renta_3a"),
                        values.get("gini"),
                        values.get("p80_p20"),
                        values.get("tamano_hogar"),
                        values.get("hogares_con_menores"),
                        values.get("personas_solas"),
                        BACKFILL_SOURCE,
                    )
                    rows_written += 1
                    for feature, value in values.items():
                        if value is not None:
                            values_by_feature[feature].append(float(value))

    feature_means = {
        feature: round(sum(values) / len(values), 6)
        for feature, values in values_by_feature.items()
        if values
    }
    return rows_written, feature_means


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def _publish_artifact(run_dir: Path, name: str, payload: Mapping[str, Any]) -> None:
    _write_json(run_dir / name, payload)
    _write_json(LATEST_ROOT / name, payload)


def _filter_barcelona_rows(frame: pd.DataFrame) -> pd.DataFrame:
    for column in CITY_COLUMNS:
        if column not in frame.columns:
            continue
        normalized = frame[column].map(_normalize_text)
        mask = normalized.str.contains(BARCELONA_MATCH, na=False) | normalized.str.contains("08019", na=False)
        filtered = frame.loc[mask].copy()
        if not filtered.empty:
            return filtered
    return frame.iloc[0:0].copy()


def _prefer_total_rows(frame: pd.DataFrame, label_columns: Sequence[str]) -> pd.DataFrame:
    reduced = frame.copy()
    for column in label_columns:
        normalized = reduced[column].map(_normalize_text)
        mask = normalized.eq("total")
        if mask.any():
            reduced = reduced.loc[mask].copy()
    return reduced


def _extract_support_year(frame: pd.DataFrame, path: Path) -> pd.Series:
    for column in YEAR_COLUMNS:
        if column not in frame.columns:
            continue
        years = frame[column].map(_parse_year_value)
        if years.notna().any():
            return years
    file_year = _parse_year_value(path.stem)
    return pd.Series([file_year] * len(frame), index=frame.index, dtype="Int64")


def _extract_support_metrics(
    dataset_id: str,
    files: Sequence[tuple[int, Path]],
    *,
    intended_features: Sequence[str],
) -> dict[str, Any]:
    by_year: dict[str, dict[str, Any]] = {}

    for _, path in files:
        try:
            frame = _read_csv(path)
        except Exception as exc:
            logger.debug("No se pudo leer soporte %s: %s", path, exc)
            continue

        filtered = _filter_barcelona_rows(frame)
        if filtered.empty:
            continue

        value_column = next((column for column in VALUE_COLUMNS if column in filtered.columns), None)
        if value_column is None:
            continue

        filtered["__year"] = _extract_support_year(filtered, path)
        filtered["__value"] = _to_numeric(filtered[value_column])
        filtered = filtered.dropna(subset=["__year", "__value"])
        if filtered.empty:
            continue

        label_columns = [
            column
            for column in filtered.columns
            if column not in {
                *CITY_COLUMNS,
                *YEAR_COLUMNS,
                *VALUE_COLUMNS,
                "estat",
                "__year",
                "__value",
            }
        ]

        for year, group in filtered.groupby("__year"):
            preferred = _prefer_total_rows(group, label_columns)
            bucket = preferred if not preferred.empty else group
            sample_labels = []
            for row in bucket.head(5).itertuples(index=False):
                label = " | ".join(
                    str(getattr(row, column)).strip()
                    for column in label_columns
                    if getattr(row, column, None) not in (None, "")
                )
                if label:
                    sample_labels.append(label)
            year_key = str(int(year))
            current = by_year.setdefault(
                year_key,
                {
                    "rows": 0,
                    "value_sum": 0.0,
                    "value_mean": 0.0,
                    "sample_labels": [],
                    "sample_files": [],
                },
            )
            current["rows"] += int(len(bucket))
            current["value_sum"] = round(float(current["value_sum"]) + float(bucket["__value"].sum()), 6)
            current["value_mean"] = round(float(current["value_sum"]) / max(int(current["rows"]), 1), 6)
            current["sample_labels"] = list(dict.fromkeys([*current["sample_labels"], *sample_labels]))[:10]
            current["sample_files"] = list(dict.fromkeys([*current["sample_files"], str(path)]))[:10]

    return {
        "dataset_id": dataset_id,
        "intended_features": list(intended_features),
        "years": by_year,
    }


def _build_support_citywide_metrics(
    entries: Mapping[str, Mapping[str, Any]],
    *,
    years_mode: str,
) -> dict[str, Any]:
    datasets: list[dict[str, Any]] = []
    for dataset_id in SUPPORT_CITYWIDE_DATASET_IDS:
        entry = entries.get(dataset_id)
        if not entry:
            continue
        files = _select_manifest_dataset_files(entry, years_mode=years_mode)
        datasets.append(
            _extract_support_metrics(
                dataset_id,
                files,
                intended_features=dataset_intended_demography_features(entry),
            )
        )

    return {
        "generated_at": _iso_now(),
        "datasets": datasets,
    }


def _scope_dataset_ids(scope: str) -> tuple[set[str], bool]:
    if scope == "core_spatial":
        return set(CORE_SPATIAL_DATASET_IDS), False
    if scope == "support_citywide":
        return set(), True
    if scope == "full":
        return set(CORE_SPATIAL_DATASET_IDS), True
    if scope == "bcn_batch_1":
        return set(LEGACY_BCN_BATCH_1_IDS), False
    raise ValueError(f"Scope no soportado: {scope}")


async def ejecutar_backfill(
    *,
    scope: str = "full",
    years_mode: str = "all",
    manifest_path: Path = MANIFEST_PATH,
) -> dict[str, Any]:
    manifest = _load_manifest(manifest_path)
    entries = _entry_by_dataset_id(manifest)
    warning_counter: Counter[str] = Counter()
    spatial_scope_dataset_ids, include_support = _scope_dataset_ids(scope)

    zone_map, area_by_barri = await _load_barri_context()

    raw_records: list[dict[str, Any]] = []
    dataset_summary: dict[str, Any] = {}

    if spatial_scope_dataset_ids:
        padro_files = _select_cleaned_dataset_files(
            SECTION_POPULATION_DATASET_ID,
            years_mode=years_mode,
        )
        population_records, section_population_by_year, barri_population_by_year = _prepare_section_population_support(
            padro_files,
            area_by_barri=area_by_barri,
        )
        if SECTION_POPULATION_DATASET_ID in spatial_scope_dataset_ids:
            raw_records.extend(population_records)
            dataset_summary[SECTION_POPULATION_DATASET_ID] = {
                "files": [str(path) for _, path in padro_files],
                "records": len(population_records),
                "features": ["poblacion", "edad_media", "pct_poblacio_25_44", "densidad_hab_km2"],
            }

        if "pad_mdb_nacionalitat-g_edat-q_sexe" in spatial_scope_dataset_ids:
            files = _select_cleaned_dataset_files("pad_mdb_nacionalitat-g_edat-q_sexe", years_mode=years_mode)
            records = _metric_records_from_frame_per_year(
                files,
                frame_builder=_prepare_pct_extranjeros_frame,
                feature_name="pct_extranjeros",
                metric_month=1,
            )
            raw_records.extend(records)
            dataset_summary["pad_mdb_nacionalitat-g_edat-q_sexe"] = {
                "files": [str(path) for _, path in files],
                "records": len(records),
                "features": ["pct_extranjeros"],
            }

        if "pad_mdbas_niv-educa-esta_sexe" in spatial_scope_dataset_ids:
            files = _select_cleaned_dataset_files("pad_mdbas_niv-educa-esta_sexe", years_mode=years_mode)
            records = _metric_records_from_frame_per_year(
                files,
                frame_builder=_prepare_nivel_estudios_frame,
                feature_name="nivel_estudios_alto_pct",
                metric_month=1,
            )
            raw_records.extend(records)
            dataset_summary["pad_mdbas_niv-educa-esta_sexe"] = {
                "files": [str(path) for _, path in files],
                "records": len(records),
                "features": ["nivel_estudios_alto_pct"],
            }

        weighted_specs = [
            ("renda-disponible-llars-bcn", "renta_media_hogar", "Import_Euros"),
            ("atles-renda-index-gini", "gini", "Index_Gini"),
            ("atles-renda-p80-p20-distribucio", "p80_p20", "Distribucio_P80_20"),
            ("atles-renda-mitjana", "renta_media_uc", "Mitjana_Renda_â‚¬"),
            ("atles-renda-mediana", "renta_mediana_uc", "Mediana_Renda_â‚¬"),
        ]
        renta_hogar_by_year: dict[int, dict[str, float]] = {}
        for dataset_id, feature_name, value_column in weighted_specs:
            if dataset_id not in spatial_scope_dataset_ids:
                continue

            keep_history_years = 3 if dataset_id == "renda-disponible-llars-bcn" and years_mode == "latest" else 0
            files = _select_cleaned_dataset_files(
                dataset_id,
                years_mode=years_mode,
                keep_history_years=keep_history_years,
            )
            records, year_map = _weighted_metric_records_and_map(
                files,
                feature_name=feature_name,
                value_column=value_column,
                metric_month=6,
                weights_by_section=section_population_by_year,
                warning_counter=warning_counter,
            )
            raw_records.extend(records)
            dataset_summary[dataset_id] = {
                "files": [str(path) for _, path in files],
                "records": len(records),
                "features": [feature_name],
            }
            if feature_name == "renta_media_hogar":
                renta_hogar_by_year = year_map

        if "renda-disponible-llars-bcn" in spatial_scope_dataset_ids:
            delta_records = _delta_renta_records(renta_hogar_by_year)
            raw_records.extend(delta_records)
            dataset_summary["renda-disponible-llars-bcn"]["records"] += len(delta_records)
            dataset_summary["renda-disponible-llars-bcn"]["features"].append("delta_renta_3a")

        if "pad_dom_mdbas_n-persones" in spatial_scope_dataset_ids:
            files = _select_cleaned_dataset_files("pad_dom_mdbas_n-persones", years_mode=years_mode)
            records = _metric_records_from_frame_per_year(
                files,
                frame_builder=_prepare_tamano_hogar_frame,
                feature_name="tamano_hogar",
                metric_month=1,
            )
            raw_records.extend(records)
            dataset_summary["pad_dom_mdbas_n-persones"] = {
                "files": [str(path) for _, path in files],
                "records": len(records),
                "features": ["tamano_hogar"],
            }

        if "pad_dom_mdbas_edat-0018" in spatial_scope_dataset_ids:
            files = _select_cleaned_dataset_files("pad_dom_mdbas_edat-0018", years_mode=years_mode)
            sample_frames = [
                _read_csv(path)
                for year, path in files
                if year in {1997, 2015, 2025}
            ]
            if not _dom_00_18_convention_consistent(sample_frames):
                warning_counter["hogares_con_menores:dom_00_18_convention_no_confirmed"] += 1
                dataset_summary["pad_dom_mdbas_edat-0018"] = {
                    "files": [str(path) for _, path in files],
                    "records": 0,
                    "features": ["hogares_con_menores"],
                }
            else:
                records = _metric_records_from_frame_per_year(
                    files,
                    frame_builder=_prepare_hogares_con_menores_frame,
                    feature_name="hogares_con_menores",
                    metric_month=1,
                )
                raw_records.extend(records)
                dataset_summary["pad_dom_mdbas_edat-0018"] = {
                    "files": [str(path) for _, path in files],
                    "records": len(records),
                    "features": ["hogares_con_menores"],
                }

        if "pad_sol_mdb_sexe_edat-q" in spatial_scope_dataset_ids:
            files = _select_cleaned_dataset_files("pad_sol_mdb_sexe_edat-q", years_mode=years_mode)
            records: list[dict[str, Any]] = []
            for _, path in files:
                frame = _read_csv(path)
                year = _extract_year(frame, path)
                metric_frame, missing_population = _prepare_personas_solas_frame(
                    frame,
                    population_by_barri=barri_population_by_year.get(year, {}),
                )
                if missing_population:
                    warning_counter[f"personas_solas:missing_population:{year}"] += missing_population
                records.extend(
                    _build_records_from_metric_frame(
                        metric_frame,
                        feature_name="personas_solas",
                        metric_date=date(year, 1, 1),
                    )
                )
            raw_records.extend(records)
            dataset_summary["pad_sol_mdb_sexe_edat-q"] = {
                "files": [str(path) for _, path in files],
                "records": len(records),
                "features": ["personas_solas"],
            }

    merged_records = _merge_records(raw_records)
    rows_written, feature_means = await _persist_records(merged_records, zone_map=zone_map) if merged_records else (0, {})

    citywide_support_metrics = (
        _build_support_citywide_metrics(entries, years_mode=years_mode)
        if include_support
        else {"generated_at": _iso_now(), "datasets": []}
    )

    timestamp = _utc_now().strftime("%Y%m%dT%H%M%SZ")
    run_dir = BACKFILL_ROOT / f"{timestamp}_{scope}_{years_mode}"
    run_dir.mkdir(parents=True, exist_ok=True)
    LATEST_ROOT.mkdir(parents=True, exist_ok=True)

    means_payload = {
        "generated_at": _iso_now(),
        "scope": scope,
        "years_mode": years_mode,
        "feature_means": feature_means,
    }
    report_payload = {
        "generated_at": _iso_now(),
        "scope": scope,
        "years_mode": years_mode,
        "rows_written": rows_written,
        "merged_records": len(merged_records),
        "datasets": dataset_summary,
        "feature_means": feature_means,
        "warnings": [
            {"warning": key, "count": count}
            for key, count in sorted(warning_counter.items())
        ],
    }

    _publish_artifact(run_dir, "demography_feature_means.json", means_payload)
    _publish_artifact(run_dir, "backfill_report.json", report_payload)
    _publish_artifact(run_dir, "citywide_support_metrics.json", citywide_support_metrics)

    return {
        "run_dir": str(run_dir),
        "latest_dir": str(LATEST_ROOT),
        "scope": scope,
        "years_mode": years_mode,
        "rows_written": rows_written,
        "merged_records": len(merged_records),
        "feature_means": feature_means,
        "citywide_support_datasets": len(citywide_support_metrics.get("datasets") or []),
        "warnings": report_payload["warnings"],
    }


async def _main() -> None:
    parser = argparse.ArgumentParser(description="Backfill local demografico desde CSVs del manifest")
    parser.add_argument(
        "--scope",
        default="full",
        choices=["bcn_batch_1", "core_spatial", "support_citywide", "full"],
    )
    parser.add_argument("--years", dest="years_mode", default="all", choices=["all", "latest"])
    parser.add_argument("--manifest", default=str(MANIFEST_PATH))
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    result = await ejecutar_backfill(
        scope=args.scope,
        years_mode=args.years_mode,
        manifest_path=Path(args.manifest),
    )
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    asyncio.run(_main())
