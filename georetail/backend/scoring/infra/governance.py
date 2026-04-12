from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

import numpy as np
import pandas as pd

from scoring.features import FEATURE_NAMES

REPO_ROOT = Path(__file__).resolve().parents[3]
CSV_ROOT = Path(os.environ.get("CSV_DIR", str(REPO_ROOT / "CSV")))
READINESS_ROOT = CSV_ROOT / "_meta" / "demografia_readiness"
LATEST_ROOT = READINESS_ROOT / "latest"
DEFAULT_SOURCE_DECISIONS_PATH = LATEST_ROOT / "source_decisions.json"
DEFAULT_FEATURE_DECISIONS_PATH = LATEST_ROOT / "demography_feature_decisions.json"
TEMPORAL_READINESS_ROOT = CSV_ROOT / "_meta" / "temporal_readiness"
TEMPORAL_LATEST_ROOT = TEMPORAL_READINESS_ROOT / "latest"
DEFAULT_TEMPORAL_GATES_PATH = TEMPORAL_LATEST_ROOT / "temporal_feature_gates.json"

DEMOGRAFIA_MODEL_CORE_FEATURES: tuple[str, ...] = (
    "renta_media_hogar",
    "edad_media",
    "pct_extranjeros",
    "densidad_hab_km2",
    "pct_poblacio_25_44",
    "delta_renta_3a",
    "nivel_estudios_alto_pct",
)

DEMOGRAFIA_DEFAULT_ACTIVE_FEATURES: tuple[str, ...] = (
    *DEMOGRAFIA_MODEL_CORE_FEATURES,
    "indice_potencial_consumo",
)

TEMPORAL_SEASONAL_FEATURES: tuple[str, ...] = (
    "seasonality_summer_lift",
    "seasonality_christmas_lift",
    "seasonality_rebajas_lift",
    "seasonality_volatility",
    "seasonality_peak_concentration",
)

TEMPORAL_WEEKLY_FEATURES: tuple[str, ...] = (
    "weekend_lift",
    "sunday_lift",
    "weekday_midday_share",
    "weekend_evening_share",
    "late_night_share",
    "holiday_proxy_score",
    "temporal_confianza",
)

TEMPORAL_MODELABLE_FEATURES: tuple[str, ...] = (
    *TEMPORAL_SEASONAL_FEATURES,
    *TEMPORAL_WEEKLY_FEATURES,
)

TEMPORAL_LEGACY_ACTIVE_FEATURES: tuple[str, ...] = TEMPORAL_SEASONAL_FEATURES

DEMOGRAFIA_BATCH_BCN1_FEATURES: tuple[str, ...] = (
    "gini",
    "p80_p20",
    "tamano_hogar",
    "hogares_con_menores",
    "personas_solas",
)

DEMOGRAFIA_BATCH_FINAL_FEATURES: tuple[str, ...] = (
    *DEMOGRAFIA_BATCH_BCN1_FEATURES,
    "renta_media_uc",
    "renta_mediana_uc",
)

DEMOGRAFIA_MODELABLE_FEATURES: tuple[str, ...] = (
    *DEMOGRAFIA_DEFAULT_ACTIVE_FEATURES,
    *DEMOGRAFIA_BATCH_FINAL_FEATURES,
)

# Alias de compatibilidad: el resto del stack ya usa este nombre para el pool
# de features demográficas que pueden entrar en train/evaluate/scorer.
DEMOGRAFIA_SELECTABLE_EXISTING_FEATURES: tuple[str, ...] = DEMOGRAFIA_MODELABLE_FEATURES

DEMOGRAFIA_CANDIDATE_FEATURES: tuple[str, ...] = (
    "tipo_hogar",
    "mayores_75_solos",
    "formacion_baja_pct",
    "distribucion_irpf",
    "afiliados_comercio",
    "afiliados_conocimiento",
    "afiliados_residencia_sexo",
)

DEMOGRAFIA_SOURCE_ALIAS_FEATURES: tuple[str, ...] = (
    "renta_media_persona",
    "renta_mediana_persona",
)

DEMOGRAFIA_SUPPORT_FEATURES: tuple[str, ...] = (
    "poblacion",
    "hogares",
    "viviendas",
)

DEMOGRAFIA_MANIFEST_FEATURES: tuple[str, ...] = tuple(
    dict.fromkeys(
        [
            *DEMOGRAFIA_MODEL_CORE_FEATURES,
            *DEMOGRAFIA_BATCH_FINAL_FEATURES,
            *DEMOGRAFIA_CANDIDATE_FEATURES,
            *DEMOGRAFIA_SOURCE_ALIAS_FEATURES,
            *DEMOGRAFIA_SUPPORT_FEATURES,
        ]
    )
)

DATASET_FEATURE_OVERRIDES: dict[str, tuple[str, ...]] = {
    "atles-renda-mitjana": ("renta_media_uc",),
    "atles-renda-mediana": ("renta_mediana_uc",),
}

SOURCE_SUPPORT_ONLY_DATASET_IDS: set[str] = {
    "adrh_barcelona_renta",
    "adrh_barcelona_desigualdad",
    "censph",
    "rfdbc",
    "afi",
    "irpf",
}

SOURCE_EXPERIMENTAL_ONLY_DATASET_IDS: set[str] = {
    "pad_dom_mdbas_tipus-domicili",
}

PROVIDER_PRIORITY: dict[str, int] = {
    "barcelona_open_data": 4,
    "amb_opendata": 3,
    "idescat": 2,
    "ine": 1,
}

GRANULARITY_PRIORITY: dict[str, int] = {
    "seccio_censal": 5,
    "districte": 4,
    "barri": 3,
    "municipi": 2,
    "ciutat": 1,
    "unknown": 0,
}

SOURCE_APPROVED_STATUSES = {"primary", "backup"}
SOURCE_SUPPORT_ONLY_STATUSES = {"support_only"}


def provider_priority(provider: str | None) -> int:
    return PROVIDER_PRIORITY.get((provider or "").strip().lower(), 0)


def granularity_priority(granularity: str | None) -> int:
    return GRANULARITY_PRIORITY.get((granularity or "unknown").strip().lower(), 0)


def is_demography_feature(name: str | None) -> bool:
    return (name or "").strip() in DEMOGRAFIA_MANIFEST_FEATURES


def is_demography_model_feature(name: str | None) -> bool:
    return (name or "").strip() in DEMOGRAFIA_MODELABLE_FEATURES


def dataset_intended_demography_features(entry: Mapping[str, Any]) -> list[str]:
    dataset_id = str(entry.get("dataset_id") or "").strip()
    intended = [
        str(value).strip()
        for value in (entry.get("intended_features") or [])
        if is_demography_feature(value)
    ]
    extras = list(DATASET_FEATURE_OVERRIDES.get(dataset_id, ()))
    return list(dict.fromkeys([*intended, *extras]))


def is_demography_dataset(entry: Mapping[str, Any]) -> bool:
    return bool(dataset_intended_demography_features(entry))


def dataset_is_support_only(dataset_id: str | None) -> bool:
    return (dataset_id or "").strip() in SOURCE_SUPPORT_ONLY_DATASET_IDS


def dataset_is_experimental_only(dataset_id: str | None) -> bool:
    return (dataset_id or "").strip() in SOURCE_EXPERIMENTAL_ONLY_DATASET_IDS


def default_active_demography_features() -> list[str]:
    return list(DEMOGRAFIA_DEFAULT_ACTIVE_FEATURES)


def default_active_temporal_features(*, legacy_compatible: bool = True) -> list[str]:
    if legacy_compatible:
        return list(TEMPORAL_LEGACY_ACTIVE_FEATURES)
    return []


def select_active_demography_features(
    requested: Optional[Sequence[str]] = None,
) -> list[str]:
    if not requested:
        return default_active_demography_features()

    requested_set = {str(value) for value in requested}
    return [
        feature
        for feature in DEMOGRAFIA_MODELABLE_FEATURES
        if feature in requested_set
    ]


def select_active_temporal_features(
    requested: Optional[Sequence[str]] = None,
    *,
    legacy_compatible: bool = True,
) -> list[str]:
    if requested is None:
        return default_active_temporal_features(legacy_compatible=legacy_compatible)

    requested_set = {str(value) for value in requested}
    return [
        feature
        for feature in TEMPORAL_MODELABLE_FEATURES
        if feature in requested_set
    ]


def build_model_feature_names(
    active_demography_features: Optional[Sequence[str]] = None,
    active_temporal_features: Optional[Sequence[str]] = None,
) -> list[str]:
    active = set(select_active_demography_features(active_demography_features))
    active_temporal = set(
        select_active_temporal_features(active_temporal_features)
    )
    return [
        feature
        for feature in FEATURE_NAMES
        if (
            feature not in DEMOGRAFIA_MODELABLE_FEATURES
            or feature in active
        )
        and (
            feature not in TEMPORAL_MODELABLE_FEATURES
            or feature in active_temporal
        )
    ]


def fixed_model_feature_names() -> list[str]:
    return [
        feature
        for feature in FEATURE_NAMES
        if feature not in DEMOGRAFIA_MODELABLE_FEATURES
    ]


def slice_feature_matrix(
    matrix: np.ndarray | pd.DataFrame,
    source_feature_names: Sequence[str],
    target_feature_names: Sequence[str],
) -> np.ndarray | pd.DataFrame:
    index_by_name = {name: idx for idx, name in enumerate(source_feature_names)}
    indices = [index_by_name[name] for name in target_feature_names]

    if isinstance(matrix, pd.DataFrame):
        return matrix.loc[:, list(target_feature_names)]
    return matrix[:, indices]


def coerce_json_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError:
            return []
        return [str(item) for item in decoded if isinstance(item, str)]
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray)):
        return [str(item) for item in value if isinstance(item, str)]
    return []


def coerce_json_mapping(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return decoded if isinstance(decoded, dict) else {}
    return dict(value) if isinstance(value, Mapping) else {}


def get_model_feature_names_from_record(record: Mapping[str, Any] | None) -> list[str]:
    if not record:
        return build_model_feature_names()
    metricas = coerce_json_mapping(record.get("metricas"))
    temporal_feature_names: Optional[list[str]]
    if "temporal_feature_names" in record:
        temporal_feature_names = coerce_json_list(record.get("temporal_feature_names"))
    elif "temporal_feature_names" in metricas:
        temporal_feature_names = coerce_json_list(metricas.get("temporal_feature_names"))
    else:
        temporal_feature_names = None
    return build_model_feature_names(
        coerce_json_list(record.get("demografia_feature_names")),
        temporal_feature_names,
    )


def load_json_file(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def load_source_decisions(
    path: Optional[Path | str] = None,
) -> dict[str, dict[str, Any]]:
    source_path = Path(path) if path else DEFAULT_SOURCE_DECISIONS_PATH
    if not source_path.exists():
        return {}
    payload = load_json_file(source_path)
    decisions = payload.get("datasets") or []
    return {
        entry["dataset_id"]: entry
        for entry in decisions
        if isinstance(entry, Mapping) and entry.get("dataset_id")
    }


def source_is_approved(
    dataset_id: str,
    decisions: Optional[Mapping[str, Mapping[str, Any]]] = None,
) -> bool:
    decision_map = decisions or load_source_decisions()
    entry = decision_map.get(dataset_id)
    if not entry:
        return True
    return (entry.get("decision") or "").lower() in SOURCE_APPROVED_STATUSES


def load_feature_decisions(
    path: Optional[Path | str] = None,
) -> dict[str, Any]:
    feature_path = Path(path) if path else DEFAULT_FEATURE_DECISIONS_PATH
    if not feature_path.exists():
        return {}
    return load_json_file(feature_path)


def load_active_demography_features_from_artifact(
    path: Optional[Path | str] = None,
) -> list[str]:
    payload = load_feature_decisions(path)
    active = payload.get("active_demography_features")
    selected = select_active_demography_features(coerce_json_list(active))
    return selected or default_active_demography_features()


def load_temporal_feature_gates(
    path: Optional[Path | str] = None,
) -> dict[str, Any]:
    gate_path = Path(path) if path else DEFAULT_TEMPORAL_GATES_PATH
    if not gate_path.exists():
        return {}
    return load_json_file(gate_path)


def load_active_temporal_features_from_artifact(
    path: Optional[Path | str] = None,
) -> list[str]:
    payload = load_temporal_feature_gates(path)
    active = payload.get("active_temporal_features")
    selected = select_active_temporal_features(
        coerce_json_list(active),
        legacy_compatible=False,
    )
    return selected
