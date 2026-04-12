from __future__ import annotations

import numpy as np
import pandas as pd

import scoring.infra.readiness as demografia_readiness
from pipelines.demografia.demografia import _fuente_aprobada
from scoring.infra.governance import (
    DEMOGRAFIA_BATCH_BCN1_FEATURES,
    DEMOGRAFIA_DEFAULT_ACTIVE_FEATURES,
    DEMOGRAFIA_SELECTABLE_EXISTING_FEATURES,
    build_model_feature_names,
    get_model_feature_names_from_record,
)
from scoring.features import FEATURE_NAMES


def test_build_model_feature_names_only_filters_demography_block():
    model_feature_names = build_model_feature_names(["renta_media_hogar", "edad_media"])

    assert "renta_media_hogar" in model_feature_names
    assert "edad_media" in model_feature_names
    assert "pct_extranjeros" not in model_feature_names
    assert "score_saturacion" in model_feature_names


def test_get_model_feature_names_from_record_falls_back_to_default():
    model_feature_names = get_model_feature_names_from_record({})

    assert model_feature_names == build_model_feature_names()


def test_decide_source_catalog_prefers_barcelona_open_data_when_granularity_matches(monkeypatch):
    profiles = {
        "padro_bcn": {
            "files_found": 1,
            "files_profiled": 1,
            "sample_files": ["padro.csv"],
            "row_count_estimate": 1000,
            "columns": ["codi_barri", "pct_extranjeros"],
            "non_null_ratio": 0.95,
            "geo_keys": ["barri"],
            "granularity_detected": "barri",
            "years": ["2024"],
            "errors": [],
        },
        "idescat_extranjeros": {
            "files_found": 1,
            "files_profiled": 1,
            "sample_files": ["idescat.csv"],
            "row_count_estimate": 1500,
            "columns": ["codi_barri", "pct_extranjeros"],
            "non_null_ratio": 0.95,
            "geo_keys": ["barri"],
            "granularity_detected": "barri",
            "years": ["2024"],
            "errors": [],
        },
    }

    monkeypatch.setattr(
        demografia_readiness,
        "_profile_dataset",
        lambda entry: profiles[entry["dataset_id"]],
    )

    result = demografia_readiness._decide_source_catalog(
        [
            {
                "dataset_id": "padro_bcn",
                "provider": "barcelona_open_data",
                "title": "Padro BCN",
                "priority": "barcelona_primary",
                "intended_features": ["pct_extranjeros"],
            },
            {
                "dataset_id": "idescat_extranjeros",
                "provider": "idescat",
                "title": "IDESCAT extranjeros",
                "priority": "secondary",
                "intended_features": ["pct_extranjeros"],
            },
        ]
    )

    decisions = {item["dataset_id"]: item for item in result["datasets"]}
    assert decisions["padro_bcn"]["decision"] == "primary"
    assert decisions["idescat_extranjeros"]["decision"] == "backup"


def test_decide_source_catalog_marks_citywide_sources_as_support_only(monkeypatch):
    profiles = {
        "adrh_barcelona_renta": {
            "files_found": 1,
            "files_profiled": 1,
            "sample_files": ["adrh.csv"],
            "row_count_estimate": 300,
            "columns": ["municipios", "indicadores_de_renta_media_y_mediana", "periodo", "total"],
            "non_null_ratio": 0.98,
            "geo_keys": [],
            "granularity_detected": "unknown",
            "years": ["2015", "2023"],
            "errors": [],
        }
    }

    monkeypatch.setattr(
        demografia_readiness,
        "_profile_dataset",
        lambda entry: profiles[entry["dataset_id"]],
    )

    result = demografia_readiness._decide_source_catalog(
        [
            {
                "dataset_id": "adrh_barcelona_renta",
                "provider": "ine",
                "title": "ADRH Barcelona",
                "priority": "ine_support",
                "intended_features": ["renta_media_uc", "renta_mediana_uc"],
            }
        ]
    )

    decision = result["datasets"][0]
    assert decision["decision"] == "support_only"


def test_select_demography_features_preserves_core_features(monkeypatch):
    class DummyRFECV:
        def __init__(self, *args, **kwargs):
            self.cv_results_ = {"mean_test_score": np.array([0.71, 0.72])}

        def fit(self, X, y):
            fixed_count = len(demografia_readiness._FIXED_MODEL_FEATURES) + len(
                DEMOGRAFIA_DEFAULT_ACTIVE_FEATURES
            )
            support = np.zeros(X.shape[1], dtype=bool)
            support[:fixed_count] = True
            support[fixed_count] = True
            self.support_ = support
            return self

    monkeypatch.setattr(demografia_readiness, "RFECV", DummyRFECV)

    X = np.tile(np.arange(len(FEATURE_NAMES), dtype=np.float32), (20, 1))
    X[:, FEATURE_NAMES.index("renta_media_hogar")] = np.linspace(20000, 50000, 20)
    X[:, FEATURE_NAMES.index("edad_media")] = np.linspace(30, 60, 20)
    X[:, FEATURE_NAMES.index("pct_extranjeros")] = np.linspace(0.1, 0.3, 20)
    X[:, FEATURE_NAMES.index("densidad_hab_km2")] = np.linspace(12000, 28000, 20)
    X[:, FEATURE_NAMES.index("pct_poblacio_25_44")] = np.linspace(0.18, 0.42, 20)
    X[:, FEATURE_NAMES.index("delta_renta_3a")] = np.linspace(-0.05, 0.18, 20)
    X[:, FEATURE_NAMES.index("nivel_estudios_alto_pct")] = np.linspace(0.12, 0.46, 20)
    X[:, FEATURE_NAMES.index("indice_potencial_consumo")] = np.linspace(25, 78, 20)
    X[:, FEATURE_NAMES.index("gini")] = np.linspace(27, 36, 20)
    y = np.array([0, 1] * 10, dtype=np.int32)
    audit = pd.DataFrame(
        {
            f"{feature}_present": [True] * len(y)
            for feature in DEMOGRAFIA_SELECTABLE_EXISTING_FEATURES
        }
    )

    result = demografia_readiness._select_demography_features(X, y, audit, [])
    decisions = {item["feature"]: item for item in result["decisions"]}

    assert decisions["renta_media_hogar"]["decision"] == "use"
    assert decisions["edad_media"]["decision"] == "use"
    assert decisions["indice_potencial_consumo"]["decision"] == "use"
    assert any(
        decisions[feature]["decision"] == "use"
        for feature in DEMOGRAFIA_BATCH_BCN1_FEATURES
    )
    assert "edad_media" in result["proposed_active_demography_features"]


def test_fuente_aprobada_acepta_si_una_fuente_esta_aprobada():
    decisions = {
        "a": {"decision": "discard"},
        "b": {"decision": "backup"},
    }

    assert _fuente_aprobada(decisions, "a", "b") is True
    assert _fuente_aprobada(decisions, "a") is False
