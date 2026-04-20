from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from scoring.features import FEATURE_NAMES
from scoring.infra.governance import (
    build_model_feature_names,
    get_model_feature_names_from_record,
)
import scoring.ml.train as train
import scoring.temporal_readiness as temporal_readiness


def test_build_model_feature_names_keeps_legacy_seasonal_but_not_weekly_by_default():
    feature_names = build_model_feature_names()

    assert "seasonality_summer_lift" in feature_names
    assert "weekend_lift" not in feature_names


def test_get_model_feature_names_from_record_reads_temporal_metricas():
    feature_names = get_model_feature_names_from_record(
        {
            "demografia_feature_names": [],
            "metricas": {
                "temporal_feature_names": [
                    "seasonality_summer_lift",
                    "weekend_lift",
                ]
            },
        }
    )

    assert "seasonality_summer_lift" in feature_names
    assert "weekend_lift" in feature_names
    assert "seasonality_christmas_lift" not in feature_names


def test_weekly_gate_fails_without_historical_sources():
    frame = pd.DataFrame(
        {
            "weekend_lift": [1.1, None, None],
            "sunday_lift": [0.9, None, None],
            "weekday_midday_share": [0.15, None, None],
            "weekend_evening_share": [0.12, None, None],
            "late_night_share": [0.08, None, None],
            "holiday_proxy_score": [55.0, None, None],
            "temporal_confianza": [0.7, None, None],
        }
    )
    source_stats = {
        "popular_times_historical": False,
        "popular_times_current_zone_coverage": 0.9,
        "aforaments_detail_years": [2025],
        "aforaments_historical_detail": False,
    }

    result = temporal_readiness._evaluate_weekly_gate(frame, source_stats)

    assert result["status"] == "fail"
    assert any("popular_times" in reason for reason in result["reasons"])


def test_seasonal_gate_passes_with_depth_and_plausibility():
    snapshot = pd.DataFrame(
        {
            "seasonality_summer_lift": [1.05, 0.98, 1.10, 1.02],
            "seasonality_christmas_lift": [1.08, 1.00, 1.12, 1.03],
            "seasonality_rebajas_lift": [1.01, 0.99, 1.05, 1.00],
            "seasonality_volatility": [0.18, 0.22, 0.16, 0.20],
            "seasonality_peak_concentration": [1.40, 1.25, 1.55, 1.30],
        }
    )
    rows = []
    for zona_id in ["z1", "z2", "z3", "z4"]:
        for month in range(1, 13):
            rows.append(
                {
                    "zona_id": zona_id,
                    "bucket_date": f"2024-{month:02d}-01",
                    "intensidad": 100 + month,
                }
            )
    monthly = pd.DataFrame(rows)

    result = temporal_readiness._evaluate_seasonal_gate(
        snapshot,
        monthly,
        dataset_zone_ids=["z1", "z2", "z3", "z4", "z5", "z6"],
    )

    assert result["status"] == "pass"
    assert result["zone_bucket_ratio_12"] >= 0.30


@pytest.mark.asyncio
async def test_train_blocks_when_temporal_gate_fails(monkeypatch, tmp_path):
    artifact = tmp_path / ".tmp_temporal_feature_gates.json"
    artifact.write_text(
        json.dumps(
            {
                "overall_gate": {
                    "status": "fail",
                    "accepted": False,
                    "active_temporal_features": [],
                },
                "active_temporal_features": [],
            }
        ),
        encoding="utf-8",
    )

    async def _fake_resumen_dataset(sector=None):
        return {
            "total_negocios": 300,
            "supervivientes": 180,
            "ratio_supervivencia": 0.6,
            "fracasos": 120,
        }

    async def _fake_construir_dataset(sector=None):
        X = np.ones((20, len(FEATURE_NAMES)), dtype=np.float32)
        y = np.array([0, 1] * 10, dtype=np.int32)
        meta = pd.DataFrame({"zona_id": [f"z{i}" for i in range(20)]})
        return X, y, meta

    monkeypatch.setattr(train, "resumen_dataset", _fake_resumen_dataset)
    monkeypatch.setattr(train, "construir_dataset", _fake_construir_dataset)

    try:
        result = await train.entrenar_modelo(
            sector="todos",
            temporal_features_path=str(artifact),
        )
    finally:
        artifact.unlink(missing_ok=True)

    assert result["guardado"] is False
    assert "Gate temporal estricto no superado" in result["motivo"]
