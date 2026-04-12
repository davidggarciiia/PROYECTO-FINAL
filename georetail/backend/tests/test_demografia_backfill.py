from __future__ import annotations

import pandas as pd
import pytest

from pipelines.demografia.demografia_backfill import (
    _dom_00_18_convention_consistent,
    _prepare_hogares_con_menores_frame,
    _prepare_personas_solas_frame,
    _prepare_tamano_hogar_frame,
    _prepare_weighted_barri_frame,
)


def test_prepare_weighted_barri_frame_uses_section_population():
    frame = pd.DataFrame(
        {
            "Codi_Barri": ["1", "1"],
            "Seccio_Censal": ["1", "2"],
            "Index_Gini": ["10", "40"],
        }
    )

    metric_frame, fallback_used = _prepare_weighted_barri_frame(
        frame,
        value_column="Index_Gini",
        weights_by_section={
            ("000001", 1): 100.0,
            ("000001", 2): 300.0,
        },
    )

    assert fallback_used is False
    assert metric_frame.iloc[0]["barri_code"] == "000001"
    assert metric_frame.iloc[0]["metric"] == pytest.approx(32.5)


def test_prepare_weighted_barri_frame_falls_back_to_simple_mean_if_weights_missing():
    frame = pd.DataFrame(
        {
            "Codi_Barri": ["1", "1"],
            "Seccio_Censal": ["1", "2"],
            "Distribucio_P80_20": ["2.0", "4.0"],
        }
    )

    metric_frame, fallback_used = _prepare_weighted_barri_frame(
        frame,
        value_column="Distribucio_P80_20",
        weights_by_section={("000001", 1): 100.0},
    )

    assert fallback_used is True
    assert metric_frame.iloc[0]["metric"] == pytest.approx(3.0)


def test_prepare_tamano_hogar_frame_computes_weighted_household_size():
    frame = pd.DataFrame(
        {
            "Codi_Barri": ["1", "1", "1"],
            "Valor": ["10", "20", "5"],
            "N_PERSONES_AGG": ["1", "2", "4"],
        }
    )

    result = _prepare_tamano_hogar_frame(frame)

    assert result.iloc[0]["barri_code"] == "000001"
    assert result.iloc[0]["metric"] == pytest.approx((10 + 40 + 20) / 35.0)


def test_dom_00_18_convention_requires_same_domain_in_sample_years():
    consistent = [
        pd.DataFrame({"DOM_00_18": ["1", "2", "3", "4", "5"]}),
        pd.DataFrame({"DOM_00_18": ["1", "2", "3", "4", "5"]}),
    ]
    inconsistent = [
        pd.DataFrame({"DOM_00_18": ["1", "2", "3", "4", "5"]}),
        pd.DataFrame({"DOM_00_18": ["0", "1", "2", "3", "4"]}),
    ]

    assert _dom_00_18_convention_consistent(consistent) is True
    assert _dom_00_18_convention_consistent(inconsistent) is False


def test_prepare_hogares_con_menores_frame_uses_dom_00_18_equals_1_as_no_minors():
    frame = pd.DataFrame(
        {
            "Codi_Barri": ["1", "1", "1"],
            "Valor": ["50", "30", "20"],
            "DOM_00_18": ["1", "2", "3"],
        }
    )

    result = _prepare_hogares_con_menores_frame(frame)

    assert result.iloc[0]["metric"] == pytest.approx(0.5)


def test_prepare_personas_solas_frame_discards_non_numeric_tokens():
    frame = pd.DataFrame(
        {
            "Codi_Barri": ["1", "1", "1"],
            "Valor": ["100", "..", "50"],
        }
    )

    result, missing_population = _prepare_personas_solas_frame(
        frame,
        population_by_barri={"000001": 1000.0},
    )

    assert missing_population == 0
    assert result.iloc[0]["metric"] == pytest.approx(0.15)
