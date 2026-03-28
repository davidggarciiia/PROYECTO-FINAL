"""Tests for scoring/flujo_peatonal.py fusion model."""
from __future__ import annotations

import pytest
from scoring.flujo_peatonal import (
    PESOS_BASE,
    calcular_flujo_score,
    calcular_flujo_score_batch,
    fuentes_disponibles,
)


# ---------------------------------------------------------------------------
# calcular_flujo_score — tests básicos
# ---------------------------------------------------------------------------

def test_todas_fuentes():
    """Con las 4 fuentes disponibles el resultado está en rango."""
    score = calcular_flujo_score(80.0, 30000.0, 8000.0, 0.7)
    assert 0 <= score <= 100


def test_sin_vcity():
    """Sin vcity, los pesos se redistribuyen y el score sigue siendo razonable."""
    score_completo = calcular_flujo_score(80.0, 30000.0, 8000.0, 0.7)
    score_sin_vcity = calcular_flujo_score(80.0, None, 8000.0, 0.7)
    assert 0 <= score_sin_vcity <= 100
    # Sin vcity el score debe ser algo razonable (no 0)
    assert score_sin_vcity > 10


def test_solo_ratio_locales():
    """Con solo ratio_locales (peor caso), devuelve valor razonable."""
    score = calcular_flujo_score(None, None, None, 0.6)
    assert score == pytest.approx(0.6 * 100, abs=5)


def test_fuentes_disponibles():
    row = {
        "flujo_popular_times_score": 70.0,
        "vcity_flujo_peatonal": None,
        "flujo_peatonal_total": 5000.0,
        "ratio_locales_comerciales": 0.5,
    }
    fuentes = fuentes_disponibles(row)
    assert "popular_times" in fuentes
    assert "vcity" not in fuentes
    assert "vianants" in fuentes


# ---------------------------------------------------------------------------
# calcular_flujo_score — normalización de fuentes raw
# ---------------------------------------------------------------------------

def test_vcity_normalizado_a_maximo():
    """vcity igual al máximo de referencia da score vcity=100."""
    # Con solo vcity disponible, score == 100
    score = calcular_flujo_score(None, 50_000.0, None, None)
    assert score == pytest.approx(100.0, abs=1)


def test_vianants_normalizado_a_maximo():
    """vianants igual al máximo de referencia da score vianants=100."""
    score = calcular_flujo_score(None, None, 15_000.0, None)
    assert score == pytest.approx(100.0, abs=1)


def test_vcity_por_encima_del_maximo_se_clamp():
    """Valores vcity superiores al máximo de referencia no superan 100."""
    score = calcular_flujo_score(None, 200_000.0, None, None)
    assert score == pytest.approx(100.0, abs=0.01)


def test_valores_cero_devuelve_cero():
    """Todas las fuentes en cero → score 0."""
    score = calcular_flujo_score(0.0, 0.0, 0.0, 0.0)
    assert score == pytest.approx(0.0, abs=0.01)


def test_sin_ninguna_fuente_devuelve_fallback():
    """Sin ninguna fuente disponible devuelve el fallback conservador."""
    score = calcular_flujo_score(None, None, None, None)
    assert score == pytest.approx(30.0, abs=0.01)


# ---------------------------------------------------------------------------
# calcular_flujo_score — redistribución de pesos
# ---------------------------------------------------------------------------

def test_pesos_suman_100_sin_una_fuente():
    """Con 3 fuentes activas, la suma de pesos redistribuidos es 1.0."""
    from scoring.flujo_peatonal import PESOS_BASE

    disponibles = {k: v for k, v in PESOS_BASE.items() if k != "vcity"}
    peso_total = sum(disponibles.values())
    pesos_adj = {k: v / peso_total for k, v in disponibles.items()}
    assert sum(pesos_adj.values()) == pytest.approx(1.0, abs=1e-9)


def test_score_mayor_con_mas_fuentes():
    """
    Con las 4 fuentes activas y valores altos, el score debe ser mayor
    que si solo hay una fuente de bajo peso (ratio_locales).
    """
    score_completo = calcular_flujo_score(80.0, 40000.0, 12000.0, 0.8)
    score_solo_ratio = calcular_flujo_score(None, None, None, 0.5)
    assert score_completo > score_solo_ratio


# ---------------------------------------------------------------------------
# calcular_flujo_score_batch
# ---------------------------------------------------------------------------

def test_batch_longitud_correcta():
    """La salida tiene la misma longitud que la entrada."""
    rows = [
        {"flujo_popular_times_score": 60.0, "vcity_flujo_peatonal": 25000.0,
         "flujo_peatonal_total": 7000.0, "ratio_locales_comerciales": 0.4},
        {"flujo_popular_times_score": None, "vcity_flujo_peatonal": None,
         "flujo_peatonal_total": None, "ratio_locales_comerciales": 0.3},
    ]
    scores = calcular_flujo_score_batch(rows)
    assert len(scores) == 2


def test_batch_rango_valido():
    """Todos los scores del batch están en [0, 100]."""
    rows = [
        {"flujo_popular_times_score": 50.0, "vcity_flujo_peatonal": 20000.0,
         "flujo_peatonal_total": 5000.0, "ratio_locales_comerciales": 0.35},
        {"flujo_popular_times_score": 100.0, "vcity_flujo_peatonal": 50000.0,
         "flujo_peatonal_total": 15000.0, "ratio_locales_comerciales": 1.0},
        {"flujo_popular_times_score": 0.0, "vcity_flujo_peatonal": 0.0,
         "flujo_peatonal_total": 0.0, "ratio_locales_comerciales": 0.0},
    ]
    scores = calcular_flujo_score_batch(rows)
    for s in scores:
        assert 0 <= s <= 100


def test_batch_alias_popular_times():
    """La función batch acepta tanto popular_times_score como flujo_popular_times_score."""
    row_alias = {"popular_times_score": 70.0, "vcity_flujo_peatonal": None,
                 "flujo_peatonal_total": None, "ratio_locales_comerciales": None}
    row_col = {"flujo_popular_times_score": 70.0, "vcity_flujo_peatonal": None,
               "flujo_peatonal_total": None, "ratio_locales_comerciales": None}
    scores_alias = calcular_flujo_score_batch([row_alias])
    scores_col   = calcular_flujo_score_batch([row_col])
    assert scores_alias[0] == pytest.approx(scores_col[0], abs=0.01)


# ---------------------------------------------------------------------------
# fuentes_disponibles
# ---------------------------------------------------------------------------

def test_fuentes_todas_presentes():
    row = {
        "flujo_popular_times_score": 55.0,
        "vcity_flujo_peatonal": 20000.0,
        "flujo_peatonal_total": 6000.0,
        "ratio_locales_comerciales": 0.4,
    }
    fuentes = fuentes_disponibles(row)
    assert set(fuentes) == {"popular_times", "vcity", "vianants", "ratio_locales"}


def test_fuentes_ninguna_presente():
    row = {
        "flujo_popular_times_score": None,
        "vcity_flujo_peatonal": None,
        "flujo_peatonal_total": None,
        "ratio_locales_comerciales": None,
    }
    fuentes = fuentes_disponibles(row)
    assert fuentes == []


def test_fuentes_ratio_locales_cero_cuenta_como_disponible():
    """ratio_locales=0.0 es un valor legítimo y cuenta como fuente disponible."""
    row = {
        "flujo_popular_times_score": None,
        "vcity_flujo_peatonal": None,
        "flujo_peatonal_total": None,
        "ratio_locales_comerciales": 0.0,
    }
    fuentes = fuentes_disponibles(row)
    assert "ratio_locales" in fuentes


# ---------------------------------------------------------------------------
# PESOS_BASE invariantes
# ---------------------------------------------------------------------------

def test_pesos_base_suman_uno():
    """Los pesos base deben sumar exactamente 1.0."""
    assert sum(PESOS_BASE.values()) == pytest.approx(1.0, abs=1e-9)


def test_pesos_base_claves_correctas():
    """PESOS_BASE tiene exactamente las 4 fuentes esperadas."""
    assert set(PESOS_BASE.keys()) == {"popular_times", "vcity", "vianants", "ratio_locales"}
