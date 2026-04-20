"""
Tests for scoring/flujo_peatonal.py fusion model.

Nota de diseño (2026-04): el scoring usa sólo dos fuentes — VCity (70%) y
Google Popular Times (30%). Los parámetros `vianants_intensitat` y
`ratio_locales` se aceptan por compatibilidad de firma pero NO contribuyen
al score (se prefiere la estimación VCity, homogénea en toda la ciudad,
frente a sensores Vianants puntuales).
"""
from __future__ import annotations

import pytest
from scoring.dimensiones.flujo_peatonal import (
    PESOS_BASE,
    VCITY_MAX_BARCELONA,
    VIANANTS_MAX_BARCELONA,
    calcular_flujo_score,
    calcular_flujo_score_batch,
    flujo_peatonal_explain,
    fuentes_disponibles,
)


# ---------------------------------------------------------------------------
# calcular_flujo_score — tests básicos
# ---------------------------------------------------------------------------

def test_todas_fuentes_rango_valido():
    """Con popular_times + vcity el resultado está en rango."""
    score = calcular_flujo_score(80.0, 30000.0, 8000.0, 0.7)
    assert 0 <= score <= 100


def test_sin_vcity_usa_popular_times_solo():
    """Sin vcity pero con popular_times → score = popular_times."""
    score = calcular_flujo_score(80.0, None, 8000.0, 0.7)
    assert 0 <= score <= 100
    assert score > 10  # 80 de popular_times debe dar algo razonable


def test_solo_ratio_locales_cae_a_fallback():
    """ratio_locales se ignora; sin vcity ni popular_times → fallback 40.0."""
    score = calcular_flujo_score(None, None, None, 0.6)
    assert score == pytest.approx(40.0, abs=0.01)


def test_solo_vianants_cae_a_fallback():
    """vianants_intensitat se ignora; sin vcity ni popular_times → fallback 40.0."""
    score = calcular_flujo_score(None, None, 15_000.0, None)
    assert score == pytest.approx(40.0, abs=0.01)


def test_fuentes_disponibles_reporta_legacy():
    row = {
        "flujo_popular_times_score": 70.0,
        "vcity_flujo_peatonal": None,
        "flujo_peatonal_total": 5000.0,
        "ratio_locales_comerciales": 0.5,
    }
    fuentes = fuentes_disponibles(row)
    # Las fuentes legacy aparecen para que la UI de debug las vea,
    # aunque no afecten al score.
    assert "popular_times" in fuentes
    assert "vcity" not in fuentes
    assert "vianants" in fuentes
    assert "ratio_locales" in fuentes


# ---------------------------------------------------------------------------
# calcular_flujo_score — normalización de fuentes raw
# ---------------------------------------------------------------------------

def test_vcity_normalizado_a_maximo():
    """vcity igual al máximo de referencia da score vcity=100."""
    score = calcular_flujo_score(None, 50_000.0, None, None)
    assert score == pytest.approx(100.0, abs=1)


def test_vcity_por_encima_del_maximo_se_clampa():
    """Valores vcity superiores al máximo de referencia no superan 100."""
    score = calcular_flujo_score(None, 200_000.0, None, None)
    assert score == pytest.approx(100.0, abs=0.01)


def test_valores_cero_devuelve_cero():
    """Popular_times y vcity en cero → score 0."""
    score = calcular_flujo_score(0.0, 0.0, 0.0, 0.0)
    assert score == pytest.approx(0.0, abs=0.01)


def test_sin_ninguna_fuente_devuelve_fallback():
    """Sin ninguna fuente disponible devuelve el fallback conservador 40.0."""
    score = calcular_flujo_score(None, None, None, None)
    assert score == pytest.approx(40.0, abs=0.01)


# ---------------------------------------------------------------------------
# calcular_flujo_score — fusión
# ---------------------------------------------------------------------------

def test_score_mayor_con_ambas_fuentes_altas():
    """
    Con popular_times + vcity altos, el score supera claramente al fallback
    de sin fuentes.
    """
    score_completo = calcular_flujo_score(80.0, 40000.0, 12000.0, 0.8)
    score_fallback = calcular_flujo_score(None, None, None, None)
    assert score_completo > score_fallback


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


# ---------------------------------------------------------------------------
# PESOS_BASE invariantes
# ---------------------------------------------------------------------------

def test_pesos_base_suman_uno():
    """Los pesos base deben sumar exactamente 1.0."""
    assert sum(PESOS_BASE.values()) == pytest.approx(1.0, abs=1e-9)


def test_pesos_base_solo_vcity_y_popular_times():
    """PESOS_BASE contiene exactamente las dos fuentes activas del scorer."""
    assert set(PESOS_BASE.keys()) == {"vcity", "popular_times"}


def test_constantes_publicas_exportadas():
    """VCITY_MAX_BARCELONA es la referencia de normalización activa."""
    assert VCITY_MAX_BARCELONA == pytest.approx(50_000.0)
    # Vianants se mantiene exportado por compatibilidad histórica pero ya no se usa.
    assert VIANANTS_MAX_BARCELONA == pytest.approx(15_000.0)


# ---------------------------------------------------------------------------
# flujo_peatonal_explain — shape y sources
# ---------------------------------------------------------------------------

class TestFlujoExplain:
    def test_estructura_completa(self):
        """explain devuelve score, sources con 4 claves (shape legacy) y sources_available."""
        row = {
            "flujo_popular_times_score": 80.0,
            "vcity_flujo_peatonal": 30_000.0,
            "flujo_peatonal_total": 8_000.0,
            "ratio_locales_comerciales": 0.5,
        }
        result = flujo_peatonal_explain(row)
        assert "score" in result
        assert "sources" in result
        assert "sources_available" in result
        # Forma externa mantiene las 4 claves por compat
        assert set(result["sources"].keys()) == {"popular_times", "vcity", "vianants", "ratio_locales"}

    def test_vianants_y_ratio_locales_marcadas_missing(self):
        """Vianants y ratio_locales siempre aparecen como missing/weight=0 en explain."""
        row = {
            "flujo_popular_times_score": 80.0,
            "vcity_flujo_peatonal": 30_000.0,
            "flujo_peatonal_total": 8_000.0,
            "ratio_locales_comerciales": 0.5,
        }
        result = flujo_peatonal_explain(row)
        for legacy in ("vianants", "ratio_locales"):
            assert result["sources"][legacy]["missing"] is True
            assert result["sources"][legacy]["weight"] == pytest.approx(0.0)
            assert result["sources"][legacy]["contribution"] == pytest.approx(0.0)

    def test_sources_available_cuenta_solo_activas(self):
        """sources_available cuenta sólo popular_times + vcity."""
        row = {
            "flujo_popular_times_score": 50.0,
            "vcity_flujo_peatonal": 20_000.0,
            "flujo_peatonal_total": 7_000.0,    # legacy, no cuenta
            "ratio_locales_comerciales": 0.25,   # legacy, no cuenta
        }
        result = flujo_peatonal_explain(row)
        assert result["sources_available"] == 2

    def test_fuente_ausente_marca_missing(self):
        """Si vcity=None y popular_times presente → sources_available=1 y weights redistribuidos."""
        row = {
            "flujo_popular_times_score": 70.0,
            "vcity_flujo_peatonal": None,
            "flujo_peatonal_total": 5_000.0,
            "ratio_locales_comerciales": 0.3,
        }
        result = flujo_peatonal_explain(row)
        vcity = result["sources"]["vcity"]
        assert vcity["missing"] is True
        assert vcity["weight"] == pytest.approx(0.0)
        assert result["sources"]["popular_times"]["weight"] == pytest.approx(1.0)
        assert result["sources_available"] == 1

    def test_sin_ninguna_fuente_devuelve_fallback(self):
        """Sin popular_times ni vcity: score=30.0 (legacy) y sources_available=0."""
        row = {
            "flujo_popular_times_score": None,
            "vcity_flujo_peatonal": None,
            "flujo_peatonal_total": None,
            "ratio_locales_comerciales": None,
        }
        result = flujo_peatonal_explain(row)
        assert result["score"] == pytest.approx(30.0)
        assert result["sources_available"] == 0
        for src in result["sources"].values():
            assert src["missing"] is True

    def test_pesos_activos_suman_uno(self):
        """Con vcity + popular_times la suma de pesos activos es 1.0."""
        row = {
            "flujo_popular_times_score": 60.0,
            "vcity_flujo_peatonal": 20_000.0,
        }
        result = flujo_peatonal_explain(row)
        total_w = result["sources"]["popular_times"]["weight"] + result["sources"]["vcity"]["weight"]
        assert total_w == pytest.approx(1.0, abs=1e-6)

    def test_alias_popular_times_score(self):
        """explain acepta 'popular_times_score' como alias."""
        row_alias = {"popular_times_score": 65.0, "vcity_flujo_peatonal": 10_000.0}
        row_col   = {"flujo_popular_times_score": 65.0, "vcity_flujo_peatonal": 10_000.0}
        assert (
            flujo_peatonal_explain(row_alias)["score"]
            == pytest.approx(flujo_peatonal_explain(row_col)["score"], abs=0.01)
        )


# ---------------------------------------------------------------------------
# vcity_shopping_rate (mig 035) — modulador de calidad del flujo
# ---------------------------------------------------------------------------

class TestVcityShoppingRate:
    """
    quality_mult = 1 + 0.5·(rate − 0.20), cap [0.85, 1.25].
    Se aplica a todas las ramas de flujo no-fallback. None → 1.0 (neutro).
    """

    def test_rate_bajo_005_da_score_menor_que_neutro(self):
        s_neutro = calcular_flujo_score(70.0, 20_000.0, None, None)
        s_bajo   = calcular_flujo_score(
            70.0, 20_000.0, None, None, vcity_shopping_rate=0.05
        )
        assert s_bajo < s_neutro

    def test_rate_alto_060_da_score_mayor_que_neutro(self):
        s_neutro = calcular_flujo_score(70.0, 20_000.0, None, None)
        s_alto   = calcular_flujo_score(
            70.0, 20_000.0, None, None, vcity_shopping_rate=0.60
        )
        if s_neutro < 95.0:
            assert s_alto > s_neutro

    def test_rate_none_es_idempotente_con_baseline(self):
        s_sin_key = calcular_flujo_score(70.0, 20_000.0, None, None)
        s_none    = calcular_flujo_score(
            70.0, 20_000.0, None, None, vcity_shopping_rate=None
        )
        assert s_sin_key == pytest.approx(s_none, abs=0.01)

    def test_rate_020_no_modifica(self):
        """rate=0.20 es el baseline: quality_mult = 1.0 → score idéntico."""
        s_neutro = calcular_flujo_score(70.0, 20_000.0, None, None)
        s_020    = calcular_flujo_score(
            70.0, 20_000.0, None, None, vcity_shopping_rate=0.20
        )
        assert s_020 == pytest.approx(s_neutro, abs=0.01)
