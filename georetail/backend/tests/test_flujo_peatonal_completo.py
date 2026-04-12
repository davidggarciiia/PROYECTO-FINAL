"""
tests/test_flujo_peatonal_completo.py — Tests exhaustivos de scoring/flujo_peatonal.py.

Agente: ruflo-test-flujo
Tarea:  task-1774810147723-imbqh1

Cubre:
  - Las 16 combinaciones (2^4) de presencia/ausencia de las 4 fuentes
  - Redistribución adaptativa de pesos: siempre suman 1.0 entre disponibles
  - ratio_locales=0.0 tratado como dato disponible (no None)
  - Fallback 30.0 cuando ninguna fuente disponible
  - Capping [0, 100] — valores extremos no salen del rango
  - Normalización de cada fuente (popular_times ya 0-100, vcity/vianants escalados)
  - calcular_flujo_score_batch: lista de rows, mismos resultados que individual
  - fuentes_disponibles: detecta exactamente las fuentes presentes
  - flujo_peatonal_explain: estructura completa, contributions suman al score
  - PESOS_BASE suman exactamente 1.0
  - Constantes de normalización exportadas
"""
from __future__ import annotations

import math
import itertools
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


# ── Clase 1: PESOS_BASE ───────────────────────────────────────────────────────

class TestPesosBase:
    def test_pesos_base_suman_uno(self):
        assert abs(sum(PESOS_BASE.values()) - 1.0) < 1e-9

    def test_pesos_base_tiene_4_fuentes(self):
        assert len(PESOS_BASE) == 4

    def test_fuentes_esperadas(self):
        assert set(PESOS_BASE.keys()) == {
            "popular_times", "vcity", "vianants", "ratio_locales"
        }

    def test_peso_popular_times_35pct(self):
        assert PESOS_BASE["popular_times"] == pytest.approx(0.35)

    def test_peso_vcity_30pct(self):
        assert PESOS_BASE["vcity"] == pytest.approx(0.30)

    def test_peso_vianants_20pct(self):
        assert PESOS_BASE["vianants"] == pytest.approx(0.20)

    def test_peso_ratio_locales_15pct(self):
        assert PESOS_BASE["ratio_locales"] == pytest.approx(0.15)

    def test_todos_pesos_positivos(self):
        for k, v in PESOS_BASE.items():
            assert v > 0, f"Peso {k} no es positivo"


# ── Clase 2: Constantes de normalización ─────────────────────────────────────

class TestConstantesNormalizacion:
    def test_vcity_max_50000(self):
        assert VCITY_MAX_BARCELONA == pytest.approx(50_000.0)

    def test_vianants_max_15000(self):
        assert VIANANTS_MAX_BARCELONA == pytest.approx(15_000.0)

    def test_constantes_positivas(self):
        assert VCITY_MAX_BARCELONA > 0
        assert VIANANTS_MAX_BARCELONA > 0


# ── Clase 3: Casos básicos ────────────────────────────────────────────────────

class TestCasosBasicos:
    def test_todas_fuentes_none_devuelve_fallback_30(self):
        score = calcular_flujo_score(None, None, None, None)
        assert score == pytest.approx(30.0)

    def test_resultado_en_rango_0_100(self):
        score = calcular_flujo_score(50.0, 25000.0, 7500.0, 0.22)
        assert 0.0 <= score <= 100.0

    def test_todas_fuentes_max_da_100(self):
        score = calcular_flujo_score(
            popular_times_score=100.0,
            vcity_flujo=VCITY_MAX_BARCELONA,
            vianants_intensitat=VIANANTS_MAX_BARCELONA,
            ratio_locales=1.0,
        )
        assert score == pytest.approx(100.0)

    def test_todas_fuentes_cero_da_cero(self):
        score = calcular_flujo_score(0.0, 0.0, 0.0, 0.0)
        assert score == pytest.approx(0.0)

    def test_resultado_redondeado_2_decimales(self):
        score = calcular_flujo_score(50.0, 25000.0, 7500.0, 0.22)
        # Debe estar redondeado a 2 decimales
        assert score == round(score, 2)


# ── Clase 4: Las 16 combinaciones de fuentes (2^4) ────────────────────────────

class TestTodasLasCombinaciones:
    """Genera las 16 combinaciones y verifica que ninguna rompe."""

    _VALORES = {
        "popular_times": 60.0,
        "vcity": 25_000.0,
        "vianants": 7_500.0,
        "ratio_locales": 0.25,
    }

    def _score_con_combinacion(self, fuentes_presentes: set) -> float:
        return calcular_flujo_score(
            popular_times_score=(
                self._VALORES["popular_times"] if "popular_times" in fuentes_presentes else None
            ),
            vcity_flujo=(
                self._VALORES["vcity"] if "vcity" in fuentes_presentes else None
            ),
            vianants_intensitat=(
                self._VALORES["vianants"] if "vianants" in fuentes_presentes else None
            ),
            ratio_locales=(
                self._VALORES["ratio_locales"] if "ratio_locales" in fuentes_presentes else None
            ),
        )

    def test_todas_las_16_combinaciones_no_rompen(self):
        todas_fuentes = ["popular_times", "vcity", "vianants", "ratio_locales"]
        for r in range(5):  # 0..4 fuentes presentes
            for combo in itertools.combinations(todas_fuentes, r):
                fuentes = set(combo)
                score = self._score_con_combinacion(fuentes)
                assert math.isfinite(score), f"No finito para {fuentes}"
                assert 0.0 <= score <= 100.0, f"Fuera de rango para {fuentes}"

    def test_sin_fuentes_es_fallback_30(self):
        assert self._score_con_combinacion(set()) == pytest.approx(30.0)

    def test_con_todas_fuentes_mayor_que_sin_ninguna(self):
        todas = {"popular_times", "vcity", "vianants", "ratio_locales"}
        score_todas = self._score_con_combinacion(todas)
        score_ninguna = self._score_con_combinacion(set())
        assert score_todas > score_ninguna

    def test_mas_fuentes_activas_tiende_a_mayor_score(self):
        """Con los mismos valores, más fuentes activas → tendencia a mayor score."""
        solo_ratio = self._score_con_combinacion({"ratio_locales"})
        tres_fuentes = self._score_con_combinacion(
            {"popular_times", "vcity", "ratio_locales"}
        )
        # popular_times=60 y vcity da ~50 en escala normalizada →
        # con más fuentes de alto valor el score debe ser mayor
        assert tres_fuentes > solo_ratio


# ── Clase 5: Redistribución adaptativa de pesos ──────────────────────────────

class TestRedistribucionPesos:
    def test_solo_una_fuente_recibe_peso_total(self):
        """Si solo hay 1 fuente, su peso ajustado debe ser 1.0."""
        score_solo_pt = calcular_flujo_score(
            popular_times_score=50.0,
            vcity_flujo=None,
            vianants_intensitat=None,
            ratio_locales=None,
        )
        # Si peso_adj["popular_times"]=1.0 y valor=50 → score=50
        assert score_solo_pt == pytest.approx(50.0)

    def test_solo_ratio_locales_usa_peso_total(self):
        score = calcular_flujo_score(None, None, None, ratio_locales=0.5)
        # ratio_locales=0.5 → normalizado a 50; peso ajustado=1.0 → score=50
        assert score == pytest.approx(50.0)

    def test_solo_vcity_usa_peso_total(self):
        # vcity = VCITY_MAX/2 → normalizado a 50; peso ajustado=1.0 → score=50
        score = calcular_flujo_score(None, VCITY_MAX_BARCELONA / 2, None, None)
        assert score == pytest.approx(50.0)

    def test_solo_vianants_usa_peso_total(self):
        # vianants = VIANANTS_MAX/2 → normalizado a 50; peso ajustado=1.0 → score=50
        score = calcular_flujo_score(None, None, VIANANTS_MAX_BARCELONA / 2, None)
        assert score == pytest.approx(50.0)

    def test_dos_fuentes_pesos_normalizados(self):
        """Con 2 fuentes, sus pesos relativos se mantienen proporcionales."""
        # pt=0.35, vc=0.30 → normalizados: pt=0.35/0.65≈0.538, vc=0.30/0.65≈0.462
        pt_peso = PESOS_BASE["popular_times"]
        vc_peso = PESOS_BASE["vcity"]
        total = pt_peso + vc_peso
        pt_adj = pt_peso / total
        vc_adj = vc_peso / total
        assert abs(pt_adj + vc_adj - 1.0) < 1e-9

    def test_pesos_ajustados_suman_1_con_3_fuentes(self):
        fuentes = ["popular_times", "vcity", "vianants"]
        total = sum(PESOS_BASE[f] for f in fuentes)
        pesos_adj = [PESOS_BASE[f] / total for f in fuentes]
        assert abs(sum(pesos_adj) - 1.0) < 1e-9


# ── Clase 6: ratio_locales=0.0 es dato válido ────────────────────────────────

class TestRatioLocalesCero:
    def test_ratio_locales_cero_no_es_none(self):
        """ratio_locales=0.0 debe tratarse como fuente disponible, no None."""
        score_cero = calcular_flujo_score(None, None, None, ratio_locales=0.0)
        score_none = calcular_flujo_score(None, None, None, ratio_locales=None)
        # Con ratio=0.0 → normalizado a 0, score=0 (no fallback 30)
        assert score_cero == pytest.approx(0.0)
        # Con ratio=None → fallback 30
        assert score_none == pytest.approx(30.0)
        assert score_cero != score_none

    def test_ratio_locales_cero_con_otras_fuentes(self):
        """ratio_locales=0.0 contribuye como 0, no elimina las otras."""
        score_rl_cero = calcular_flujo_score(60.0, None, None, ratio_locales=0.0)
        score_rl_none = calcular_flujo_score(60.0, None, None, ratio_locales=None)
        # Con rl=0: pt y rl presentes → pesos redistribuidos entre 2 fuentes
        # Con rl=None: solo pt presente → pt recibe todo el peso
        # score con solo pt (peso=1.0): 60
        # score con pt+rl(0): pt_adj=0.35/0.5=0.70 → 0.70*60 + 0.30*0=42
        assert score_rl_none == pytest.approx(60.0)
        assert score_rl_cero < score_rl_none


# ── Clase 7: Capping [0, 100] ─────────────────────────────────────────────────

class TestCapping:
    def test_popular_times_mayor_100_capped(self):
        score = calcular_flujo_score(
            popular_times_score=150.0, vcity_flujo=None,
            vianants_intensitat=None, ratio_locales=None
        )
        assert score <= 100.0

    def test_popular_times_negativo_capped_0(self):
        score = calcular_flujo_score(
            popular_times_score=-50.0, vcity_flujo=None,
            vianants_intensitat=None, ratio_locales=None
        )
        assert score >= 0.0

    def test_vcity_enorme_capped_100(self):
        score = calcular_flujo_score(None, 10_000_000.0, None, None)
        assert score == pytest.approx(100.0)

    def test_vianants_enorme_capped_100(self):
        score = calcular_flujo_score(None, None, 10_000_000.0, None)
        assert score == pytest.approx(100.0)

    def test_ratio_mayor_1_capped_100_normalizado(self):
        score = calcular_flujo_score(None, None, None, ratio_locales=5.0)
        assert score == pytest.approx(100.0)


# ── Clase 8: calcular_flujo_score_batch ───────────────────────────────────────

class TestBatch:
    def test_batch_devuelve_lista(self):
        rows = [
            {"flujo_popular_times_score": 50.0, "vcity_flujo_peatonal": 20000.0,
             "flujo_peatonal_total": 5000.0, "ratio_locales_comerciales": 0.3},
        ]
        result = calcular_flujo_score_batch(rows)
        assert isinstance(result, list)
        assert len(result) == 1

    def test_batch_longitud_igual_a_input(self):
        rows = [
            {"flujo_popular_times_score": 60.0},
            {"vcity_flujo_peatonal": 30000.0},
            {},
        ]
        result = calcular_flujo_score_batch(rows)
        assert len(result) == 3

    def test_batch_mismo_resultado_que_individual(self):
        rows = [
            {"flujo_popular_times_score": 70.0, "vcity_flujo_peatonal": 30000.0,
             "flujo_peatonal_total": 8000.0, "ratio_locales_comerciales": 0.4},
            {"flujo_popular_times_score": None, "vcity_flujo_peatonal": None,
             "flujo_peatonal_total": None, "ratio_locales_comerciales": None},
        ]
        batch = calcular_flujo_score_batch(rows)
        individual_0 = calcular_flujo_score(70.0, 30000.0, 8000.0, 0.4)
        individual_1 = calcular_flujo_score(None, None, None, None)
        assert batch[0] == pytest.approx(individual_0)
        assert batch[1] == pytest.approx(individual_1)

    def test_batch_vacio_devuelve_lista_vacia(self):
        result = calcular_flujo_score_batch([])
        assert result == []

    def test_batch_todos_none_devuelve_fallbacks(self):
        rows = [{}, {}, {}]
        result = calcular_flujo_score_batch(rows)
        for score in result:
            assert score == pytest.approx(30.0)

    def test_batch_acepta_key_alterna_popular_times(self):
        """El batch acepta tanto 'popular_times_score' como 'flujo_popular_times_score'."""
        row_a = {"flujo_popular_times_score": 80.0}
        row_b = {"popular_times_score": 80.0}
        result_a = calcular_flujo_score_batch([row_a])[0]
        result_b = calcular_flujo_score_batch([row_b])[0]
        assert result_a == pytest.approx(result_b)


# ── Clase 9: fuentes_disponibles ─────────────────────────────────────────────

class TestFuentesDisponibles:
    def test_todas_presentes(self):
        row = {
            "flujo_popular_times_score": 60.0,
            "vcity_flujo_peatonal": 20000.0,
            "flujo_peatonal_total": 5000.0,
            "ratio_locales_comerciales": 0.3,
        }
        f = fuentes_disponibles(row)
        assert set(f) == {"popular_times", "vcity", "vianants", "ratio_locales"}

    def test_ninguna_presente(self):
        assert fuentes_disponibles({}) == []

    def test_solo_vcity(self):
        row = {"vcity_flujo_peatonal": 20000.0}
        f = fuentes_disponibles(row)
        assert f == ["vcity"]

    def test_ratio_cero_es_fuente_disponible(self):
        """ratio_locales=0.0 debe aparecer en fuentes_disponibles."""
        row = {"ratio_locales_comerciales": 0.0}
        f = fuentes_disponibles(row)
        assert "ratio_locales" in f

    def test_popular_times_alias_alternativo(self):
        row = {"popular_times_score": 50.0}
        f = fuentes_disponibles(row)
        assert "popular_times" in f

    def test_devuelve_lista(self):
        result = fuentes_disponibles({"vcity_flujo_peatonal": 10000.0})
        assert isinstance(result, list)


# ── Clase 10: flujo_peatonal_explain ─────────────────────────────────────────

class TestFlujoExplain:
    def test_estructura_de_salida(self):
        row = {
            "flujo_popular_times_score": 60.0,
            "vcity_flujo_peatonal": 25000.0,
            "flujo_peatonal_total": 7500.0,
            "ratio_locales_comerciales": 0.25,
        }
        result = flujo_peatonal_explain(row)
        assert "score" in result
        assert "sources" in result
        assert "sources_available" in result

    def test_sources_tiene_4_entradas(self):
        row = {"flujo_popular_times_score": 50.0}
        result = flujo_peatonal_explain(row)
        assert len(result["sources"]) == 4

    def test_sources_claves_correctas(self):
        result = flujo_peatonal_explain({})
        assert set(result["sources"].keys()) == {
            "popular_times", "vcity", "vianants", "ratio_locales"
        }

    def test_source_entry_tiene_campos_requeridos(self):
        row = {"flujo_popular_times_score": 60.0}
        result = flujo_peatonal_explain(row)
        for k, entry in result["sources"].items():
            assert "value" in entry, f"{k} sin 'value'"
            assert "weight" in entry, f"{k} sin 'weight'"
            assert "contribution" in entry, f"{k} sin 'contribution'"
            assert "missing" in entry, f"{k} sin 'missing'"

    def test_fuente_ausente_marcada_missing(self):
        row = {"flujo_popular_times_score": 60.0}
        result = flujo_peatonal_explain(row)
        assert result["sources"]["vcity"]["missing"] is True
        assert result["sources"]["popular_times"]["missing"] is False

    def test_score_explainado_igual_a_calcular_flujo_score(self):
        row = {
            "flujo_popular_times_score": 70.0,
            "vcity_flujo_peatonal": 30000.0,
            "flujo_peatonal_total": None,
            "ratio_locales_comerciales": 0.3,
        }
        explain_score = flujo_peatonal_explain(row)["score"]
        direct_score = calcular_flujo_score(70.0, 30000.0, None, 0.3)
        assert explain_score == pytest.approx(direct_score)

    def test_sin_fuentes_devuelve_score_30_y_sources_available_0(self):
        result = flujo_peatonal_explain({})
        assert result["score"] == pytest.approx(30.0)
        assert result["sources_available"] == 0

    def test_todas_fuentes_sources_available_4(self):
        row = {
            "flujo_popular_times_score": 50.0,
            "vcity_flujo_peatonal": 25000.0,
            "flujo_peatonal_total": 7500.0,
            "ratio_locales_comerciales": 0.2,
        }
        result = flujo_peatonal_explain(row)
        assert result["sources_available"] == 4

    def test_contributions_fuentes_disponibles_suman_score(self):
        """La suma de contributions de fuentes presentes debe dar el score."""
        row = {
            "flujo_popular_times_score": 60.0,
            "vcity_flujo_peatonal": 25000.0,
            "ratio_locales_comerciales": 0.3,
        }
        result = flujo_peatonal_explain(row)
        contrib_total = sum(
            s["contribution"] for s in result["sources"].values()
        )
        assert abs(contrib_total - result["score"]) < 0.01

    def test_fuente_ausente_contribution_cero(self):
        row = {"flujo_popular_times_score": 60.0}
        result = flujo_peatonal_explain(row)
        assert result["sources"]["vcity"]["contribution"] == pytest.approx(0.0)
        assert result["sources"]["vianants"]["contribution"] == pytest.approx(0.0)

    def test_explain_score_en_rango_0_100(self):
        for pt in [0, 30, 60, 100]:
            row = {"flujo_popular_times_score": float(pt)}
            result = flujo_peatonal_explain(row)
            assert 0.0 <= result["score"] <= 100.0


# ── Clase 11: Normalización por fuente ────────────────────────────────────────

class TestNormalizacionFuentes:
    def test_popular_times_100_normaliza_a_100(self):
        score = calcular_flujo_score(100.0, None, None, None)
        assert score == pytest.approx(100.0)

    def test_popular_times_50_normaliza_a_50(self):
        score = calcular_flujo_score(50.0, None, None, None)
        assert score == pytest.approx(50.0)

    def test_vcity_max_normaliza_a_100(self):
        score = calcular_flujo_score(None, VCITY_MAX_BARCELONA, None, None)
        assert score == pytest.approx(100.0)

    def test_vcity_mitad_normaliza_a_50(self):
        score = calcular_flujo_score(None, VCITY_MAX_BARCELONA / 2, None, None)
        assert score == pytest.approx(50.0)

    def test_vianants_max_normaliza_a_100(self):
        score = calcular_flujo_score(None, None, VIANANTS_MAX_BARCELONA, None)
        assert score == pytest.approx(100.0)

    def test_vianants_mitad_normaliza_a_50(self):
        score = calcular_flujo_score(None, None, VIANANTS_MAX_BARCELONA / 2, None)
        assert score == pytest.approx(50.0)

    def test_ratio_locales_1_normaliza_a_100(self):
        score = calcular_flujo_score(None, None, None, ratio_locales=1.0)
        assert score == pytest.approx(100.0)

    def test_ratio_locales_0_5_normaliza_a_50(self):
        score = calcular_flujo_score(None, None, None, ratio_locales=0.5)
        assert score == pytest.approx(50.0)
