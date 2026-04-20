"""
Tests para _calcular_score_base y el gate de recálculo (ADR implícito:
flujo peatonal de El Born iba a 20 mientras score_global cacheado seguía
en 86 — flujo pesaba 25%, matemáticamente incoherente).

Motor se encarga de:
  1. Recalcular siempre que los pesos vengan modulados por idea.
  2. Recalcular cuando el cache XGBoost diverge > 15 pts del valor derivado
     de las dimensiones manuales.
"""
from __future__ import annotations

import pytest

from scoring.motor import (
    _PESOS_DEFAULT,
    _calcular_score_base,
    _son_pesos_default,
)


class TestPesosDefault:
    def test_exactos_son_default(self):
        assert _son_pesos_default(_PESOS_DEFAULT) is True

    def test_tolerancia_005_ok(self):
        casi = {**_PESOS_DEFAULT, "peso_flujo": 0.2504}
        assert _son_pesos_default(casi) is True

    def test_mas_que_tolerancia_no_es_default(self):
        modulado = {**_PESOS_DEFAULT, "peso_flujo": 0.40}
        assert _son_pesos_default(modulado) is False

    def test_dict_vacio_es_default_trivial(self):
        assert _son_pesos_default({}) is True

    def test_keys_no_peso_se_ignoran(self):
        mezcla = {**_PESOS_DEFAULT, "otra_cosa": 999}
        assert _son_pesos_default(mezcla) is True


# Escenario "El Born": cache XGBoost dice global=86, las dimensiones manuales
# frescas dan flujo=20. Matemáticamente, con pesos default 25/25/15/15/10/5/5
# el global derivado cae a ~55. El gate fuerza recálculo.
_SCORES_EL_BORN = {
    "zona_id": "el_born_demo",
    "modelo_version": "xgboost_v1",
    "score_global": 86.0,             # cache obsoleto
    "score_flujo_peatonal": 20.0,     # fresco, bajísimo
    "score_demografia": 70.0,
    "score_competencia": 55.0,
    "score_transporte": 80.0,
    "score_dinamismo": 65.0,
    "score_seguridad": 50.0,
    "score_turismo": 75.0,
    "score_precio_alquiler": 40.0,
}


class TestDivergenciaGate:
    def test_el_born_fuerza_recalculo(self):
        scores = dict(_SCORES_EL_BORN)
        nuevo = _calcular_score_base(scores, dict(_PESOS_DEFAULT))
        # Debe bajar claramente respecto al 86 cacheado.
        assert nuevo < 75.0, f"se esperaba recalcular; quedó {nuevo}"
        # Y debe marcar el override para trazabilidad.
        assert "manual_override" in (scores.get("modelo_version") or "")

    def test_cache_coherente_se_preserva(self):
        """
        Si el cache y las dimensiones son consistentes, respetamos el cache
        (es más rápido y respeta la precisión de XGBoost).
        """
        scores = {
            "zona_id": "eixample_coherente",
            "modelo_version": "xgboost_v1",
            "score_global": 72.0,
            "score_flujo_peatonal": 75.0,
            "score_demografia": 72.0,
            "score_competencia": 68.0,
            "score_transporte": 78.0,
            "score_dinamismo": 70.0,
            "score_seguridad": 65.0,
            "score_turismo": 70.0,
            "score_precio_alquiler": 65.0,
        }
        out = _calcular_score_base(scores, dict(_PESOS_DEFAULT))
        assert out == pytest.approx(72.0, abs=0.1)
        assert "manual_override" not in (scores.get("modelo_version") or "")


class TestPesosModulados:
    def test_pesos_modulados_fuerzan_recalc_aunque_cache_coherente(self):
        """
        Si el usuario tiene una idea que modula los pesos, ignoramos el cache
        y recalculamos SIEMPRE. Así dos ideas distintas pueden dar rankings
        distintos sobre la misma zona.
        """
        scores = dict(_SCORES_EL_BORN)
        scores["score_global"] = 72.0  # incluso si el cache cuadra
        pesos_mod = {**_PESOS_DEFAULT, "peso_flujo": 0.40, "peso_demo": 0.10}
        out = _calcular_score_base(scores, pesos_mod)
        # Con peso_flujo=0.40 y flujo=20 la media ponderada baja mucho.
        assert out < 60.0, f"peso modulado debería bajar el score: {out}"
        assert "manual_override" in (scores.get("modelo_version") or "")

    def test_ideas_distintas_dan_scores_distintos(self):
        base = dict(_SCORES_EL_BORN)
        # Idea 1: cafetería specialty → pondera demografía alta y turismo
        pesos_specialty = {
            **_PESOS_DEFAULT,
            "peso_demo": 0.35,
            "peso_turismo": 0.15,
            "peso_flujo": 0.20,
        }
        # Idea 2: bar de barrio → pondera flujo y competencia, poco turismo
        pesos_barrio = {
            **_PESOS_DEFAULT,
            "peso_flujo": 0.35,
            "peso_competencia": 0.25,
            "peso_turismo": 0.02,
        }

        scores_a = dict(base)
        scores_b = dict(base)
        out_a = _calcular_score_base(scores_a, pesos_specialty)
        out_b = _calcular_score_base(scores_b, pesos_barrio)
        assert out_a != out_b, "ideas distintas deberían dar scores distintos"
