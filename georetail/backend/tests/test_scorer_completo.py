"""
tests/test_scorer_completo.py — Tests exhaustivos del scorer manual (scorer.py).

Agente: ruflo-test-scorer
Tarea:  task-1774810133286-tcg7a0

Cubre:
  - Verificación analítica exacta de cada una de las 8 dimensiones
  - Bonus costero (dist_playa_m) en turismo: <300m, <700m, <1500m, >1500m
  - Score global = suma ponderada de dimensiones
  - Monotonía: más flujo/renta/etc → mayor score correspondiente
  - Semántica None vs 0: renta=0 legítima, saturacion=0 sin competencia
  - Defaults cuando datos ausentes (sector vacío, datos zona vacíos)
  - _score_neutro: formato y valores
  - Claves de salida completas en _score_manual
  - Capping 0-100 en todos los scores de dimensión
"""
from __future__ import annotations

import math
import pytest

from scoring.scorer import _score_manual, _score_neutro


# ── Helpers ────────────────────────────────────────────────────────────────────

def _sector_default():
    """Sector con pesos estándar que suman 1.0."""
    return {
        "peso_flujo": 0.25,
        "peso_demo": 0.20,
        "peso_competencia": 0.15,
        "peso_precio": 0.15,
        "peso_transporte": 0.10,
        "peso_seguridad": 0.05,
        "peso_turismo": 0.05,
        "peso_entorno": 0.05,
    }


def _datos_medios():
    """Zona con datos que producen scores razonables en todas las dimensiones."""
    return {
        "flujo_peatonal_total": 1500,       # score ~50
        "renta_media_hogar": 38500,         # (38500-17000)/430 ≈ 50
        "score_saturacion": 50,             # s_comp = 50
        "precio_m2": 26.5,                  # (45-26.5)/0.37 ≈ 50
        "num_lineas_transporte": 10,        # 10*5=50
        "incidencias_por_1000hab": 62.5,    # (120-62.5)/1.15 ≈ 50
        "score_turismo": 50,
        "pct_locales_vacios": 0.05,
        "tasa_rotacion_anual": 0.05,        # 100-0.05*200-0.05*100 = 100-10-5=85... let's use lower
    }


# ── Clase 1: Verificación analítica de cada dimensión ─────────────────────────

class TestDimensionFlujo:
    def test_flujo_cero_da_score_cero(self):
        datos = {"flujo_peatonal_total": 0}
        r = _score_manual(datos, {})
        assert r["score_flujo_peatonal"] == 0.0

    def test_flujo_3000_da_score_100(self):
        datos = {"flujo_peatonal_total": 3000}
        r = _score_manual(datos, {})
        assert r["score_flujo_peatonal"] == 100.0

    def test_flujo_1500_da_score_50(self):
        datos = {"flujo_peatonal_total": 1500}
        r = _score_manual(datos, {})
        assert r["score_flujo_peatonal"] == 50.0

    def test_flujo_mayor_3000_capped_100(self):
        datos = {"flujo_peatonal_total": 9999}
        r = _score_manual(datos, {})
        assert r["score_flujo_peatonal"] == 100.0

    def test_flujo_none_trata_como_cero(self):
        datos = {}
        r = _score_manual(datos, {})
        assert r["score_flujo_peatonal"] == 0.0

    def test_flujo_formula_exacta(self):
        # score = min(100, flujo / 30)
        for flujo, esperado in [(600, 20.0), (900, 30.0), (2100, 70.0)]:
            r = _score_manual({"flujo_peatonal_total": flujo}, {})
            assert abs(r["score_flujo_peatonal"] - esperado) < 0.01, f"flujo={flujo}"


class TestDimensionDemografia:
    def test_renta_minima_17000_da_score_cero(self):
        datos = {"renta_media_hogar": 17000}
        r = _score_manual(datos, {})
        assert r["score_demografia"] == 0.0

    def test_renta_60000_da_score_100(self):
        # (60000-17000)/430 = 43000/430 ≈ 100
        datos = {"renta_media_hogar": 60000}
        r = _score_manual(datos, {})
        assert r["score_demografia"] == 100.0

    def test_renta_none_usa_default_30000(self):
        # sin datos usa 30000: (30000-17000)/430 ≈ 30.2
        datos_sin = {}
        datos_con = {"renta_media_hogar": 30000}
        r_sin = _score_manual(datos_sin, {})
        r_con = _score_manual(datos_con, {})
        assert abs(r_sin["score_demografia"] - r_con["score_demografia"]) < 0.01

    def test_renta_cero_es_legitimo_no_imputa(self):
        """renta=0 debe usar 0, no imputar a 30000."""
        datos_cero = {"renta_media_hogar": 0}
        datos_none = {}
        r_cero = _score_manual(datos_cero, {})
        r_none = _score_manual(datos_none, {})
        # Con renta=0: (0-17000)/430 → negativo → capped 0
        assert r_cero["score_demografia"] == 0.0
        # Con None: usa 30000 → score ~30
        assert r_none["score_demografia"] > 0.0
        assert r_cero["score_demografia"] != r_none["score_demografia"]

    def test_renta_muy_baja_capped_cero(self):
        datos = {"renta_media_hogar": 5000}
        r = _score_manual(datos, {})
        assert r["score_demografia"] == 0.0

    def test_renta_alta_capped_100(self):
        datos = {"renta_media_hogar": 100000}
        r = _score_manual(datos, {})
        assert r["score_demografia"] == 100.0


class TestDimensionCompetencia:
    def test_saturacion_cero_da_score_100(self):
        """saturacion=0 = sin competencia = score máximo."""
        datos = {"score_saturacion": 0}
        r = _score_manual(datos, {})
        assert r["score_competencia"] == 100.0

    def test_saturacion_100_da_score_cero(self):
        datos = {"score_saturacion": 100}
        r = _score_manual(datos, {})
        assert r["score_competencia"] == 0.0

    def test_saturacion_none_usa_default_50(self):
        datos_sin = {}
        datos_con = {"score_saturacion": 50}
        r_sin = _score_manual(datos_sin, {})
        r_con = _score_manual(datos_con, {})
        assert abs(r_sin["score_competencia"] - r_con["score_competencia"]) < 0.01

    def test_saturacion_cero_distinto_de_none(self):
        """saturacion=0 legítimo, NO debe usar default=50."""
        r_cero = _score_manual({"score_saturacion": 0}, {})
        r_none = _score_manual({}, {})
        assert r_cero["score_competencia"] == 100.0
        assert r_none["score_competencia"] == 50.0

    def test_saturacion_50_da_score_50(self):
        datos = {"score_saturacion": 50}
        r = _score_manual(datos, {})
        assert r["score_competencia"] == 50.0

    def test_saturacion_mayor_100_capped_cero(self):
        datos = {"score_saturacion": 150}
        r = _score_manual(datos, {})
        assert r["score_competencia"] == 0.0


class TestDimensionPrecio:
    def test_precio_8_da_score_100(self):
        # (45-8)/0.37 ≈ 100
        datos = {"precio_m2": 8.0}
        r = _score_manual(datos, {})
        assert r["score_precio_alquiler"] == 100.0

    def test_precio_45_da_score_cero(self):
        datos = {"precio_m2": 45.0}
        r = _score_manual(datos, {})
        assert r["score_precio_alquiler"] == 0.0

    def test_precio_none_usa_default_20(self):
        datos_sin = {}
        datos_con = {"precio_m2": 20}
        r_sin = _score_manual(datos_sin, {})
        r_con = _score_manual(datos_con, {})
        assert abs(r_sin["score_precio_alquiler"] - r_con["score_precio_alquiler"]) < 0.01

    def test_precio_cero_es_legitimo_capped_100(self):
        """precio_m2=0 no debe imputarse a 20; da score máximo."""
        r_cero = _score_manual({"precio_m2": 0}, {})
        r_none = _score_manual({}, {})
        assert r_cero["score_precio_alquiler"] == 100.0
        assert r_none["score_precio_alquiler"] < 100.0

    def test_precio_muy_alto_capped_cero(self):
        datos = {"precio_m2": 999}
        r = _score_manual(datos, {})
        assert r["score_precio_alquiler"] == 0.0


class TestDimensionTransporte:
    def test_cero_lineas_da_score_cero(self):
        datos = {"num_lineas_transporte": 0}
        r = _score_manual(datos, {})
        assert r["score_transporte"] == 0.0

    def test_20_lineas_da_score_100(self):
        datos = {"num_lineas_transporte": 20}
        r = _score_manual(datos, {})
        assert r["score_transporte"] == 100.0

    def test_10_lineas_da_score_50(self):
        datos = {"num_lineas_transporte": 10}
        r = _score_manual(datos, {})
        assert r["score_transporte"] == 50.0

    def test_mas_de_20_capped_100(self):
        datos = {"num_lineas_transporte": 50}
        r = _score_manual(datos, {})
        assert r["score_transporte"] == 100.0

    def test_none_trata_como_cero(self):
        r = _score_manual({}, {})
        assert r["score_transporte"] == 0.0


class TestDimensionSeguridad:
    def test_incidencias_120_da_score_cero(self):
        datos = {"incidencias_por_1000hab": 120.0}
        r = _score_manual(datos, {})
        assert r["score_seguridad"] == 0.0

    def test_incidencias_muy_altas_capped_cero(self):
        datos = {"incidencias_por_1000hab": 200.0}
        r = _score_manual(datos, {})
        assert r["score_seguridad"] == 0.0

    def test_incidencias_bajas_dan_score_alto(self):
        datos = {"incidencias_por_1000hab": 5.0}
        r = _score_manual(datos, {})
        assert r["score_seguridad"] > 90.0

    def test_none_usa_default_35(self):
        datos_sin = {}
        datos_con = {"incidencias_por_1000hab": 35}
        r_sin = _score_manual(datos_sin, {})
        r_con = _score_manual(datos_con, {})
        assert abs(r_sin["score_seguridad"] - r_con["score_seguridad"]) < 0.01


class TestDimensionTurismo:
    def test_sin_dist_playa_usa_score_turismo_base(self):
        datos = {"score_turismo": 60}
        r = _score_manual(datos, {})
        assert r["score_turismo"] == 60.0

    def test_dist_playa_menor_300_sube_a_85_minimo(self):
        datos = {"score_turismo": 40, "dist_playa_m": 200}
        r = _score_manual(datos, {})
        assert r["score_turismo"] >= 85.0

    def test_dist_playa_menor_700_sube_a_70_minimo(self):
        datos = {"score_turismo": 30, "dist_playa_m": 500}
        r = _score_manual(datos, {})
        assert r["score_turismo"] >= 70.0

    def test_dist_playa_menor_1500_sube_a_55_minimo(self):
        datos = {"score_turismo": 20, "dist_playa_m": 1000}
        r = _score_manual(datos, {})
        assert r["score_turismo"] >= 55.0

    def test_dist_playa_mayor_1500_no_bonus(self):
        datos = {"score_turismo": 30, "dist_playa_m": 2000}
        r = _score_manual(datos, {})
        assert r["score_turismo"] == 30.0

    def test_bonus_no_rebaja_score_alto(self):
        """Si el score_turismo ya es 90 y dist_playa<300, se mantiene 90."""
        datos = {"score_turismo": 90, "dist_playa_m": 100}
        r = _score_manual(datos, {})
        assert r["score_turismo"] == 90.0

    def test_sin_score_turismo_usa_default_40(self):
        datos = {}
        r = _score_manual(datos, {})
        assert r["score_turismo"] == 40.0

    def test_capping_100(self):
        datos = {"score_turismo": 120}
        r = _score_manual(datos, {})
        assert r["score_turismo"] == 100.0


class TestDimensionEntorno:
    def test_sin_vacios_ni_rotacion_score_alto(self):
        datos = {"pct_locales_vacios": 0.0, "tasa_rotacion_anual": 0.0}
        r = _score_manual(datos, {})
        assert r["score_entorno_comercial"] == 100.0

    def test_vacios_altos_bajan_score(self):
        datos = {"pct_locales_vacios": 0.40, "tasa_rotacion_anual": 0.0}
        r = _score_manual(datos, {})
        # 100 - 0.40*200 = 20
        assert abs(r["score_entorno_comercial"] - 20.0) < 0.1

    def test_rotacion_alta_baja_score(self):
        datos = {"pct_locales_vacios": 0.0, "tasa_rotacion_anual": 0.50}
        r = _score_manual(datos, {})
        # 100 - 0.50*100 = 50
        assert abs(r["score_entorno_comercial"] - 50.0) < 0.1

    def test_score_no_puede_ser_negativo(self):
        datos = {"pct_locales_vacios": 0.80, "tasa_rotacion_anual": 1.0}
        r = _score_manual(datos, {})
        assert r["score_entorno_comercial"] >= 0.0

    def test_none_usa_defaults(self):
        r_sin = _score_manual({}, {})
        r_con = _score_manual({"pct_locales_vacios": 0.15, "tasa_rotacion_anual": 0.18}, {})
        assert abs(r_sin["score_entorno_comercial"] - r_con["score_entorno_comercial"]) < 0.01


# ── Clase 2: Score global ponderado ───────────────────────────────────────────

class TestScoreGlobalPonderado:
    def test_score_global_es_suma_ponderada(self):
        """Verificar que score_global = Σ(score_dim × peso_dim)."""
        datos = {
            "flujo_peatonal_total": 1500,
            "renta_media_hogar": 38500,
            "score_saturacion": 50,
            "precio_m2": 26.5,
            "num_lineas_transporte": 10,
            "incidencias_por_1000hab": 62.5,
            "score_turismo": 50,
            "pct_locales_vacios": 0.05,
            "tasa_rotacion_anual": 0.05,
        }
        sector = _sector_default()
        r = _score_manual(datos, sector)

        esperado = (
            r["score_flujo_peatonal"]    * sector["peso_flujo"] +
            r["score_demografia"]        * sector["peso_demo"] +
            r["score_competencia"]       * sector["peso_competencia"] +
            r["score_precio_alquiler"]   * sector["peso_precio"] +
            r["score_transporte"]        * sector["peso_transporte"] +
            r["score_seguridad"]         * sector["peso_seguridad"] +
            r["score_turismo"]           * sector["peso_turismo"] +
            r["score_entorno_comercial"] * sector["peso_entorno"]
        )
        assert abs(r["score_global"] - round(esperado, 1)) < 0.05

    def test_score_global_rango_0_100(self):
        for flujo in [0, 500, 1500, 3000]:
            r = _score_manual({"flujo_peatonal_total": flujo}, _sector_default())
            assert 0.0 <= r["score_global"] <= 100.0

    def test_sector_vacio_usa_pesos_default(self):
        """Sin sector, los pesos default del código deben usarse sin excepción."""
        r = _score_manual({}, {})
        assert "score_global" in r
        assert 0.0 <= r["score_global"] <= 100.0

    def test_todos_max_da_cerca_100(self):
        datos = {
            "flujo_peatonal_total": 3000,
            "renta_media_hogar": 60000,
            "score_saturacion": 0,
            "precio_m2": 8.0,
            "num_lineas_transporte": 20,
            "incidencias_por_1000hab": 5.0,
            "score_turismo": 100,
            "pct_locales_vacios": 0.0,
            "tasa_rotacion_anual": 0.0,
        }
        r = _score_manual(datos, _sector_default())
        assert r["score_global"] >= 95.0

    def test_todos_min_da_cerca_0(self):
        datos = {
            "flujo_peatonal_total": 0,
            "renta_media_hogar": 5000,
            "score_saturacion": 100,
            "precio_m2": 50.0,
            "num_lineas_transporte": 0,
            "incidencias_por_1000hab": 200.0,
            "score_turismo": 0,
            "pct_locales_vacios": 1.0,
            "tasa_rotacion_anual": 1.0,
        }
        r = _score_manual(datos, _sector_default())
        assert r["score_global"] <= 5.0


# ── Clase 3: Monotonía ─────────────────────────────────────────────────────────

class TestMonotonia:
    def test_mas_flujo_mayor_score_flujo(self):
        r1 = _score_manual({"flujo_peatonal_total": 500}, {})
        r2 = _score_manual({"flujo_peatonal_total": 2000}, {})
        assert r2["score_flujo_peatonal"] > r1["score_flujo_peatonal"]

    def test_mas_renta_mayor_score_demo(self):
        r1 = _score_manual({"renta_media_hogar": 20000}, {})
        r2 = _score_manual({"renta_media_hogar": 50000}, {})
        assert r2["score_demografia"] > r1["score_demografia"]

    def test_menor_saturacion_mayor_score_competencia(self):
        r1 = _score_manual({"score_saturacion": 80}, {})
        r2 = _score_manual({"score_saturacion": 20}, {})
        assert r2["score_competencia"] > r1["score_competencia"]

    def test_menor_precio_mayor_score_precio(self):
        r1 = _score_manual({"precio_m2": 35}, {})
        r2 = _score_manual({"precio_m2": 15}, {})
        assert r2["score_precio_alquiler"] > r1["score_precio_alquiler"]

    def test_mas_lineas_mayor_score_transporte(self):
        r1 = _score_manual({"num_lineas_transporte": 3}, {})
        r2 = _score_manual({"num_lineas_transporte": 12}, {})
        assert r2["score_transporte"] > r1["score_transporte"]

    def test_menos_incidencias_mayor_score_seguridad(self):
        r1 = _score_manual({"incidencias_por_1000hab": 90}, {})
        r2 = _score_manual({"incidencias_por_1000hab": 20}, {})
        assert r2["score_seguridad"] > r1["score_seguridad"]

    def test_menor_dist_playa_mayor_score_turismo(self):
        base = {"score_turismo": 30}
        r1 = _score_manual({**base, "dist_playa_m": 2000}, {})
        r2 = _score_manual({**base, "dist_playa_m": 200}, {})
        assert r2["score_turismo"] >= r1["score_turismo"]

    def test_menos_vacios_mayor_score_entorno(self):
        r1 = _score_manual({"pct_locales_vacios": 0.40, "tasa_rotacion_anual": 0.2}, {})
        r2 = _score_manual({"pct_locales_vacios": 0.05, "tasa_rotacion_anual": 0.1}, {})
        assert r2["score_entorno_comercial"] > r1["score_entorno_comercial"]


# ── Clase 4: Formato de salida ────────────────────────────────────────────────

class TestFormatoSalida:
    _CLAVES_ESPERADAS = {
        "score_global",
        "score_flujo_peatonal",
        "score_demografia",
        "score_competencia",
        "score_precio_alquiler",
        "score_transporte",
        "score_seguridad",
        "score_turismo",
        "score_entorno_comercial",
        "probabilidad_supervivencia",
        "shap_values",
        "modelo_version",
    }

    def test_score_manual_devuelve_11_claves(self):
        r = _score_manual({}, {})
        assert set(r.keys()) == self._CLAVES_ESPERADAS

    def test_probabilidad_supervivencia_es_none_en_manual(self):
        r = _score_manual({}, {})
        assert r["probabilidad_supervivencia"] is None

    def test_shap_values_es_none_en_manual(self):
        r = _score_manual({}, {})
        assert r["shap_values"] is None

    def test_modelo_version_es_manual_v1(self):
        r = _score_manual({}, {})
        assert r["modelo_version"] == "manual_v1"

    def test_score_global_es_float_redondeado(self):
        r = _score_manual({"flujo_peatonal_total": 1234}, _sector_default())
        # Debe estar redondeado a 1 decimal
        assert r["score_global"] == round(r["score_global"], 1)

    def test_todos_los_scores_son_float(self):
        r = _score_manual({"flujo_peatonal_total": 1000}, _sector_default())
        score_keys = [k for k in r if k.startswith("score_") and k != "score_global"]
        for k in score_keys:
            assert isinstance(r[k], float), f"{k} no es float"

    def test_no_nan_en_ninguna_clave(self):
        r = _score_manual({}, _sector_default())
        for k, v in r.items():
            if isinstance(v, float):
                assert not math.isnan(v), f"{k} es NaN"

    def test_no_inf_en_ninguna_clave(self):
        r = _score_manual({}, _sector_default())
        for k, v in r.items():
            if isinstance(v, float):
                assert math.isfinite(v), f"{k} es infinito"


# ── Clase 5: _score_neutro ────────────────────────────────────────────────────

class TestScoreNeutro:
    def test_score_neutro_devuelve_50_en_todo(self):
        r = _score_neutro()
        score_keys = [k for k in r if k.startswith("score_")]
        for k in score_keys:
            assert r[k] == 50.0, f"{k} != 50.0"

    def test_score_neutro_claves_completas(self):
        r = _score_neutro()
        claves_esperadas = {
            "score_global", "score_flujo_peatonal", "score_demografia",
            "score_competencia", "score_precio_alquiler", "score_transporte",
            "score_seguridad", "score_turismo", "score_entorno_comercial",
            "probabilidad_supervivencia", "shap_values", "modelo_version",
        }
        assert set(r.keys()) == claves_esperadas

    def test_score_neutro_modelo_version_fallback(self):
        r = _score_neutro()
        assert r["modelo_version"] == "fallback"

    def test_score_neutro_probabilidad_none(self):
        r = _score_neutro()
        assert r["probabilidad_supervivencia"] is None

    def test_score_neutro_shap_none(self):
        r = _score_neutro()
        assert r["shap_values"] is None


# ── Clase 6: Pesos de sector personalizados ───────────────────────────────────

class TestPesosSector:
    def test_sector_flujo_heavy_amplifica_flujo(self):
        """Con peso_flujo=1.0 y el resto 0, score_global = score_flujo."""
        datos = {"flujo_peatonal_total": 1500}
        sector = {"peso_flujo": 1.0, "peso_demo": 0, "peso_competencia": 0,
                  "peso_precio": 0, "peso_transporte": 0, "peso_seguridad": 0,
                  "peso_turismo": 0, "peso_entorno": 0}
        r = _score_manual(datos, sector)
        assert abs(r["score_global"] - r["score_flujo_peatonal"]) < 0.1

    def test_sector_demo_heavy_amplifica_demo(self):
        datos = {"renta_media_hogar": 50000}
        sector = {"peso_flujo": 0, "peso_demo": 1.0, "peso_competencia": 0,
                  "peso_precio": 0, "peso_transporte": 0, "peso_seguridad": 0,
                  "peso_turismo": 0, "peso_entorno": 0}
        r = _score_manual(datos, sector)
        assert abs(r["score_global"] - r["score_demografia"]) < 0.1

    def test_pesos_distintos_dan_scores_distintos(self):
        datos = {"flujo_peatonal_total": 2000, "renta_media_hogar": 50000}
        s1 = {**_sector_default(), "peso_flujo": 0.50, "peso_demo": 0.10}
        s2 = {**_sector_default(), "peso_flujo": 0.10, "peso_demo": 0.50}
        r1 = _score_manual(datos, s1)
        r2 = _score_manual(datos, s2)
        assert r1["score_global"] != r2["score_global"]
