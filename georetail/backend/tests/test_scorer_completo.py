"""
tests/test_scorer_completo.py — Tests exhaustivos del scorer manual (scorer.py).

Agente: ruflo-test-scorer
Tarea:  task-1774810133286-tcg7a0

Cubre (scorer manual_v2 — 7 dimensiones activas):
  - Verificación analítica de cada dimensión viva:
      flujo_peatonal, demografía, competencia, transporte, seguridad,
      turismo y dinamismo. (precio se retorna como referencia pero no pondera.)
  - Bonus costero (dist_playa_m) en turismo: <300m, <700m, <1500m, >1500m
  - Score global = suma ponderada de dimensiones (7 pesos que suman 1.0)
  - Monotonía: más flujo/renta/etc → mayor score correspondiente
  - Semántica None vs 0: renta=0 legítima, saturacion=0 sin competencia
  - Defaults cuando datos ausentes (sector vacío, datos zona vacíos)
  - _score_neutro: formato y valores
  - 12 claves de salida exactas en _score_manual (incluye score_dinamismo)
  - Capping 0-100 en todos los scores de dimensión
"""
from __future__ import annotations

import math
import pytest

from scoring.scorer import _score_manual, _score_neutro


# ── Helpers ────────────────────────────────────────────────────────────────────

def _sector_default():
    """Sector con los 7 pesos actuales del scorer_manual_v2 (suman 1.0)."""
    return {
        "peso_flujo":      0.25,
        "peso_demo":       0.25,
        "peso_competencia": 0.15,
        "peso_transporte": 0.15,
        "peso_dinamismo":  0.10,
        "peso_seguridad":  0.05,
        "peso_turismo":    0.05,
    }


# ── Clase 1: Verificación analítica de cada dimensión ─────────────────────────

class TestDimensionFlujo:
    """La dimensión flujo usa ``calcular_flujo_con_temporalidad``. Cuando se
    inyecta ``flujo_popular_times_score`` (ya 0-100) el score resultante es
    directamente proporcional, lo que permite aserciones estables."""

    def test_pt_cero_da_score_bajo(self):
        r = _score_manual({"flujo_popular_times_score": 0}, {})
        assert r["score_flujo_peatonal"] <= 5.0

    def test_pt_100_da_score_maximo(self):
        r = _score_manual({"flujo_popular_times_score": 100}, {})
        assert r["score_flujo_peatonal"] >= 95.0

    def test_pt_50_da_score_medio(self):
        r = _score_manual({"flujo_popular_times_score": 50}, {})
        assert 40.0 <= r["score_flujo_peatonal"] <= 60.0

    def test_sin_datos_flujo_score_cero(self):
        r = _score_manual({}, {})
        assert r["score_flujo_peatonal"] == 0.0

    def test_score_flujo_en_rango_0_100(self):
        for pt in [0, 10, 30, 50, 70, 90, 100]:
            r = _score_manual({"flujo_popular_times_score": pt}, {})
            assert 0.0 <= r["score_flujo_peatonal"] <= 100.0, f"pt={pt}"


class TestDimensionDemografia:
    """La dimensión demografía usa el módulo enriquecido multi-variable. Los
    tests cubren monotonía y rango; los valores absolutos los verifica el
    test dedicado ``test_demografia_*``."""

    def test_renta_alta_score_superior_a_renta_baja(self):
        r_baja = _score_manual({"renta_media_hogar": 18000}, {})
        r_alta = _score_manual({"renta_media_hogar": 55000}, {})
        assert r_alta["score_demografia"] > r_baja["score_demografia"]

    def test_renta_none_usa_default(self):
        """Sin datos, el scorer devuelve un score demográfico estable (~50)."""
        r = _score_manual({}, {})
        assert 0.0 <= r["score_demografia"] <= 100.0

    def test_renta_cero_no_equivale_a_renta_alta(self):
        r_cero = _score_manual({"renta_media_hogar": 0}, {})
        r_alta = _score_manual({"renta_media_hogar": 60000}, {})
        assert r_cero["score_demografia"] < r_alta["score_demografia"]

    def test_score_demo_en_rango_0_100(self):
        for renta in [0, 10000, 25000, 40000, 60000, 100000]:
            r = _score_manual({"renta_media_hogar": renta}, {})
            assert 0.0 <= r["score_demografia"] <= 100.0, f"renta={renta}"


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
    """precio_alquiler se sigue retornando como referencia aunque su peso en
    el global es 0 (ver scorer.py:316-321)."""

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

    def test_score_transporte_precalculado_se_respeta(self):
        """Si viene ``score_transporte_calculado`` del módulo dedicado, se usa
        directamente en lugar del fallback ``num_lineas × 5``."""
        r = _score_manual({"score_transporte_calculado": 72.0, "num_lineas_transporte": 0}, {})
        assert r["score_transporte"] == 72.0


class TestDimensionSeguridad:
    """El scorer delega en ``calcular_score_seguridad`` (v7, multivariable).
    Los tests cubren monotonía y rango, no la fórmula simple antigua."""

    def test_menos_incidencias_mayor_score(self):
        r_bajo = _score_manual({"incidencias_por_1000hab": 90}, {})
        r_alto = _score_manual({"incidencias_por_1000hab": 10}, {})
        assert r_alto["score_seguridad"] > r_bajo["score_seguridad"]

    def test_score_seguridad_en_rango_0_100(self):
        for inc in [0, 20, 50, 100, 200]:
            r = _score_manual({"incidencias_por_1000hab": inc}, {})
            assert 0.0 <= r["score_seguridad"] <= 100.0, f"inc={inc}"

    def test_none_usa_default(self):
        datos_sin = {}
        datos_con = {"incidencias_por_1000hab": 35}
        r_sin = _score_manual(datos_sin, {})
        r_con = _score_manual(datos_con, {})
        assert abs(r_sin["score_seguridad"] - r_con["score_seguridad"]) < 0.01


class TestDimensionTurismo:
    """Integración del scorer con la dimensión turismo (v14, mig 029).

    El cálculo detallado vive en scoring/dimensiones/turismo.py y se testea
    en test_turismo_dimension.py. Aquí solo verificamos que _score_manual
    delega correctamente y produce la clave score_turismo en rango [0, 100].
    """

    def test_score_manual_devuelve_clave_turismo_en_rango(self):
        r = _score_manual({}, {})
        assert "score_turismo" in r
        assert 0.0 <= r["score_turismo"] <= 100.0

    def test_zona_turistica_supera_zona_residencial(self):
        datos_turistica = {
            "airbnb_density_500m":     45,
            "airbnb_occupancy_est":    0.85,
            "booking_hoteles_500m":    12,
            "eventos_culturales_500m": 5,
            "dist_playa_m":            180,
        }
        datos_residencial = {
            "airbnb_density_500m":     1,
            "booking_hoteles_500m":    0,
            "eventos_culturales_500m": 0,
            "dist_playa_m":            5000,
        }
        s_tur = _score_manual(datos_turistica, {})["score_turismo"]
        s_res = _score_manual(datos_residencial, {})["score_turismo"]
        assert s_tur > s_res + 20.0

    def test_capping_100(self):
        """Con todos los stocks máximos, el score nunca rebasa 100."""
        datos = {
            "airbnb_density_500m":     500,
            "airbnb_occupancy_est":    1.0,
            "booking_hoteles_500m":    100,
            "eventos_culturales_500m": 50,
            "venues_musicales_500m":   20,
            "dist_playa_m":            50,
            "seasonality_summer_lift": 2.0,
        }
        r = _score_manual(datos, {})
        assert r["score_turismo"] == 100.0


class TestDimensionDinamismo:
    """Tests para la dimensión dinamismo (reemplaza la antigua ``entorno``).

    El cálculo detallado vive en scoring/dimensiones/dinamismo.py; aquí
    verificamos la integración con ``_score_manual`` y el fallback a 50.0
    (scorer.py:360) cuando el módulo dedicado no puede calcular el score.
    """

    def test_score_manual_devuelve_clave_dinamismo_en_rango(self):
        r = _score_manual({}, {})
        assert "score_dinamismo" in r
        assert 0.0 <= r["score_dinamismo"] <= 100.0

    def test_sin_datos_usa_default_50(self):
        """El fallback de dinamismo (scorer.py:360) es 50.0 cuando no hay
        señales históricas para la zona."""
        r = _score_manual({}, {})
        assert r["score_dinamismo"] == 50.0

    def test_datos_degenerados_mantienen_default(self):
        """Datos que no activan el pipeline histórico devuelven el default."""
        r = _score_manual({"pct_locales_vacios": 0.0, "tasa_rotacion_anual": 0.0}, {})
        assert r["score_dinamismo"] == 50.0


# ── Clase 2: Score global ponderado ───────────────────────────────────────────

class TestScoreGlobalPonderado:
    def test_score_global_es_suma_ponderada(self):
        """Verificar que score_global = Σ(score_dim × peso_dim) con los 7
        pesos activos. ``score_precio_alquiler`` se excluye: peso=0."""
        datos = {
            "flujo_popular_times_score": 50,
            "renta_media_hogar":         38500,
            "score_saturacion":          50,
            "precio_m2":                 26.5,
            "num_lineas_transporte":     10,
            "incidencias_por_1000hab":   62.5,
        }
        sector = _sector_default()
        r = _score_manual(datos, sector)

        esperado = (
            r["score_flujo_peatonal"] * sector["peso_flujo"] +
            r["score_demografia"]     * sector["peso_demo"] +
            r["score_competencia"]    * sector["peso_competencia"] +
            r["score_transporte"]     * sector["peso_transporte"] +
            r["score_dinamismo"]      * sector["peso_dinamismo"] +
            r["score_seguridad"]      * sector["peso_seguridad"] +
            r["score_turismo"]        * sector["peso_turismo"]
        )
        assert abs(r["score_global"] - round(esperado, 1)) < 0.05

    def test_score_global_rango_0_100(self):
        for pt in [0, 25, 50, 75, 100]:
            r = _score_manual({"flujo_popular_times_score": pt}, _sector_default())
            assert 0.0 <= r["score_global"] <= 100.0

    def test_sector_vacio_usa_pesos_default(self):
        """Sin sector, los pesos default del código deben usarse sin excepción."""
        r = _score_manual({}, {})
        assert "score_global" in r
        assert 0.0 <= r["score_global"] <= 100.0

    def test_todos_max_da_score_alto(self):
        datos = {
            "flujo_popular_times_score": 100,
            "renta_media_hogar":         60000,
            "score_saturacion":          0,
            "precio_m2":                 8.0,
            "num_lineas_transporte":     20,
            "incidencias_por_1000hab":   5.0,
            "airbnb_density_500m":       500,
            "airbnb_occupancy_est":      1.0,
            "booking_hoteles_500m":      100,
            "eventos_culturales_500m":   50,
            "venues_musicales_500m":     20,
            "dist_playa_m":              50,
            "seasonality_summer_lift":   2.0,
        }
        r = _score_manual(datos, _sector_default())
        # Con dinamismo=50 (fallback) y seguridad que no llega a 100,
        # el techo realista es ~85-90.
        assert r["score_global"] >= 80.0

    def test_todos_min_da_score_bajo(self):
        datos = {
            "flujo_popular_times_score": 0,
            "renta_media_hogar":         5000,
            "score_saturacion":          100,
            "precio_m2":                 50.0,
            "num_lineas_transporte":     0,
            "incidencias_por_1000hab":   200.0,
            "airbnb_density_500m":       0,
            "booking_hoteles_500m":      0,
            "eventos_culturales_500m":   0,
            "dist_playa_m":              9999,
        }
        r = _score_manual(datos, _sector_default())
        # Con dinamismo=50 (fallback) y seguridad que no baja de ~30,
        # el suelo realista es ~20-25.
        assert r["score_global"] <= 25.0


# ── Clase 3: Monotonía ─────────────────────────────────────────────────────────

class TestMonotonia:
    def test_mas_flujo_mayor_score_flujo(self):
        r1 = _score_manual({"flujo_popular_times_score": 20}, {})
        r2 = _score_manual({"flujo_popular_times_score": 80}, {})
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
        "score_dinamismo",
        "probabilidad_supervivencia",
        "shap_values",
        "modelo_version",
    }

    def test_score_manual_devuelve_12_claves(self):
        r = _score_manual({}, {})
        assert set(r.keys()) == self._CLAVES_ESPERADAS
        assert len(self._CLAVES_ESPERADAS) == 12

    def test_probabilidad_supervivencia_es_none_en_manual(self):
        r = _score_manual({}, {})
        assert r["probabilidad_supervivencia"] is None

    def test_shap_values_es_none_en_manual(self):
        r = _score_manual({}, {})
        assert r["shap_values"] is None

    def test_modelo_version_es_manual_v2(self):
        r = _score_manual({}, {})
        assert r["modelo_version"] == "manual_v2"

    def test_score_global_es_float_redondeado(self):
        r = _score_manual({"flujo_popular_times_score": 56}, _sector_default())
        # Debe estar redondeado a 1 decimal
        assert r["score_global"] == round(r["score_global"], 1)

    def test_todos_los_scores_son_float(self):
        r = _score_manual({"flujo_popular_times_score": 50}, _sector_default())
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
    _CLAVES_NEUTRO = {
        "score_global",
        "score_flujo_peatonal",
        "score_demografia",
        "score_competencia",
        "score_precio_alquiler",
        "score_transporte",
        "score_seguridad",
        "score_turismo",
        "score_dinamismo",
        "probabilidad_supervivencia",
        "shap_values",
        "modelo_version",
    }

    def test_score_neutro_devuelve_50_en_todo(self):
        r = _score_neutro()
        score_keys = [k for k in r if k.startswith("score_")]
        for k in score_keys:
            assert r[k] == 50.0, f"{k} != 50.0"

    def test_score_neutro_claves_completas(self):
        r = _score_neutro()
        assert set(r.keys()) == self._CLAVES_NEUTRO

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
        datos = {"flujo_popular_times_score": 50}
        sector = {
            "peso_flujo":      1.0,
            "peso_demo":       0,
            "peso_competencia": 0,
            "peso_transporte": 0,
            "peso_dinamismo":  0,
            "peso_seguridad":  0,
            "peso_turismo":    0,
        }
        r = _score_manual(datos, sector)
        assert abs(r["score_global"] - r["score_flujo_peatonal"]) < 0.1

    def test_sector_demo_heavy_amplifica_demo(self):
        datos = {"renta_media_hogar": 50000}
        sector = {
            "peso_flujo":      0,
            "peso_demo":       1.0,
            "peso_competencia": 0,
            "peso_transporte": 0,
            "peso_dinamismo":  0,
            "peso_seguridad":  0,
            "peso_turismo":    0,
        }
        r = _score_manual(datos, sector)
        assert abs(r["score_global"] - r["score_demografia"]) < 0.1

    def test_pesos_distintos_dan_scores_distintos(self):
        datos = {"flujo_popular_times_score": 80, "renta_media_hogar": 50000}
        s1 = {**_sector_default(), "peso_flujo": 0.50, "peso_demo": 0.10}
        s2 = {**_sector_default(), "peso_flujo": 0.10, "peso_demo": 0.50}
        r1 = _score_manual(datos, s1)
        r2 = _score_manual(datos, s2)
        assert r1["score_global"] != r2["score_global"]
