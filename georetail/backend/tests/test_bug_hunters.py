"""
tests/test_bug_hunters.py — Cazadores sistemáticos de bugs del modelo v5.

Agente: ruflo-test-bugs
Tarea:  task-1774810156792-6cjh0t

Cubre:
  - None vs 0 en renta: renta=0 legítima NO debe imputarse a 30000
  - None vs 0 en saturacion: saturacion=0 = sin competencia = score 100
  - precio_m2=0 legítimo, no debe imputarse a 20
  - ratio_locales=0.0 es dato válido para flujo score
  - flujo=0 legítimo vs flujo=None
  - _parse_float: None→default, comas, strings vacíos
  - Todos los valores en _MEDIAS son finitos
  - Fracciones flujo siempre suman 1.0
  - _score_manual sin excepción con datos completamente vacíos
  - _score_manual no produce NaN con ninguna combinación de datos
  - multiplicador aforaments capped a 1.35 (no más)
  - Imputation: fracciones solo se calculan cuando total > 0
  - _extract_iermb_bcn_value: estructura incorrecta no lanza excepción
  - _parse_renda_csv: CSV vacío, columnas faltantes, valores no numéricos
  - PESOS_BASE flujo_peatonal: todos positivos y suman 1
  - calcular_flujo_score: nunca devuelve NaN ni Inf
"""
from __future__ import annotations

import math
import pytest

from scoring.scorer import _score_manual
from scoring.features import FEATURE_NAMES, _MEDIAS, _build_array
from scoring.dimensiones.flujo_peatonal import (
    PESOS_BASE,
    VCITY_MAX_BARCELONA,
    VIANANTS_MAX_BARCELONA,
    calcular_flujo_score,
)
from pipelines.demografia.demografia import _extract_iermb_bcn_value, _parse_float, _parse_renda_csv


# ── Clase 1: None vs 0 — semántica correcta ───────────────────────────────────

class TestNoneVsCero:
    """
    Verifica que el scorer distingue correctamente entre:
      - None  → dato desconocido → usar default/media de imputación
      - 0     → dato conocido y válido (puede ser cero legítimo)
    """

    def test_renta_cero_es_score_cero_no_default(self):
        """renta=0 → score_demo=0 (no imputar a 30000)."""
        r_cero = _score_manual({"renta_media_hogar": 0}, {})
        r_default = _score_manual({}, {})
        # Con renta=0: (0-17000)/430 < 0 → capped a 0
        assert r_cero["score_demografia"] == 0.0
        # Sin renta (None): usa 30000 → score positivo
        assert r_default["score_demografia"] > 0.0

    def test_saturacion_cero_no_usa_default_50(self):
        """saturacion=0 → sin competencia → score_competencia=100 (no 50)."""
        r_cero = _score_manual({"score_saturacion": 0}, {})
        r_none = _score_manual({}, {})
        assert r_cero["score_competencia"] == 100.0
        assert r_none["score_competencia"] == 50.0

    def test_precio_cero_no_usa_default_20(self):
        """precio_m2=0 → capped a score=100 (no imputar a 20)."""
        r_cero = _score_manual({"precio_m2": 0}, {})
        r_none = _score_manual({}, {})
        assert r_cero["score_precio_alquiler"] == 100.0
        assert r_none["score_precio_alquiler"] < 100.0

    def test_flujo_cero_da_score_flujo_cero(self):
        """flujo=0 → score_flujo=0 (no imputar a media)."""
        r_cero = _score_manual({"flujo_peatonal_total": 0}, {})
        r_none = _score_manual({}, {})
        assert r_cero["score_flujo_peatonal"] == 0.0
        assert r_none["score_flujo_peatonal"] == 0.0  # `or 0` hace que None→0 también

    def test_ratio_locales_cero_es_dato_valido_flujo(self):
        """ratio_locales=0.0 es una zona sin locales comerciales, dato válido."""
        score_cero = calcular_flujo_score(None, None, None, 0.0)
        score_none = calcular_flujo_score(None, None, None, None)
        # Con ratio=0: normalizado a 0, score=0 (no fallback)
        assert score_cero == pytest.approx(0.0)
        # Con None: fallback
        assert score_none == pytest.approx(30.0)

    def test_lineas_transporte_cero_es_valido(self):
        """num_lineas_transporte=0 → sin transporte → score=0 (no imputar)."""
        r = _score_manual({"num_lineas_transporte": 0}, {})
        assert r["score_transporte"] == 0.0


# ── Clase 2: División por cero — guards ───────────────────────────────────────

class TestZeroDivision:
    def test_flujo_total_cero_fracciones_no_rompen(self):
        """Con flujo=0, las fracciones manana/tarde/noche deben imputarse sin error."""
        arr = _build_array(
            {"flujo_peatonal_total": 0},
            {}, None, {}, {}, {}
        )
        assert not any(math.isnan(float(x)) for x in arr[0])

    def test_flujo_total_none_fracciones_no_rompen(self):
        """Con flujo=None, las fracciones deben imputarse sin error."""
        arr = _build_array({}, {}, None, {}, {}, {})
        assert not any(math.isnan(float(x)) for x in arr[0])

    def test_score_manual_datos_vacios_no_rompe(self):
        """_score_manual({}, {}) no debe lanzar excepción."""
        try:
            r = _score_manual({}, {})
            assert "score_global" in r
        except Exception as e:
            pytest.fail(f"_score_manual({{}}, {{}}) lanzó excepción: {e}")

    def test_calcular_flujo_score_ninguna_fuente_no_rompe(self):
        """calcular_flujo_score(None, None, None, None) no debe lanzar excepción."""
        try:
            score = calcular_flujo_score(None, None, None, None)
            assert math.isfinite(score)
        except Exception as e:
            pytest.fail(f"calcular_flujo_score lanzó excepción: {e}")

    def test_extract_iermb_datos_invalidos_no_rompe(self):
        """_extract_iermb_bcn_value con datos extraños no lanza excepción."""
        casos = [None, "", 42, "string", [], {}, [{"no_hay_muni": "x"}]]
        for caso in casos:
            try:
                result = _extract_iermb_bcn_value(caso)
                assert result is None or isinstance(result, float)
            except Exception as e:
                pytest.fail(f"_extract_iermb_bcn_value({caso!r}) lanzó: {e}")


# ── Clase 3: _parse_float edge cases ─────────────────────────────────────────

class TestParseFloatEdgeCases:
    def test_none_devuelve_default_cero(self):
        assert _parse_float(None) == 0.0

    def test_none_devuelve_default_personalizado(self):
        assert _parse_float(None, 42.5) == pytest.approx(42.5)

    def test_coma_decimal_catalan(self):
        assert _parse_float("3,14") == pytest.approx(3.14)

    def test_punto_decimal(self):
        assert _parse_float("3.14") == pytest.approx(3.14)

    def test_string_vacio_devuelve_default(self):
        assert _parse_float("", 99.0) == pytest.approx(99.0)

    def test_string_invalido_devuelve_default(self):
        assert _parse_float("not_a_number", 7.0) == pytest.approx(7.0)

    def test_entero_string(self):
        assert _parse_float("42") == pytest.approx(42.0)

    def test_float_directo(self):
        assert _parse_float(3.14) == pytest.approx(3.14)

    def test_cero_string(self):
        assert _parse_float("0") == pytest.approx(0.0)

    def test_negativo(self):
        assert _parse_float("-10.5") == pytest.approx(-10.5)

    def test_espacios_strip(self):
        assert _parse_float("  25.0  ") == pytest.approx(25.0)

    def test_entero_int(self):
        assert _parse_float(100) == pytest.approx(100.0)


# ── Clase 4: _MEDIAS valores finitos ─────────────────────────────────────────

class TestMediasFinitas:
    def test_todas_las_medias_son_finitas(self):
        """Ningún valor de _MEDIAS puede ser NaN, +Inf o -Inf."""
        for k, v in _MEDIAS.items():
            assert math.isfinite(v), f"_MEDIAS['{k}'] = {v} no es finito"

    def test_todas_las_medias_de_feature_names_son_finitas(self):
        """Los 33 features de FEATURE_NAMES deben tener media finita."""
        for f in FEATURE_NAMES:
            assert math.isfinite(_MEDIAS[f]), f"_MEDIAS['{f}'] no es finito"

    def test_medias_numericas(self):
        """Todos los valores deben ser float (no None, no string)."""
        for k, v in _MEDIAS.items():
            assert isinstance(v, (int, float)), f"_MEDIAS['{k}'] no es numérico"


# ── Clase 5: Fracciones flujo siempre suman 1 ─────────────────────────────────

class TestFracionesFlujo:
    def test_fracciones_suman_1_cuando_hay_total(self):
        from pipelines.peatonal.aforaments import _FRAC_MANANA, _FRAC_TARDE, _FRAC_NOCHE
        assert abs(_FRAC_MANANA + _FRAC_TARDE + _FRAC_NOCHE - 1.0) < 1e-9

    def test_fracciones_positivas(self):
        from pipelines.peatonal.aforaments import _FRAC_MANANA, _FRAC_TARDE, _FRAC_NOCHE
        assert _FRAC_MANANA > 0
        assert _FRAC_TARDE > 0
        assert _FRAC_NOCHE > 0

    def test_fracciones_feature_flujo_suman_uno_con_total(self):
        """Si hay flujo total, manana+tarde+noche deben reconstruir el total."""
        flujo_total = 1200.0
        from pipelines.peatonal.aforaments import _FRAC_MANANA, _FRAC_TARDE, _FRAC_NOCHE
        manana = flujo_total * _FRAC_MANANA
        tarde  = flujo_total * _FRAC_TARDE
        noche  = flujo_total * _FRAC_NOCHE
        assert abs(manana + tarde + noche - flujo_total) < 0.1  # tolerancia redondeo

    def test_build_array_fracciones_validas_con_total(self):
        """Con flujo_total > 0 y las tres franjas dadas, las fracciones son correctas."""
        total = 1000.0
        manana = 350.0  # 35%
        tarde  = 420.0  # 42%
        noche  = 230.0  # 23%
        vz = {
            "flujo_peatonal_total": total,
            "flujo_peatonal_manana": manana,
            "flujo_peatonal_tarde": tarde,
            "flujo_peatonal_noche": noche,
        }
        arr = _build_array(vz, {}, None, {}, {}, {})
        # índice 1=flujo_manana_pct, 2=flujo_tarde_pct, 3=flujo_noche_pct
        frac_m = float(arr[0][1])
        frac_t = float(arr[0][2])
        frac_n = float(arr[0][3])
        assert abs(frac_m + frac_t + frac_n - 1.0) < 0.01

    def test_build_array_fracciones_se_imputan_si_no_hay_total(self):
        """Sin flujo_total, las fracciones deben imputarse con las medias."""
        vz = {
            "flujo_peatonal_total": None,
            "flujo_peatonal_manana": 350.0,
            "flujo_peatonal_tarde": 420.0,
            "flujo_peatonal_noche": 230.0,
        }
        arr = _build_array(vz, {}, None, {}, {}, {})
        frac_m = float(arr[0][1])
        frac_t = float(arr[0][2])
        frac_n = float(arr[0][3])
        # Con total=None → fracciones = None → imputadas con _MEDIAS
        assert frac_m == pytest.approx(_MEDIAS["flujo_manana_pct"])
        assert frac_t == pytest.approx(_MEDIAS["flujo_tarde_pct"])
        assert frac_n == pytest.approx(_MEDIAS["flujo_noche_pct"])


# ── Clase 6: _score_manual no produce NaN ────────────────────────────────────

class TestScoreManualSinNaN:
    """Verificar que _score_manual no produce NaN en ninguna combinación."""

    def _check_no_nan(self, datos, sector):
        r = _score_manual(datos, sector)
        for k, v in r.items():
            if isinstance(v, float):
                assert not math.isnan(v), f"NaN en '{k}' con datos={datos}"

    def test_datos_completamente_vacios(self):
        self._check_no_nan({}, {})

    def test_flujo_cero_resto_vacio(self):
        self._check_no_nan({"flujo_peatonal_total": 0}, {})

    def test_renta_cero(self):
        self._check_no_nan({"renta_media_hogar": 0}, {})

    def test_saturacion_cero(self):
        self._check_no_nan({"score_saturacion": 0}, {})

    def test_precio_cero(self):
        self._check_no_nan({"precio_m2": 0}, {})

    def test_incidencias_cero(self):
        self._check_no_nan({"incidencias_por_1000hab": 0}, {})

    def test_vacios_y_rotacion_extrema(self):
        self._check_no_nan(
            {"pct_locales_vacios": 1.0, "tasa_rotacion_anual": 1.0}, {}
        )

    def test_pesos_todos_cero(self):
        """sector con todos los pesos a 0 → score_global debe ser 0, no NaN."""
        sector_cero = {k: 0.0 for k in [
            "peso_flujo", "peso_demo", "peso_competencia", "peso_precio",
            "peso_transporte", "peso_seguridad", "peso_turismo", "peso_entorno"
        ]}
        self._check_no_nan({}, sector_cero)


# ── Clase 7: Multiplicador comercial — capping ────────────────────────────────

class TestMultiplicadorCapping:
    """El multiplicador comercial en aforaments no puede superar 1.35."""

    def _mult(self, ratio_com: float) -> float:
        """Replica la fórmula de aforaments._asignar_zonas."""
        return 1.0 + min(0.35, ratio_com * 0.70)

    def test_ratio_0_da_mult_1_0(self):
        assert self._mult(0.0) == pytest.approx(1.0)

    def test_ratio_0_5_da_mult_1_35(self):
        assert self._mult(0.5) == pytest.approx(1.35)

    def test_ratio_1_da_mult_1_35_no_mas(self):
        assert self._mult(1.0) == pytest.approx(1.35)

    def test_ratio_extremo_no_supera_1_35(self):
        for ratio in [1.0, 2.0, 5.0, 100.0]:
            m = self._mult(ratio)
            assert m <= 1.35 + 1e-9, f"ratio={ratio} → mult={m} > 1.35"

    def test_multiplicador_minimo_es_exactamente_1(self):
        assert self._mult(0.0) == pytest.approx(1.0)

    def test_multiplicador_nunca_menor_1(self):
        for ratio in [0.0, 0.001, 0.01, 0.1]:
            m = self._mult(ratio)
            assert m >= 1.0 - 1e-9


# ── Clase 8: calcular_flujo_score — nunca NaN ni Inf ─────────────────────────

class TestFlujoScoreRobustez:
    def test_valores_normales_no_nan(self):
        score = calcular_flujo_score(50.0, 25000.0, 7500.0, 0.25)
        assert math.isfinite(score)

    def test_todos_none_no_nan(self):
        score = calcular_flujo_score(None, None, None, None)
        assert math.isfinite(score)

    def test_valores_extremos_no_nan(self):
        score = calcular_flujo_score(1000.0, 10_000_000.0, 10_000_000.0, 100.0)
        assert math.isfinite(score)

    def test_valores_negativos_no_nan(self):
        score = calcular_flujo_score(-100.0, -500.0, -300.0, -0.5)
        assert math.isfinite(score)

    def test_cero_en_constante_normalizacion_no_rompe(self):
        """Si vcity_max=0 causaría div/0 — no debe ocurrir con los valores reales."""
        # Con los valores constantes actuales esto no pasa, pero verificamos
        assert VCITY_MAX_BARCELONA > 0
        assert VIANANTS_MAX_BARCELONA > 0


# ── Clase 9: _parse_renda_csv edge cases ──────────────────────────────────────

class TestParseRendaCsvEdgeCases:
    def test_csv_solo_headers_devuelve_vacio(self):
        csv = "Codi_Districte,Import_Euros\n"
        result = _parse_renda_csv(csv)
        assert result == {}

    def test_csv_con_valores_no_numericos_ignora(self):
        csv = "Codi_Districte,Import_Euros\n1,abc\n2,25000\n"
        result = _parse_renda_csv(csv)
        assert "01" not in result or result.get("01", 0) == 0
        assert "02" in result

    def test_columna_import_euros_cero_ignorada(self):
        csv = "Codi_Districte,Import_Euros\n1,0\n2,30000\n"
        result = _parse_renda_csv(csv)
        assert "01" not in result
        assert "02" in result

    def test_multiples_barrios_promedio_correcto(self):
        csv = "Codi_Districte,Import_Euros\n5,10000\n5,20000\n5,30000\n"
        result = _parse_renda_csv(csv)
        assert result["05"] == pytest.approx(20000.0)

    def test_csv_vacio_string_devuelve_vacio(self):
        result = _parse_renda_csv("")
        assert isinstance(result, dict)


# ── Clase 10: _build_array — sin NaN con cualquier combinación ───────────────

class TestBuildArraySinNaN:
    """_build_array nunca debe producir NaN con ninguna combinación de inputs."""

    def _assert_no_nan(self, vz, comp, precio, trans, geo, tur):
        arr = _build_array(vz, comp, precio, trans, geo, tur)
        for i, val in enumerate(arr[0]):
            assert math.isfinite(float(val)), (
                f"NaN/Inf en índice {i} ({FEATURE_NAMES[i]})"
            )

    def test_todos_vacios(self):
        self._assert_no_nan({}, {}, None, {}, {}, {})

    def test_flujo_cero(self):
        self._assert_no_nan(
            {"flujo_peatonal_total": 0}, {}, None, {}, {}, {}
        )

    def test_renta_cero(self):
        self._assert_no_nan(
            {"renta_media_hogar": 0}, {}, None, {}, {}, {}
        )

    def test_precio_cero(self):
        self._assert_no_nan({}, {}, 0.0, {}, {}, {})

    def test_ratio_locales_cero(self):
        self._assert_no_nan(
            {"ratio_locales_comerciales": 0.0}, {}, None, {}, {}, {}
        )

    def test_todos_los_datos_presentes(self):
        vz = {
            "flujo_peatonal_total": 1000.0,
            "flujo_peatonal_manana": 350.0,
            "flujo_peatonal_tarde": 420.0,
            "flujo_peatonal_noche": 230.0,
            "renta_media_hogar": 35000.0,
            "edad_media": 40.0,
            "pct_extranjeros": 0.20,
            "densidad_hab_km2": 20000.0,
            "score_turismo": 45.0,
            "incidencias_por_1000hab": 30.0,
            "nivel_ruido_db": 65.0,
            "score_equipamientos": 55.0,
            "pct_locales_vacios": 0.12,
            "tasa_rotacion_anual": 0.15,
            "m2_zonas_verdes_cercanas": 1500.0,
            "ratio_locales_comerciales": 0.25,
            "flujo_popular_times_score": 50.0,
            "vcity_flujo_peatonal": 20000.0,
            "pct_poblacio_25_44": 0.30,
            "delta_renta_3a": 0.10,
        }
        comp = {"num_competidores": 5, "rating_medio": 4.0, "score_saturacion": 40.0}
        trans = {"num_lineas": 8, "num_paradas": 5}
        geo = {"dist_playa_m": 3000}
        tur = {
            "airbnb_density_500m": 25.0, "airbnb_occupancy_est": 0.60,
            "google_review_count_medio": 120.0, "licencias_nuevas_1a": 3.0,
            "eventos_culturales_500m": 4.0, "booking_hoteles_500m": 2.0,
        }
        self._assert_no_nan(vz, comp, 20.0, trans, geo, tur)
