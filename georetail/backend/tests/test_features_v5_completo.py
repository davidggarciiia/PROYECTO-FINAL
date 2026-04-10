"""
tests/test_features_v5_completo.py — Tests exhaustivos para scoring/features.py v5.1.

Cubre:
  - FEATURE_NAMES: 33 features, sin duplicados, posiciones exactas v1→v5.1
  - _MEDIAS: cubre los 33 FEATURE_NAMES, valores plausibles por dominio
  - _build_array: shape (1,33), dtype float32, sin NaN/inf
  - Imputación: cada feature usa _MEDIAS cuando el dato es None/ausente
  - Valores reales: cuando los datos están presentes se usan directamente
  - Fracciones de flujo horario: cálculo y suma=1
  - v5 features (idx 30-31): pct_poblacio_25_44, delta_renta_3a
  - Backward compatibility: posiciones v1-v4 no alteradas
  - Monotonía: más renta → mayor valor en vector
  - Coherencia batch: misma zona, mismos datos → misma fila
"""
from __future__ import annotations

import numpy as np
import pytest

from scoring.features import _build_array, FEATURE_NAMES, _MEDIAS

N_FEATURES = 36


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def vz_completo_v5():
    return {
        "flujo_peatonal_total":    1_500.0,
        "flujo_peatonal_manana":     525.0,
        "flujo_peatonal_tarde":      630.0,
        "flujo_peatonal_noche":      345.0,
        "renta_media_hogar":       42_000.0,
        "edad_media":                 38.0,
        "pct_extranjeros":             0.18,
        "densidad_hab_km2":        25_000.0,
        "pct_locales_vacios":          0.08,
        "tasa_rotacion_anual":         0.12,
        "score_turismo":              70.0,
        "incidencias_por_1000hab":    20.0,
        "nivel_ruido_db":             62.0,
        "score_equipamientos":        75.0,
        "m2_zonas_verdes_cercanas":  2_000.0,
        "ratio_locales_comerciales":   0.30,
        "airbnb_density_500m":         40.0,
        "airbnb_occupancy_est":         0.68,
        "licencias_nuevas_1a":          6.0,
        "eventos_culturales_500m":      5.0,
        "booking_hoteles_500m":         4.0,
        "flujo_popular_times_score":   55.0,
        "vcity_flujo_peatonal":     20_000.0,
        # v5
        "pct_poblacio_25_44":          0.32,
        "delta_renta_3a":              0.11,
    }


@pytest.fixture
def comp_std():
    return {"num_competidores": 7, "rating_medio": 4.2, "score_saturacion": 45.0}


@pytest.fixture
def trans_std():
    return {"num_lineas": 9, "num_paradas": 6}


@pytest.fixture
def geo_std():
    return {"dist_playa_m": 1_000.0}


@pytest.fixture
def tur_std():
    return {
        "airbnb_density_500m": 40.0,
        "airbnb_occupancy_est": 0.68,
        "google_review_count_medio": 180.0,
        "licencias_nuevas_1a": 6.0,
        "eventos_culturales_500m": 5.0,
        "booking_hoteles_500m": 4.0,
    }


def _arr(vz=None, comp=None, precio=None, trans=None, geo=None, tur=None):
    return _build_array(
        vz or {}, comp or {}, precio, trans or {}, geo or {}, tur or {}
    )


# ─────────────────────────────────────────────────────────────────────────────
# 1. Estructura FEATURE_NAMES
# ─────────────────────────────────────────────────────────────────────────────

class TestFeatureNamesEstructura:
    def test_longitud_exacta_32(self):
        assert len(FEATURE_NAMES) == N_FEATURES

    def test_sin_duplicados(self):
        assert len(set(FEATURE_NAMES)) == N_FEATURES

    def test_todos_strings_no_vacios(self):
        for f in FEATURE_NAMES:
            assert isinstance(f, str) and f.strip() != ""

    def test_primer_feature_es_flujo_total(self):
        assert FEATURE_NAMES[0] == "flujo_peatonal_total"

    def test_posicion_v1_features_no_cambia(self):
        v1 = [
            "flujo_peatonal_total","flujo_manana_pct","flujo_tarde_pct","flujo_noche_pct",
            "renta_media_hogar","edad_media","pct_extranjeros","densidad_hab_km2",
            "num_competidores_300m","rating_medio_competidores","score_saturacion",
            "precio_m2_alquiler","pct_locales_vacios","tasa_rotacion_anual",
            "score_turismo","incidencias_por_1000hab","nivel_ruido_db",
            "score_equipamientos","num_lineas_transporte","num_paradas_500m",
            "m2_zonas_verdes_cercanas",
        ]
        for i, name in enumerate(v1):
            assert FEATURE_NAMES[i] == name, f"idx {i}: esperado {name}, got {FEATURE_NAMES[i]}"

    def test_posiciones_v2(self):
        assert FEATURE_NAMES[21] == "dist_playa_m"
        assert FEATURE_NAMES[22] == "ratio_locales_comerciales"

    def test_posiciones_v3(self):
        assert FEATURE_NAMES[23] == "airbnb_density_500m"
        assert FEATURE_NAMES[24] == "airbnb_occupancy_est"
        assert FEATURE_NAMES[25] == "google_review_count_medio"
        assert FEATURE_NAMES[26] == "licencias_nuevas_1a"
        assert FEATURE_NAMES[27] == "eventos_culturales_500m"
        assert FEATURE_NAMES[28] == "booking_hoteles_500m"

    def test_posicion_v4_flujo_score(self):
        assert FEATURE_NAMES[29] == "flujo_peatonal_score"

    def test_posicion_v5_pct_poblacio(self):
        assert FEATURE_NAMES[30] == "pct_poblacio_25_44"

    def test_posicion_v5_delta_renta(self):
        assert FEATURE_NAMES[31] == "delta_renta_3a"

    def test_v5_1_es_el_ultimo(self):
        assert FEATURE_NAMES[-1] == "ratio_complementarios"
        assert FEATURE_NAMES[-2] == "pct_vulnerables"
        assert FEATURE_NAMES[-3] == "score_aglomeracion"

    def test_posicion_v5_1_nivel_estudios(self):
        assert FEATURE_NAMES[32] == "nivel_estudios_alto_pct"


# ─────────────────────────────────────────────────────────────────────────────
# 2. _MEDIAS: cobertura y valores plausibles
# ─────────────────────────────────────────────────────────────────────────────

class TestMediasCobertura:
    def test_todos_feature_names_en_medias(self):
        for f in FEATURE_NAMES:
            assert f in _MEDIAS, f"'{f}' no tiene default en _MEDIAS"

    def test_todos_valores_finitos(self):
        for k, v in _MEDIAS.items():
            assert np.isfinite(v), f"_MEDIAS['{k}'] = {v} no es finito"

    def test_fracciones_pct_en_rango_0_1(self):
        pct_keys = [k for k in _MEDIAS if k.startswith("pct_")]
        for k in pct_keys:
            assert 0.0 <= _MEDIAS[k] <= 1.0, f"_MEDIAS['{k}'] = {_MEDIAS[k]} fuera de [0,1]"

    def test_flujo_total_media_positiva(self):
        assert _MEDIAS["flujo_peatonal_total"] > 0

    def test_renta_media_plausible_bcn(self):
        assert 15_000 <= _MEDIAS["renta_media_hogar"] <= 80_000

    def test_densidad_media_plausible_bcn(self):
        assert 5_000 <= _MEDIAS["densidad_hab_km2"] <= 60_000

    def test_pct_poblacio_25_44_media_plausible(self):
        v = _MEDIAS["pct_poblacio_25_44"]
        assert 0.10 <= v <= 0.50

    def test_delta_renta_3a_media_plausible(self):
        v = _MEDIAS["delta_renta_3a"]
        assert -0.30 <= v <= 0.50

    def test_precio_m2_media_plausible_bcn(self):
        assert 8.0 <= _MEDIAS["precio_m2_alquiler"] <= 45.0

    def test_airbnb_density_media_positiva(self):
        assert _MEDIAS["airbnb_density_500m"] >= 0

    def test_flujo_peatonal_score_media_en_rango(self):
        assert 0 <= _MEDIAS["flujo_peatonal_score"] <= 100


# ─────────────────────────────────────────────────────────────────────────────
# 3. _build_array: estructura del array
# ─────────────────────────────────────────────────────────────────────────────

class TestBuildArrayEstructura:
    def test_shape_1_32_datos_completos(self, vz_completo_v5, comp_std, trans_std, geo_std, tur_std):
        arr = _build_array(vz_completo_v5, comp_std, 18.0, trans_std, geo_std, tur_std)
        assert arr.shape == (1, N_FEATURES)

    def test_shape_1_32_dicts_vacios(self):
        arr = _arr()
        assert arr.shape == (1, N_FEATURES)

    def test_dtype_float32(self, vz_completo_v5, comp_std, trans_std, geo_std, tur_std):
        arr = _build_array(vz_completo_v5, comp_std, 18.0, trans_std, geo_std, tur_std)
        assert arr.dtype == np.float32

    def test_sin_nan(self, vz_completo_v5, comp_std, trans_std, geo_std, tur_std):
        arr = _build_array(vz_completo_v5, comp_std, 18.0, trans_std, geo_std, tur_std)
        assert not np.any(np.isnan(arr)), "Array contiene NaN"

    def test_sin_inf(self, vz_completo_v5, comp_std, trans_std, geo_std, tur_std):
        arr = _build_array(vz_completo_v5, comp_std, 18.0, trans_std, geo_std, tur_std)
        assert not np.any(np.isinf(arr)), "Array contiene inf"

    def test_sin_nan_dicts_vacios(self):
        arr = _arr()
        assert not np.any(np.isnan(arr))

    def test_sin_inf_dicts_vacios(self):
        arr = _arr()
        assert not np.any(np.isinf(arr))

    def test_2d_array(self):
        assert _arr().ndim == 2


# ─────────────────────────────────────────────────────────────────────────────
# 4. Imputación con _MEDIAS
# ─────────────────────────────────────────────────────────────────────────────

class TestImputacion:
    # Las features calculadas dinámicamente no se imputan de _MEDIAS
    _COMPUTED = {"flujo_peatonal_score"}

    def test_dicts_vacios_imputa_todas_las_medias(self):
        arr = _arr()
        for i, f in enumerate(FEATURE_NAMES):
            if f in self._COMPUTED:
                continue
            assert arr[0, i] == pytest.approx(_MEDIAS[f], rel=1e-4), \
                f"idx {i} ({f}): esperado media={_MEDIAS[f]}, got {arr[0,i]}"

    def test_pct_poblacio_25_44_none_usa_media(self):
        arr = _arr(vz={"pct_poblacio_25_44": None})
        idx = FEATURE_NAMES.index("pct_poblacio_25_44")
        assert arr[0, idx] == pytest.approx(_MEDIAS["pct_poblacio_25_44"], rel=1e-4)

    def test_delta_renta_3a_none_usa_media(self):
        arr = _arr(vz={"delta_renta_3a": None})
        idx = FEATURE_NAMES.index("delta_renta_3a")
        assert arr[0, idx] == pytest.approx(_MEDIAS["delta_renta_3a"], rel=1e-4)

    def test_pct_poblacio_ausente_usa_media(self):
        arr = _arr(vz={})  # sin la key
        idx = FEATURE_NAMES.index("pct_poblacio_25_44")
        assert arr[0, idx] == pytest.approx(_MEDIAS["pct_poblacio_25_44"], rel=1e-4)

    def test_renta_none_usa_media(self):
        arr = _arr(vz={"renta_media_hogar": None})
        idx = FEATURE_NAMES.index("renta_media_hogar")
        assert arr[0, idx] == pytest.approx(_MEDIAS["renta_media_hogar"], rel=1e-4)

    def test_precio_none_usa_media(self):
        arr = _arr(precio=None)
        idx = FEATURE_NAMES.index("precio_m2_alquiler")
        assert arr[0, idx] == pytest.approx(_MEDIAS["precio_m2_alquiler"], rel=1e-4)

    def test_airbnb_none_usa_media(self):
        arr = _arr(tur={})
        idx = FEATURE_NAMES.index("airbnb_density_500m")
        assert arr[0, idx] == pytest.approx(_MEDIAS["airbnb_density_500m"], rel=1e-4)

    def test_dist_playa_none_usa_media(self):
        arr = _arr(geo={})
        idx = FEATURE_NAMES.index("dist_playa_m")
        assert arr[0, idx] == pytest.approx(_MEDIAS["dist_playa_m"], rel=1e-4)


# ─────────────────────────────────────────────────────────────────────────────
# 5. Valores reales cuando están disponibles
# ─────────────────────────────────────────────────────────────────────────────

class TestValoresReales:
    def test_pct_poblacio_25_44_real(self):
        arr = _arr(vz={"pct_poblacio_25_44": 0.32})
        idx = FEATURE_NAMES.index("pct_poblacio_25_44")
        assert arr[0, idx] == pytest.approx(0.32, rel=1e-4)

    def test_delta_renta_3a_real(self):
        arr = _arr(vz={"delta_renta_3a": 0.11})
        idx = FEATURE_NAMES.index("delta_renta_3a")
        assert arr[0, idx] == pytest.approx(0.11, rel=1e-4)

    def test_delta_renta_negativo_real(self):
        arr = _arr(vz={"delta_renta_3a": -0.05})
        idx = FEATURE_NAMES.index("delta_renta_3a")
        assert arr[0, idx] == pytest.approx(-0.05, rel=1e-4)

    def test_renta_real(self):
        arr = _arr(vz={"renta_media_hogar": 42_000.0})
        idx = FEATURE_NAMES.index("renta_media_hogar")
        assert arr[0, idx] == pytest.approx(42_000.0, rel=1e-4)

    def test_precio_real(self):
        arr = _arr(precio=22.5)
        idx = FEATURE_NAMES.index("precio_m2_alquiler")
        assert arr[0, idx] == pytest.approx(22.5, rel=1e-4)

    def test_num_competidores_real(self):
        arr = _arr(comp={"num_competidores": 12})
        idx = FEATURE_NAMES.index("num_competidores_300m")
        assert arr[0, idx] == pytest.approx(12.0, rel=1e-4)

    def test_dist_playa_real(self):
        arr = _arr(geo={"dist_playa_m": 800.0})
        idx = FEATURE_NAMES.index("dist_playa_m")
        assert arr[0, idx] == pytest.approx(800.0, rel=1e-4)

    def test_airbnb_density_real(self):
        arr = _arr(tur={"airbnb_density_500m": 55.0, "airbnb_occupancy_est": 0.70,
                        "google_review_count_medio": 200.0, "licencias_nuevas_1a": 8.0,
                        "eventos_culturales_500m": 6.0, "booking_hoteles_500m": 3.0})
        idx = FEATURE_NAMES.index("airbnb_density_500m")
        assert arr[0, idx] == pytest.approx(55.0, rel=1e-4)

    def test_m2_zonas_verdes_real(self):
        arr = _arr(vz={"m2_zonas_verdes_cercanas": 3_500.0})
        idx = FEATURE_NAMES.index("m2_zonas_verdes_cercanas")
        assert arr[0, idx] == pytest.approx(3_500.0, rel=1e-4)


# ─────────────────────────────────────────────────────────────────────────────
# 6. Fracciones de flujo horario
# ─────────────────────────────────────────────────────────────────────────────

class TestFraccionesFlujo:
    def test_fraccion_manana_correcta(self):
        # 300/1200 = 0.25
        arr = _arr(vz={"flujo_peatonal_total": 1200,
                        "flujo_peatonal_manana": 300,
                        "flujo_peatonal_tarde": 600,
                        "flujo_peatonal_noche": 300})
        m = arr[0, FEATURE_NAMES.index("flujo_manana_pct")]
        assert m == pytest.approx(0.25, rel=1e-4)

    def test_fraccion_tarde_correcta(self):
        arr = _arr(vz={"flujo_peatonal_total": 1200,
                        "flujo_peatonal_manana": 300,
                        "flujo_peatonal_tarde": 600,
                        "flujo_peatonal_noche": 300})
        t = arr[0, FEATURE_NAMES.index("flujo_tarde_pct")]
        assert t == pytest.approx(0.50, rel=1e-4)

    def test_fraccion_noche_correcta(self):
        arr = _arr(vz={"flujo_peatonal_total": 1200,
                        "flujo_peatonal_manana": 300,
                        "flujo_peatonal_tarde": 600,
                        "flujo_peatonal_noche": 300})
        n = arr[0, FEATURE_NAMES.index("flujo_noche_pct")]
        assert n == pytest.approx(0.25, rel=1e-4)

    def test_fracciones_suman_1(self):
        arr = _arr(vz={"flujo_peatonal_total": 1200,
                        "flujo_peatonal_manana": 360,
                        "flujo_peatonal_tarde": 504,
                        "flujo_peatonal_noche": 336})
        m = arr[0, FEATURE_NAMES.index("flujo_manana_pct")]
        t = arr[0, FEATURE_NAMES.index("flujo_tarde_pct")]
        n = arr[0, FEATURE_NAMES.index("flujo_noche_pct")]
        assert m + t + n == pytest.approx(1.0, rel=1e-4)

    def test_flujo_cero_no_crashea(self):
        arr = _arr(vz={"flujo_peatonal_total": 0})
        assert not np.any(np.isnan(arr))

    def test_flujo_none_no_crashea(self):
        arr = _arr(vz={"flujo_peatonal_total": None})
        assert arr.shape == (1, N_FEATURES)

    def test_flujo_total_cero_fracciones_son_media(self):
        arr = _arr(vz={"flujo_peatonal_total": 0,
                        "flujo_peatonal_manana": 0,
                        "flujo_peatonal_tarde": 0,
                        "flujo_peatonal_noche": 0})
        idx_m = FEATURE_NAMES.index("flujo_manana_pct")
        # Con flujo=0, las fracciones son None → se imputan con _MEDIAS
        assert arr[0, idx_m] == pytest.approx(_MEDIAS["flujo_manana_pct"], rel=1e-4)


# ─────────────────────────────────────────────────────────────────────────────
# 7. Monotonía: más de algo → más en el vector
# ─────────────────────────────────────────────────────────────────────────────

class TestMonotonia:
    def test_mas_renta_mayor_valor_en_vector(self):
        idx = FEATURE_NAMES.index("renta_media_hogar")
        arr_baja = _arr(vz={"renta_media_hogar": 20_000})
        arr_alta = _arr(vz={"renta_media_hogar": 50_000})
        assert arr_alta[0, idx] > arr_baja[0, idx]

    def test_mayor_pct_poblacio_25_44_mayor_valor(self):
        idx = FEATURE_NAMES.index("pct_poblacio_25_44")
        arr_bajo = _arr(vz={"pct_poblacio_25_44": 0.15})
        arr_alto = _arr(vz={"pct_poblacio_25_44": 0.40})
        assert arr_alto[0, idx] > arr_bajo[0, idx]

    def test_mayor_delta_renta_mayor_valor(self):
        idx = FEATURE_NAMES.index("delta_renta_3a")
        arr_bajo = _arr(vz={"delta_renta_3a": -0.05})
        arr_alto = _arr(vz={"delta_renta_3a": 0.20})
        assert arr_alto[0, idx] > arr_bajo[0, idx]

    def test_mas_flujo_mayor_valor_en_vector(self):
        idx = FEATURE_NAMES.index("flujo_peatonal_total")
        arr_bajo = _arr(vz={"flujo_peatonal_total": 100})
        arr_alto = _arr(vz={"flujo_peatonal_total": 2000})
        assert arr_alto[0, idx] > arr_bajo[0, idx]


# ─────────────────────────────────────────────────────────────────────────────
# 8. Compatibilidad de posiciones entre versiones
# ─────────────────────────────────────────────────────────────────────────────

class TestCompatibilidadVersiones:
    def test_v5_no_desplaza_indices_v4(self):
        """Los 30 primeros features (v4) mantienen sus posiciones en v5."""
        expected_v4 = [
            "flujo_peatonal_total", "flujo_manana_pct", "flujo_tarde_pct",
            "flujo_noche_pct", "renta_media_hogar", "edad_media",
            "pct_extranjeros", "densidad_hab_km2", "num_competidores_300m",
            "rating_medio_competidores", "score_saturacion", "precio_m2_alquiler",
            "pct_locales_vacios", "tasa_rotacion_anual", "score_turismo",
            "incidencias_por_1000hab", "nivel_ruido_db", "score_equipamientos",
            "num_lineas_transporte", "num_paradas_500m", "m2_zonas_verdes_cercanas",
            "dist_playa_m", "ratio_locales_comerciales",
            "airbnb_density_500m", "airbnb_occupancy_est",
            "google_review_count_medio", "licencias_nuevas_1a",
            "eventos_culturales_500m", "booking_hoteles_500m",
            "flujo_peatonal_score",
        ]
        for i, name in enumerate(expected_v4):
            assert FEATURE_NAMES[i] == name, \
                f"REGRESIÓN v5: idx {i} era '{name}', ahora es '{FEATURE_NAMES[i]}'"

    def test_nuevas_features_v5_al_final(self):
        assert FEATURE_NAMES[30] == "pct_poblacio_25_44"
        assert FEATURE_NAMES[31] == "delta_renta_3a"
