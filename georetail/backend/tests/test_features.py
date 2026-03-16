"""
tests/test_features.py — Tests para scoring/features.py (función pura _build_array).

_build_array es la única función sin I/O del módulo; construye el
vector de 21 features para XGBoost a partir de dicts de datos de zona.

Cubre:
  - Shape del array resultante (1 fila × 21 features)
  - Dtype float32
  - Orden de features coincide con FEATURE_NAMES
  - Imputación con _MEDIAS cuando el dato es None/ausente
  - Cálculo correcto de fracciones de flujo por franja horaria
  - Flujo total cero no lanza ZeroDivisionError
  - Todos los valores son finitos (no NaN / inf)
"""
import pytest
import numpy as np

from scoring.features import _build_array, FEATURE_NAMES, _MEDIAS


# ─── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def vz_completo():
    return {
        "flujo_peatonal_total":   1_200.0,
        "flujo_peatonal_manana":    360.0,
        "flujo_peatonal_tarde":     600.0,
        "flujo_peatonal_noche":     240.0,
        "renta_media_hogar":     38_000.0,
        "edad_media":               40.0,
        "pct_extranjeros":          0.20,
        "densidad_hab_km2":      22_000.0,
        "pct_locales_vacios":        0.10,
        "tasa_rotacion_anual":       0.15,
        "score_turismo":            60.0,
        "incidencias_por_1000hab":  25.0,
        "nivel_ruido_db":           65.0,
        "score_equipamientos":      70.0,
        "m2_zonas_verdes_cercanas": 1_500.0,
    }


@pytest.fixture
def comp_completo():
    return {
        "num_competidores": 6,
        "rating_medio":     4.1,
        "score_saturacion": 50.0,
    }


@pytest.fixture
def trans_completo():
    return {"num_lineas": 7, "num_paradas": 5}


# ─── Tests de estructura del array ───────────────────────────────────────────

class TestEstructuraArray:
    def test_shape_es_1_por_21_features(self, vz_completo, comp_completo, trans_completo):
        arr = _build_array(vz_completo, comp_completo, 18.0, trans_completo)
        assert arr.shape == (1, len(FEATURE_NAMES))

    def test_shape_es_1_por_21_con_dicts_vacios(self):
        arr = _build_array({}, {}, None, {})
        assert arr.shape == (1, len(FEATURE_NAMES))

    def test_dtype_es_float32(self, vz_completo, comp_completo, trans_completo):
        arr = _build_array(vz_completo, comp_completo, 18.0, trans_completo)
        assert arr.dtype == np.float32

    def test_feature_names_tiene_21_elementos(self):
        assert len(FEATURE_NAMES) == 21

    def test_no_hay_duplicados_en_feature_names(self):
        assert len(FEATURE_NAMES) == len(set(FEATURE_NAMES))


# ─── Tests de imputación con medias ──────────────────────────────────────────

class TestImputacion:
    def test_datos_vacios_usa_todas_las_medias(self):
        arr = _build_array({}, {}, None, {})
        vec = arr[0]
        for i, feat in enumerate(FEATURE_NAMES):
            assert vec[i] == pytest.approx(_MEDIAS[feat], rel=1e-5), (
                f"Feature {feat}: esperado {_MEDIAS[feat]}, obtenido {vec[i]}"
            )

    def test_precio_none_usa_media(self, vz_completo, comp_completo, trans_completo):
        arr_sin = _build_array(vz_completo, comp_completo, None,  trans_completo)
        arr_med = _build_array(vz_completo, comp_completo, 18.0, trans_completo)
        idx = FEATURE_NAMES.index("precio_m2_alquiler")
        # Sin precio → media; con precio 18.0 → media (coincide)
        assert arr_sin[0][idx] == pytest.approx(_MEDIAS["precio_m2_alquiler"], rel=1e-5)

    def test_renta_none_usa_media(self, comp_completo, trans_completo):
        vz = {"flujo_peatonal_total": 1000, "renta_media_hogar": None}
        arr = _build_array(vz, comp_completo, 18.0, trans_completo)
        idx = FEATURE_NAMES.index("renta_media_hogar")
        assert arr[0][idx] == pytest.approx(_MEDIAS["renta_media_hogar"], rel=1e-5)

    def test_medias_cubre_todos_los_feature_names(self):
        for feat in FEATURE_NAMES:
            assert feat in _MEDIAS, f"Feature {feat!r} no tiene media de imputación"


# ─── Tests de cálculo de fracciones de flujo ──────────────────────────────────

class TestFraccionesFlujo:
    def test_fraccion_manana_correcta(self, vz_completo, comp_completo, trans_completo):
        # 360 / 1200 = 0.30
        arr = _build_array(vz_completo, comp_completo, 18.0, trans_completo)
        idx = FEATURE_NAMES.index("flujo_manana_pct")
        assert arr[0][idx] == pytest.approx(0.30, rel=1e-5)

    def test_fraccion_tarde_correcta(self, vz_completo, comp_completo, trans_completo):
        # 600 / 1200 = 0.50
        arr = _build_array(vz_completo, comp_completo, 18.0, trans_completo)
        idx = FEATURE_NAMES.index("flujo_tarde_pct")
        assert arr[0][idx] == pytest.approx(0.50, rel=1e-5)

    def test_fraccion_noche_correcta(self, vz_completo, comp_completo, trans_completo):
        # 240 / 1200 = 0.20
        arr = _build_array(vz_completo, comp_completo, 18.0, trans_completo)
        idx = FEATURE_NAMES.index("flujo_noche_pct")
        assert arr[0][idx] == pytest.approx(0.20, rel=1e-5)

    def test_fracciones_suman_1(self, vz_completo, comp_completo, trans_completo):
        arr = _build_array(vz_completo, comp_completo, 18.0, trans_completo)
        m = arr[0][FEATURE_NAMES.index("flujo_manana_pct")]
        t = arr[0][FEATURE_NAMES.index("flujo_tarde_pct")]
        n = arr[0][FEATURE_NAMES.index("flujo_noche_pct")]
        assert m + t + n == pytest.approx(1.0, rel=1e-5)

    def test_flujo_total_cero_no_crashea(self, comp_completo, trans_completo):
        vz = {"flujo_peatonal_total": 0, "flujo_peatonal_manana": 0,
              "flujo_peatonal_tarde": 0, "flujo_peatonal_noche": 0}
        arr = _build_array(vz, comp_completo, 18.0, trans_completo)
        # Las fracciones deben ser la media de imputación (no NaN)
        idx_m = FEATURE_NAMES.index("flujo_manana_pct")
        assert not np.isnan(arr[0][idx_m])

    def test_flujo_none_no_crashea(self, comp_completo, trans_completo):
        vz = {"flujo_peatonal_total": None}
        arr = _build_array(vz, comp_completo, 18.0, trans_completo)
        assert arr.shape == (1, 21)


# ─── Tests de valores del array ───────────────────────────────────────────────

class TestValoresArray:
    def test_todos_los_valores_son_finitos(self, vz_completo, comp_completo, trans_completo):
        arr = _build_array(vz_completo, comp_completo, 18.0, trans_completo)
        assert np.all(np.isfinite(arr)), "El array contiene NaN o inf"

    def test_todos_los_valores_son_finitos_con_datos_vacios(self):
        arr = _build_array({}, {}, None, {})
        assert np.all(np.isfinite(arr))

    def test_flujo_total_correcto(self, vz_completo, comp_completo, trans_completo):
        arr = _build_array(vz_completo, comp_completo, 18.0, trans_completo)
        idx = FEATURE_NAMES.index("flujo_peatonal_total")
        assert arr[0][idx] == pytest.approx(1_200.0, rel=1e-5)

    def test_num_competidores_correcto(self, vz_completo, comp_completo, trans_completo):
        arr = _build_array(vz_completo, comp_completo, 18.0, trans_completo)
        idx = FEATURE_NAMES.index("num_competidores_300m")
        assert arr[0][idx] == pytest.approx(6.0, rel=1e-5)

    def test_num_lineas_transporte_correcto(self, vz_completo, comp_completo, trans_completo):
        arr = _build_array(vz_completo, comp_completo, 18.0, trans_completo)
        idx = FEATURE_NAMES.index("num_lineas_transporte")
        assert arr[0][idx] == pytest.approx(7.0, rel=1e-5)

    def test_precio_m2_correcto(self, vz_completo, comp_completo, trans_completo):
        arr = _build_array(vz_completo, comp_completo, 25.0, trans_completo)
        idx = FEATURE_NAMES.index("precio_m2_alquiler")
        assert arr[0][idx] == pytest.approx(25.0, rel=1e-5)
