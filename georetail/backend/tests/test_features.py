"""
tests/test_features.py — Tests para scoring/features.py (función pura _build_array).

_build_array es la única función sin I/O del módulo; construye el
vector de 33 features para XGBoost (v5.1) a partir de dicts de datos de zona.

Cubre:
  - Shape del array resultante (1 fila × 33 features)
  - Dtype float32
  - Orden de features coincide con FEATURE_NAMES
  - Imputación con _MEDIAS cuando el dato es None/ausente
  - Cálculo correcto de fracciones de flujo por franja horaria
  - Flujo total cero no lanza ZeroDivisionError
  - Todos los valores son finitos (no NaN / inf)
  - Features v3 (índices 23-28): airbnb_density_500m, airbnb_occupancy_est,
    google_review_count_medio, licencias_nuevas_1a, eventos_culturales_500m,
    booking_hoteles_500m
  - Feature v4 (índice 29): flujo_peatonal_score (fusión ponderada 4 fuentes)
  - Features v5 (índices 30-31): pct_poblacio_25_44, delta_renta_3a
  - Feature v5.1 (índice 32): nivel_estudios_alto_pct
"""
import pytest
import numpy as np

from scoring.features import _build_array, FEATURE_NAMES, _MEDIAS

# Número total de features en v6 (v5.1 + 3 competition features: score_aglomeracion, pct_vulnerables, ratio_complementarios)
N_FEATURES_V5 = len(FEATURE_NAMES)
N_FEATURES_V4 = N_FEATURES_V5  # alias compatibilidad — v4 era 30, ahora v5.1=33
N_FEATURES_V3 = N_FEATURES_V5  # alias de compatibilidad — tests usan la constante antigua


# ─── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def vz_completo():
    """Variables de zona completas incluyendo v2 y v3."""
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
        # v2
        "ratio_locales_comerciales": 0.25,
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


@pytest.fixture
def geo_completo():
    """Features geográficas de nivel zona (v2)."""
    return {"dist_playa_m": 1200.0}


@pytest.fixture
def tur_completo():
    """Features de turismo y dinamismo comercial (v3)."""
    return {
        "airbnb_density_500m":       35.0,
        "airbnb_occupancy_est":       0.70,
        "google_review_count_medio": 200.0,
        "licencias_nuevas_1a":         5.0,
        "eventos_culturales_500m":     4.0,
        "booking_hoteles_500m":        3.0,
    }


# ─── Tests de estructura del array ───────────────────────────────────────────

class TestEstructuraArray:
    def test_shape_es_1_por_29_features(
        self, vz_completo, comp_completo, trans_completo, geo_completo, tur_completo
    ):
        arr = _build_array(vz_completo, comp_completo, 18.0, trans_completo, geo_completo, tur_completo)
        assert arr.shape == (1, N_FEATURES_V3)

    def test_shape_es_1_por_29_con_dicts_vacios(self):
        arr = _build_array({}, {}, None, {}, {}, {})
        assert arr.shape == (1, N_FEATURES_V3)

    def test_dtype_es_float32(
        self, vz_completo, comp_completo, trans_completo, geo_completo, tur_completo
    ):
        arr = _build_array(vz_completo, comp_completo, 18.0, trans_completo, geo_completo, tur_completo)
        assert arr.dtype == np.float32

    def test_feature_names_tiene_30_elementos(self):
        assert len(FEATURE_NAMES) == N_FEATURES_V4

    def test_no_hay_duplicados_en_feature_names(self):
        assert len(FEATURE_NAMES) == len(set(FEATURE_NAMES))

    def test_feature_names_contiene_v3_features(self):
        v3_features = [
            "airbnb_density_500m",
            "airbnb_occupancy_est",
            "google_review_count_medio",
            "licencias_nuevas_1a",
            "eventos_culturales_500m",
            "booking_hoteles_500m",
        ]
        for feat in v3_features:
            assert feat in FEATURE_NAMES, f"Feature v3 {feat!r} no está en FEATURE_NAMES"

    def test_feature_names_contiene_v2_features(self):
        assert "dist_playa_m" in FEATURE_NAMES
        assert "ratio_locales_comerciales" in FEATURE_NAMES

    def test_v3_features_estan_en_indices_23_a_28(self):
        """Las 6 features v3 deben ocupar los índices 23-28 (tras v1 y v2)."""
        v3_features = FEATURE_NAMES[23:29]
        assert len(v3_features) == 6
        assert v3_features[0] == "airbnb_density_500m"
        assert v3_features[-1] == "booking_hoteles_500m"

    def test_v4_flujo_score_es_indice_29(self):
        """La feature v4 flujo_peatonal_score ocupa el índice 29."""
        assert FEATURE_NAMES[29] == "flujo_peatonal_score"

    def test_v5_pct_poblacio_25_44_es_indice_30(self):
        """La feature v5 pct_poblacio_25_44 ocupa el índice 30."""
        assert FEATURE_NAMES[30] == "pct_poblacio_25_44"

    def test_v5_delta_renta_3a_indice_31(self):
        """La feature v5 delta_renta_3a ocupa el índice 31."""
        assert FEATURE_NAMES[31] == "delta_renta_3a"

    def test_v5_1_nivel_estudios_es_indice_32(self):
        """Mantiene índices estables para v5-v14."""
        assert FEATURE_NAMES[32] == "nivel_estudios_alto_pct"
        assert FEATURE_NAMES[35] == "ratio_complementarios"
        assert FEATURE_NAMES[46] == "gini"
        assert FEATURE_NAMES[50] == "personas_solas"
        assert FEATURE_NAMES[51] == "renta_media_uc"
        assert FEATURE_NAMES[52] == "renta_mediana_uc"
        assert FEATURE_NAMES[58] == "seasonality_summer_lift"
        assert FEATURE_NAMES[62] == "seasonality_peak_concentration"
        assert FEATURE_NAMES[63] == "weekend_lift"
        assert FEATURE_NAMES[69] == "temporal_confianza"


# ─── Tests de imputación con medias ──────────────────────────────────────────

class TestImputacion:
    def test_datos_vacios_usa_todas_las_medias(self):
        arr = _build_array({}, {}, None, {}, {}, {})
        vec = arr[0]
        # flujo_peatonal_score (v4) es calculado en tiempo real por calcular_flujo_score(),
        # no imputado de _MEDIAS. Con todas las fuentes a None devuelve el fallback (30.0).
        _COMPUTED_FEATURES = {"flujo_peatonal_score"}
        for i, feat in enumerate(FEATURE_NAMES):
            if feat in _COMPUTED_FEATURES:
                continue
            assert vec[i] == pytest.approx(_MEDIAS[feat], rel=1e-5), (
                f"Feature {feat}: esperado {_MEDIAS[feat]}, obtenido {vec[i]}"
            )

    def test_flujo_peatonal_score_con_datos_vacios_es_fallback(self):
        """Con todos los inputs None, flujo_peatonal_score devuelve el fallback conservador."""
        arr = _build_array({}, {}, None, {}, {}, {})
        idx = FEATURE_NAMES.index("flujo_peatonal_score")
        assert arr[0][idx] == pytest.approx(30.0, abs=0.01)

    def test_precio_none_usa_media(
        self, vz_completo, comp_completo, trans_completo, geo_completo, tur_completo
    ):
        arr = _build_array(vz_completo, comp_completo, None, trans_completo, geo_completo, tur_completo)
        idx = FEATURE_NAMES.index("precio_m2_alquiler")
        assert arr[0][idx] == pytest.approx(_MEDIAS["precio_m2_alquiler"], rel=1e-5)

    def test_renta_none_usa_media(
        self, comp_completo, trans_completo, geo_completo, tur_completo
    ):
        vz = {"flujo_peatonal_total": 1000, "renta_media_hogar": None}
        arr = _build_array(vz, comp_completo, 18.0, trans_completo, geo_completo, tur_completo)
        idx = FEATURE_NAMES.index("renta_media_hogar")
        assert arr[0][idx] == pytest.approx(_MEDIAS["renta_media_hogar"], rel=1e-5)

    def test_medias_cubre_todos_los_feature_names(self):
        for feat in FEATURE_NAMES:
            assert feat in _MEDIAS, f"Feature {feat!r} no tiene media de imputación"

    def test_v3_airbnb_density_none_usa_media(
        self, vz_completo, comp_completo, trans_completo, geo_completo
    ):
        """Sin datos de turismo, airbnb_density_500m debe imputarse con la media."""
        arr = _build_array(vz_completo, comp_completo, 18.0, trans_completo, geo_completo, {})
        idx = FEATURE_NAMES.index("airbnb_density_500m")
        assert arr[0][idx] == pytest.approx(_MEDIAS["airbnb_density_500m"], rel=1e-5)

    def test_v3_google_reviews_none_usa_media(
        self, vz_completo, comp_completo, trans_completo, geo_completo
    ):
        arr = _build_array(vz_completo, comp_completo, 18.0, trans_completo, geo_completo, {})
        idx = FEATURE_NAMES.index("google_review_count_medio")
        assert arr[0][idx] == pytest.approx(_MEDIAS["google_review_count_medio"], rel=1e-5)

    def test_v3_dist_playa_none_usa_media(
        self, vz_completo, comp_completo, trans_completo, tur_completo
    ):
        arr = _build_array(vz_completo, comp_completo, 18.0, trans_completo, {}, tur_completo)
        idx = FEATURE_NAMES.index("dist_playa_m")
        assert arr[0][idx] == pytest.approx(_MEDIAS["dist_playa_m"], rel=1e-5)


# ─── Tests de cálculo de fracciones de flujo ──────────────────────────────────

class TestFraccionesFlujo:
    def test_fraccion_manana_correcta(
        self, vz_completo, comp_completo, trans_completo, geo_completo, tur_completo
    ):
        # 360 / 1200 = 0.30
        arr = _build_array(vz_completo, comp_completo, 18.0, trans_completo, geo_completo, tur_completo)
        idx = FEATURE_NAMES.index("flujo_manana_pct")
        assert arr[0][idx] == pytest.approx(0.30, rel=1e-5)

    def test_fraccion_tarde_correcta(
        self, vz_completo, comp_completo, trans_completo, geo_completo, tur_completo
    ):
        # 600 / 1200 = 0.50
        arr = _build_array(vz_completo, comp_completo, 18.0, trans_completo, geo_completo, tur_completo)
        idx = FEATURE_NAMES.index("flujo_tarde_pct")
        assert arr[0][idx] == pytest.approx(0.50, rel=1e-5)

    def test_fraccion_noche_correcta(
        self, vz_completo, comp_completo, trans_completo, geo_completo, tur_completo
    ):
        # 240 / 1200 = 0.20
        arr = _build_array(vz_completo, comp_completo, 18.0, trans_completo, geo_completo, tur_completo)
        idx = FEATURE_NAMES.index("flujo_noche_pct")
        assert arr[0][idx] == pytest.approx(0.20, rel=1e-5)

    def test_fracciones_suman_1(
        self, vz_completo, comp_completo, trans_completo, geo_completo, tur_completo
    ):
        arr = _build_array(vz_completo, comp_completo, 18.0, trans_completo, geo_completo, tur_completo)
        m = arr[0][FEATURE_NAMES.index("flujo_manana_pct")]
        t = arr[0][FEATURE_NAMES.index("flujo_tarde_pct")]
        n = arr[0][FEATURE_NAMES.index("flujo_noche_pct")]
        assert m + t + n == pytest.approx(1.0, rel=1e-5)

    def test_flujo_total_cero_no_crashea(
        self, comp_completo, trans_completo, geo_completo, tur_completo
    ):
        vz = {"flujo_peatonal_total": 0, "flujo_peatonal_manana": 0,
              "flujo_peatonal_tarde": 0, "flujo_peatonal_noche": 0}
        arr = _build_array(vz, comp_completo, 18.0, trans_completo, geo_completo, tur_completo)
        # Las fracciones deben ser la media de imputación (no NaN)
        idx_m = FEATURE_NAMES.index("flujo_manana_pct")
        assert not np.isnan(arr[0][idx_m])

    def test_flujo_none_no_crashea(
        self, comp_completo, trans_completo, geo_completo, tur_completo
    ):
        vz = {"flujo_peatonal_total": None}
        arr = _build_array(vz, comp_completo, 18.0, trans_completo, geo_completo, tur_completo)
        assert arr.shape == (1, N_FEATURES_V3)


# ─── Tests de valores del array ───────────────────────────────────────────────

class TestValoresArray:
    def test_todos_los_valores_son_finitos(
        self, vz_completo, comp_completo, trans_completo, geo_completo, tur_completo
    ):
        arr = _build_array(vz_completo, comp_completo, 18.0, trans_completo, geo_completo, tur_completo)
        assert np.all(np.isfinite(arr)), "El array contiene NaN o inf"

    def test_todos_los_valores_son_finitos_con_datos_vacios(self):
        arr = _build_array({}, {}, None, {}, {}, {})
        assert np.all(np.isfinite(arr))

    def test_flujo_total_correcto(
        self, vz_completo, comp_completo, trans_completo, geo_completo, tur_completo
    ):
        arr = _build_array(vz_completo, comp_completo, 18.0, trans_completo, geo_completo, tur_completo)
        idx = FEATURE_NAMES.index("flujo_peatonal_total")
        assert arr[0][idx] == pytest.approx(1_200.0, rel=1e-5)

    def test_num_competidores_correcto(
        self, vz_completo, comp_completo, trans_completo, geo_completo, tur_completo
    ):
        arr = _build_array(vz_completo, comp_completo, 18.0, trans_completo, geo_completo, tur_completo)
        idx = FEATURE_NAMES.index("num_competidores_300m")
        assert arr[0][idx] == pytest.approx(6.0, rel=1e-5)

    def test_num_lineas_transporte_correcto(
        self, vz_completo, comp_completo, trans_completo, geo_completo, tur_completo
    ):
        arr = _build_array(vz_completo, comp_completo, 18.0, trans_completo, geo_completo, tur_completo)
        idx = FEATURE_NAMES.index("num_lineas_transporte")
        assert arr[0][idx] == pytest.approx(7.0, rel=1e-5)

    def test_precio_m2_correcto(
        self, vz_completo, comp_completo, trans_completo, geo_completo, tur_completo
    ):
        arr = _build_array(vz_completo, comp_completo, 25.0, trans_completo, geo_completo, tur_completo)
        idx = FEATURE_NAMES.index("precio_m2_alquiler")
        assert arr[0][idx] == pytest.approx(25.0, rel=1e-5)

    def test_v3_airbnb_density_correcto(
        self, vz_completo, comp_completo, trans_completo, geo_completo, tur_completo
    ):
        arr = _build_array(vz_completo, comp_completo, 18.0, trans_completo, geo_completo, tur_completo)
        idx = FEATURE_NAMES.index("airbnb_density_500m")
        assert arr[0][idx] == pytest.approx(35.0, rel=1e-5)

    def test_v3_airbnb_occupancy_correcto(
        self, vz_completo, comp_completo, trans_completo, geo_completo, tur_completo
    ):
        arr = _build_array(vz_completo, comp_completo, 18.0, trans_completo, geo_completo, tur_completo)
        idx = FEATURE_NAMES.index("airbnb_occupancy_est")
        assert arr[0][idx] == pytest.approx(0.70, rel=1e-5)

    def test_v3_google_reviews_neutralizado_hasta_historificar(
        self, vz_completo, comp_completo, trans_completo, geo_completo, tur_completo
    ):
        arr = _build_array(vz_completo, comp_completo, 18.0, trans_completo, geo_completo, tur_completo)
        idx = FEATURE_NAMES.index("google_review_count_medio")
        assert arr[0][idx] == pytest.approx(_MEDIAS["google_review_count_medio"], rel=1e-5)

    def test_v3_licencias_nuevas_correcto(
        self, vz_completo, comp_completo, trans_completo, geo_completo, tur_completo
    ):
        arr = _build_array(vz_completo, comp_completo, 18.0, trans_completo, geo_completo, tur_completo)
        idx = FEATURE_NAMES.index("licencias_nuevas_1a")
        assert arr[0][idx] == pytest.approx(5.0, rel=1e-5)

    def test_v3_eventos_culturales_correcto(
        self, vz_completo, comp_completo, trans_completo, geo_completo, tur_completo
    ):
        arr = _build_array(vz_completo, comp_completo, 18.0, trans_completo, geo_completo, tur_completo)
        idx = FEATURE_NAMES.index("eventos_culturales_500m")
        assert arr[0][idx] == pytest.approx(4.0, rel=1e-5)

    def test_v3_booking_hoteles_correcto(
        self, vz_completo, comp_completo, trans_completo, geo_completo, tur_completo
    ):
        arr = _build_array(vz_completo, comp_completo, 18.0, trans_completo, geo_completo, tur_completo)
        idx = FEATURE_NAMES.index("booking_hoteles_500m")
        assert arr[0][idx] == pytest.approx(3.0, rel=1e-5)

    def test_v2_dist_playa_correcto(
        self, vz_completo, comp_completo, trans_completo, geo_completo, tur_completo
    ):
        arr = _build_array(vz_completo, comp_completo, 18.0, trans_completo, geo_completo, tur_completo)
        idx = FEATURE_NAMES.index("dist_playa_m")
        assert arr[0][idx] == pytest.approx(1200.0, rel=1e-5)


# ─── Tests de posición de features ────────────────────────────────────────────

class TestPosicionFeatures:
    """Verifica que los índices de features son correctos y no cambian entre versiones."""

    def test_flujo_peatonal_total_es_indice_0(self):
        assert FEATURE_NAMES[0] == "flujo_peatonal_total"

    def test_m2_zonas_verdes_es_indice_20(self):
        assert FEATURE_NAMES[20] == "m2_zonas_verdes_cercanas"

    def test_dist_playa_m_es_indice_21(self):
        """v2: dist_playa_m debe estar en índice 21."""
        assert FEATURE_NAMES[21] == "dist_playa_m"

    def test_ratio_locales_es_indice_22(self):
        """v2: ratio_locales_comerciales debe estar en índice 22."""
        assert FEATURE_NAMES[22] == "ratio_locales_comerciales"

    def test_airbnb_density_es_indice_23(self):
        """v3: airbnb_density_500m debe estar en índice 23."""
        assert FEATURE_NAMES[23] == "airbnb_density_500m"

    def test_booking_hoteles_es_indice_28(self):
        """v3: booking_hoteles_500m debe estar en el último índice (28)."""
        assert FEATURE_NAMES[28] == "booking_hoteles_500m"
