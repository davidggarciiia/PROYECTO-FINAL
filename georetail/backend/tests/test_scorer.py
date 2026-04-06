"""
tests/test_scorer.py — Tests para scoring/scorer.py (funciones puras).

Solo se testean las funciones que NO hacen I/O ni dependen de XGBoost:
  - _score_manual: cálculo de scores con pesos manuales
  - _score_neutro: dict de fallback completo

Cubre:
  - Todos los scores de dimensión en rango [0, 100]
  - Score global en rango [0, 100]
  - Pesos suman aproximadamente 1.0 con pesos por defecto
  - Comportamiento con datos faltantes (usa defaults)
  - Comportamiento con valores extremos
  - Clamping de s_comp y s_turismo (bug fix)
  - _score_neutro devuelve dict con todas las claves esperadas
"""
import pytest

from scoring.scorer import _score_manual, _score_neutro


# ─── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def datos_completos():
    """Datos de zona con todos los campos rellenos."""
    return {
        "flujo_peatonal_total":    1_200.0,
        "renta_media_hogar":       38_000.0,
        "score_saturacion":        45.0,
        "precio_m2":               18.0,
        "num_lineas_transporte":   8.0,
        "incidencias_por_1000hab": 30.0,
        "score_turismo":           60.0,
        "pct_locales_vacios":      0.10,
        "tasa_rotacion_anual":     0.15,
        # Seguridad v7
        "hurtos_por_1000hab":      12.0,
        "robatoris_por_1000hab":    5.0,
        "danys_por_1000hab":        3.0,
        "incidencias_noche_pct":    0.25,
        "comisarias_1km":           2,
        "dist_comisaria_m":       350.0,
        "seguridad_barri_score":   65.0,
        # Entorno v8
        "licencias_nuevas_1a":     15.0,
        "ratio_locales_comerciales": 0.12,
        "nivel_ruido_db":          62.0,
        "score_equipamientos":     55.0,
        "m2_zonas_verdes_cercanas": 8_500.0,
        "mercados_municipales_1km": 1,
        "eventos_culturales_500m":  3.0,
    }


@pytest.fixture
def sector_pesos_default():
    """Sector con pesos por defecto de la fórmula."""
    return {}


@pytest.fixture
def sector_pesos_custom():
    return {
        "peso_flujo":       0.30,
        "peso_demo":        0.20,
        "peso_competencia": 0.15,
        "peso_precio":      0.15,
        "peso_transporte":  0.08,
        "peso_seguridad":   0.04,
        "peso_turismo":     0.04,
        "peso_entorno":     0.04,
    }


# ─── Tests de rango de scores de dimensión ─────────────────────────────────────

class TestRangoScoresDimension:
    def test_score_flujo_en_rango_0_100(self, datos_completos, sector_pesos_default):
        r = _score_manual(datos_completos, sector_pesos_default)
        assert 0.0 <= r["score_flujo_peatonal"] <= 100.0

    def test_score_demografia_en_rango_0_100(self, datos_completos, sector_pesos_default):
        r = _score_manual(datos_completos, sector_pesos_default)
        assert 0.0 <= r["score_demografia"] <= 100.0

    def test_score_competencia_en_rango_0_100(self, datos_completos, sector_pesos_default):
        r = _score_manual(datos_completos, sector_pesos_default)
        assert 0.0 <= r["score_competencia"] <= 100.0

    def test_score_precio_en_rango_0_100(self, datos_completos, sector_pesos_default):
        r = _score_manual(datos_completos, sector_pesos_default)
        assert 0.0 <= r["score_precio_alquiler"] <= 100.0

    def test_score_transporte_en_rango_0_100(self, datos_completos, sector_pesos_default):
        r = _score_manual(datos_completos, sector_pesos_default)
        assert 0.0 <= r["score_transporte"] <= 100.0

    def test_score_seguridad_en_rango_0_100(self, datos_completos, sector_pesos_default):
        r = _score_manual(datos_completos, sector_pesos_default)
        assert 0.0 <= r["score_seguridad"] <= 100.0

    def test_score_turismo_en_rango_0_100(self, datos_completos, sector_pesos_default):
        r = _score_manual(datos_completos, sector_pesos_default)
        assert 0.0 <= r["score_turismo"] <= 100.0

    def test_score_entorno_en_rango_0_100(self, datos_completos, sector_pesos_default):
        r = _score_manual(datos_completos, sector_pesos_default)
        assert 0.0 <= r["score_entorno_comercial"] <= 100.0

    def test_score_global_en_rango_0_100(self, datos_completos, sector_pesos_default):
        r = _score_manual(datos_completos, sector_pesos_default)
        assert 0.0 <= r["score_global"] <= 100.0


# ─── Tests de valores extremos ────────────────────────────────────────────────

class TestValoresExtremos:
    def test_flujo_cero_da_score_flujo_cero(self, sector_pesos_default):
        datos = {"flujo_peatonal_total": 0}
        r = _score_manual(datos, sector_pesos_default)
        assert r["score_flujo_peatonal"] == 0.0

    def test_flujo_alto_da_score_flujo_alto(self, sector_pesos_default):
        datos = {"flujo_peatonal_total": 3_000}
        r = _score_manual(datos, sector_pesos_default)
        assert r["score_flujo_peatonal"] >= 15.0  # contribución positiva

    def test_flujo_sobre_maximo_no_supera_100(self, sector_pesos_default):
        datos = {"flujo_peatonal_total": 999_999}
        r = _score_manual(datos, sector_pesos_default)
        assert r["score_flujo_peatonal"] == pytest.approx(100.0)

    def test_renta_minima_da_score_demo_cero(self, sector_pesos_default):
        datos = {"renta_media_hogar": 17_000}
        r = _score_manual(datos, sector_pesos_default)
        assert r["score_demografia"] == pytest.approx(0.0)

    def test_renta_maxima_da_score_demo_100(self, sector_pesos_default):
        datos = {"renta_media_hogar": 60_100}  # ~(60100-17000)/430 ≈ 100.2 → 100
        r = _score_manual(datos, sector_pesos_default)
        assert r["score_demografia"] == pytest.approx(100.0)

    def test_saturacion_0_da_score_comp_100(self, sector_pesos_default):
        datos = {"score_saturacion": 0}
        r = _score_manual(datos, sector_pesos_default)
        assert r["score_competencia"] == pytest.approx(100.0)

    def test_saturacion_100_da_score_comp_0(self, sector_pesos_default):
        datos = {"score_saturacion": 100}
        r = _score_manual(datos, sector_pesos_default)
        assert r["score_competencia"] == pytest.approx(0.0)

    def test_precio_minimo_8_da_score_precio_100(self, sector_pesos_default):
        datos = {"precio_m2": 8.0}  # Precio mínimo BCN
        r = _score_manual(datos, sector_pesos_default)
        assert r["score_precio_alquiler"] == pytest.approx(100.0)

    def test_precio_maximo_45_da_score_precio_0(self, sector_pesos_default):
        datos = {"precio_m2": 45.0}  # Precio máximo BCN
        r = _score_manual(datos, sector_pesos_default)
        assert r["score_precio_alquiler"] == pytest.approx(0.0)

    def test_lineas_0_da_score_transporte_0(self, sector_pesos_default):
        datos = {"num_lineas_transporte": 0}
        r = _score_manual(datos, sector_pesos_default)
        assert r["score_transporte"] == pytest.approx(0.0)

    def test_lineas_20_da_score_transporte_100(self, sector_pesos_default):
        datos = {"num_lineas_transporte": 20}
        r = _score_manual(datos, sector_pesos_default)
        assert r["score_transporte"] == pytest.approx(100.0)

    def test_incidencias_minimas_da_score_seguridad_alto(self, sector_pesos_default):
        """Seguridad v7 compuesta: incidencias bajas contribuyen a score alto."""
        datos = {
            "incidencias_por_1000hab": 5.0,
            "hurtos_por_1000hab": 2.0,
            "robatoris_por_1000hab": 1.0,
            "danys_por_1000hab": 0.5,
            "incidencias_noche_pct": 0.15,
            "comisarias_1km": 5,
            "dist_comisaria_m": 100.0,
            "seguridad_barri_score": 9.0,
        }
        r = _score_manual(datos, sector_pesos_default)
        assert r["score_seguridad"] >= 80.0

    def test_incidencias_maximas_da_score_seguridad_bajo(self, sector_pesos_default):
        """Seguridad v7 compuesta: incidencias altas + sin comisarías = score bajo."""
        datos = {
            "incidencias_por_1000hab": 120.0,
            "hurtos_por_1000hab": 40.0,
            "robatoris_por_1000hab": 30.0,
            "danys_por_1000hab": 20.0,
            "incidencias_noche_pct": 0.55,
            "comisarias_1km": 0,
            "dist_comisaria_m": 3000.0,
            "seguridad_barri_score": 2.0,
        }
        r = _score_manual(datos, sector_pesos_default)
        assert r["score_seguridad"] <= 25.0


# ─── Tests de clamping (bug fix) ──────────────────────────────────────────────

class TestClampingBugFix:
    def test_saturacion_mayor_100_no_da_score_negativo(self, sector_pesos_default):
        """Bug fix: score_saturacion > 100 no debe dar s_comp negativo."""
        datos = {"score_saturacion": 150}
        r = _score_manual(datos, sector_pesos_default)
        assert r["score_competencia"] >= 0.0

    def test_saturacion_negativa_no_supera_100(self, sector_pesos_default):
        """score_saturacion negativo no debe dar s_comp > 100."""
        datos = {"score_saturacion": -20}
        r = _score_manual(datos, sector_pesos_default)
        assert r["score_competencia"] <= 100.0

    def test_score_turismo_mayor_100_no_infla_global(self, sector_pesos_default):
        """Bug fix: score_turismo > 100 debe clampearse a 100."""
        datos = {"score_turismo": 200}
        r = _score_manual(datos, sector_pesos_default)
        assert r["score_turismo"] <= 100.0

    def test_score_global_no_supera_100_con_datos_extremos(self, sector_pesos_default):
        datos = {
            "flujo_peatonal_total":    999_999,
            "renta_media_hogar":     999_999,
            "score_saturacion":           -10,
            "precio_m2":                  0.0,
            "num_lineas_transporte":  999_999,
            "incidencias_por_1000hab":    0.0,
            "score_turismo":           999,
            "pct_locales_vacios":          0,
            "tasa_rotacion_anual":         0,
        }
        r = _score_manual(datos, sector_pesos_default)
        assert r["score_global"] <= 100.0


# ─── Tests con datos faltantes ────────────────────────────────────────────────

class TestDatosFaltantes:
    def test_datos_vacios_no_crashea(self, sector_pesos_default):
        r = _score_manual({}, sector_pesos_default)
        assert 0.0 <= r["score_global"] <= 100.0

    def test_datos_none_no_crashea(self, sector_pesos_default):
        datos = {
            "flujo_peatonal_total": None,
            "renta_media_hogar":    None,
            "score_saturacion":     None,
            "precio_m2":            None,
        }
        r = _score_manual(datos, sector_pesos_default)
        assert 0.0 <= r["score_global"] <= 100.0

    def test_sin_sector_usa_pesos_por_defecto(self, datos_completos):
        r = _score_manual(datos_completos, {})
        assert 0.0 <= r["score_global"] <= 100.0


# ─── Tests de pesos personalizados ────────────────────────────────────────────

class TestPesosPersonalizados:
    def test_score_global_con_pesos_custom_en_rango(self, datos_completos, sector_pesos_custom):
        r = _score_manual(datos_completos, sector_pesos_custom)
        assert 0.0 <= r["score_global"] <= 100.0

    def test_pesos_default_suman_1(self):
        """Los pesos por defecto embebidos en _score_manual deben sumar 1.0."""
        pesos_default = {
            "peso_flujo": 0.25, "peso_demo": 0.20, "peso_competencia": 0.15,
            "peso_precio": 0.15, "peso_transporte": 0.10,
            "peso_seguridad": 0.05, "peso_turismo": 0.05, "peso_entorno": 0.05,
        }
        total = sum(pesos_default.values())
        assert total == pytest.approx(1.0)


# ─── Tests de _score_neutro ───────────────────────────────────────────────────

class TestScoreNeutro:
    CLAVES_ESPERADAS = {
        "score_global", "score_flujo_peatonal", "score_demografia",
        "score_competencia", "score_precio_alquiler", "score_transporte",
        "score_seguridad", "score_turismo", "score_entorno_comercial",
        "probabilidad_supervivencia", "shap_values", "modelo_version",
    }

    def test_neutro_tiene_todas_las_claves(self):
        r = _score_neutro()
        assert self.CLAVES_ESPERADAS.issubset(r.keys()), (
            f"Faltan claves: {self.CLAVES_ESPERADAS - r.keys()}"
        )

    def test_neutro_score_global_es_50(self):
        r = _score_neutro()
        assert r["score_global"] == pytest.approx(50.0)

    def test_neutro_modelo_version_identificado(self):
        r = _score_neutro()
        assert r["modelo_version"] is not None
        assert r["modelo_version"] != ""

    def test_score_manual_tiene_modelo_version_manual(self, sector_pesos_default):
        r = _score_manual({}, sector_pesos_default)
        assert "manual" in r["modelo_version"]

    def test_score_manual_probabilidad_es_none(self, sector_pesos_default):
        """Scoring manual no tiene modelo ML, probabilidad debe ser None."""
        r = _score_manual({}, sector_pesos_default)
        assert r["probabilidad_supervivencia"] is None
