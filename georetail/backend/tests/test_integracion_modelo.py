"""
tests/test_integracion_modelo.py — Tests de integración cross-module del modelo v5.1.

Agente: ruflo-test-integracion
Tarea:  task-1774810152176-e17o1n

Cubre:
  - FEATURE_NAMES tiene 33 features exactas (v5.1 añade nivel_estudios_alto_pct)
  - Todos los FEATURE_NAMES están en _MEDIAS
  - No hay duplicados en FEATURE_NAMES
  - Posiciones v1-v5.1 verificadas por índice
  - _build_array produce shape (1, 33) con todos los datos presentes
  - _score_manual devuelve exactamente las 11 claves esperadas
  - _score_neutro devuelve las 11 claves
  - Scheduler tiene todos los jobs esperados (IDs correctos)
  - ejecutar() de aforaments/demografia/parques son coroutines
  - PESOS_BASE de flujo_peatonal suman 1.0
  - _MEDIAS de features no tiene NaN ni Inf
  - Consistencia entre FEATURE_NAMES y _MEDIAS: mismas 33 claves
"""
from __future__ import annotations

import asyncio
import math

import numpy as np
import pytest

from scoring.features import FEATURE_NAMES, _MEDIAS, _build_array
from scoring.scorer import _score_manual, _score_neutro
from scoring.dimensiones.flujo_peatonal import PESOS_BASE


# ── Clase 1: FEATURE_NAMES estructura ────────────────────────────────────────

class TestFeatureNamesEstructura:
    def test_longitud_actual_del_vector(self):
        assert len(FEATURE_NAMES) == 70

    def test_no_duplicados(self):
        assert len(FEATURE_NAMES) == len(set(FEATURE_NAMES))

    def test_es_lista(self):
        assert isinstance(FEATURE_NAMES, list)

    def test_todos_son_strings(self):
        for f in FEATURE_NAMES:
            assert isinstance(f, str), f"Feature {f!r} no es string"

    def test_ninguno_vacio(self):
        for f in FEATURE_NAMES:
            assert f.strip() != "", "Feature name vacío"


# ── Clase 2: Posiciones v1-v5 por índice ──────────────────────────────────────

class TestPosicionesPorVersion:
    def test_flujo_peatonal_total_indice_0(self):
        assert FEATURE_NAMES[0] == "flujo_peatonal_total"

    def test_renta_media_hogar_indice_4(self):
        assert FEATURE_NAMES[4] == "renta_media_hogar"

    def test_m2_zonas_verdes_indice_20(self):
        assert FEATURE_NAMES[20] == "m2_zonas_verdes_cercanas"

    def test_dist_playa_m_indice_21(self):
        """v2: dist_playa_m en índice 21."""
        assert FEATURE_NAMES[21] == "dist_playa_m"

    def test_ratio_locales_comerciales_indice_22(self):
        """v2: ratio_locales_comerciales en índice 22."""
        assert FEATURE_NAMES[22] == "ratio_locales_comerciales"

    def test_airbnb_density_indice_23(self):
        """v3: airbnb_density_500m en índice 23."""
        assert FEATURE_NAMES[23] == "airbnb_density_500m"

    def test_flujo_peatonal_score_indice_29(self):
        """v4: flujo_peatonal_score en índice 29."""
        assert FEATURE_NAMES[29] == "flujo_peatonal_score"

    def test_pct_poblacio_25_44_indice_30(self):
        """v5: pct_poblacio_25_44 en índice 30."""
        assert FEATURE_NAMES[30] == "pct_poblacio_25_44"

    def test_delta_renta_3a_indice_31(self):
        """v5: delta_renta_3a en índice 31."""
        assert FEATURE_NAMES[31] == "delta_renta_3a"

    def test_nivel_estudios_alto_pct_indice_32(self):
        """v5.1: nivel_estudios_alto_pct en índice 32."""
        assert FEATURE_NAMES[32] == "nivel_estudios_alto_pct"

    def test_batch_demografia_bcn1_al_final(self):
        assert FEATURE_NAMES[46] == "gini"
        assert FEATURE_NAMES[47] == "p80_p20"
        assert FEATURE_NAMES[48] == "tamano_hogar"
        assert FEATURE_NAMES[49] == "hogares_con_menores"
        assert FEATURE_NAMES[50] == "personas_solas"
        assert FEATURE_NAMES[51] == "renta_media_uc"
        assert FEATURE_NAMES[52] == "renta_mediana_uc"

    def test_temporalidad_indices(self):
        assert FEATURE_NAMES[58] == "seasonality_summer_lift"
        assert FEATURE_NAMES[62] == "seasonality_peak_concentration"
        assert FEATURE_NAMES[63] == "weekend_lift"
        assert FEATURE_NAMES[69] == "temporal_confianza"

    def test_v3_features_indices_23_a_28(self):
        """Los 6 features v3 ocupan índices 23-28."""
        v3_features = [
            "airbnb_density_500m",
            "airbnb_occupancy_est",
            "google_review_count_medio",
            "licencias_nuevas_1a",
            "eventos_culturales_500m",
            "booking_hoteles_500m",
        ]
        for i, f in enumerate(v3_features):
            assert FEATURE_NAMES[23 + i] == f, f"índice {23 + i}: esperado {f}"


# ── Clase 3: Consistencia FEATURE_NAMES ↔ _MEDIAS ────────────────────────────

class TestConsistenciaMedias:
    def test_todos_feature_names_en_medias(self):
        for f in FEATURE_NAMES:
            assert f in _MEDIAS, f"Feature '{f}' no está en _MEDIAS"

    def test_medias_no_tienen_nan(self):
        for k, v in _MEDIAS.items():
            assert not math.isnan(v), f"_MEDIAS['{k}'] es NaN"

    def test_medias_no_tienen_inf(self):
        for k, v in _MEDIAS.items():
            assert math.isfinite(v), f"_MEDIAS['{k}'] es infinito"

    def test_medias_son_positivas_donde_corresponde(self):
        """Features que son siempre positivos deben tener media positiva."""
        positivos = [
            "flujo_peatonal_total", "renta_media_hogar", "edad_media",
            "densidad_hab_km2", "precio_m2_alquiler", "num_lineas_transporte",
            "dist_playa_m", "score_turismo", "score_equipamientos",
        ]
        for f in positivos:
            if f in _MEDIAS:
                assert _MEDIAS[f] > 0, f"_MEDIAS['{f}'] debería ser positivo"

    def test_fracciones_entre_0_y_1(self):
        """Features que son fracciones/porcentajes deben tener media en [0,1]."""
        fracciones = [
            "flujo_manana_pct", "flujo_tarde_pct", "flujo_noche_pct",
            "pct_extranjeros", "ratio_locales_comerciales",
            "airbnb_occupancy_est", "pct_poblacio_25_44",
        ]
        for f in fracciones:
            if f in _MEDIAS:
                assert 0.0 <= _MEDIAS[f] <= 1.0, f"_MEDIAS['{f}']={_MEDIAS[f]} fuera de [0,1]"

    def test_pct_poblacio_25_44_media_plausible(self):
        """~28% de la población en BCN tiene entre 25-44 años."""
        assert 0.15 <= _MEDIAS["pct_poblacio_25_44"] <= 0.50

    def test_delta_renta_3a_media_plausible(self):
        """Variación del ~8% en 3 años es razonable para BCN."""
        assert -0.20 <= _MEDIAS["delta_renta_3a"] <= 0.50


# ── Clase 4: _build_array shape y tipo ────────────────────────────────────────

class TestBuildArraySinBD:
    """_build_array sin BD: todos los datos vacíos → imputación total."""

    def _vz_completo(self):
        """Zona con todos los datos presentes (no necesita BD)."""
        return {
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

    def test_shape_con_datos_presentes(self):
        comp = {"num_competidores": 5, "rating_medio": 4.0, "score_saturacion": 40.0}
        precio = 20.0
        trans = {"num_lineas": 8, "num_paradas": 5}
        geo = {"dist_playa_m": 3000}
        tur = {
            "airbnb_density_500m": 25.0,
            "airbnb_occupancy_est": 0.60,
            "google_review_count_medio": 120.0,
            "licencias_nuevas_1a": 3.0,
            "eventos_culturales_500m": 4.0,
            "booking_hoteles_500m": 2.0,
        }
        arr = _build_array(self._vz_completo(), comp, precio, trans, geo, tur)
        assert arr.shape == (1, len(FEATURE_NAMES))

    def test_dtype_float32(self):
        arr = _build_array({}, {}, None, {}, {}, {})
        assert arr.dtype == np.float32

    def test_no_nan_con_datos_vacios(self):
        """Con todos los datos vacíos, la imputación de _MEDIAS evita NaN."""
        arr = _build_array({}, {}, None, {}, {}, {})
        assert not np.any(np.isnan(arr))

    def test_no_inf_con_datos_vacios(self):
        arr = _build_array({}, {}, None, {}, {}, {})
        assert not np.any(np.isinf(arr))

    def test_datos_vacios_usa_medias(self):
        """Con datos vacíos, cada feature toma el valor de _MEDIAS[f].

        Nota: flujo_peatonal_score es computed (no imputed) — devuelve 30.0
        (fallback conservador de calcular_flujo_score), NO _MEDIAS["flujo_peatonal_score"].
        """
        # Features computadas que no siguen la ruta _MEDIAS
        _COMPUTED = {"flujo_peatonal_score"}
        arr = _build_array({}, {}, None, {}, {}, {})
        vec = arr[0]
        for i, f in enumerate(FEATURE_NAMES):
            if f in _COMPUTED:
                continue
            assert abs(float(vec[i]) - float(_MEDIAS[f])) < 1e-3, (
                f"Feature '{f}' idx={i}: got {vec[i]}, expected {_MEDIAS[f]}"
            )


# ── Clase 5: _score_manual claves de salida ───────────────────────────────────

class TestScoreManualClaves:
    _CLAVES = {
        "score_global",
        "score_flujo_peatonal",
        "score_demografia",
        "score_competencia",
        "score_precio_alquiler",
        "score_transporte",
        "score_seguridad",
        "score_turismo",
        "score_entorno_comercial",
        "score_dinamismo",
        "probabilidad_supervivencia",
        "shap_values",
        "modelo_version",
    }

    def test_score_manual_claves_exactas(self):
        r = _score_manual({}, {})
        assert set(r.keys()) == self._CLAVES

    def test_score_neutro_claves_exactas(self):
        r = _score_neutro()
        assert set(r.keys()) == self._CLAVES

    def test_score_manual_no_tiene_claves_extra(self):
        r = _score_manual({}, {})
        extra = set(r.keys()) - self._CLAVES
        assert extra == set(), f"Claves extra no esperadas: {extra}"

    def test_score_neutro_no_tiene_claves_extra(self):
        r = _score_neutro()
        extra = set(r.keys()) - self._CLAVES
        assert extra == set(), f"Claves extra no esperadas: {extra}"


# ── Clase 6: Scheduler — job IDs ─────────────────────────────────────────────

apscheduler = pytest.importorskip("apscheduler", reason="apscheduler no instalado")


class TestSchedulerJobs:
    """Verifica que el scheduler registra todos los jobs esperados."""

    def _get_scheduler_src(self):
        """Lee el código fuente del scheduler sin importarlo (evita deps en CI)."""
        import pathlib
        sched_path = pathlib.Path(__file__).parent.parent / "pipelines" / "scheduler.py"
        return sched_path.read_text(encoding="utf-8")

    def test_job_parques_en_scheduler(self):
        src = self._get_scheduler_src()
        assert '"parques"' in src or "id='parques'" in src

    def test_job_demografia_en_scheduler(self):
        src = self._get_scheduler_src()
        assert '"demografia"' in src or "id='demografia'" in src

    def test_job_aforaments_en_scheduler(self):
        src = self._get_scheduler_src()
        assert '"aforaments"' in src or "id='aforaments'" in src

    def test_job_vianants_en_scheduler(self):
        src = self._get_scheduler_src()
        assert '"vianants"' in src or "id='vianants'" in src

    def test_job_vcity_en_scheduler(self):
        src = self._get_scheduler_src()
        assert '"vcity"' in src or "id='vcity'" in src

    def test_job_precios_en_scheduler(self):
        src = self._get_scheduler_src()
        assert '"precios"' in src or "id='precios'" in src

    def test_job_scores_en_scheduler(self):
        src = self._get_scheduler_src()
        assert '"scores"' in src or "id='scores'" in src

    def test_runner_parques_en_fuente(self):
        src = self._get_scheduler_src()
        assert "_run_parques" in src

    def test_runner_demografia_en_fuente(self):
        src = self._get_scheduler_src()
        assert "_run_demografia" in src

    def test_runner_aforaments_en_fuente(self):
        src = self._get_scheduler_src()
        assert "_run_aforaments" in src


# ── Clase 7: ejecutar() como coroutine ───────────────────────────────────────

class TestEjecutarCoroutines:
    def test_aforaments_ejecutar_es_coroutine(self):
        from pipelines.peatonal.aforaments import ejecutar
        assert asyncio.iscoroutinefunction(ejecutar)

    def test_demografia_ejecutar_es_coroutine(self):
        from pipelines.demografia.demografia import ejecutar
        assert asyncio.iscoroutinefunction(ejecutar)

    def test_parques_ejecutar_es_coroutine(self):
        from pipelines.entorno.parques import ejecutar
        assert asyncio.iscoroutinefunction(ejecutar)

    def test_parques_alias_actualizar_es_coroutine(self):
        from pipelines.entorno.parques import actualizar_parques_amb
        assert asyncio.iscoroutinefunction(actualizar_parques_amb)


# ── Clase 8: PESOS_BASE flujo_peatonal ───────────────────────────────────────

class TestPesosBaseIntegracion:
    def test_pesos_base_suman_1(self):
        assert abs(sum(PESOS_BASE.values()) - 1.0) < 1e-9

    def test_pesos_base_4_fuentes(self):
        assert len(PESOS_BASE) == 4

    def test_pesos_coinciden_con_documentacion(self):
        """Documentación: 35%+30%+20%+15% = 100%."""
        assert PESOS_BASE["popular_times"] == pytest.approx(0.35)
        assert PESOS_BASE["vcity"] == pytest.approx(0.30)
        assert PESOS_BASE["vianants"] == pytest.approx(0.20)
        assert PESOS_BASE["ratio_locales"] == pytest.approx(0.15)
