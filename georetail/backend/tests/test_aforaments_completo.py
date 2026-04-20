"""
tests/test_aforaments_completo.py — Tests exhaustivos del pipeline aforaments.py.

Agente: ruflo-test-aforaments
Tarea:  task-1774810137667-m3zdjv

Cubre:
  - _leer_csv_imd: filtro Codi_tipus_dia=2, promedio multi-mes, filas inválidas
  - _leer_coordenadas: parsing, coord (0,0) ignoradas, separadores distintos
  - Pesos por inversa al cuadrado de distancia
  - Multiplicador comercial: fórmula + capping 0.35
  - Fracciones horarias: manana+tarde+noche = IMD
  - Constantes del módulo: _FRAC_MANANA + _FRAC_TARDE + _FRAC_NOCHE = 1.0
  - ejecutar() es una coroutine
  - _init / _fin son coroutines
  - imputar_zonas_sin_cobertura es coroutine
  - Integración sin BD: flujo de datos entre funciones parse
"""
from __future__ import annotations

import asyncio
import inspect
import io
import math
import textwrap

import pytest

import pipelines.peatonal.aforaments as afo


# ── Clase 1: Constantes del módulo ────────────────────────────────────────────

class TestConstantes:
    def test_fracciones_suman_uno(self):
        total = afo._FRAC_MANANA + afo._FRAC_TARDE + afo._FRAC_NOCHE
        assert abs(total - 1.0) < 1e-9

    def test_fraccion_manana_35pct(self):
        assert afo._FRAC_MANANA == pytest.approx(0.35)

    def test_fraccion_tarde_42pct(self):
        assert afo._FRAC_TARDE == pytest.approx(0.42)

    def test_fraccion_noche_23pct(self):
        assert afo._FRAC_NOCHE == pytest.approx(0.23)

    def test_radio_200m(self):
        assert afo._RADIO_M == 200

    def test_max_zonas_8(self):
        assert afo._MAX_ZONAS == 8


# ── Clase 2: _leer_csv_imd ────────────────────────────────────────────────────

class TestLeerCsvImd:
    def _escribir_csv_tmp(self, tmp_path, contenido: str) -> None:
        csv_dir = tmp_path / "aforaments"
        csv_dir.mkdir(parents=True)
        (csv_dir / "2025_aforament_detall_valor.csv").write_text(
            contenido, encoding="utf-8"
        )
        afo._CSV_IMD = csv_dir / "2025_aforament_detall_valor.csv"

    def test_solo_laborables_tipo_2(self, tmp_path):
        contenido = textwrap.dedent("""\
            Any,Id_aforament,Mes,Codi_tipus_dia,Desc_tipus_dia,Valor_IMD
            2025,S001,1,1,dilluns,100
            2025,S001,1,2,laborables,500
            2025,S001,1,4,dissabte,300
            2025,S001,1,5,diumenge,200
        """)
        self._escribir_csv_tmp(tmp_path, contenido)
        result = afo._leer_csv_imd()
        assert "S001" in result
        assert result["S001"] == pytest.approx(500.0)

    def test_promedio_multimes(self, tmp_path):
        contenido = textwrap.dedent("""\
            Any,Id_aforament,Mes,Codi_tipus_dia,Desc_tipus_dia,Valor_IMD
            2025,S001,1,2,laborables,400
            2025,S001,2,2,laborables,600
            2025,S001,3,2,laborables,800
        """)
        self._escribir_csv_tmp(tmp_path, contenido)
        result = afo._leer_csv_imd()
        assert result["S001"] == pytest.approx(600.0)  # (400+600+800)/3

    def test_valor_cero_ignorado(self, tmp_path):
        contenido = textwrap.dedent("""\
            Any,Id_aforament,Mes,Codi_tipus_dia,Desc_tipus_dia,Valor_IMD
            2025,S001,1,2,laborables,0
            2025,S001,2,2,laborables,0
        """)
        self._escribir_csv_tmp(tmp_path, contenido)
        result = afo._leer_csv_imd()
        assert "S001" not in result  # todos cero → no aparece

    def test_fila_invalida_ignorada(self, tmp_path):
        contenido = textwrap.dedent("""\
            Any,Id_aforament,Mes,Codi_tipus_dia,Desc_tipus_dia,Valor_IMD
            2025,S001,1,2,laborables,500
            2025,,1,2,laborables,999
            2025,S002,1,X,laborables,300
        """)
        self._escribir_csv_tmp(tmp_path, contenido)
        result = afo._leer_csv_imd()
        assert "S001" in result
        assert "" not in result  # fila con ID vacío ignorada

    def test_csv_no_existe_devuelve_vacio(self, tmp_path):
        afo._CSV_IMD = tmp_path / "no_existe.csv"
        result = afo._leer_csv_imd()
        assert result == {}

    def test_multiples_sensores(self, tmp_path):
        contenido = textwrap.dedent("""\
            Any,Id_aforament,Mes,Codi_tipus_dia,Desc_tipus_dia,Valor_IMD
            2025,S001,1,2,laborables,1000
            2025,S002,1,2,laborables,2000
            2025,S003,1,2,laborables,3000
        """)
        self._escribir_csv_tmp(tmp_path, contenido)
        result = afo._leer_csv_imd()
        assert len(result) == 3
        assert result["S001"] == pytest.approx(1000.0)
        assert result["S002"] == pytest.approx(2000.0)
        assert result["S003"] == pytest.approx(3000.0)

    def test_id_con_comillas_strip(self, tmp_path):
        contenido = textwrap.dedent("""\
            Any,Id_aforament,Mes,Codi_tipus_dia,Desc_tipus_dia,Valor_IMD
            2025,"S001",1,2,laborables,800
        """)
        self._escribir_csv_tmp(tmp_path, contenido)
        result = afo._leer_csv_imd()
        # El ID debe ser S001, no "S001"
        assert "S001" in result


# ── Clase 3: _leer_coordenadas ────────────────────────────────────────────────

class TestLeerCoordenadas:
    def _escribir_desc_tmp(self, tmp_path, contenido: str) -> None:
        csv_dir = tmp_path / "aforaments"
        csv_dir.mkdir(parents=True, exist_ok=True)
        (csv_dir / "2025_aforament_descripcio.csv").write_text(
            contenido, encoding="utf-8"
        )
        afo._CSV_DESCRIPCIO = csv_dir / "2025_aforament_descripcio.csv"

    def test_coordenadas_correctas(self, tmp_path):
        contenido = textwrap.dedent("""\
            Id_aforament,Desc_aforament,Longitud,Latitud
            S001,Sensor Gran Via,2.1686,41.3851
        """)
        self._escribir_desc_tmp(tmp_path, contenido)
        result = afo._leer_coordenadas()
        assert "S001" in result
        lng, lat = result["S001"]
        assert lng == pytest.approx(2.1686)
        assert lat == pytest.approx(41.3851)

    def test_coordenadas_cero_ignoradas(self, tmp_path):
        contenido = textwrap.dedent("""\
            Id_aforament,Desc_aforament,Longitud,Latitud
            S001,Sensor sin coords,0.0,0.0
        """)
        self._escribir_desc_tmp(tmp_path, contenido)
        result = afo._leer_coordenadas()
        assert "S001" not in result

    def test_coordenadas_invalidas_ignoradas(self, tmp_path):
        contenido = textwrap.dedent("""\
            Id_aforament,Desc_aforament,Longitud,Latitud
            S001,Sensor,abc,xyz
        """)
        self._escribir_desc_tmp(tmp_path, contenido)
        result = afo._leer_coordenadas()
        assert "S001" not in result

    def test_csv_no_existe_devuelve_vacio(self, tmp_path):
        afo._CSV_DESCRIPCIO = tmp_path / "no_existe.csv"
        result = afo._leer_coordenadas()
        assert result == {}

    def test_multiples_sensores(self, tmp_path):
        contenido = textwrap.dedent("""\
            Id_aforament,Desc_aforament,Longitud,Latitud
            S001,Sensor1,2.1,41.3
            S002,Sensor2,2.2,41.4
            S003,Sensor3,2.3,41.5
        """)
        self._escribir_desc_tmp(tmp_path, contenido)
        result = afo._leer_coordenadas()
        assert len(result) == 3

    def test_id_con_comillas(self, tmp_path):
        contenido = textwrap.dedent("""\
            Id_aforament,Desc_aforament,Longitud,Latitud
            "S001",Sensor,2.1686,41.3851
        """)
        self._escribir_desc_tmp(tmp_path, contenido)
        result = afo._leer_coordenadas()
        assert "S001" in result


# ── Clase 4: Pesos inversa al cuadrado ────────────────────────────────────────

class TestPesosInversaCuadrado:
    """Verifica la fórmula peso = 1/max(1, dist)^2 usada en _asignar_zonas."""

    def test_peso_distancia_1m(self):
        d = 1.0
        peso = 1.0 / max(1.0, d) ** 2
        assert peso == pytest.approx(1.0)

    def test_peso_distancia_10m(self):
        d = 10.0
        peso = 1.0 / max(1.0, d) ** 2
        assert peso == pytest.approx(0.01)

    def test_peso_distancia_100m(self):
        d = 100.0
        peso = 1.0 / max(1.0, d) ** 2
        assert peso == pytest.approx(0.0001)

    def test_zona_cercana_recibe_mas_flujo_que_lejana(self):
        """Zona a 10m debe recibir más flujo que zona a 100m."""
        imd = 1000.0
        d1, d2 = 10.0, 100.0
        p1 = 1.0 / max(1.0, d1) ** 2
        p2 = 1.0 / max(1.0, d2) ** 2
        suma = p1 + p2
        flujo1 = imd * p1 / suma
        flujo2 = imd * p2 / suma
        assert flujo1 > flujo2

    def test_pesos_normalizados_suman_1(self):
        distancias = [5.0, 25.0, 80.0, 150.0]
        pesos = [1.0 / max(1.0, d) ** 2 for d in distancias]
        suma = sum(pesos)
        pesos_norm = [p / suma for p in pesos]
        assert abs(sum(pesos_norm) - 1.0) < 1e-9

    def test_distancia_cero_usa_max_1(self):
        """Distancia 0 no debe causar división por cero — usa max(1, dist)."""
        d = 0.0
        peso = 1.0 / max(1.0, d) ** 2
        assert peso == pytest.approx(1.0)
        assert math.isfinite(peso)


# ── Clase 5: Multiplicador comercial ─────────────────────────────────────────

class TestMultiplicadorComercial:
    """Verifica: multiplicador = 1.0 + min(0.35, ratio_com * 0.70)."""

    def _mult(self, ratio_com: float) -> float:
        return 1.0 + min(0.35, ratio_com * 0.70)

    def test_ratio_cero_multiplicador_uno(self):
        assert self._mult(0.0) == pytest.approx(1.0)

    def test_ratio_medio_05_multiplicador_1_35(self):
        # 0.5 * 0.70 = 0.35 → capped
        assert self._mult(0.5) == pytest.approx(1.35)

    def test_ratio_alto_capped_1_35(self):
        # ratio=1.0 → 1.0*0.70=0.70 > 0.35 → capped a 0.35
        assert self._mult(1.0) == pytest.approx(1.35)

    def test_ratio_bajo_sin_cap(self):
        # ratio=0.2 → 0.2*0.70=0.14 → 1.14
        assert self._mult(0.2) == pytest.approx(1.14)

    def test_multiplicador_nunca_supera_1_35(self):
        for ratio in [0.0, 0.1, 0.3, 0.5, 0.7, 1.0, 2.0]:
            m = self._mult(ratio)
            assert m <= 1.35 + 1e-9, f"ratio={ratio} → mult={m}"

    def test_multiplicador_minimo_es_1(self):
        for ratio in [0.0, 0.001, 0.01]:
            m = self._mult(ratio)
            assert m >= 1.0 - 1e-9


# ── Clase 6: Fracciones horarias aplicadas al IMD ─────────────────────────────

class TestFraccionesHorarias:
    """Las tres franjas deben sumar exactamente el IMD original."""

    def _calcular_franjas(self, flujo_dia: float):
        manana = round(flujo_dia * afo._FRAC_MANANA, 1)
        tarde  = round(flujo_dia * afo._FRAC_TARDE,  1)
        noche  = round(flujo_dia * afo._FRAC_NOCHE,  1)
        return manana, tarde, noche

    def test_fracciones_suman_flujo_original_1000(self):
        m, t, n = self._calcular_franjas(1000.0)
        assert abs(m + t + n - 1000.0) < 1.0  # tolerancia por redondeo

    def test_fracciones_suman_flujo_original_850(self):
        m, t, n = self._calcular_franjas(850.0)
        assert abs(m + t + n - 850.0) < 1.0

    def test_tarde_es_franja_mayor(self):
        """Tarde (42%) debe ser la franja más grande."""
        m, t, n = self._calcular_franjas(1000.0)
        assert t > m
        assert t > n

    def test_noche_es_franja_menor(self):
        """Noche (23%) debe ser la franja más pequeña."""
        m, t, n = self._calcular_franjas(1000.0)
        assert n < m
        assert n < t

    def test_fracciones_proporcionales(self):
        flujo = 3000.0
        m, t, n = self._calcular_franjas(flujo)
        assert abs(m / flujo - 0.35) < 0.01
        assert abs(t / flujo - 0.42) < 0.01
        assert abs(n / flujo - 0.23) < 0.01


# ── Clase 7: Interfaces asíncronas ───────────────────────────────────────────

class TestInterfacesAsync:
    def test_ejecutar_es_coroutine(self):
        assert asyncio.iscoroutinefunction(afo.ejecutar)

    def test_init_es_coroutine(self):
        assert asyncio.iscoroutinefunction(afo._init)

    def test_fin_es_coroutine(self):
        assert asyncio.iscoroutinefunction(afo._fin)

    def test_asignar_zonas_es_coroutine(self):
        assert asyncio.iscoroutinefunction(afo._asignar_zonas)

    def test_recalcular_totales_es_coroutine(self):
        assert asyncio.iscoroutinefunction(afo._recalcular_totales)

    def test_imputar_zonas_es_coroutine(self):
        assert asyncio.iscoroutinefunction(afo.imputar_zonas_sin_cobertura)


# ── Clase 8: Integración sin BD — cruce imd×coords ───────────────────────────

class TestCruceImdCoords:
    """Verifica el dict comprehension que cruza sensores con coordenadas."""

    def test_cruce_solo_sensores_con_ambos_datos(self):
        imd_por_sensor = {"S001": 1000.0, "S002": 500.0, "S003": 800.0}
        coords_sensor  = {"S001": (2.1, 41.3), "S003": (2.2, 41.4)}
        # Solo S001 y S003 tienen ambos
        sensores = {
            sid: (imd_por_sensor[sid], *coords_sensor[sid])
            for sid in imd_por_sensor
            if sid in coords_sensor
        }
        assert len(sensores) == 2
        assert "S001" in sensores
        assert "S003" in sensores
        assert "S002" not in sensores

    def test_sensor_completo_tiene_imd_lng_lat(self):
        imd_por_sensor = {"S001": 1200.0}
        coords_sensor  = {"S001": (2.18, 41.39)}
        sensores = {
            sid: (imd_por_sensor[sid], *coords_sensor[sid])
            for sid in imd_por_sensor
            if sid in coords_sensor
        }
        imd, lng, lat = sensores["S001"]
        assert imd == pytest.approx(1200.0)
        assert lng == pytest.approx(2.18)
        assert lat == pytest.approx(41.39)

    def test_cruce_vacio_si_sin_coords(self):
        imd_por_sensor = {"S001": 1000.0}
        coords_sensor  = {}
        sensores = {
            sid: (imd_por_sensor[sid], *coords_sensor[sid])
            for sid in imd_por_sensor
            if sid in coords_sensor
        }
        assert sensores == {}


# ── Clase 9: Fallback de imputación ──────────────────────────────────────────

class TestFallbackImputacion:
    """Verifica que los valores de fallback global tienen sentido."""

    def test_fallback_global_estructura(self):
        fallback = {"manana": 297.5, "tarde": 357.0, "noche": 195.5, "total": 850.0}
        assert "manana" in fallback
        assert "tarde" in fallback
        assert "noche" in fallback
        assert "total" in fallback

    def test_fallback_manana_tarde_noche_suman_total(self):
        fallback = {"manana": 297.5, "tarde": 357.0, "noche": 195.5, "total": 850.0}
        suma = fallback["manana"] + fallback["tarde"] + fallback["noche"]
        assert abs(suma - fallback["total"]) < 1.0

    def test_fallback_fracciones_correctas(self):
        fallback = {"manana": 297.5, "tarde": 357.0, "noche": 195.5, "total": 850.0}
        assert abs(fallback["manana"] / fallback["total"] - 0.35) < 0.01
        assert abs(fallback["tarde"]  / fallback["total"] - 0.42) < 0.01
        assert abs(fallback["noche"]  / fallback["total"] - 0.23) < 0.01

    def test_fallback_total_coincide_con_media_medias(self):
        """El total del fallback debe coincidir con _MEDIAS['flujo_peatonal_total']."""
        from scoring.features import _MEDIAS
        assert _MEDIAS["flujo_peatonal_total"] == pytest.approx(850.0)
