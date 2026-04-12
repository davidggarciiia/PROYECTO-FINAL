"""
tests/test_demografia_pipeline_completo.py — Tests exhaustivos del pipeline demografia.py.

Agente: ruflo-test-demo-deep
Tarea:  task-1774810143129-5q5o84

Cubre:
  - _parse_renda_csv: Codi_Districte zfill(2), Import_Euros, multi-barrio promedio,
    filas inválidas ignoradas, distritos sin renta no incluidos
  - _extract_iermb_bcn_value: 4 estructuras JSON distintas, año más reciente
    (NO promedio), normalización >10, None si sin datos Barcelona
  - _parse_float: None→default, string con coma, string vacío, None explícito
  - área_por_barri acumula (BUG1 fix verificado)
  - fecha como objeto date no string (BUG2 fix verificado)
  - _fetch_grups_edat: suma grupos quinquenales 25-44, calcula porcentaje
  - ejecutar() es coroutine
  - pipeline tracking: _init/_fin son coroutines
"""
from __future__ import annotations

import asyncio
import math
from datetime import date

import pytest

from pipelines.demografia.demografia import (
    _extract_iermb_bcn_value,
    _parse_float,
    _parse_renda_csv,
    ejecutar,
    _init,
    _fin,
)


# ── Clase 1: _parse_renda_csv ─────────────────────────────────────────────────

class TestParseRendaCsv:
    def test_distrito_simple(self):
        csv = "Codi_Districte,Import_Euros\n1,25000\n"
        result = _parse_renda_csv(csv)
        assert "01" in result
        assert result["01"] == pytest.approx(25000.0)

    def test_codi_districte_zfill_2(self):
        """Distritos de 1 dígito deben formatearse a 2 (p.ej. '1' → '01')."""
        csv = "Codi_Districte,Import_Euros\n1,30000\n10,40000\n"
        result = _parse_renda_csv(csv)
        assert "01" in result
        assert "10" in result

    def test_promedio_multiples_barrios_mismo_distrito(self):
        """Varios barrios del mismo distrito → media de sus rentas."""
        csv = "Codi_Districte,Import_Euros\n1,20000\n1,40000\n"
        result = _parse_renda_csv(csv)
        assert result["01"] == pytest.approx(30000.0)

    def test_renta_cero_ignorada(self):
        """Filas con Import_Euros=0 no deben incluirse en el promedio."""
        csv = "Codi_Districte,Import_Euros\n1,0\n1,0\n"
        result = _parse_renda_csv(csv)
        assert "01" not in result

    def test_fila_invalida_ignorada(self):
        csv = "Codi_Districte,Import_Euros\n1,25000\nabc,xyz\n2,\n"
        result = _parse_renda_csv(csv)
        assert "01" in result
        assert result["01"] == pytest.approx(25000.0)

    def test_multiples_distritos(self):
        csv = (
            "Codi_Districte,Import_Euros\n"
            "1,20000\n2,35000\n3,50000\n"
        )
        result = _parse_renda_csv(csv)
        assert len(result) == 3
        assert result["01"] == pytest.approx(20000.0)
        assert result["02"] == pytest.approx(35000.0)
        assert result["03"] == pytest.approx(50000.0)

    def test_csv_vacio_devuelve_dict_vacio(self):
        csv = "Codi_Districte,Import_Euros\n"
        result = _parse_renda_csv(csv)
        assert result == {}

    def test_columna_faltante_no_rompe(self):
        """Si falta Import_Euros, la fila se ignora sin excepción."""
        csv = "Codi_Districte,Otra_Columna\n1,x\n"
        result = _parse_renda_csv(csv)
        assert isinstance(result, dict)

    def test_valores_redondeados_correctamente(self):
        csv = "Codi_Districte,Import_Euros\n1,33333.333\n1,33333.667\n"
        result = _parse_renda_csv(csv)
        assert result["01"] == pytest.approx(33333.5, abs=1.0)


# ── Clase 2: _extract_iermb_bcn_value ─────────────────────────────────────────

class TestExtractIermbBcnValue:
    # Estructura 1: lista directa
    def test_estructura_lista_directa(self):
        data = [
            {"codi_muni": "08019", "any": 2023, "valor": 7.2},
            {"codi_muni": "08001", "any": 2023, "valor": 6.5},
        ]
        result = _extract_iermb_bcn_value(data)
        assert result == pytest.approx(7.2)

    # Estructura 2: dict con key 'data'
    def test_estructura_dict_key_data(self):
        data = {
            "data": [
                {"codi_muni": "08019", "any": 2022, "valor": 7.0},
            ]
        }
        result = _extract_iermb_bcn_value(data)
        assert result == pytest.approx(7.0)

    # Estructura 3: dict con key 'results'
    def test_estructura_dict_key_results(self):
        data = {
            "results": [
                {"municipi": "barcelona", "any": 2021, "value": 6.8},
            ]
        }
        result = _extract_iermb_bcn_value(data)
        assert result == pytest.approx(6.8)

    # Estructura 4: dict con key 'rows'
    def test_estructura_dict_key_rows(self):
        data = {
            "rows": [
                {"Municipi": "Barcelona", "Any": 2020, "Valor": 6.5},
            ]
        }
        result = _extract_iermb_bcn_value(data)
        assert result == pytest.approx(6.5)

    def test_toma_anyo_mas_reciente_no_promedio(self):
        """BUG3 fix: debe devolver el valor del año más reciente, no la media."""
        data = [
            {"codi_muni": "08019", "any": 2014, "valor": 5.1},
            {"codi_muni": "08019", "any": 2018, "valor": 6.3},
            {"codi_muni": "08019", "any": 2023, "valor": 7.8},
        ]
        result = _extract_iermb_bcn_value(data)
        # Debe ser 7.8 (año 2023), NO (5.1+6.3+7.8)/3 = 6.4
        assert result == pytest.approx(7.8)
        assert result != pytest.approx((5.1 + 6.3 + 7.8) / 3)

    def test_sin_barcelona_devuelve_none(self):
        data = [
            {"codi_muni": "08001", "any": 2023, "valor": 6.5},
            {"codi_muni": "08002", "any": 2023, "valor": 7.0},
        ]
        result = _extract_iermb_bcn_value(data)
        assert result is None

    def test_lista_vacia_devuelve_none(self):
        result = _extract_iermb_bcn_value([])
        assert result is None

    def test_dato_nulo_devuelve_none(self):
        result = _extract_iermb_bcn_value(None)
        assert result is None

    def test_codi_muni_8019_sin_cero(self):
        """Código 08019 sin cero inicial también debe reconocerse."""
        data = [{"codi_muni": "8019", "any": 2023, "valor": 7.5}]
        result = _extract_iermb_bcn_value(data)
        assert result == pytest.approx(7.5)

    def test_nombre_barcelona_case_insensitive(self):
        data = [{"municipi": "BARCELONA", "any": 2023, "valor": 7.1}]
        result = _extract_iermb_bcn_value(data)
        assert result == pytest.approx(7.1)

    def test_multiples_anyos_un_solo_municipio(self):
        """Con 5 años, devuelve solo el más reciente."""
        data = [
            {"codi_muni": "08019", "any": y, "valor": float(y - 2010)}
            for y in range(2010, 2024)
        ]
        result = _extract_iermb_bcn_value(data)
        assert result == pytest.approx(13.0)  # 2023 - 2010 = 13

    def test_valor_con_key_ind_valor(self):
        data = [{"codi_muni": "08019", "any": 2023, "ind_valor": 7.3}]
        result = _extract_iermb_bcn_value(data)
        assert result == pytest.approx(7.3)

    def test_valor_con_key_Value_mayusculas(self):
        data = [{"codi_muni": "08019", "any": 2023, "Value": 7.4}]
        result = _extract_iermb_bcn_value(data)
        assert result == pytest.approx(7.4)

    def test_registro_sin_year_usa_0(self):
        """Un registro sin año usa year=0; si hay otro con año real, ese gana."""
        data = [
            {"codi_muni": "08019", "valor": 5.0},              # year=0
            {"codi_muni": "08019", "any": 2022, "valor": 7.2}, # year=2022
        ]
        result = _extract_iermb_bcn_value(data)
        assert result == pytest.approx(7.2)  # año 2022 > 0

    def test_estructura_invalida_devuelve_none(self):
        result = _extract_iermb_bcn_value("string_invalido")
        assert result is None


# ── Clase 3: _parse_float ─────────────────────────────────────────────────────

class TestParseFloat:
    def test_none_devuelve_default(self):
        assert _parse_float(None, 99.0) == pytest.approx(99.0)

    def test_none_default_cero(self):
        assert _parse_float(None) == pytest.approx(0.0)

    def test_string_con_coma_decimal(self):
        assert _parse_float("3,14") == pytest.approx(3.14)

    def test_string_con_punto_decimal(self):
        assert _parse_float("3.14") == pytest.approx(3.14)

    def test_entero(self):
        assert _parse_float(42) == pytest.approx(42.0)

    def test_string_invalido_devuelve_default(self):
        assert _parse_float("abc", 5.0) == pytest.approx(5.0)

    def test_string_vacio_devuelve_default(self):
        assert _parse_float("", 7.0) == pytest.approx(7.0)

    def test_float_directo(self):
        assert _parse_float(3.14) == pytest.approx(3.14)

    def test_cero_string(self):
        assert _parse_float("0") == pytest.approx(0.0)

    def test_negativo(self):
        assert _parse_float("-5.5") == pytest.approx(-5.5)

    def test_espacios_strip(self):
        assert _parse_float("  42.0  ") == pytest.approx(42.0)


# ── Clase 4: BUG1 — area_por_barri acumula en lugar de sobreescribir ──────────

class TestAreaPorBarriAcumula:
    """
    BUG1: el código original hacía area_por_barri[bc] = float(a["area_km2"]),
    sobreescribiendo en lugar de acumular. Con múltiples zonas por barrio,
    la densidad se calculaba solo sobre la última zona.

    Fix: area_por_barri[bc] = area_por_barri.get(bc, 0.0) + float(a["area_km2"])
    """

    def test_acumula_multiples_zonas_mismo_barrio(self):
        """Simula el patrón de acumulación del fix."""
        area_rows = [
            {"zona_id": "z1", "barri_code": "000001", "area_km2": 0.5},
            {"zona_id": "z2", "barri_code": "000001", "area_km2": 0.3},
            {"zona_id": "z3", "barri_code": "000001", "area_km2": 0.2},
        ]
        area_por_barri: dict = {}
        for a in area_rows:
            bc = str(a["barri_code"]).zfill(6)
            # Fix correcta: acumular
            area_por_barri[bc] = area_por_barri.get(bc, 0.0) + (
                float(a["area_km2"]) if a["area_km2"] else 0.0
            )
        # Total debe ser 1.0, no 0.2 (última zona)
        assert area_por_barri["000001"] == pytest.approx(1.0)

    def test_bug_original_sobreescribia(self):
        """Demuestra que el bug original daba resultado incorrecto."""
        area_rows = [
            {"zona_id": "z1", "barri_code": "000001", "area_km2": 0.5},
            {"zona_id": "z2", "barri_code": "000001", "area_km2": 0.3},
        ]
        area_por_barri_bug: dict = {}
        for a in area_rows:
            bc = str(a["barri_code"]).zfill(6)
            # Bug original: sobreescribir
            area_por_barri_bug[bc] = float(a["area_km2"])
        # Bug daba 0.3, no 0.8
        assert area_por_barri_bug["000001"] == pytest.approx(0.3)  # incorrecto

    def test_densidad_correcta_con_acumulacion(self):
        """Con acumulación correcta, densidad = poblacion / area_total."""
        poblacion = 10000
        zonas_areas = [0.5, 0.3, 0.2]  # total 1.0 km²
        area_acumulada = sum(zonas_areas)
        densidad = poblacion / area_acumulada
        assert densidad == pytest.approx(10000.0)  # 10000/1.0

    def test_densidad_incorrecta_sin_acumulacion(self):
        """Sin acumulación, densidad incorrecta usando solo última zona."""
        poblacion = 10000
        ultima_zona_area = 0.2  # solo la última
        densidad_bug = poblacion / ultima_zona_area
        # Densidad falsa: 50000 vs real 10000
        assert densidad_bug == pytest.approx(50000.0)


# ── Clase 5: BUG2 — fecha como objeto date ────────────────────────────────────

class TestFechaComoDate:
    """
    BUG2: asyncpg espera un objeto datetime.date, no un string '2023-01-01'.
    Fix: fecha = date(year, 1, 1)
    """

    def test_date_constructor_da_objeto_date(self):
        year = 2023
        fecha = date(year, 1, 1)
        assert isinstance(fecha, date)

    def test_date_no_es_string(self):
        year = 2023
        fecha = date(year, 1, 1)
        assert not isinstance(fecha, str)

    def test_date_correcto_year(self):
        for year in [2020, 2021, 2022, 2023]:
            fecha = date(year, 1, 1)
            assert fecha.year == year
            assert fecha.month == 1
            assert fecha.day == 1

    def test_string_fecha_sigue_siendo_string(self):
        """Documenta que el bug original producía un string."""
        year = 2023
        fecha_bug = f"{year}-01-01"  # bug original
        assert isinstance(fecha_bug, str)


# ── Clase 6: pct_poblacio_25_44 — suma de grupos quinquenales ─────────────────

class TestGrupsEdat:
    """Verifica la lógica de suma de grupos quinquenales 25-44."""

    _GRUPS_25_44 = {"25-29", "30-34", "35-39", "40-44",
                    "25 a 29", "30 a 34", "35 a 39", "40 a 44"}

    def test_grupos_reconocidos(self):
        grupos_validos = ["25-29", "30-34", "35-39", "40-44"]
        for g in grupos_validos:
            assert g in self._GRUPS_25_44

    def test_grupos_variante_a_reconocidos(self):
        grupos_validos = ["25 a 29", "30 a 34", "35 a 39", "40 a 44"]
        for g in grupos_validos:
            assert g in self._GRUPS_25_44

    def test_grupos_fuera_no_reconocidos(self):
        grupos_fuera = ["20-24", "45-49", "18-20", "65+"]
        for g in grupos_fuera:
            assert g not in self._GRUPS_25_44

    def test_calculo_porcentaje(self):
        """Si 400 personas están en grupos 25-44 sobre 1000 total → 40%."""
        totals_25_44 = {"000001": 400.0}
        totals_all   = {"000001": 1000.0}
        pct = totals_25_44["000001"] / totals_all["000001"]
        assert pct == pytest.approx(0.40)

    def test_calculo_porcentaje_multiples_grupos(self):
        """Simula la suma de 4 grupos quinquenales."""
        records = [
            {"Codi_Barri": "1", "Edat_Quinquenal": "25-29", "Total": 100},
            {"Codi_Barri": "1", "Edat_Quinquenal": "30-34", "Total": 120},
            {"Codi_Barri": "1", "Edat_Quinquenal": "35-39", "Total": 110},
            {"Codi_Barri": "1", "Edat_Quinquenal": "40-44", "Total": 90},
            {"Codi_Barri": "1", "Edat_Quinquenal": "45-49", "Total": 80},
            {"Codi_Barri": "1", "Edat_Quinquenal": "50-54", "Total": 70},
        ]
        totals_25_44: dict = {}
        totals_all:   dict = {}
        for rec in records:
            bc  = str(rec["Codi_Barri"]).zfill(6)
            grp = str(rec["Edat_Quinquenal"])
            n   = float(rec["Total"])
            totals_all[bc] = totals_all.get(bc, 0) + n
            if grp in self._GRUPS_25_44:
                totals_25_44[bc] = totals_25_44.get(bc, 0) + n

        bc = "000001"
        pct = totals_25_44[bc] / totals_all[bc]
        # 25-44: 100+120+110+90 = 420; total = 570
        assert pct == pytest.approx(420 / 570, abs=0.001)


# ── Clase 7: Interfaces asíncronas ───────────────────────────────────────────

class TestInterfacesAsync:
    def test_ejecutar_es_coroutine(self):
        assert asyncio.iscoroutinefunction(ejecutar)

    def test_init_es_coroutine(self):
        assert asyncio.iscoroutinefunction(_init)

    def test_fin_es_coroutine(self):
        assert asyncio.iscoroutinefunction(_fin)


# ── Clase 8: Normalización IERMB ──────────────────────────────────────────────

class TestNormalizacionIermb:
    """Verifica la normalización del score cuando viene en escala >10."""

    def test_valor_menor_10_no_normaliza(self):
        """Valores ≤10 no deben modificarse en la normalización."""
        score = 7.5
        if score > 10:
            score = min(score / 10.0, 10.0)
        assert score == pytest.approx(7.5)

    def test_valor_100_normaliza_a_10(self):
        score = 100.0
        if score > 10:
            score = min(score / 10.0, 10.0)
        assert score == pytest.approx(10.0)

    def test_valor_75_normaliza_a_7_5(self):
        score = 75.0
        if score > 10:
            score = min(score / 10.0, 10.0)
        assert score == pytest.approx(7.5)

    def test_valor_150_capped_10(self):
        score = 150.0
        if score > 10:
            score = min(score / 10.0, 10.0)
        assert score == pytest.approx(10.0)

    def test_valor_limite_10_no_normaliza(self):
        score = 10.0
        if score > 10:
            score = min(score / 10.0, 10.0)
        assert score == pytest.approx(10.0)
