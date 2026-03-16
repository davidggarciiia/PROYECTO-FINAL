"""
tests/test_estimador.py — Tests para financiero/estimador.py (funciones puras).

Solo se testean las funciones que NO hacen I/O (helpers privados).
Las funciones async con DB (estimar_parametros y _get_*) se cubren
con integration tests en una suite separada.

Cubre:
  - _ticket: con y sin precio_nivel de Google Places
  - _clientes_flujo: con y sin datos de flujo peatonal
  - _clientes_cita: negocios por cita
  - _alquiler: datos reales / precio_m2 / fallback genérico
  - _salarios: cálculo con Seguridad Social
  - _otros_fijos: suministros + seguro + gestoría + mantenimiento
  - _reforma: benchmarks sectoriales
  - _equip: equipamiento mínimo-máximo
  - _dias: con y sin datos de competencia
  - _otros_ini: licencias + costes apertura
  - PE dataclass: validación de campos
"""
import math
import pytest

from financiero.estimador import (
    PE,
    ParametrosEstimados,
    _ticket,
    _clientes_flujo,
    _clientes_cita,
    _alquiler,
    _salarios,
    _otros_fijos,
    _reforma,
    _equip,
    _dias,
    _otros_ini,
    _FACTOR_CONSERVADOR,
    _FACTOR_OPTIMISTA,
    _SS_EMPRESA,
)


# ─── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def bench_restauracion():
    return {
        "ticket_medio_min":            12.0,
        "ticket_medio_max":            45.0,
        "horas_apertura_dia":           9.0,
        "conversion_rate_min":         0.005,
        "conversion_rate_max":          0.02,
        "empleados_por_m2":            20.0,
        "salario_base_mensual_convenio": 1_650.0,
        "margen_bruto_tipico":          0.65,
        "coste_suministros_por_m2":     5.0,
        "seguro_rc_mensual":           100.0,
        "coste_gestoria_mensual":      185.0,
        "reforma_m2_min":              300.0,
        "reforma_m2_max":              700.0,
        "equipamiento_base_min":     5_000.0,
        "equipamiento_base_max":    20_000.0,
        "dias_apertura_mes_tipico":     26,
        "coste_licencias_apertura":  2_000.0,
        "otros_iniciales_fijos":     2_000.0,
        "is_appointment_based":        False,
        "clientes_dia_por_puesto_min":  2.0,
        "clientes_dia_por_puesto_max":  5.0,
    }


@pytest.fixture
def bench_tatuajes(bench_restauracion):
    return {**bench_restauracion,
            "ticket_medio_min":  80.0, "ticket_medio_max": 400.0,
            "is_appointment_based": True,
            "empleados_por_m2": 15.0}


@pytest.fixture
def vz_con_flujo():
    return {
        "flujo_peatonal_manana": 200,
        "flujo_peatonal_tarde":  400,
        "flujo_peatonal_noche":  150,
    }


@pytest.fixture
def vz_sin_flujo():
    return {"flujo_peatonal_manana": 0, "flujo_peatonal_tarde": 0, "flujo_peatonal_noche": 0}


@pytest.fixture
def comp_con_datos():
    return {"num_competidores": 4, "score_saturacion": 50}


# ─── Tests PE dataclass ────────────────────────────────────────────────────────

class TestPE:
    def test_pe_crea_con_campos_basicos(self):
        pe = PE(valor=100.0, fuente="test", confianza="alta")
        assert pe.valor == 100.0
        assert pe.fuente == "test"
        assert pe.confianza == "alta"

    def test_pe_rango_por_defecto_es_cero(self):
        pe = PE(valor=50.0, fuente="x", confianza="media")
        assert pe.rango_min == 0.0
        assert pe.rango_max == 0.0

    def test_pe_rango_personalizado(self):
        pe = PE(valor=50.0, fuente="x", confianza="media", rango_min=30.0, rango_max=70.0)
        assert pe.rango_min == 30.0
        assert pe.rango_max == 70.0


# ─── Tests _ticket ─────────────────────────────────────────────────────────────

class TestTicket:
    def test_ticket_con_precio_nivel_conocido_restauracion(self, bench_restauracion):
        pe = _ticket("restauracion", precio_nivel=2.0, bench=bench_restauracion)
        # precio_nivel=2 → 19.0 según _TICKET_POR_NIVEL
        assert pe.valor == pytest.approx(19.0)
        assert pe.confianza == "media"

    def test_ticket_con_precio_nivel_fuera_de_rango_se_clampea(self, bench_restauracion):
        """precio_nivel=5 → clampeado a 4."""
        pe = _ticket("restauracion", precio_nivel=5.0, bench=bench_restauracion)
        # clamped to level 4 → 65.0
        assert pe.valor == pytest.approx(65.0)

    def test_ticket_sin_precio_nivel_usa_benchmark(self, bench_restauracion):
        pe = _ticket("restauracion", precio_nivel=None, bench=bench_restauracion)
        bmid = (bench_restauracion["ticket_medio_min"] + bench_restauracion["ticket_medio_max"]) / 2
        assert pe.valor == pytest.approx(round(bmid, 2))
        assert pe.confianza == "baja"

    def test_ticket_sector_desconocido_usa_benchmark(self, bench_restauracion):
        pe = _ticket("sector_inexistente", precio_nivel=2.0, bench=bench_restauracion)
        # Sector desconocido no tiene entrada en _TICKET_POR_NIVEL → usa bmid
        bmid = (bench_restauracion["ticket_medio_min"] + bench_restauracion["ticket_medio_max"]) / 2
        assert pe.valor == pytest.approx(round(bmid, 2))

    def test_ticket_rango_min_max_es_el_benchmark(self, bench_restauracion):
        pe = _ticket("restauracion", precio_nivel=None, bench=bench_restauracion)
        assert pe.rango_min == round(bench_restauracion["ticket_medio_min"])
        assert pe.rango_max == round(bench_restauracion["ticket_medio_max"])


# ─── Tests _clientes_flujo ─────────────────────────────────────────────────────

class TestClientesFlujo:
    def test_con_flujo_devuelve_tupla_de_dos_PE(self, vz_con_flujo, bench_restauracion,
                                                   comp_con_datos):
        cc, co = _clientes_flujo("restauracion", vz_con_flujo, comp_con_datos, bench_restauracion)
        assert isinstance(cc, PE)
        assert isinstance(co, PE)

    def test_optimista_mayor_igual_conservador(self, vz_con_flujo, bench_restauracion,
                                                comp_con_datos):
        cc, co = _clientes_flujo("restauracion", vz_con_flujo, comp_con_datos, bench_restauracion)
        assert co.valor >= cc.valor

    def test_con_flujo_confianza_es_media(self, vz_con_flujo, bench_restauracion, comp_con_datos):
        cc, co = _clientes_flujo("restauracion", vz_con_flujo, comp_con_datos, bench_restauracion)
        assert cc.confianza == "media"
        assert co.confianza == "media"

    def test_sin_flujo_confianza_es_baja(self, vz_sin_flujo, bench_restauracion, comp_con_datos):
        cc, co = _clientes_flujo("restauracion", vz_sin_flujo, comp_con_datos, bench_restauracion)
        assert cc.confianza == "baja"
        assert co.confianza == "baja"

    def test_valor_minimo_es_1(self, vz_sin_flujo, bench_restauracion):
        cc, co = _clientes_flujo("restauracion", vz_sin_flujo, {}, bench_restauracion)
        assert cc.valor >= 1.0
        assert co.valor >= 1.0

    def test_factor_conservador_aplicado(self, vz_con_flujo, bench_restauracion, comp_con_datos):
        """El conservador debe ser ~_FACTOR_CONSERVADOR del optimista (±margen)."""
        cc, co = _clientes_flujo("restauracion", vz_con_flujo, comp_con_datos, bench_restauracion)
        ratio = cc.valor / co.valor
        # Ratio esperado: _FACTOR_CONSERVADOR / _FACTOR_OPTIMISTA
        expected = _FACTOR_CONSERVADOR / _FACTOR_OPTIMISTA
        assert ratio == pytest.approx(expected, rel=0.1)

    def test_alta_saturacion_reduce_clientes(self, vz_con_flujo, bench_restauracion):
        comp_saturado = {"num_competidores": 4, "score_saturacion": 90}
        comp_normal   = {"num_competidores": 4, "score_saturacion": 50}
        cc_sat, _ = _clientes_flujo("restauracion", vz_con_flujo, comp_saturado, bench_restauracion)
        cc_nor, _ = _clientes_flujo("restauracion", vz_con_flujo, comp_normal,   bench_restauracion)
        assert cc_sat.valor < cc_nor.valor

    def test_baja_saturacion_aumenta_clientes(self, vz_con_flujo, bench_restauracion):
        comp_bajo    = {"num_competidores": 4, "score_saturacion": 10}
        comp_normal  = {"num_competidores": 4, "score_saturacion": 50}
        cc_bajo, _ = _clientes_flujo("restauracion", vz_con_flujo, comp_bajo,   bench_restauracion)
        cc_nor,  _ = _clientes_flujo("restauracion", vz_con_flujo, comp_normal, bench_restauracion)
        assert cc_bajo.valor > cc_nor.valor


# ─── Tests _clientes_cita ──────────────────────────────────────────────────────

class TestClientesCita:
    def test_retorna_tupla_dos_PE(self, bench_tatuajes):
        cc, co = _clientes_cita(m2=60.0, bench=bench_tatuajes)
        assert isinstance(cc, PE)
        assert isinstance(co, PE)

    def test_optimista_mayor_igual_conservador(self, bench_tatuajes):
        cc, co = _clientes_cita(m2=60.0, bench=bench_tatuajes)
        assert co.valor >= cc.valor

    def test_valor_minimo_es_1(self, bench_tatuajes):
        cc, co = _clientes_cita(m2=5.0, bench=bench_tatuajes)
        assert cc.valor >= 1.0
        assert co.valor >= 1.0

    def test_mas_m2_mas_puestos_mas_clientes(self, bench_tatuajes):
        cc_chico, _ = _clientes_cita(m2=30.0,  bench=bench_tatuajes)
        cc_grande, _= _clientes_cita(m2=120.0, bench=bench_tatuajes)
        assert cc_grande.valor > cc_chico.valor

    def test_empleados_por_m2_cero_no_crashea(self, bench_tatuajes):
        """e_m2=0 no debe lanzar ZeroDivisionError (bug fix)."""
        bench_malo = {**bench_tatuajes, "empleados_por_m2": 0}
        cc, co = _clientes_cita(m2=60.0, bench=bench_malo)
        assert cc.valor >= 1.0


# ─── Tests _alquiler ───────────────────────────────────────────────────────────

class TestAlquiler:
    def test_dato_real_tiene_prioridad(self):
        local = {"alquiler_mensual": 1_500.0}
        vz    = {"precio_m2": 25.0}
        pe = _alquiler(local, vz, m2=60.0)
        assert pe.valor == 1_500
        assert pe.confianza == "alta"

    def test_precio_m2_cuando_no_hay_dato_real(self):
        local = {"alquiler_mensual": None}
        vz    = {"precio_m2": 20.0}
        pe = _alquiler(local, vz, m2=60.0)
        assert pe.valor == round(20.0 * 60.0)
        assert pe.confianza == "media"

    def test_fallback_generico_18_eur_m2(self):
        local = {}
        vz    = {}
        pe = _alquiler(local, vz, m2=60.0)
        assert pe.valor == round(60.0 * 18.0)
        assert pe.confianza == "baja"

    def test_alquiler_0_en_local_usa_precio_m2(self):
        """alquiler_mensual=0 (falsy) no debe usarse — debe caer a precio_m2."""
        local = {"alquiler_mensual": 0}
        vz    = {"precio_m2": 20.0}
        pe = _alquiler(local, vz, m2=50.0)
        assert pe.valor == round(20.0 * 50.0)


# ─── Tests _salarios ───────────────────────────────────────────────────────────

class TestSalarios:
    def test_calculo_un_empleado(self, bench_restauracion):
        pe = _salarios(n=1, bench=bench_restauracion)
        esperado = round(1 * 1_650.0 * (1 + _SS_EMPRESA))
        assert pe.valor == esperado

    def test_calculo_tres_empleados(self, bench_restauracion):
        pe = _salarios(n=3, bench=bench_restauracion)
        esperado = round(3 * 1_650.0 * (1 + _SS_EMPRESA))
        assert pe.valor == esperado

    def test_valor_incluye_ss_empresa(self, bench_restauracion):
        pe = _salarios(n=2, bench=bench_restauracion)
        sin_ss = 2 * 1_650.0
        assert pe.valor > sin_ss  # SS añade ~31%

    def test_rango_min_max_razonables(self, bench_restauracion):
        pe = _salarios(n=2, bench=bench_restauracion)
        assert pe.rango_min < pe.valor < pe.rango_max


# ─── Tests _otros_fijos ────────────────────────────────────────────────────────

class TestOtrosFijos:
    def test_total_es_suma_de_componentes(self, bench_restauracion):
        m2 = 60.0
        pe = _otros_fijos(m2, bench_restauracion)
        sumi = round(m2 * bench_restauracion["coste_suministros_por_m2"])
        seg  = round(bench_restauracion["seguro_rc_mensual"])
        ges  = round(bench_restauracion["coste_gestoria_mensual"])
        mant = round(m2 * 1.5)
        assert pe.valor == sumi + seg + ges + mant

    def test_m2_mayor_aumenta_total(self, bench_restauracion):
        pe_chico = _otros_fijos(30.0,  bench_restauracion)
        pe_grande= _otros_fijos(120.0, bench_restauracion)
        assert pe_grande.valor > pe_chico.valor

    def test_confianza_es_media(self, bench_restauracion):
        pe = _otros_fijos(60.0, bench_restauracion)
        assert pe.confianza == "media"


# ─── Tests _reforma ────────────────────────────────────────────────────────────

class TestReforma:
    def test_valor_es_media_benchmarks(self, bench_restauracion):
        m2 = 60.0
        rmin = bench_restauracion["reforma_m2_min"]
        rmax = bench_restauracion["reforma_m2_max"]
        pe = _reforma(m2, bench_restauracion)
        assert pe.valor == round(m2 * (rmin + rmax) / 2)

    def test_rango_min_menor_que_valor(self, bench_restauracion):
        pe = _reforma(60.0, bench_restauracion)
        assert pe.rango_min < pe.valor

    def test_rango_max_mayor_que_valor(self, bench_restauracion):
        pe = _reforma(60.0, bench_restauracion)
        assert pe.rango_max > pe.valor

    def test_confianza_es_baja(self, bench_restauracion):
        pe = _reforma(60.0, bench_restauracion)
        assert pe.confianza == "baja"


# ─── Tests _equip ──────────────────────────────────────────────────────────────

class TestEquip:
    def test_valor_es_media_min_max(self, bench_restauracion):
        pe = _equip(bench_restauracion)
        esperado = round((bench_restauracion["equipamiento_base_min"] +
                          bench_restauracion["equipamiento_base_max"]) / 2)
        assert pe.valor == esperado

    def test_rango_min_max_corresponden_al_benchmark(self, bench_restauracion):
        pe = _equip(bench_restauracion)
        assert pe.rango_min == bench_restauracion["equipamiento_base_min"]
        assert pe.rango_max == bench_restauracion["equipamiento_base_max"]


# ─── Tests _dias ───────────────────────────────────────────────────────────────

class TestDias:
    def test_con_datos_de_competidores_usa_valor_real(self, bench_restauracion):
        pe = _dias(dias_comp=24.5, bench=bench_restauracion)
        # Python 3 usa redondeo bancario: round(24.5) = 24 (nearest even)
        assert pe.valor == round(24.5)
        assert pe.confianza == "alta"

    def test_sin_datos_usa_benchmark(self, bench_restauracion):
        pe = _dias(dias_comp=None, bench=bench_restauracion)
        assert pe.valor == bench_restauracion["dias_apertura_mes_tipico"]
        assert pe.confianza == "media"

    def test_dias_cero_usa_benchmark(self, bench_restauracion):
        pe = _dias(dias_comp=0, bench=bench_restauracion)
        assert pe.valor == bench_restauracion["dias_apertura_mes_tipico"]

    def test_rango_min_max_son_20_y_31(self, bench_restauracion):
        pe = _dias(dias_comp=26, bench=bench_restauracion)
        assert pe.rango_min == 20
        assert pe.rango_max == 31


# ─── Tests _otros_ini ─────────────────────────────────────────────────────────

class TestOtrosIni:
    def test_total_es_suma_lic_mas_fijos(self, bench_restauracion):
        pe = _otros_ini(bench_restauracion)
        lic = bench_restauracion["coste_licencias_apertura"]
        fij = bench_restauracion["otros_iniciales_fijos"]
        assert pe.valor == round(lic + fij)

    def test_confianza_es_media(self, bench_restauracion):
        pe = _otros_ini(bench_restauracion)
        assert pe.confianza == "media"

    def test_rango_min_menor_que_valor(self, bench_restauracion):
        pe = _otros_ini(bench_restauracion)
        assert pe.rango_min < pe.valor

    def test_rango_max_mayor_que_valor(self, bench_restauracion):
        pe = _otros_ini(bench_restauracion)
        assert pe.rango_max > pe.valor


# ─── Tests ParametrosEstimados dataclass ──────────────────────────────────────

class TestParametrosEstimados:
    def test_instancia_por_defecto_tiene_valores_coherentes(self):
        p = ParametrosEstimados()
        assert p.dias_apertura_mes.valor == 26
        assert p.coste_mercancia_pct.valor == pytest.approx(0.4)
        assert p.num_empleados == 1

    def test_todos_los_campos_pe_tienen_confianza_valida(self):
        p = ParametrosEstimados()
        campos_pe = [
            p.ticket_medio, p.clientes_dia_conservador, p.clientes_dia_optimista,
            p.dias_apertura_mes, p.alquiler_mensual, p.salarios_mensual,
            p.otros_fijos_mensual, p.coste_mercancia_pct,
            p.reforma_local, p.equipamiento, p.deposito_fianza, p.otros_iniciales,
        ]
        niveles_validos = {"alta", "media", "baja"}
        for campo in campos_pe:
            assert campo.confianza in niveles_validos, (
                f"Campo con confianza inválida: {campo.confianza!r}"
            )
