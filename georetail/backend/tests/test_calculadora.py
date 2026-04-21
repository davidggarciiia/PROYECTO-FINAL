"""
tests/test_calculadora.py — Tests para financiero/calculadora.py

Cubre:
  - Estructura de la proyección (36 meses exactos)
  - Curva de rampa de arranque (meses 1-12)
  - Cálculo de inversión inicial
  - Cálculo de EBITDA por escenario
  - KPIs: ROI, payback, breakeven
  - Casos límite: inversión cero, breakeven imposible
"""
import math
import pytest

from financiero.calculadora import calcular_proyeccion, _RAMP


# ─── Fixture: parámetros base realistas ────────────────────────────────────────

@pytest.fixture
def params_base():
    return {
        "ticket_medio":                25.0,
        "clientes_dia_conservador":    20.0,
        "clientes_dia_optimista":      35.0,
        "dias_apertura_mes":           26.0,
        "alquiler_mensual":          1_800.0,
        "salarios_mensual":          3_000.0,
        "otros_fijos_mensual":         800.0,
        "coste_mercancia_pct":          0.35,
        "reforma_local":            25_000.0,
        "equipamiento":             12_000.0,
        "deposito_fianza":           3_600.0,
        "otros_iniciales":           2_500.0,
    }


@pytest.fixture
def params_cero_inversion(params_base):
    """Inversión inicial = 0 para test de ROI con denominador cero."""
    return {**params_base, "reforma_local": 0, "equipamiento": 0,
            "deposito_fianza": 0, "otros_iniciales": 0}


# ─── Tests de estructura ────────────────────────────────────────────────────────

class TestEstructuraProyeccion:
    async def test_proyeccion_tiene_36_meses(self, params_base):
        resultado = await calcular_proyeccion(params_base)
        assert len(resultado["proyeccion"]) == 36

    async def test_meses_numerados_del_1_al_36(self, params_base):
        resultado = await calcular_proyeccion(params_base)
        meses = [m["mes"] for m in resultado["proyeccion"]]
        assert meses == list(range(1, 37))

    async def test_resultado_contiene_todas_las_claves_kpi(self, params_base):
        resultado = await calcular_proyeccion(params_base)
        claves_esperadas = {
            "inversion_total",
            "ingresos_anuales_conservador", "ingresos_anuales_optimista",
            "ingresos_a2_conservador",      "ingresos_a2_optimista",
            "ebitda_anual_conservador",     "ebitda_anual_optimista",
            "roi_3a_conservador",           "roi_3a_optimista",
            "payback_meses_conservador",    "payback_meses_optimista",
            "breakeven_clientes_dia",
            "proyeccion",
        }
        assert claves_esperadas.issubset(resultado.keys())

    async def test_cada_mes_contiene_todas_las_claves(self, params_base):
        resultado = await calcular_proyeccion(params_base)
        claves_mes = {
            "mes", "ingresos_conservador", "ingresos_optimista",
            "costes_variables", "costes_fijos",
            "ebitda_conservador", "ebitda_optimista",
            "acumulado_conservador", "acumulado_optimista",
            "ramp_factor",
        }
        for mes in resultado["proyeccion"]:
            assert claves_mes.issubset(mes.keys()), f"Mes {mes['mes']} incompleto"


# ─── Tests de la curva de rampa ────────────────────────────────────────────────

class TestRampa:
    def test_ramp_tiene_12_valores(self):
        assert len(_RAMP) == 12

    def test_ramp_mes1_es_40_pct(self):
        assert _RAMP[0] == 0.40

    def test_ramp_mes12_es_100_pct(self):
        assert _RAMP[11] == 1.00

    def test_ramp_es_monotonamente_creciente(self):
        for i in range(len(_RAMP) - 1):
            assert _RAMP[i] < _RAMP[i + 1], f"Ramp no crece en posición {i}"

    async def test_ramp_factor_mes1_es_040(self, params_base):
        resultado = await calcular_proyeccion(params_base)
        assert resultado["proyeccion"][0]["ramp_factor"] == pytest.approx(0.40)

    async def test_ramp_factor_mes12_es_100(self, params_base):
        resultado = await calcular_proyeccion(params_base)
        assert resultado["proyeccion"][11]["ramp_factor"] == pytest.approx(1.00)

    async def test_ramp_factor_mes13_en_adelante_es_100(self, params_base):
        resultado = await calcular_proyeccion(params_base)
        for mes in resultado["proyeccion"][12:]:
            assert mes["ramp_factor"] == pytest.approx(1.00), (
                f"Mes {mes['mes']} debería tener ramp=1.0"
            )


# ─── Tests de cálculo financiero ───────────────────────────────────────────────

class TestCalculoFinanciero:
    async def test_inversion_total_correcta(self, params_base):
        resultado = await calcular_proyeccion(params_base)
        esperada = (params_base["reforma_local"] + params_base["equipamiento"] +
                    params_base["deposito_fianza"] + params_base["otros_iniciales"])
        assert resultado["inversion_total"] == round(esperada)

    async def test_ingresos_optimistas_mayores_que_conservadores(self, params_base):
        resultado = await calcular_proyeccion(params_base)
        for mes in resultado["proyeccion"]:
            assert mes["ingresos_optimista"] >= mes["ingresos_conservador"], (
                f"Mes {mes['mes']}: optimista < conservador"
            )

    async def test_costes_fijos_constantes_todos_los_meses(self, params_base):
        resultado = await calcular_proyeccion(params_base)
        cf_esperado = round(
            params_base["alquiler_mensual"] +
            params_base["salarios_mensual"] +
            params_base["otros_fijos_mensual"]
        )
        for mes in resultado["proyeccion"]:
            assert mes["costes_fijos"] == cf_esperado, (
                f"Mes {mes['mes']}: costes_fijos={mes['costes_fijos']} ≠ {cf_esperado}"
            )

    async def test_ingresos_mes1_son_40pct_de_mes12(self, params_base):
        resultado = await calcular_proyeccion(params_base)
        ing_mes1  = resultado["proyeccion"][0]["ingresos_conservador"]
        ing_mes12 = resultado["proyeccion"][11]["ingresos_conservador"]
        # ramp_1 / ramp_12 = 0.40 / 1.00
        ratio = ing_mes1 / ing_mes12
        assert ratio == pytest.approx(0.40, abs=1e-6)

    async def test_acumulado_empieza_negativo(self, params_base):
        resultado = await calcular_proyeccion(params_base)
        # El acumulado mes 1 = -inversion + ebitda_mes1
        # Como la inversión es grande, debe ser negativo
        assert resultado["proyeccion"][0]["acumulado_conservador"] < 0

    async def test_acumulado_es_monotono_o_crece(self, params_base):
        """Con parámetros realistas, el acumulado conservador nunca baja más de lo permitido."""
        resultado = await calcular_proyeccion(params_base)
        # Solo verificamos que el acumulado al final (mes 36) > acumulado mes 1
        ac1  = resultado["proyeccion"][0]["acumulado_conservador"]
        ac36 = resultado["proyeccion"][-1]["acumulado_conservador"]
        assert ac36 > ac1


# ─── Tests de KPIs ─────────────────────────────────────────────────────────────

class TestKPIs:
    async def test_payback_conservador_mayor_igual_optimista(self, params_base):
        resultado = await calcular_proyeccion(params_base)
        assert resultado["payback_meses_conservador"] >= resultado["payback_meses_optimista"]

    async def test_roi_optimista_mayor_que_conservador(self, params_base):
        resultado = await calcular_proyeccion(params_base)
        assert resultado["roi_3a_optimista"] >= resultado["roi_3a_conservador"]

    async def test_breakeven_es_entero_positivo(self, params_base):
        resultado = await calcular_proyeccion(params_base)
        be = resultado["breakeven_clientes_dia"]
        assert isinstance(be, int)
        assert be >= 1

    async def test_roi_cero_cuando_inversion_es_cero(self, params_cero_inversion):
        resultado = await calcular_proyeccion(params_cero_inversion)
        assert resultado["roi_3a_conservador"] == 0
        assert resultado["roi_3a_optimista"]   == 0

    async def test_payback_999_cuando_nunca_se_recupera(self, params_base):
        """Si los ingresos no superan los costes, payback = 999 (centinela)."""
        # Costes fijos muy altos para que nunca se recupere la inversión
        p = {**params_base,
             "alquiler_mensual":   50_000.0,
             "salarios_mensual":   50_000.0,
             "otros_fijos_mensual": 20_000.0}
        resultado = await calcular_proyeccion(p)
        assert resultado["payback_meses_conservador"] == 999

    async def test_breakeven_valido_con_coste_mercancia_alto(self, params_base):
        """coste_mercancia_pct cercano a 1 no debe crashear."""
        p = {**params_base, "coste_mercancia_pct": 0.98}
        resultado = await calcular_proyeccion(p)
        assert resultado["breakeven_clientes_dia"] >= 1

    async def test_ingresos_a2_mayores_que_a1_conservador(self, params_base):
        """Año 2 (meses 13-24, ramp=1.0) debería tener más ingresos que año 1 (con ramp)."""
        resultado = await calcular_proyeccion(params_base)
        assert resultado["ingresos_a2_conservador"] > resultado["ingresos_anuales_conservador"]


# ─── Tests de robustez ante división por cero (bug fix) ────────────────────────

class TestRobustez:
    async def test_no_crashea_con_ticket_cero(self, params_base):
        """Después del fix, ticket_medio=0 no debe lanzar ZeroDivisionError."""
        p = {**params_base, "ticket_medio": 0.0}
        resultado = await calcular_proyeccion(p)
        # El breakeven debe ser el mínimo (1) cuando no se puede calcular
        assert resultado["breakeven_clientes_dia"] >= 1

    async def test_no_crashea_con_dias_cero(self, params_base):
        """dias_apertura_mes=0 no debe lanzar ZeroDivisionError."""
        p = {**params_base, "dias_apertura_mes": 0.0}
        resultado = await calcular_proyeccion(p)
        assert resultado["breakeven_clientes_dia"] >= 1


# ─── Tests de regresión: SS patronal se aplica UNA SOLA VEZ (BUG-P1-4) ─────────

class TestSalariosSinDuplicar:
    """
    Regresión para BUG-P1-4 (auditoría 2026-04-21): la SS patronal (~31%) debe
    aplicarse una sola vez en `estimador._salarios()`. `calcular_proyeccion`
    NO puede volver a multiplicar el coste salarial.

    Ver: docs/vault/03-Auditoria/bugs-medios/BUG-P1-4-ss-patronal-no-duplicada.md
    """

    async def test_costes_fijos_usan_salarios_mensual_as_is(self, params_base):
        """
        `costes_fijos` mensual = alquiler + salarios + otros_fijos.
        Sin multiplicador oculto sobre salarios.
        """
        resultado = await calcular_proyeccion(params_base)
        esperado = round(
            params_base["alquiler_mensual"] +
            params_base["salarios_mensual"] +
            params_base["otros_fijos_mensual"]
        )
        for mes in resultado["proyeccion"]:
            assert mes["costes_fijos"] == esperado, (
                f"Mes {mes['mes']}: costes_fijos={mes['costes_fijos']} ≠ {esperado}. "
                "Sospecha de multiplicador oculto en calculadora (SS duplicada)."
            )

    async def test_delta_costes_fijos_es_lineal_en_salarios(self, params_base):
        """
        Si duplicamos `salarios_mensual`, `costes_fijos` crece exactamente en
        `salarios_original`. Un multiplicador oculto daría crecimiento no lineal.
        """
        resultado_base = await calcular_proyeccion(params_base)
        params_2x = {**params_base, "salarios_mensual": params_base["salarios_mensual"] * 2}
        resultado_2x = await calcular_proyeccion(params_2x)

        delta = resultado_2x["proyeccion"][0]["costes_fijos"] - resultado_base["proyeccion"][0]["costes_fijos"]
        assert delta == pytest.approx(params_base["salarios_mensual"], abs=1), (
            f"Delta costes_fijos={delta} ≠ salarios_mensual={params_base['salarios_mensual']}. "
            "Sugiere multiplicador oculto."
        )

    async def test_integracion_estimador_calculadora_preserva_ss_unica_vez(self):
        """
        Pipeline real: estimador._salarios() genera un PE con SS ya aplicada.
        Ese valor entra a calculadora sin re-multiplicar.

        Formula esperada: n × salario_base × (1 + _SS_EMPRESA) — UNA vez.
        """
        from financiero.estimador import _salarios, _SS_EMPRESA

        bench = {"salario_base_mensual_convenio": 1_650.0}
        n = 2
        pe_salarios = _salarios(n, bench)

        # Snapshot del cálculo esperado (1 aplicación de SS).
        salario_esperado = round(n * 1_650.0 * (1 + _SS_EMPRESA))
        assert pe_salarios.valor == salario_esperado, (
            "estimador._salarios aplicó SS más de una vez o con factor incorrecto."
        )

        # Ahora pasamos al calculador: costes_fijos - alquiler - otros = salarios exactos.
        params = {
            "ticket_medio":                25.0,
            "clientes_dia_conservador":    20.0,
            "clientes_dia_optimista":      35.0,
            "dias_apertura_mes":           26.0,
            "alquiler_mensual":          1_800.0,
            "salarios_mensual":          pe_salarios.valor,
            "otros_fijos_mensual":         800.0,
            "coste_mercancia_pct":          0.35,
            "reforma_local":            25_000.0,
            "equipamiento":             12_000.0,
            "deposito_fianza":           3_600.0,
            "otros_iniciales":           2_500.0,
        }
        resultado = await calcular_proyeccion(params)
        cf = resultado["proyeccion"][0]["costes_fijos"]
        salarios_reconstruidos = cf - round(params["alquiler_mensual"]) - round(params["otros_fijos_mensual"])

        assert salarios_reconstruidos == pe_salarios.valor, (
            f"salarios reconstruidos={salarios_reconstruidos} ≠ pe_salarios.valor={pe_salarios.valor}. "
            "calculadora re-multiplicó el salario."
        )

        # Comprobación extra: ningún factor 1.31² ≈ 1.72 en juego.
        salario_con_doble_ss = round(n * 1_650.0 * (1 + _SS_EMPRESA) ** 2)
        assert salarios_reconstruidos != salario_con_doble_ss, (
            "costes_fijos coincide con SS aplicada DOS veces — bug P1-4 reintroducido."
        )
