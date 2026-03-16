"""
financiero/calculadora.py — Calculadora financiera. Python puro, sin I/O.

Genera proyección mensual de 36 meses con:
- Rampa de arranque meses 1-12 (inicio al 40%, llega al 100% en mes 12)
- Escenario conservador y optimista
- ROI, payback, breakeven
"""
from __future__ import annotations
import math


# Curva de arranque: mes 1=40% de capacidad, mes 12=100%, meses 13+=100%
_RAMP = [0.40,0.50,0.58,0.65,0.71,0.76,0.81,0.86,0.90,0.94,0.97,1.00]


async def calcular_proyeccion(params: dict) -> dict:
    """
    Genera la proyección financiera completa.

    Params esperados:
      ticket_medio, clientes_dia_conservador, clientes_dia_optimista,
      dias_apertura_mes, alquiler_mensual, salarios_mensual,
      otros_fijos_mensual, coste_mercancia_pct,
      reforma_local, equipamiento, deposito_fianza, otros_iniciales
    """
    p = params

    # ── Inversión inicial ──────────────────────────────────────────────────────
    inversion = (p["reforma_local"] + p["equipamiento"] +
                 p["deposito_fianza"] + p["otros_iniciales"])

    # ── Costes fijos mensuales totales ─────────────────────────────────────────
    cf_mes = p["alquiler_mensual"] + p["salarios_mensual"] + p["otros_fijos_mensual"]

    # ── Proyección mensual 36 meses ────────────────────────────────────────────
    proyeccion = []
    acum_c = -inversion  # acumulado escenario conservador (negativo al inicio)
    acum_o = -inversion

    for mes in range(1, 37):
        ramp = _RAMP[min(mes-1, 11)]

        # Ingresos
        ing_c = p["ticket_medio"] * p["clientes_dia_conservador"] * p["dias_apertura_mes"] * ramp
        ing_o = p["ticket_medio"] * p["clientes_dia_optimista"]   * p["dias_apertura_mes"] * ramp

        # Coste de mercancía (variable)
        cogs_c = ing_c * p["coste_mercancia_pct"]
        cogs_o = ing_o * p["coste_mercancia_pct"]

        # Margen bruto
        mb_c = ing_c - cogs_c
        mb_o = ing_o - cogs_o

        # EBITDA = margen bruto - costes fijos
        ebitda_c = mb_c - cf_mes
        ebitda_o = mb_o - cf_mes

        acum_c += ebitda_c
        acum_o += ebitda_o

        proyeccion.append({
            "mes": mes,
            "ingresos_conservador":  round(ing_c),
            "ingresos_optimista":    round(ing_o),
            "costes_variables":      round(cogs_c),  # mismo % ambos escenarios
            "costes_fijos":          round(cf_mes),
            "ebitda_conservador":    round(ebitda_c),
            "ebitda_optimista":      round(ebitda_o),
            "acumulado_conservador": round(acum_c),
            "acumulado_optimista":   round(acum_o),
            "ramp_factor":           ramp,
        })

    # ── KPIs anuales (régimen estable = mes 13+) ──────────────────────────────
    meses_estables = proyeccion[12:]  # meses 13-36
    ing_anual_c = sum(m["ingresos_conservador"] for m in proyeccion[:12])
    ing_anual_o = sum(m["ingresos_optimista"]   for m in proyeccion[:12])
    # Año 2 (meses 13-24) y año 3 (25-36)
    ing_a2_c = sum(m["ingresos_conservador"] for m in proyeccion[12:24])
    ing_a2_o = sum(m["ingresos_optimista"]   for m in proyeccion[12:24])

    ebitda_a1_c = sum(m["ebitda_conservador"] for m in proyeccion[:12])
    ebitda_a1_o = sum(m["ebitda_optimista"]   for m in proyeccion[:12])

    # ROI a 3 años = beneficio neto 36 meses / inversión inicial
    ben_3a_c = proyeccion[-1]["acumulado_conservador"] + inversion  # beneficio neto total
    ben_3a_o = proyeccion[-1]["acumulado_optimista"]   + inversion
    roi_c = ben_3a_c / inversion if inversion > 0 else 0
    roi_o = ben_3a_o / inversion if inversion > 0 else 0

    # Payback: mes en que acumulado > 0
    pb_c = next((m["mes"] for m in proyeccion if m["acumulado_conservador"] >= 0), 999)
    pb_o = next((m["mes"] for m in proyeccion if m["acumulado_optimista"]   >= 0), 999)

    # Breakeven clientes/día en régimen estable (ramp=1.0)
    # Ingresos breakeven = costes fijos + costes variables (basado en coste_mercancia_pct)
    # ing_be = cf_mes / (1 - coste_mercancia_pct)
    # clientes_be = ing_be / (ticket × dias)
    ing_be = cf_mes / max(0.01, 1 - p["coste_mercancia_pct"])
    denominador_be = (p["ticket_medio"] or 0) * (p["dias_apertura_mes"] or 0)
    clientes_be_raw = ing_be / denominador_be if denominador_be > 0 else float("inf")
    breakeven_clientes = max(1, math.ceil(clientes_be_raw)) if math.isfinite(clientes_be_raw) else 1

    return {
        "inversion_total":              round(inversion),
        "ingresos_anuales_conservador": round(ing_anual_c),
        "ingresos_anuales_optimista":   round(ing_anual_o),
        "ingresos_a2_conservador":      round(ing_a2_c),
        "ingresos_a2_optimista":        round(ing_a2_o),
        "ebitda_anual_conservador":     round(ebitda_a1_c),
        "ebitda_anual_optimista":       round(ebitda_a1_o),
        "roi_3a_conservador":           round(roi_c, 3),
        "roi_3a_optimista":             round(roi_o, 3),
        "payback_meses_conservador":    pb_c,
        "payback_meses_optimista":      pb_o,
        "breakeven_clientes_dia":       breakeven_clientes,
        "proyeccion":                   proyeccion,
    }
