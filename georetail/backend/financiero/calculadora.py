"""
financiero/calculadora.py — Modelo financiero realista de 12 pasos. Python puro, sin I/O.

Principios:
  - Un único clients_per_day (base) validado entra al calculador
  - Escenarios se derivan como × 0.60 / × 1.00 / × 1.20 del base (cap por max_capacity)
  - MAX_OCCUPANCY=0.80: la ramp nunca excede el 80% — ningún negocio opera siempre al 100%
  - Productividad del personal limita los clientes efectivos antes de calcular
  - validation_flags expone todas las anomalías detectables al usuario
"""
from __future__ import annotations
import math

# Curva de arranque mensual (12 valores → meses 13+ = 100%)
_RAMP_NUEVO    = [0.40, 0.50, 0.58, 0.65, 0.71, 0.76, 0.81, 0.86, 0.90, 0.94, 0.97, 1.00]
_RAMP_TRASPASO = [0.60, 0.68, 0.75, 0.81, 0.86, 0.90, 0.94, 0.97, 0.99, 1.00, 1.00, 1.00]

# Factores de escenario — FIJOS, no configurables
_SCENARIO_FACTORS: dict[str, float] = {
    "conservador": 0.60,
    "base":        1.00,
    "optimista":   1.20,
}

# ── Ocupación sostenible (exportado para api/financiero.py) ───────────────────
# Ningún negocio físico opera al 100% de su potencial teórico de forma continua.
# El techo realista es 80%. La ramp llega como máximo a este valor.
MAX_OCCUPANCY   = 0.80
_OCCUPANCY_WARN = 0.85   # flag si la ocupación efectiva supera este umbral

# ── Productividad del personal ────────────────────────────────────────────────
_STAFF_EFFICIENCY = 0.80  # eficiencia operativa del equipo (tiempos muertos, etc.)

# Rango (clientes/empleado/día) por tipo de negocio
_PRODUCTIVITY_RATES: dict[str, tuple[float, float]] = {
    "appointment_based": ( 5.0,  7.0),
    "retail_walkin":     ( 8.0, 15.0),
    "restaurant":        (10.0, 20.0),
    "hybrid":            ( 6.0, 12.0),
}

# ── Umbrales de coherencia global ─────────────────────────────────────────────
_MAX_PROFIT_RATIO    = 0.50    # margen neto > 50% → flag severo (irreal)
_NET_MARGIN_WARN     = 0.35    # margen neto > 35% → flag leve (revisar)
_NET_MARGIN_WEAK     = 0.05    # margen neto < 5%  → negocio muy frágil
_MAX_ROI_FLAG        = 3.00    # ROI 3 años > 300% → flag
_MAX_REVENUE_PER_EMP = 30_000  # €/mes por empleado — por encima es sospechoso
_MIN_REVENUE_PER_EMP = 2_500   # €/mes por empleado — por debajo es ineficiente

# ── Volatilidad determinista ──────────────────────────────────────────────────
_VOLATILITY_BAND = 0.15  # banda ±15% de variabilidad (sin aleatoriedad)


async def calcular_proyeccion(params: dict, tipo_negocio: str = "nuevo") -> dict:
    """
    Modelo de 12 pasos para la viabilidad financiera de un negocio físico.

    Params v3 (adicionales al modelo base):
        num_empleados        — número de empleados del equipo
        business_model_type  — tipo de modelo de negocio (appointment_based / retail_walkin / ...)

    Reglas críticas v3:
        - La ramp se capa en MAX_OCCUPANCY: progresión real es 40%→80% (nuevo) o 60%→80% (traspaso)
        - Los clientes nunca superan la capacidad de atención del personal
        - ing_estable y break-even se calculan con MAX_OCCUPANCY para coherencia total
        - Las bandas de volatilidad (±15%) se añaden a cada fila como ingresos_base_low/high
    """
    p          = params
    ramp_curve = _RAMP_TRASPASO if tipo_negocio == "traspaso" else _RAMP_NUEVO

    validation_flags: list[str] = []

    # ── PASO 1: Normalización de entradas ──────────────────────────────────────
    clients_base        = float(p["clients_per_day"])
    max_cap             = float(p.get("max_capacity", clients_base * 3.0))
    num_empleados       = max(1, int(p.get("num_empleados", 1)))
    business_model_type = str(p.get("business_model_type", "retail_walkin"))

    # ── PASO 2: Capacidad real del personal ───────────────────────────────────
    rates            = _PRODUCTIVITY_RATES.get(business_model_type, _PRODUCTIVITY_RATES["retail_walkin"])
    productivity_avg = (rates[0] + rates[1]) / 2
    max_staff_cap    = num_empleados * productivity_avg * _STAFF_EFFICIENCY

    if max_staff_cap > 0 and clients_base > max_staff_cap:
        original     = clients_base
        clients_base = math.floor(max_staff_cap * 10) / 10  # techo hacia abajo sin redondear arriba
        validation_flags.append(
            f"Capacidad del personal insuficiente: {num_empleados} empleado(s) pueden atender "
            f"máx. {math.ceil(max_staff_cap)} clientes/día "
            f"(productividad {productivity_avg:.0f} clientes/empleado/día × eficiencia {_STAFF_EFFICIENCY:.0%}). "
            f"Clientes ajustados: {original:.0f} → {clients_base:.0f}."
        )

    # ── PASO 3: Detección de sobredimensión de plantilla ──────────────────────
    empleados_necesarios = math.ceil(clients_base / max(0.1, productivity_avg * _STAFF_EFFICIENCY))
    if num_empleados > empleados_necesarios + 1:
        validation_flags.append(
            f"Sobredimensión de plantilla: {num_empleados} empleados para {round(clients_base)} "
            f"clientes/día — bastarían ~{empleados_necesarios}. "
            f"El exceso incrementa costes fijos sin generar ingresos adicionales."
        )

    # ── PASO 4: Unificación demanda vs. capacidad física ──────────────────────
    if clients_base > max_cap:
        validation_flags.append(
            f"Demanda base ({clients_base:.0f}) > capacidad máxima ({max_cap:.0f}): ajustado a capacidad"
        )
        clients_base = max_cap

    clients_base = max(1.0, clients_base)

    # ── Inversión inicial ──────────────────────────────────────────────────────
    inversion = (
        p["reforma_local"] + p["equipamiento"] +
        p["deposito_fianza"] + p["otros_iniciales"]
    )

    # ── Costes fijos mensuales ─────────────────────────────────────────────────
    cf_mes      = p["alquiler_mensual"] + p["salarios_mensual"] + p["otros_fijos_mensual"]
    margen_unit = 1 - p["coste_mercancia_pct"]

    # ── PASO 8: Break-even con ocupación real ─────────────────────────────────
    # Con MAX_OCCUPANCY, el ingreso efectivo es ticket × dias × 0.80 por cliente-día.
    # El break-even requiere más clientes base que en un modelo sin ocupación.
    ing_be     = cf_mes / max(0.01, margen_unit)
    denom_be   = (p["ticket_medio"] or 0.01) * (p["dias_apertura_mes"] or 1) * MAX_OCCUPANCY
    be_raw     = ing_be / denom_be
    be_clients = max(1, math.ceil(be_raw)) if math.isfinite(be_raw) else 1

    # ── PASO 12: Clientes por escenario ───────────────────────────────────────
    scenario_clients: dict[str, float] = {
        s: max(1.0, min(round(clients_base * f, 1), max_cap))
        for s, f in _SCENARIO_FACTORS.items()
    }

    # ── Proyección 36 meses ────────────────────────────────────────────────────
    proyeccion: list[dict] = []
    acum: dict[str, float] = {s: -inversion for s in _SCENARIO_FACTORS}

    for mes in range(1, 37):
        raw_ramp = ramp_curve[min(mes - 1, 11)]
        # Techo de ocupación: la ramp no puede superar MAX_OCCUPANCY (0.80).
        # Resultado: nuevo → 40%→80% / traspaso → 60%→80% (nunca llega al 100% teórico).
        ramp = min(raw_ramp, MAX_OCCUPANCY)

        row: dict = {"mes": mes, "ramp_factor": round(ramp, 3), "costes_fijos": round(cf_mes)}
        base_cogs = 0.0

        for s in _SCENARIO_FACTORS:
            cl     = scenario_clients[s]
            ing    = p["ticket_medio"] * cl * p["dias_apertura_mes"] * ramp
            cogs   = ing * p["coste_mercancia_pct"]
            ebitda = ing - cogs - cf_mes
            acum[s] += ebitda

            row[f"ingresos_{s}"]  = round(ing)
            row[f"ebitda_{s}"]    = round(ebitda)
            row[f"acumulado_{s}"] = round(acum[s])
            if s == "base":
                base_cogs = round(cogs)
                # Bandas de volatilidad deterministas ±15% (sin aleatoriedad)
                row["ingresos_base_low"]  = round(ing * (1 - _VOLATILITY_BAND))
                row["ingresos_base_high"] = round(ing * (1 + _VOLATILITY_BAND))

        row["costes_variables"] = base_cogs
        proyeccion.append(row)

    # ── KPIs ──────────────────────────────────────────────────────────────────
    def _sum_anual(key: str) -> int:
        return round(sum(r[key] for r in proyeccion[:12]))

    def _payback(s: str) -> int:
        return next((r["mes"] for r in proyeccion if r[f"acumulado_{s}"] >= 0), 999)

    def _roi(s: str) -> float:
        return round((proyeccion[-1][f"acumulado_{s}"] + inversion) / inversion, 3) if inversion > 0 else 0.0

    # ── PASO 5-7: Régimen estable con ocupación real (escenario base) ─────────
    cl_base      = scenario_clients["base"]
    ing_estable  = p["ticket_medio"] * cl_base * p["dias_apertura_mes"] * MAX_OCCUPANCY
    cogs_estable = ing_estable * p["coste_mercancia_pct"]
    ben_estable  = ing_estable - cogs_estable - cf_mes

    # Ocupación efectiva = (clientes base × MAX_OCCUPANCY) / capacidad máxima
    ocupacion_efectiva = (cl_base * MAX_OCCUPANCY) / max_cap if max_cap > 0 else 0.0

    # ── VALIDACIONES DE COHERENCIA GLOBAL ─────────────────────────────────────

    if ing_estable > 0:
        net_margin = ben_estable / ing_estable

        if net_margin > _MAX_PROFIT_RATIO:
            pct = round(net_margin * 100)
            validation_flags.append(
                f"Margen neto {pct}% > 50% — irreal. Verifica que los costes están completos "
                f"(seguridad social, amortizaciones, contabilidad, imprevistos)."
            )
        elif net_margin > _NET_MARGIN_WARN:
            pct = round(net_margin * 100)
            validation_flags.append(
                f"Margen neto {pct}% supera el 35% — posiblemente optimista. "
                f"Revisa si faltan costes ocultos o la estimación de ingresos es agresiva."
            )
        elif 0 < net_margin < _NET_MARGIN_WEAK:
            validation_flags.append(
                "Margen neto < 5% — negocio muy frágil: cualquier imprevisto puede generar pérdidas."
            )

    # Ingresos por empleado en régimen estable
    if num_empleados > 0 and ing_estable > 0:
        ing_emp = ing_estable / num_empleados
        if ing_emp > _MAX_REVENUE_PER_EMP:
            validation_flags.append(
                f"Ingresos por empleado muy elevados ({round(ing_emp):,} €/mes) — "
                f"considera si la plantilla tiene suficiente capacidad para esa demanda."
            )
        elif ing_emp < _MIN_REVENUE_PER_EMP:
            validation_flags.append(
                f"Ingresos por empleado bajos ({round(ing_emp):,} €/mes) — "
                f"posible sobredimensión de plantilla o ticket medio insuficiente."
            )

    # Ocupación efectiva > 85%
    if ocupacion_efectiva > _OCCUPANCY_WARN:
        validation_flags.append(
            f"Ocupación efectiva {round(ocupacion_efectiva * 100)}% > 85% sostenida — "
            f"muy difícil de mantener. Considera ampliar capacidad o revisar la demanda base."
        )

    # ROI > 300%
    roi_base = _roi("base")
    if roi_base > _MAX_ROI_FLAG:
        validation_flags.append(
            f"ROI a 3 años de {round(roi_base * 100)}% supera el 300% — "
            f"revisa inversión inicial, costes fijos y ticket medio."
        )

    # ── Estructura de costes ──────────────────────────────────────────────────
    estructura_costes = {
        "alquiler":         round(p["alquiler_mensual"]),
        "personal":         round(p["salarios_mensual"]),
        "variable":         round(cogs_estable),
        "otros":            round(p["otros_fijos_mensual"]),
        "beneficio":        round(max(0.0, ben_estable)),
        "perdida":          round(abs(min(0.0, ben_estable))),
        "ingresos_totales": round(ing_estable),
    }

    # ── Gráfico break-even (coherente con MAX_OCCUPANCY) ─────────────────────
    chart_max = max(cl_base * 2.0, float(be_clients) * 2.5, 50.0)
    break_even_chart = [
        {
            "clientes":       round(chart_max * i / 20, 1),
            "ingresos":       round(
                p["ticket_medio"] * (chart_max * i / 20) * p["dias_apertura_mes"] * MAX_OCCUPANCY
            ),
            "costes_totales": round(
                p["ticket_medio"] * (chart_max * i / 20) * p["dias_apertura_mes"]
                * MAX_OCCUPANCY * p["coste_mercancia_pct"] + cf_mes
            ),
        }
        for i in range(21)
    ]

    return {
        "clients_per_day":              cl_base,
        "max_capacity":                 max_cap,
        "max_staff_capacity":           round(max_staff_cap, 1),
        "ocupacion_efectiva":           round(ocupacion_efectiva, 3),
        "scenario_clients":             scenario_clients,
        "inversion_total":              round(inversion),
        "ingresos_anuales_conservador": _sum_anual("ingresos_conservador"),
        "ingresos_anuales_base":        _sum_anual("ingresos_base"),
        "ingresos_anuales_optimista":   _sum_anual("ingresos_optimista"),
        "ebitda_anual_conservador":     _sum_anual("ebitda_conservador"),
        "ebitda_anual_base":            _sum_anual("ebitda_base"),
        "ebitda_anual_optimista":       _sum_anual("ebitda_optimista"),
        "roi_3a_conservador":           _roi("conservador"),
        "roi_3a_base":                  roi_base,
        "roi_3a_optimista":             _roi("optimista"),
        "payback_meses_conservador":    _payback("conservador"),
        "payback_meses_base":           _payback("base"),
        "payback_meses_optimista":      _payback("optimista"),
        "breakeven_clientes_dia":       be_clients,
        "proyeccion":                   proyeccion,
        "estructura_costes":            estructura_costes,
        "break_even_chart":             break_even_chart,
        "validation_flags":             list(dict.fromkeys(validation_flags)),
    }
