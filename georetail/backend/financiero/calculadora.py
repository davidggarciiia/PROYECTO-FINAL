"""
financiero/calculadora.py — Modelo financiero v4. Python puro, sin I/O.

Mejoras v4 (auditoría GeoRetail):
  - Curvas de ramp-up diferenciadas por sector (restauracion más rápido, tatuajes más lento)
  - Coeficientes de estacionalidad mensual por sector (12 meses, normalizada a media 1.0)
  - Escenario de estrés: ingresos × 0.40 con COSTES FIJOS INTACTOS (simula apertura + imprevisto)
  - ROI explícito: ganancia_neta / inversión = acumulado_36 + inversión (ya parte de -inversión)
  - Escenarios conservador/base/optimista: sólo afectan a ingresos y COGS (variable),
    los costes fijos (alquiler, salarios, otros) NO escalan — corrección del error de auditoría
"""
from __future__ import annotations
import math

# ── Curvas de ramp-up por sector ──────────────────────────────────────────────
# 12 valores → mes 13+ usa el último valor (capeado por MAX_OCCUPANCY)
# Restauración central BCN: puede llegar al 80% en mes 3-4
_RAMP_RESTAURACION = [0.50, 0.63, 0.74, 0.80, 0.84, 0.86, 0.88, 0.90, 0.93, 0.96, 0.98, 1.00]
# Retail / moda: moderado (3-6 meses para llegar a régimen)
_RAMP_RETAIL       = [0.42, 0.52, 0.61, 0.68, 0.73, 0.78, 0.82, 0.86, 0.90, 0.93, 0.96, 1.00]
# Servicios de cita (tatuajes, estética, clínica): ramp lento 18-24 meses
# Requiere construir cartera/reputación — curva de 24 valores
_RAMP_APPOINTMENT  = [
    0.22, 0.28, 0.34, 0.39, 0.44, 0.49, 0.54, 0.58, 0.62, 0.66, 0.69, 0.72,
    0.75, 0.77, 0.79, 0.80, 0.80, 0.80, 0.80, 0.80, 0.80, 0.80, 0.80, 1.00,
]
# Híbrido (shisha, etc.): intermedio
_RAMP_HYBRID       = [0.40, 0.50, 0.59, 0.66, 0.72, 0.77, 0.81, 0.85, 0.88, 0.91, 0.95, 1.00]
# Traspaso (con cartera heredada): arranque acelerado para todos
_RAMP_TRASPASO     = [0.60, 0.68, 0.75, 0.81, 0.86, 0.90, 0.94, 0.97, 0.99, 1.00, 1.00, 1.00]

# Mapa sector → curva (para negocios nuevos)
_RAMP_POR_SECTOR: dict[str, list[float]] = {
    "restauracion":  _RAMP_RESTAURACION,
    "moda":          _RAMP_RETAIL,
    "supermercado":  _RAMP_RETAIL,
    "farmacia":      _RAMP_RETAIL,
    "tatuajes":      _RAMP_APPOINTMENT,
    "estetica":      _RAMP_APPOINTMENT,
    "clinica":       _RAMP_APPOINTMENT,
    "peluqueria":    _RAMP_APPOINTMENT,
    "shisha_lounge": _RAMP_HYBRID,
}

# ── Estacionalidad mensual por sector ─────────────────────────────────────────
# Índice 0 = Enero. Normalizado: la media de los 12 coeficientes ≈ 1.0
# Fuente: benchmarks sector retail/restauración BCN + patronal sectorial
_SEASONAL: dict[str, list[float]] = {
    # Restauración: pico Dic, caída Ago (-25%), punta Jun-Jul turismo
    "restauracion": [0.85, 0.82, 0.91, 0.96, 1.03, 1.06, 1.04, 0.75, 1.05, 1.04, 1.00, 1.22],
    # Moda: pico Nav/Dic, rebajas Ene-Feb (-35%), caída Ago
    "moda":         [0.65, 0.68, 0.88, 0.97, 1.07, 0.96, 0.86, 0.80, 1.00, 1.06, 1.12, 1.38],
    # Tatuajes: relativamente estable, ligero pico verano (turistas)
    "tatuajes":     [0.88, 0.84, 0.96, 1.01, 1.05, 1.12, 1.10, 0.97, 1.02, 1.00, 0.97, 0.97],
    # Estética: pico Nav y primavera (bodas), caída Ago
    "estetica":     [0.90, 0.87, 1.00, 1.04, 1.06, 1.04, 0.90, 0.84, 1.00, 1.00, 1.05, 1.15],
    # Peluquería: pico Nav y verano (bodas), caída Feb
    "peluqueria":   [0.90, 0.87, 0.98, 1.00, 1.03, 1.07, 0.96, 0.85, 1.00, 0.98, 1.00, 1.21],
    # Farmacia: pico Ene (gripes), estable resto, sin picos grandes
    "farmacia":     [1.12, 1.02, 0.96, 0.93, 0.91, 0.88, 0.90, 0.91, 0.95, 1.00, 1.05, 1.10],
    # Supermercado: estable, leve pico navidad
    "supermercado": [0.96, 0.91, 0.96, 0.99, 1.01, 1.01, 1.05, 1.01, 1.01, 1.00, 1.02, 1.09],
    # Shisha lounge: pico verano y navidad, flojo Feb
    "shisha_lounge":[0.88, 0.84, 0.94, 1.00, 1.06, 1.12, 1.13, 0.86, 1.00, 1.00, 1.03, 1.12],
    # Default: sin estacionalidad
    "_default":     [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
}

# Factores de escenario — FIJOS
_SCENARIO_FACTORS: dict[str, float] = {
    "conservador": 0.60,
    "base":        1.00,
    "optimista":   1.20,
}

# Factor estrés: simula apertura en malas condiciones + evento inesperado
# (obra en la calle, crisis de sector, baja clave en mes 1-4)
_STRESS_FACTOR = 0.40

# ── Ocupación sostenible ──────────────────────────────────────────────────────
MAX_OCCUPANCY   = 0.80  # default global (exportado para compatibilidad)
_OCCUPANCY_WARN = 0.85

# Ocupación máxima sostenible por sector — refleja diferencias operativas reales
_MAX_OCCUPANCY_SECTOR: dict[str, float] = {
    "restauracion": 0.90,  # alta rotación, turnos
    "supermercado": 0.85,
    "moda":         0.85,
    "farmacia":     0.82,
    "shisha_lounge":0.80,
    "estetica":     0.75,  # cita previa, preparación entre clientes
    "peluqueria":   0.75,
    "tatuajes":     0.70,  # sesiones largas, higiene entre clientes
    "clinica":      0.75,
}


def get_max_occupancy(sector: str) -> float:
    """Devuelve la ocupación máxima sostenible para el sector dado."""
    return _MAX_OCCUPANCY_SECTOR.get(sector, MAX_OCCUPANCY)

# ── Productividad del personal ────────────────────────────────────────────────
_STAFF_EFFICIENCY = 0.80

_PRODUCTIVITY_RATES: dict[str, tuple[float, float]] = {
    "appointment_based": ( 5.0,  7.0),
    "retail_walkin":     ( 8.0, 15.0),
    "restaurant":        (10.0, 20.0),
    "hybrid":            ( 6.0, 12.0),
}

# ── Umbrales de coherencia global ─────────────────────────────────────────────
_MAX_PROFIT_RATIO    = 0.50
_NET_MARGIN_WARN     = 0.35
_NET_MARGIN_WEAK     = 0.05
_MAX_ROI_FLAG        = 3.00
_MAX_REVENUE_PER_EMP = 30_000
_MIN_REVENUE_PER_EMP = 2_500

_VOLATILITY_BAND = 0.15


async def calcular_proyeccion(
    params: dict,
    tipo_negocio: str = "nuevo",
    sector: str = "_default",
    mes_apertura: int = 1,
) -> dict:
    """
    Modelo financiero de 12 pasos con mejoras v4:
    - Ramp-up diferenciado por sector y tipo de negocio
    - Estacionalidad mensual (asume apertura en Enero como referencia conservadora)
    - Escenario de estrés (×0.40 ingresos, costes fijos intactos)
    - ROI = ganancia_neta / inversión (documentado explícitamente)

    Los escenarios conservador/base/optimista sólo escalan ingresos y costes variables.
    Los costes fijos (alquiler, salarios, otros) son CONSTANTES en todos los escenarios.
    """
    p = params

    # Seleccionar curva de ramp-up
    if tipo_negocio == "traspaso":
        ramp_curve = _RAMP_TRASPASO
    else:
        ramp_curve = _RAMP_POR_SECTOR.get(sector, _RAMP_RETAIL)

    seasonal_coeffs = _SEASONAL.get(sector, _SEASONAL["_default"])

    # Ocupación máxima del sector (CAMBIO 4: sector-specific, no global)
    max_occ = _MAX_OCCUPANCY_SECTOR.get(sector, MAX_OCCUPANCY)

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
        # CAMBIO 1: solo flag, sin modificar clients_base (corrección ya aplicada upstream)
        validation_flags.append(
            f"Capacidad del personal: {num_empleados} empleado(s) pueden atender "
            f"máx. {math.ceil(max_staff_cap)} clientes/día "
            f"(productividad {productivity_avg:.0f}/empleado/día × eficiencia {_STAFF_EFFICIENCY:.0%}). "
            f"Los ajustes de demanda se aplican antes de llegar aquí."
        )

    # ── PASO 3: Sobredimensión de plantilla ──────────────────────────────────
    empleados_necesarios = math.ceil(clients_base / max(0.1, productivity_avg * _STAFF_EFFICIENCY))
    if num_empleados > empleados_necesarios + 1:
        validation_flags.append(
            f"Sobredimensión de plantilla: {num_empleados} empleados para {round(clients_base)} "
            f"clientes/día — bastarían ~{empleados_necesarios}. "
            f"El exceso incrementa costes fijos sin generar ingresos adicionales."
        )

    # ── PASO 4: Safety cap — no debería activarse si el pipeline upstream funciona ──
    if clients_base > max_cap:
        validation_flags.append(
            f"Demanda base ({clients_base:.0f}) supera la capacidad máxima ({max_cap:.0f}). "
            f"Verifica las correcciones upstream."
        )
    clients_base = max(1.0, min(clients_base, max_cap))

    # ── Inversión inicial ──────────────────────────────────────────────────────
    inversion = (
        p["reforma_local"] + p["equipamiento"] +
        p["deposito_fianza"] + p["otros_iniciales"]
    )

    # ── Costes FIJOS mensuales (NO escalan entre escenarios) ──────────────────
    cf_mes      = p["alquiler_mensual"] + p["salarios_mensual"] + p["otros_fijos_mensual"]
    margen_unit = 1 - p["coste_mercancia_pct"]

    # ── PASO 8: Break-even ───────────────────────────────────────────────────
    ing_be     = cf_mes / max(0.01, margen_unit)
    denom_be   = (p["ticket_medio"] or 0.01) * (p["dias_apertura_mes"] or 1) * max_occ
    be_raw     = ing_be / denom_be
    be_clients = max(1, math.ceil(be_raw)) if math.isfinite(be_raw) else 1

    # ── Clientes por escenario (sólo escala ingresos) ─────────────────────────
    scenario_clients: dict[str, float] = {
        s: max(1.0, min(round(clients_base * f, 1), max_cap))
        for s, f in _SCENARIO_FACTORS.items()
    }
    stress_clients = max(1.0, min(round(clients_base * _STRESS_FACTOR, 1), max_cap))

    # ── Proyección 36 meses ────────────────────────────────────────────────────
    proyeccion: list[dict] = []
    acum: dict[str, float] = {s: -inversion for s in _SCENARIO_FACTORS}
    acum_stress = -inversion  # estrés parte de la misma inversión inicial

    for mes in range(1, 37):
        ramp_raw = ramp_curve[min(mes - 1, len(ramp_curve) - 1)]
        ramp     = min(ramp_raw, max_occ)

        # CAMBIO 2: estacionalidad desde el mes real de apertura
        seasonal = seasonal_coeffs[(mes_apertura - 1 + mes - 1) % 12]

        row: dict = {"mes": mes, "ramp_factor": round(ramp, 3), "costes_fijos": round(cf_mes)}
        base_cogs = 0.0

        for s in _SCENARIO_FACTORS:
            cl     = scenario_clients[s]
            # Los ingresos escalan por el factor de escenario Y la estacionalidad
            # Los costes fijos son CONSTANTES (no escalan con los ingresos)
            ing    = p["ticket_medio"] * cl * p["dias_apertura_mes"] * ramp * seasonal
            cogs   = ing * p["coste_mercancia_pct"]
            ebitda = ing - cogs - cf_mes  # cf_mes es constante para todos los escenarios
            acum[s] += ebitda

            row[f"ingresos_{s}"]  = round(ing)
            row[f"ebitda_{s}"]    = round(ebitda)
            row[f"acumulado_{s}"] = round(acum[s])

            if s == "base":
                base_cogs = round(cogs)
                row["ingresos_base_low"]  = round(ing * (1 - _VOLATILITY_BAND))
                row["ingresos_base_high"] = round(ing * (1 + _VOLATILITY_BAND))

        # Escenario estrés: ingresos × 0.40 con costes fijos INTACTOS
        # Simula los peores primeros meses (obra en la calle, baja de trabajador clave, etc.)
        ing_stress    = p["ticket_medio"] * stress_clients * p["dias_apertura_mes"] * ramp * seasonal
        cogs_stress   = ing_stress * p["coste_mercancia_pct"]
        ebitda_stress = ing_stress - cogs_stress - cf_mes  # cf_mes intacto (crítico)
        acum_stress  += ebitda_stress

        row["ingresos_stress"]  = round(ing_stress)
        row["ebitda_stress"]    = round(ebitda_stress)
        row["acumulado_stress"] = round(acum_stress)
        row["costes_variables"] = base_cogs
        proyeccion.append(row)

    # ── KPIs ──────────────────────────────────────────────────────────────────
    def _sum_anual(key: str) -> int:
        return round(sum(r[key] for r in proyeccion[:12]))

    def _payback(s: str) -> int:
        return next((r["mes"] for r in proyeccion if r[f"acumulado_{s}"] >= 0), 999)

    def _roi(s: str) -> float:
        """
        ROI = ganancia_neta / inversión.
        acumulado_36 parte de -inversión y acumula EBITDA mensual.
        Por tanto: acumulado_36 = -inversión + EBITDA_total
        ROI = (acumulado_36 + inversión) / inversión = EBITDA_total / inversión
        """
        return round((proyeccion[-1][f"acumulado_{s}"] + inversion) / inversion, 3) if inversion > 0 else 0.0

    roi_stress = round((proyeccion[-1]["acumulado_stress"] + inversion) / inversion, 3) if inversion > 0 else 0.0
    payback_stress = next((r["mes"] for r in proyeccion if r["acumulado_stress"] >= 0), 999)

    # ── PASO 5-7: Régimen estable base ────────────────────────────────────────
    cl_base      = scenario_clients["base"]
    ing_estable  = p["ticket_medio"] * cl_base * p["dias_apertura_mes"] * max_occ
    cogs_estable = ing_estable * p["coste_mercancia_pct"]
    ben_estable  = ing_estable - cogs_estable - cf_mes

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

    if ocupacion_efectiva > _OCCUPANCY_WARN:
        validation_flags.append(
            f"Ocupación efectiva {round(ocupacion_efectiva * 100)}% > 85% sostenida — "
            f"muy difícil de mantener. Considera ampliar capacidad o revisar la demanda base."
        )

    roi_base = _roi("base")
    if roi_base > _MAX_ROI_FLAG:
        validation_flags.append(
            f"ROI a 3 años de {round(roi_base * 100)}% supera el 300% — "
            f"revisa inversión inicial, costes fijos y ticket medio."
        )

    # Alerta estacionalidad: si el sector tiene alta variabilidad
    seasonal_max = max(seasonal_coeffs)
    seasonal_min = min(seasonal_coeffs)
    seasonal_range = seasonal_max - seasonal_min
    if seasonal_range >= 0.40:
        min_month = ["Ene","Feb","Mar","Abr","May","Jun","Jul","Ago","Sep","Oct","Nov","Dic"][seasonal_coeffs.index(seasonal_min)]
        max_month = ["Ene","Feb","Mar","Abr","May","Jun","Jul","Ago","Sep","Oct","Nov","Dic"][seasonal_coeffs.index(seasonal_max)]
        validation_flags.append(
            f"Alta estacionalidad detectada para el sector: pico en {max_month} "
            f"({round(seasonal_max*100)}%) vs. mínimo en {min_month} ({round(seasonal_min*100)}%). "
            f"La tesorería en temporada baja puede ser negativa aunque el modelo anual sea positivo."
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

    # ── Gráfico break-even ────────────────────────────────────────────────────
    chart_max = max(cl_base * 2.0, float(be_clients) * 2.5, 50.0)
    break_even_chart = [
        {
            "clientes":       round(chart_max * i / 20, 1),
            "ingresos":       round(
                p["ticket_medio"] * (chart_max * i / 20) * p["dias_apertura_mes"] * max_occ
            ),
            "costes_totales": round(
                p["ticket_medio"] * (chart_max * i / 20) * p["dias_apertura_mes"]
                * max_occ * p["coste_mercancia_pct"] + cf_mes
            ),
        }
        for i in range(21)
    ]

    # ── Análisis de sensibilidad (impacto ±10% por variable sobre EBITDA año 1) ──
    # Calcula el cambio en EBITDA anual base si cada variable sube un 10%
    ebitda_base_anual = _sum_anual("ebitda_base")
    ticket_base  = p["ticket_medio"]
    cl_b         = cl_base
    dias         = p["dias_apertura_mes"]
    cogs_pct     = p["coste_mercancia_pct"]

    def _ebitda_a1_con(new_ticket=None, new_cl=None, new_cogs=None,
                       new_alquiler=None, new_salarios=None, new_otros=None):
        t  = new_ticket   or ticket_base
        cl = new_cl       or cl_b
        cg = new_cogs     if new_cogs is not None else cogs_pct
        al = new_alquiler or p["alquiler_mensual"]
        sa = new_salarios or p["salarios_mensual"]
        ot = new_otros    or p["otros_fijos_mensual"]
        total = 0.0
        for row in proyeccion[:12]:
            ramp = row["ramp_factor"]
            seas = seasonal_coeffs[(row["mes"] - 1) % 12]
            ing  = t * cl * dias * ramp * seas
            total += ing * (1 - cg) - (al + sa + ot)
        return round(total)

    sens = [
        {
            "variable": "ticket_medio",
            "label":    "Ticket medio +10%",
            "impacto_ebitda": _ebitda_a1_con(new_ticket=ticket_base * 1.10) - ebitda_base_anual,
        },
        {
            "variable": "clientes_dia",
            "label":    "Clientes/día +10%",
            "impacto_ebitda": _ebitda_a1_con(new_cl=cl_b * 1.10) - ebitda_base_anual,
        },
        {
            "variable": "alquiler",
            "label":    "Alquiler +10%",
            "impacto_ebitda": _ebitda_a1_con(new_alquiler=p["alquiler_mensual"] * 1.10) - ebitda_base_anual,
        },
        {
            "variable": "salarios",
            "label":    "Salarios +10%",
            "impacto_ebitda": _ebitda_a1_con(new_salarios=p["salarios_mensual"] * 1.10) - ebitda_base_anual,
        },
        {
            "variable": "coste_mercancia",
            "label":    "Coste mercancía +10%",
            "impacto_ebitda": _ebitda_a1_con(new_cogs=cogs_pct * 1.10) - ebitda_base_anual,
        },
    ]
    # Ordenar por impacto absoluto (mayor al menor)
    sens.sort(key=lambda x: abs(x["impacto_ebitda"]), reverse=True)

    return {
        "clients_per_day":              cl_base,
        "max_capacity":                 max_cap,
        "max_occupancy_usado":          round(max_occ, 3),
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
        "ebitda_anual_stress":          _sum_anual("ebitda_stress"),
        "roi_3a_conservador":           _roi("conservador"),
        "roi_3a_base":                  roi_base,
        "roi_3a_optimista":             _roi("optimista"),
        "roi_3a_stress":                roi_stress,
        "payback_meses_conservador":    _payback("conservador"),
        "payback_meses_base":           _payback("base"),
        "payback_meses_optimista":      _payback("optimista"),
        "payback_meses_stress":         payback_stress,
        "breakeven_clientes_dia":       be_clients,
        "proyeccion":                   proyeccion,
        "estructura_costes":            estructura_costes,
        "break_even_chart":             break_even_chart,
        "validation_flags":             list(dict.fromkeys(validation_flags)),
        "sensibilidad":                 sens,
        "sector_usado":                 sector,
        "seasonal_coeffs":              seasonal_coeffs,
    }
