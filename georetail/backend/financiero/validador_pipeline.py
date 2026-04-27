"""
financiero/validador_pipeline.py — Pipeline determinista de clasificación, validación y corrección.

Ejecuta 4 pasos secuenciales antes de calcular_proyeccion():

  PASO 1   → Clasificación (sector, modelo capacidad, CNAE, confianza)
  PASO 1.5 → Restricciones físicas (aforo_max por m²)
  PASO 2   → Benchmarks base embebidos por subsector
  PASO 3   → 8 validaciones duras obligatorias (capacidad, temporal, espacio,
              productividad, ticket, costes salariales, unit economics)
  PASO 4   → Output estructurado JSON-serializable

Pure Python, sin I/O. Llamar desde api/financiero.py antes de calcular_proyeccion().
El PipelineResult enriquece correcciones_aplicadas y validation_flags de la respuesta.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Optional


# ── Taxonomía ──────────────────────────────────────────────────────────────────
# sector_code → (sector_display, subsector_display, modelo, cnae, tipo_coste, confidence)
_TAXONOMY: dict[str, tuple[str, str, str, str, str, float]] = {
    "restauracion":  ("Restauración",           "Restauración casual",        "space",   "5610", "stock",    0.95),
    "bar":           ("Restauración",           "Bar / pub",                  "space",   "5630", "stock",    0.90),
    "cafeteria":     ("Restauración",           "Cafetería / pastelería",     "space",   "5630", "stock",    0.90),
    "shisha_lounge": ("Ocio y entretenimiento", "Shisha lounge",              "space",   "5630", "stock",    0.85),
    "moda":          ("Retail moda",            "Moda generalista",           "traffic", "4771", "stock",    0.92),
    "supermercado":  ("Retail alimentación",    "Supermercado / bazar",       "traffic", "4711", "stock",    0.95),
    "farmacia":      ("Salud y farmacia",        "Farmacia",                  "traffic", "4773", "stock",    0.95),
    "electronica":   ("Retail tecnología",      "Electrónica / telefonía",    "traffic", "4742", "stock",    0.88),
    "tatuajes":      ("Belleza y estética",     "Tatuajes y piercings",       "labor",   "9602", "servicio", 0.93),
    "estetica":      ("Belleza y estética",     "Estética y tratamientos",    "labor",   "9602", "servicio", 0.92),
    "peluqueria":    ("Belleza y estética",     "Peluquería",                 "labor",   "9602", "servicio", 0.95),
    "clinica":       ("Salud",                  "Clínica médica",             "labor",   "8690", "servicio", 0.90),
    "fisioterapia":  ("Salud",                  "Fisioterapia",               "labor",   "8621", "servicio", 0.92),
    "dentista":      ("Salud",                  "Clínica dental",             "labor",   "8621", "servicio", 0.92),
}

_DEFAULT_TAXONOMY = ("Comercio / Servicios", "Otro", "traffic", "4799", "stock", 0.50)

# ── Benchmarks por sector ──────────────────────────────────────────────────────
# ticket_range:    [min €, p50 €, max €]
# cogs_range:      [min_pct, max_pct]  (0–1)
# duracion_min:    minutos por servicio (solo modelo "labor")
# productividad:   clientes/hora/empleado (labor/traffic) | null para space
# rotacion:        renovaciones de la capacidad total por hora (solo modelo "space")
#                  Ejemplo: 0.30 → restaurante de 40 plazas hace ~96 cubiertos/8h
# salario_base:    €/mes convenio mínimo sectorial

_BENCH: dict[str, dict] = {
    "restauracion":  {
        "ticket_range": [12.0, 22.0, 50.0], "cogs_range": [0.28, 0.38],
        "duracion_min": None, "productividad": None, "rotacion": 0.30, "salario_base": 1400.0,
    },
    "bar":           {
        "ticket_range": [5.0, 12.0, 25.0],  "cogs_range": [0.25, 0.38],
        "duracion_min": None, "productividad": None, "rotacion": 0.50, "salario_base": 1400.0,
    },
    "cafeteria":     {
        "ticket_range": [4.0, 8.0, 18.0],   "cogs_range": [0.25, 0.40],
        "duracion_min": None, "productividad": None, "rotacion": 0.55, "salario_base": 1400.0,
    },
    "shisha_lounge": {
        "ticket_range": [20.0, 35.0, 75.0], "cogs_range": [0.15, 0.30],
        "duracion_min": None, "productividad": None, "rotacion": 0.15, "salario_base": 1400.0,
    },
    "moda":          {
        "ticket_range": [30.0, 75.0, 200.0],"cogs_range": [0.40, 0.60],
        "duracion_min": None, "productividad": 10.0, "rotacion": None, "salario_base": 1400.0,
    },
    "supermercado":  {
        "ticket_range": [20.0, 35.0, 70.0], "cogs_range": [0.55, 0.75],
        "duracion_min": None, "productividad": 15.0, "rotacion": None, "salario_base": 1400.0,
    },
    "farmacia":      {
        "ticket_range": [12.0, 24.0, 60.0], "cogs_range": [0.30, 0.45],
        "duracion_min": None, "productividad": 12.0, "rotacion": None, "salario_base": 1500.0,
    },
    "electronica":   {
        "ticket_range": [30.0, 80.0, 300.0],"cogs_range": [0.45, 0.65],
        "duracion_min": None, "productividad": 8.0,  "rotacion": None, "salario_base": 1400.0,
    },
    "tatuajes":      {
        "ticket_range": [80.0, 150.0, 450.0],"cogs_range": [0.08, 0.20],
        "duracion_min": 120.0,"productividad": 0.5,  "rotacion": None, "salario_base": 1400.0,
    },
    "estetica":      {
        "ticket_range": [30.0, 65.0, 160.0],"cogs_range": [0.10, 0.25],
        "duracion_min": 60.0, "productividad": 0.8,  "rotacion": None, "salario_base": 1400.0,
    },
    "peluqueria":    {
        "ticket_range": [15.0, 35.0, 80.0], "cogs_range": [0.10, 0.20],
        "duracion_min": 45.0, "productividad": 1.0,  "rotacion": None, "salario_base": 1400.0,
    },
    "clinica":       {
        "ticket_range": [40.0, 90.0, 200.0],"cogs_range": [0.08, 0.20],
        "duracion_min": 45.0, "productividad": 1.0,  "rotacion": None, "salario_base": 1800.0,
    },
    "fisioterapia":  {
        "ticket_range": [35.0, 60.0, 100.0],"cogs_range": [0.05, 0.15],
        "duracion_min": 45.0, "productividad": 1.1,  "rotacion": None, "salario_base": 1600.0,
    },
    "dentista":      {
        "ticket_range": [40.0, 80.0, 200.0],"cogs_range": [0.10, 0.25],
        "duracion_min": 60.0, "productividad": 0.9,  "rotacion": None, "salario_base": 1800.0,
    },
    "_default":      {
        "ticket_range": [15.0, 40.0, 150.0],"cogs_range": [0.20, 0.50],
        "duracion_min": None, "productividad": 8.0,  "rotacion": None, "salario_base": 1400.0,
    },
}

# Factor de utilización realista sostenible por modelo
_FACTOR_UTIL: dict[str, float] = {"labor": 0.85, "space": 0.75, "traffic": 0.60}

# m² mínimos por persona para el cálculo de aforo
_M2_POR_PERSONA: dict[str, float] = {"traffic": 2.5, "labor": 3.0, "space": 1.5}

_SS_EMPRESA = 0.30  # Seguridad Social a cargo de la empresa sobre salario bruto


# ── Data classes ───────────────────────────────────────────────────────────────

@dataclass
class BusinessInput:
    """Parámetros brutos del negocio — entrada del pipeline."""
    sector:               str
    m2_local:             float
    empleados:            int
    horas_apertura:       float          # horas/día
    clientes_dia:         float
    ticket_medio:         float          # €
    cogs_pct:             float          # 0–1
    salario_base_mensual: float          # € bruto/mes por empleado
    duracion_servicio:    Optional[float] = None   # minutos (labor)
    rotacion_mesas:       Optional[float] = None   # renovaciones/hora capacidad (space)


@dataclass
class PipelineResult:
    classification:         dict
    benchmarks:             dict
    constraints:            dict
    operational_validation: dict
    corrections:            list[dict]
    flags:                  list[str]

    def to_dict(self) -> dict:
        return {
            "classification":         self.classification,
            "benchmarks":             self.benchmarks,
            "constraints":            self.constraints,
            "operational_validation": self.operational_validation,
            "corrections":            self.corrections,
            "flags":                  self.flags,
        }


class _Adj:
    """Estado mutable durante las validaciones. Acumula correcciones y flags."""
    __slots__ = ("inp", "corrections", "flags", "clientes", "ticket", "cogs", "salario")

    def __init__(
        self, inp: BusinessInput, corrections: list[dict], flags: list[str]
    ) -> None:
        self.inp         = inp
        self.corrections = corrections
        self.flags       = flags
        self.clientes    = float(inp.clientes_dia)
        self.ticket      = float(inp.ticket_medio)
        self.cogs        = float(inp.cogs_pct)
        self.salario     = float(inp.salario_base_mensual)

    def fix(self, f: str, orig: float, new: float, reason: str) -> None:
        if abs(orig - new) > 0.001:
            self.corrections.append({
                "field":     f,
                "original":  str(round(orig, 2)),
                "corrected": str(round(new, 2)),
                "reason":    reason,
            })

    def add_flag(self, flag: str) -> None:
        if flag not in self.flags:
            self.flags.append(flag)


# ── Main entry point ───────────────────────────────────────────────────────────

def run_pipeline(inp: BusinessInput) -> PipelineResult:
    """
    Ejecuta el pipeline completo y devuelve PipelineResult.

    Uso desde api/financiero.py:
        result = run_pipeline(BusinessInput(...))
        v["clients_per_day"] = min(v["clients_per_day"], result.constraints["adjusted_clients"])
        validation_flags.extend(result.flags)
        correcciones_raw.extend(result.corrections)
    """
    corrections: list[dict] = []
    flags: list[str] = []

    # PASO 1 — Clasificación
    classification = _paso1_clasificar(inp)
    modelo = classification["model"]
    if classification["classification_confidence"] < 0.70:
        if "low_classification_confidence" not in flags:
            flags.append("low_classification_confidence")

    # PASO 1.5 — Aforo
    aforo_max = _paso1_5_aforo(inp.m2_local, modelo)

    # PASO 2 — Benchmarks
    bench    = _BENCH.get(inp.sector, _BENCH["_default"])
    duracion = inp.duracion_servicio or bench["duracion_min"]
    rotacion = inp.rotacion_mesas or bench["rotacion"]
    benchmarks = _paso2_benchmarks(bench, modelo)

    # PASO 3 — Validaciones duras
    adj = _Adj(inp, corrections, flags)

    _v1_capacidad_vs_demanda(adj, modelo, duracion, rotacion, aforo_max)
    _v3_validacion_temporal(adj, modelo, duracion)
    _v4_coherencia_espacio(adj, aforo_max)
    _v5_productividad_irreal(adj, modelo)
    _v6_ticket_fuera_mercado(adj, bench)
    _v7_costes_salariales(adj, bench)
    _v8_unit_economics(adj)

    # Métricas operativas
    theo_max      = _capacidad_teorica(inp, modelo, duracion, rotacion, aforo_max)
    hours_req     = _hours_required(adj.clientes, duracion, modelo, inp.horas_apertura, inp.empleados)
    required_staff = _required_staff(adj.clientes, modelo, inp.horas_apertura, duracion, bench)

    return PipelineResult(
        classification=classification,
        benchmarks=benchmarks,
        constraints={
            "max_daily_clients": round(theo_max, 1),
            "adjusted_clients":  round(adj.clientes, 1),
            "required_staff":    required_staff,
        },
        operational_validation={
            "theoretical_max_capacity": round(theo_max, 1),
            "utilization_rate":         round(_FACTOR_UTIL[modelo], 2),
            "hours_required_total":     round(hours_req, 1),
        },
        corrections=corrections,
        flags=flags,
    )


# ── Paso 1 — Clasificación ─────────────────────────────────────────────────────

def _paso1_clasificar(inp: BusinessInput) -> dict:
    entry = _TAXONOMY.get(inp.sector, None)
    if entry:
        sector_d, subsector_d, modelo, cnae, tipo_coste, conf = entry
    else:
        sector_d, subsector_d, modelo, cnae, tipo_coste, conf = _DEFAULT_TAXONOMY

    return {
        "sector":                  sector_d,
        "subsector":               subsector_d,
        "model":                   modelo,
        "cnae":                    cnae,
        "tipo_coste_variable":     tipo_coste,
        "classification_confidence": conf,
    }


# ── Paso 1.5 — Aforo ──────────────────────────────────────────────────────────

def _paso1_5_aforo(m2: float, modelo: str) -> float:
    m2_persona = _M2_POR_PERSONA.get(modelo, 2.5)
    return max(1.0, m2 / m2_persona)


# ── Paso 2 — Benchmarks ───────────────────────────────────────────────────────

def _paso2_benchmarks(bench: dict, modelo: str) -> dict:
    prod = bench["productividad"]
    if modelo == "labor":
        prod_str = f"{prod} clientes/hora/empleado" if prod else "0.5–1.2 clientes/hora/empleado"
    elif modelo == "traffic":
        prod_str = f"{prod} clientes/hora/empleado" if prod else "8–12 clientes/hora/empleado"
    else:
        rot = bench["rotacion"]
        prod_str = f"{rot} renovaciones/hora de aforo" if rot else "n/d"

    dur = bench["duracion_min"]
    return {
        "ticket_range": bench["ticket_range"],
        "cogs_range":   bench["cogs_range"],
        "productivity": prod_str,
        "service_time": f"{int(dur)} min" if dur else None,
    }


# ── Validaciones ──────────────────────────────────────────────────────────────

def _v1_capacidad_vs_demanda(
    adj: _Adj,
    modelo: str,
    duracion: Optional[float],
    rotacion: Optional[float],
    aforo_max: float,
) -> None:
    """Capacidad física vs demanda — ajuste con factor de utilización."""
    inp = adj.inp
    factor = _FACTOR_UTIL[modelo]

    if modelo == "labor":
        if not duracion or duracion <= 0:
            return
        cap_max = (inp.empleados * inp.horas_apertura * 60.0) / duracion
        if adj.clientes > cap_max:
            nuevo = round(cap_max * factor, 1)
            adj.fix(
                "clientes_dia", adj.clientes, nuevo,
                f"Modelo labor: capacidad máxima {round(cap_max, 1)} clientes/día "
                f"({inp.empleados} empl × {inp.horas_apertura}h × {factor:.0%} utilización). "
                f"Ajustado a {nuevo}.",
            )
            adj.clientes = nuevo

    elif modelo == "traffic":
        cap_min = inp.empleados * inp.horas_apertura * 8.0
        if adj.clientes < cap_min * 0.3:
            # No forzamos al alza, solo anotamos si es muy bajo
            adj.add_flag("underutilized")

    elif modelo == "space":
        if not rotacion or rotacion <= 0:
            return
        cap_max = aforo_max * rotacion * inp.horas_apertura
        if adj.clientes > cap_max:
            nuevo = round(cap_max * factor, 1)
            adj.fix(
                "clientes_dia", adj.clientes, nuevo,
                f"Modelo space: capacidad máxima {round(cap_max, 1)} cubiertos/día "
                f"(aforo {round(aforo_max, 0)} × {rotacion:.2f} rot/h × {inp.horas_apertura}h "
                f"× {factor:.0%} utilización). Ajustado a {nuevo}.",
            )
            adj.clientes = nuevo


def _v3_validacion_temporal(
    adj: _Adj,
    modelo: str,
    duracion: Optional[float],
) -> None:
    """Comprueba que los clientes son físicamente posibles en el tiempo de apertura."""
    if modelo != "labor" or not duracion or duracion <= 0:
        return
    inp = adj.inp
    horas_necesarias = (adj.clientes * duracion) / (inp.empleados * 60.0)
    if horas_necesarias > inp.horas_apertura:
        cap_corregida = round((inp.empleados * inp.horas_apertura * 60.0) / duracion, 1)
        adj.fix(
            "clientes_dia", adj.clientes, cap_corregida,
            f"Tiempo insuficiente: {round(horas_necesarias, 1)}h necesarias > "
            f"{inp.horas_apertura}h de apertura. Ajustado a {cap_corregida} clientes.",
        )
        adj.clientes = cap_corregida
        adj.add_flag("time_bottleneck")


def _v4_coherencia_espacio(adj: _Adj, aforo_max: float) -> None:
    """Clientes simultáneos no pueden superar el aforo físico."""
    inp = adj.inp
    if inp.horas_apertura <= 0:
        return
    clientes_simultaneos = adj.clientes / inp.horas_apertura
    if clientes_simultaneos > aforo_max:
        nuevo = round(aforo_max * inp.horas_apertura * 0.5, 1)
        adj.fix(
            "clientes_dia", adj.clientes, nuevo,
            f"Clientes simultáneos estimados ({round(clientes_simultaneos, 1)}) "
            f"superan el aforo físico ({round(aforo_max, 0)}). Ajustado a {nuevo}.",
        )
        adj.clientes = nuevo
        adj.add_flag("physical_space_limit")


def _v5_productividad_irreal(adj: _Adj, modelo: str) -> None:
    """Detecta productividad irreal (infrautilización o sobrecapacidad)."""
    inp = adj.inp
    if inp.empleados <= 0 or inp.horas_apertura <= 0:
        return
    prod_real = adj.clientes / (inp.empleados * inp.horas_apertura)

    if modelo == "traffic" and prod_real < 5.0:
        adj.add_flag("underutilized")

    elif modelo == "labor" and prod_real > 2.0:
        cap_max = inp.empleados * inp.horas_apertura * 2.0
        adj.fix(
            "clientes_dia", adj.clientes, round(cap_max, 1),
            f"Productividad irreal: {round(prod_real, 2)} clientes/h/empleado > 2.0 "
            f"para modelo labor. Ajustado a {round(cap_max, 1)}.",
        )
        adj.clientes = round(cap_max, 1)
        adj.add_flag("overcapacity")


def _v6_ticket_fuera_mercado(adj: _Adj, bench: dict) -> None:
    """Ticket medio fuera del rango de mercado del sector."""
    tmin, _, tmax = bench["ticket_range"]
    flag_needed = False

    if adj.ticket > tmax:
        adj.fix(
            "ticket_medio", adj.ticket, tmax,
            f"Ticket {round(adj.ticket, 2)} € supera el máximo de mercado ({tmax} €). Ajustado.",
        )
        adj.ticket = tmax
        flag_needed = True

    elif adj.ticket < tmin:
        adj.fix(
            "ticket_medio", adj.ticket, tmin,
            f"Ticket {round(adj.ticket, 2)} € inferior al mínimo de mercado ({tmin} €). Ajustado.",
        )
        adj.ticket = tmin
        flag_needed = True

    if flag_needed:
        adj.add_flag("unrealistic_ticket")


def _v7_costes_salariales(adj: _Adj, bench: dict) -> None:
    """Salario bruto + SS empresa no puede ser inferior al convenio sectorial."""
    sal_min = bench["salario_base"]
    coste_min = sal_min * (1.0 + _SS_EMPRESA)
    coste_declarado = adj.salario * (1.0 + _SS_EMPRESA)

    if coste_declarado < coste_min:
        adj.fix(
            "salario_base_mensual", adj.salario, sal_min,
            f"Salario declarado ({round(adj.salario, 2)} €) inferior al convenio "
            f"sectorial mínimo ({sal_min} €). Coste empresa corregido a "
            f"{round(coste_min, 2)} €/empleado/mes.",
        )
        adj.salario = sal_min
        adj.add_flag("invalid_costs")


def _v8_unit_economics(adj: _Adj) -> None:
    """Margen unitario y cobertura del coste de personal."""
    margen_unit = adj.ticket * (1.0 - adj.cogs)

    if margen_unit <= 0:
        adj.add_flag("negative_unit_economics")
        return

    inp = adj.inp
    if inp.empleados > 0 and adj.clientes > 0:
        coste_personal_dia = (adj.salario * (1.0 + _SS_EMPRESA) * inp.empleados) / 26.0
        coste_por_cliente = coste_personal_dia / adj.clientes
        if margen_unit < coste_por_cliente:
            adj.add_flag("unprofitable_unit")


# ── Helpers ────────────────────────────────────────────────────────────────────

def _capacidad_teorica(
    inp: BusinessInput,
    modelo: str,
    duracion: Optional[float],
    rotacion: Optional[float],
    aforo_max: float,
) -> float:
    if modelo == "labor":
        if duracion and duracion > 0:
            return (inp.empleados * inp.horas_apertura * 60.0) / duracion
        return inp.empleados * inp.horas_apertura * 1.0
    if modelo == "space":
        if rotacion and rotacion > 0:
            return aforo_max * rotacion * inp.horas_apertura
        return aforo_max * 2.0
    # traffic
    bench = _BENCH.get(inp.sector, _BENCH["_default"])
    prod = bench["productividad"] or 8.0
    return inp.empleados * inp.horas_apertura * prod


def _hours_required(
    clientes: float,
    duracion: Optional[float],
    modelo: str,
    horas_apertura: float,
    empleados: int,
) -> float:
    if modelo == "labor" and duracion and duracion > 0 and empleados > 0:
        return (clientes * duracion) / (empleados * 60.0)
    return horas_apertura


def _required_staff(
    clientes: float,
    modelo: str,
    horas_apertura: float,
    duracion: Optional[float],
    bench: dict,
) -> int:
    if modelo == "labor" and duracion and duracion > 0:
        return max(1, math.ceil((clientes * duracion) / (horas_apertura * 60.0)))
    prod = bench.get("productividad") or 8.0
    return max(1, math.ceil(clientes / (horas_apertura * prod)))
