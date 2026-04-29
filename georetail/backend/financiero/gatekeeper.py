"""
financiero/gatekeeper.py — Financial Engineering Gatekeeper (8-layer deterministic pipeline)

Transforms raw business inputs into physically-constrained revenue stream structures.
Pure function: no I/O, no side effects.

Layers:
  1  Stream decomposition by sector
  2  Per-stream parameter binding
  3  Shared resources
  4  Hard physical constraints (SHIELD)
  5  Audit and correction
  6  Adjusted metrics per stream
  7  Global constraints
  8  JSON output
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Optional

StreamType = Literal["traffic", "space", "labor", "asset"]
Priority   = Literal["primary", "secondary"]

# ─── CAPA 1: Sector → stream topology ─────────────────────────────────────────

_SECTOR_STREAMS: dict[str, list[tuple[str, str]]] = {
    "restauracion":  [("space",   "primary"),  ("traffic", "secondary")],
    "moda":          [("traffic", "primary")],
    "estetica":      [("labor",   "primary")],
    "tatuajes":      [("labor",   "primary")],
    "peluqueria":    [("labor",   "primary")],
    "shisha_lounge": [("space",   "primary")],
    "salud":         [("labor",   "primary")],
    "clinica":       [("labor",   "primary")],
    "deporte":       [("space",   "primary")],
    "educacion":     [("labor",   "primary"),  ("space",   "secondary")],
    "alimentacion":  [("traffic", "primary")],
    "supermercado":  [("traffic", "primary")],
    "farmacia":      [("traffic", "primary")],
    "_default":      [("traffic", "primary")],
}

# CAPA 4: utilisation factors and m2/person per stream type
_UTIL: dict[str, float] = {"traffic": 0.60, "space": 0.75, "labor": 0.85, "asset": 0.90}
_M2PP: dict[str, float] = {"traffic": 2.5,  "space": 1.5,  "labor": 3.0,  "asset": 5.0}

# ─── Input / Output dataclasses ───────────────────────────────────────────────

@dataclass
class GatekeeperInput:
    sector:               str
    total_m2:             float
    hours_open_per_day:   float
    total_staff:          int
    avg_ticket:           float
    flujo_peatonal_dia:   float          = 0.0
    conversion_rate:      float          = 0.05
    service_duration_min: Optional[float] = None
    seats_or_capacity:    Optional[float] = None
    avg_stay_min:         Optional[float] = None
    asset_units:          float          = 0.0
    asset_cycles_per_day: float          = 0.0


@dataclass
class GatekeeperResult:
    business_composition: dict
    shared_resources:     dict
    constraints:          dict
    corrections:          list[dict]
    confidence_score:     float

    def to_dict(self) -> dict:
        return {
            "business_composition": self.business_composition,
            "shared_resources":     self.shared_resources,
            "constraints":          self.constraints,
            "corrections":          self.corrections,
            "confidence_score":     self.confidence_score,
        }


# ─── Public entry point ───────────────────────────────────────────────────────

def run_gatekeeper(inp: GatekeeperInput) -> GatekeeperResult:
    """Execute the 8-layer gatekeeper pipeline. Returns a GatekeeperResult."""
    corrections:    list[dict] = []
    stream_results: list[dict] = []
    total_clients   = 0.0
    total_hours_req = 0.0

    for stype, priority in _SECTOR_STREAMS.get(inp.sector, _SECTOR_STREAMS["_default"]):
        name = f"{stype}_{inp.sector}"
        logic_params, daily_clients = _compute_stream(stype, inp, corrections, name)
        total_clients   += daily_clients
        total_hours_req += _hours_required(stype, daily_clients, inp)
        stream_results.append({
            "type":     stype,
            "name":     name,
            "priority": priority,
            "logic_params": logic_params,
            "adjusted_metrics": {
                "daily_clients": round(daily_clients, 1),
                "avg_ticket":    round(inp.avg_ticket, 2),
            },
        })

    # CAPA 7: global constraints
    staff_hrs = inp.total_staff * inp.hours_open_per_day
    util_pct  = min(total_hours_req / staff_hrs, 1.0) if staff_hrs > 0 else 0.0

    if total_hours_req > staff_hrs:
        corrections.append(_corr(
            "hours_required_total",
            round(total_hours_req, 2), round(staff_hrs, 2),
            "Total hours required exceeded staff capacity (staff × hours_open).",
        ))
        total_hours_req = staff_hrs

    confidence = max(0.30, round(1.0 - len(corrections) * 0.10, 2))

    return GatekeeperResult(
        business_composition={
            "is_hybrid": len(stream_results) > 1,
            "streams":   stream_results,
        },
        shared_resources={
            "total_m2":           inp.total_m2,
            "hours_open_per_day": inp.hours_open_per_day,
            "total_staff":        inp.total_staff,
        },
        constraints={
            "max_capacity":          round(total_clients, 1),
            "staff_utilization_pct": round(util_pct, 3),
            "hours_required_total":  round(total_hours_req, 2),
        },
        corrections=corrections,
        confidence_score=confidence,
    )


# ─── Per-stream computation (CAPAS 2+4+5+6) ───────────────────────────────────

def _compute_stream(
    stype: str, inp: GatekeeperInput, corrections: list[dict], name: str,
) -> tuple[dict, float]:
    if stype == "traffic": return _traffic(inp, corrections, name)
    if stype == "space":   return _space(inp, corrections, name)
    if stype == "labor":   return _labor(inp, corrections, name)
    if stype == "asset":   return _asset(inp, corrections, name)
    return {}, 0.0


def _traffic(inp: GatekeeperInput, corrections: list[dict], name: str) -> tuple[dict, float]:
    conv    = min(max(inp.conversion_rate, 0.0), 1.0)
    clients = inp.flujo_peatonal_dia * conv

    # CAPA 4: max_operational_capacity = m2 / 2.5
    max_op   = inp.total_m2 / _M2PP["traffic"]
    # CAPA 4: productivity max = 20 clients/hr/employee
    max_prod = inp.total_staff * inp.hours_open_per_day * 20 if inp.total_staff > 0 else max_op

    if clients > max_op:
        corrections.append(_corr(f"{name}.daily_clients", clients, max_op,
            "Exceeds max_operational_capacity (m2 / 2.5)."))
        clients = max_op
    if clients > max_prod:
        corrections.append(_corr(f"{name}.daily_clients", clients, max_prod,
            "Exceeds max staff productivity (20 clients/hr/employee)."))
        clients = max_prod

    return (
        {"avg_ticket": inp.avg_ticket, "conversion_rate": round(conv, 4)},
        round(clients * _UTIL["traffic"], 1),
    )


def _space(inp: GatekeeperInput, corrections: list[dict], name: str) -> tuple[dict, float]:
    # CAPA 4: max_capacity_space = m2 / 1.5
    max_space = inp.total_m2 / _M2PP["space"]
    seats     = inp.seats_or_capacity if inp.seats_or_capacity else max_space

    if seats > max_space:
        corrections.append(_corr(f"{name}.seats_or_capacity", seats, max_space,
            "Exceeds max_capacity_space (m2 / 1.5)."))
        seats = max_space

    stay      = inp.avg_stay_min or 60.0
    rotations = (inp.hours_open_per_day * 60.0) / stay if stay > 0 else 1.0
    clients   = seats * rotations * _UTIL["space"]

    return (
        {"seats_or_capacity": round(seats, 1), "avg_stay_min": round(stay, 1),
         "avg_ticket": inp.avg_ticket},
        round(clients, 1),
    )


def _labor(inp: GatekeeperInput, corrections: list[dict], name: str) -> tuple[dict, float]:
    dur       = inp.service_duration_min or 45.0
    # CAPA 4: max_labor_capacity = (staff × hours × 60) / duration_min
    max_labor = (inp.total_staff * inp.hours_open_per_day * 60.0) / dur if dur > 0 else 0.0
    # CAPA 4: productivity cap = 1.2 clients/hr/employee
    max_prod  = inp.total_staff * inp.hours_open_per_day * 1.2

    if max_labor > max_prod:
        corrections.append(_corr(f"{name}.labor_capacity",
            round(max_labor, 1), round(max_prod, 1),
            "Labor capacity capped at 1.2 clients/hr/employee."))
        max_labor = max_prod

    clients = max_labor * _UTIL["labor"]

    return (
        {"avg_ticket": inp.avg_ticket, "service_duration_min": round(dur, 1)},
        round(clients, 1),
    )


def _asset(inp: GatekeeperInput, corrections: list[dict], name: str) -> tuple[dict, float]:
    clients = inp.asset_units * inp.asset_cycles_per_day * _UTIL["asset"]
    return (
        {"units": inp.asset_units, "cycles_per_day": inp.asset_cycles_per_day,
         "avg_ticket": inp.avg_ticket},
        round(clients, 1),
    )


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _hours_required(stype: str, daily_clients: float, inp: GatekeeperInput) -> float:
    if stype == "labor":
        dur = inp.service_duration_min or 45.0
        return (daily_clients * dur) / 60.0
    if stype == "traffic":
        return daily_clients / 20.0  # 20 clients/hr/employee
    if stype == "space":
        stay = inp.avg_stay_min or 60.0
        return daily_clients * (stay / 60.0) * 0.20
    return 0.0


def _corr(field: str, original: float, corrected: float, reason: str) -> dict:
    return {
        "field":     field,
        "original":  round(original, 2),
        "corrected": round(corrected, 2),
        "reason":    reason,
    }
