"""
financiero/core.py — Núcleo de cálculo financiero v2 (FASE 1 — solo lectura).

Funciones puras (sin I/O, sin DB) para calcular capacidad física, demanda,
empleados y clientes finales con una jerarquía única y sin duplicados.

JERARQUÍA OBLIGATORIA:
  1. modelo    = get_modelo(sector)              → SECTOR_REGISTRY, inmutable
  2. bench     = merge(sector_bench, sub_bench)  → fuente única de benchmarks
  3. capacity  = physical_capacity(...)          → hard cap físico, UNA fórmula/modelo
  4. empleados = derive_staff(...)               → consecuencia de capacity, no causa
  5. demand    = estimate_demand(...)            → independiente de capacity y empleados
  6. clients   = final_clients(demand, capacity) → min(demand, capacity). Sin más.

INVARIANTES:
  - Un negocio tiene exactamente 1 modelo: "labor" | "space" | "traffic"
  - capacity es siempre un límite duro (clients nunca lo supera)
  - Los empleados no generan demanda; limitan la capacidad
  - No hay correcciones sobre clients después de final_clients()

ESTADO: Fase 1 — módulo aislado. No modifica código existente.
Ver estimador.py (legacy) y validador_pipeline.py (legacy) para el flujo actual.
"""
from __future__ import annotations

import logging
import math
from typing import Optional

logger = logging.getLogger(__name__)

# ── Constantes únicas (otros módulos deben importar desde aquí) ───────────────

SS_EMPRESA       = 0.31   # Seguridad Social empresa sobre salario bruto (~31%)
DEFAULT_OCCUPANCY = 0.65  # Ocupación base conservadora para negocios de cita
MAX_CAPTURE_RATE  = 0.15  # Nunca captar más del 15% del flujo peatonal diario

# Factor de utilización sostenible por modelo (refleja paradas, imprevistos, etc.)
_UTIL: dict[str, float] = {
    "labor":   0.85,   # 85% — hay huecos entre citas, tiempo de limpieza
    "space":   0.75,   # 75% — no todas las mesas ocupadas a la vez
    "traffic": 0.60,   # 60% — flujo peatonal irregular por franjas horarias
}

# m² de espacio físico por persona simultánea (aforo)
_M2_PER_PERSON: dict[str, float] = {
    "labor":   3.0,    # silla/camilla + espacio de trabajo
    "space":   1.5,    # mesa de restaurante incluye pasillo (código técnico)
    "traffic": 2.5,    # retail: clientes se mueven, necesitan más espacio
}


# ── FUNCIÓN 1: Clasificación ──────────────────────────────────────────────────

def get_modelo(sector: str) -> str:
    """
    Modelo operativo del sector desde el registro canónico.

    Devuelve "labor" | "space" | "traffic".
    Es inmutable: ningún bench, LLM ni subsector puede sobreescribir este valor.
    Si necesitas que un subsector cambie el modelo, actualiza SECTOR_REGISTRY.
    """
    from financiero.sector_taxonomy import get_sector_profile
    return get_sector_profile(sector).pipeline_model


# ── FUNCIÓN 2: Capacidad física ───────────────────────────────────────────────

def physical_capacity(
    modelo: str,
    m2: float,
    bench: dict,
    empleados: int,
    horas: float,
) -> float:
    """
    Capacidad física máxima diaria (clientes/día). Hard cap — nunca se supera.

    Fórmula por modelo:
      labor:   slots = (empleados × horas × 60) / dur_min;  cap = slots × util
      space:   aforo = m2 / m2_per_person;  cap = aforo × rotacion × horas × util
      traffic: cap = empleados × horas × productividad × util

    bench esperado:
      labor:   "duracion_min" o "duracion_servicio_min" (minutos/servicio)
      space:   "rotacion" o "rotacion_mesas" (renovaciones/hora de aforo)
      traffic: "productividad" (clientes/hora/empleado)
      opcional: "m2_por_persona" (override del default por modelo)
    """
    util = _UTIL.get(modelo, 0.70)

    if modelo == "labor":
        dur = float(
            bench.get("duracion_min")
            or bench.get("duracion_servicio_min")
            or 45.0
        )
        dur = max(dur, 5.0)  # mínimo físico: ningún servicio dura menos de 5 min
        slots = (empleados * horas * 60.0) / dur
        return round(slots * util, 1)

    if modelo == "space":
        m2_persona = float(bench.get("m2_por_persona") or _M2_PER_PERSON["space"])
        aforo      = max(1.0, m2 / m2_persona)
        rotacion   = float(
            bench.get("rotacion")
            or bench.get("rotacion_mesas")
            or 0.3
        )
        return round(aforo * rotacion * horas * util, 1)

    # traffic
    prod = float(bench.get("productividad") or 8.0)
    return round(empleados * horas * prod * util, 1)


# ── FUNCIÓN 3: Empleados ──────────────────────────────────────────────────────

def derive_staff(
    modelo: str,
    m2: float,
    bench: dict,
) -> int:
    """
    Número de empleados derivado de la capacidad física.

    labor:   floor(m2 / emp_m2) — 1 empleado = 1 puesto de trabajo simultáneo
    space/traffic: ceil(m2 / emp_m2) — ratio operacional de personal por m²

    Usar floor para labor garantiza que derive_staff() devuelve exactamente
    el mismo número de puestos que physical_capacity() usa internamente.

    bench esperado: "empleados_por_m2" (m² de local por empleado/puesto)
    """
    emp_m2 = max(1.0, float(bench.get("empleados_por_m2") or 20.0))
    if modelo == "labor":
        return max(1, math.floor(m2 / emp_m2))
    return max(1, math.ceil(m2 / emp_m2))


# ── FUNCIÓN 4: Demanda estimada ───────────────────────────────────────────────

def estimate_demand(
    modelo: str,
    m2: float,
    bench: dict,
    vz: dict,
    comp: dict,
) -> float:
    """
    Demanda estimada (clientes/día). Independiente de capacity y empleados.

    labor:   demanda por agenda — puestos × clientes_por_puesto × ocupación
    space:   demanda por flujo peatonal (el aforo físico limita en physical_capacity)
    traffic: demanda por flujo peatonal × conversión × cuota de mercado

    bench esperado:
      labor:   "empleados_por_m2", "clientes_dia_por_puesto_min/max"
      otros:   "conversion_rate_min", "conversion_rate_max"
    vz esperado:   "flujo_peatonal_manana/tarde/noche"
    comp esperado: "num_competidores", "score_saturacion"
    """
    if modelo == "labor":
        emp_m2  = max(1.0, float(bench.get("empleados_por_m2") or 15.0))
        puestos = max(1, math.floor(m2 / emp_m2))
        cmin    = float(bench.get("clientes_dia_por_puesto_min") or 2.0)
        cmax    = float(bench.get("clientes_dia_por_puesto_max") or 5.0)
        return max(1.0, round(puestos * (cmin + cmax) / 2.0 * DEFAULT_OCCUPANCY, 1))

    # space y traffic: flujo peatonal
    fm = float(vz.get("flujo_peatonal_manana") or 0)
    ft = float(vz.get("flujo_peatonal_tarde")  or 0)
    fn = float(vz.get("flujo_peatonal_noche")  or 0)
    flujo_total = fm + ft + fn

    if flujo_total <= 0:
        # Sin datos de flujo → fallback muy conservador (mejor que 0)
        logger.debug("estimate_demand: sin datos de flujo peatonal, usando fallback=10")
        return max(1.0, float(bench.get("clientes_dia_fallback") or 10.0))

    conv_min = float(bench.get("conversion_rate_min") or 0.005)
    conv_max = float(bench.get("conversion_rate_max") or 0.020)
    conv     = (conv_min + conv_max) / 2.0

    nc    = max(0, int(comp.get("num_competidores") or 0))
    share = 1.0 / (nc + 1)
    sat   = float(comp.get("score_saturacion") or 50)
    if sat > 75:
        share *= 0.80
    elif sat < 25:
        share *= 1.20

    raw    = flujo_total * conv * share
    capped = min(raw, flujo_total * MAX_CAPTURE_RATE)
    return max(1.0, round(capped, 1))


# ── FUNCIÓN 5: Clientes finales ───────────────────────────────────────────────

def final_clients(demand: float, capacity: float) -> float:
    """
    Clientes finales diarios. La única operación que une demanda y capacidad.

    clients = min(demand, capacity)

    No hay correcciones posteriores a esta línea. Si el resultado no es correcto,
    el problema está en demand o capacity — no añadir más filtros aquí.
    """
    return max(1.0, round(min(demand, capacity), 1))


# ── Tests (ejecutar con: python -m financiero.core) ───────────────────────────

def _run_tests() -> None:
    """
    Suite de asserts para validar las invariantes del módulo.
    Se ejecuta sin DB ni dependencias externas.
    """
    print("financiero/core.py — tests\n")
    failures = 0

    def check(name: str, condition: bool, detail: str = "") -> None:
        nonlocal failures
        status = "OK" if condition else "FAIL"
        print(f"  [{status}] {name}" + (f" — {detail}" if detail else ""))
        if not condition:
            failures += 1

    # ── TEST 1: Labor (barbería) ───────────────────────────────────────────────
    bench_barber = {
        "empleados_por_m2": 14.0,
        "duracion_min": 30.0,
        "clientes_dia_por_puesto_min": 6.0,
        "clientes_dia_por_puesto_max": 9.0,
    }
    m2, horas = 50.0, 9.0
    n = derive_staff("labor", m2, bench_barber)        # floor(50/14) = 3
    cap = physical_capacity("labor", m2, bench_barber, n, horas)
    dem = estimate_demand("labor", m2, bench_barber, {}, {})
    cli = final_clients(dem, cap)

    check("labor.derive_staff == 3", n == 3, f"got {n}")
    check("labor.capacity > demand", cap > dem, f"cap={cap} dem={dem}")
    check("labor.clients == demand (no capacity constraint)", abs(cli - dem) < 0.1, f"cli={cli} dem={dem}")
    check("labor.capacity formula", abs(cap - (3 * 9 * 60 / 30) * 0.85) < 0.5,
          f"expected~{(3*9*60/30)*0.85:.1f} got {cap}")
    check("labor.demand formula (3 puestos × 7.5 avg × 0.65 occ)",
          abs(dem - round(3 * 7.5 * 0.65, 1)) < 0.1, f"got {dem}")

    # ── TEST 2: Space (restaurante) ────────────────────────────────────────────
    bench_rest = {
        "empleados_por_m2": 8.0,
        "rotacion": 0.30,
        "m2_por_persona": 1.5,
        "conversion_rate_min": 0.05,
        "conversion_rate_max": 0.10,
    }
    m2_rest, horas_rest = 60.0, 9.0
    n_rest = derive_staff("space", m2_rest, bench_rest)    # ceil(60/8) = 8
    cap_rest = physical_capacity("space", m2_rest, bench_rest, n_rest, horas_rest)
    vz_rest = {"flujo_peatonal_manana": 200, "flujo_peatonal_tarde": 400, "flujo_peatonal_noche": 300}
    dem_rest = estimate_demand("space", m2_rest, bench_rest, vz_rest, {"num_competidores": 3})
    cli_rest = final_clients(dem_rest, cap_rest)

    aforo_esperado = 60.0 / 1.5   # = 40 plazas
    cap_esperada   = round(aforo_esperado * 0.30 * 9.0 * 0.75, 1)
    check("space.derive_staff == 8", n_rest == 8, f"got {n_rest}")
    check("space.capacity formula (aforo x rot x horas x util)",
          abs(cap_rest - cap_esperada) < 0.5, f"expected~{cap_esperada} got {cap_rest}")
    check("space.clients <= capacity", cli_rest <= cap_rest, f"cli={cli_rest} cap={cap_rest}")
    check("space.clients <= demand (flujo limita, no capacity)", cli_rest <= dem_rest + 0.1,
          f"cli={cli_rest} dem={dem_rest}")

    # ── TEST 3: Traffic (supermercado) ─────────────────────────────────────────
    bench_super = {
        "empleados_por_m2": 20.0,
        "productividad": 15.0,
        "conversion_rate_min": 0.08,
        "conversion_rate_max": 0.15,
    }
    m2_super, horas_super = 120.0, 10.0
    n_super = derive_staff("traffic", m2_super, bench_super)   # ceil(120/20) = 6
    cap_super = physical_capacity("traffic", m2_super, bench_super, n_super, horas_super)
    vz_super = {"flujo_peatonal_manana": 800, "flujo_peatonal_tarde": 1200, "flujo_peatonal_noche": 400}
    dem_super = estimate_demand("traffic", m2_super, bench_super, vz_super, {"num_competidores": 1})
    cli_super = final_clients(dem_super, cap_super)

    flujo_super = 2400
    cap_esperada_super = round(6 * 10 * 15 * 0.60, 1)
    check("traffic.derive_staff == 6", n_super == 6, f"got {n_super}")
    check("traffic.capacity formula (empl x horas x prod x util)",
          abs(cap_super - cap_esperada_super) < 0.5, f"expected~{cap_esperada_super} got {cap_super}")
    check("traffic.demand respeta cap 15% flujo",
          dem_super <= flujo_super * MAX_CAPTURE_RATE + 0.1, f"dem={dem_super} max={flujo_super*MAX_CAPTURE_RATE}")
    check("traffic.clients <= capacity", cli_super <= cap_super, f"cli={cli_super} cap={cap_super}")
    check("traffic.clients <= demand", cli_super <= dem_super + 0.1,
          f"cli={cli_super} dem={dem_super}")

    # ── TEST 4: Invariante final_clients ──────────────────────────────────────
    check("final_clients: demand < capacity -> returns demand",
          abs(final_clients(10.0, 100.0) - 10.0) < 0.01)
    check("final_clients: capacity < demand -> returns capacity",
          abs(final_clients(100.0, 10.0) - 10.0) < 0.01)
    check("final_clients: never < 1.0",
          final_clients(0.0, 0.0) >= 1.0)

    # ── TEST 5: get_modelo (importa taxonomy) ────────────────────────────────
    try:
        check("get_modelo(estetica) == labor",    get_modelo("estetica")    == "labor")
        check("get_modelo(restauracion) == space", get_modelo("restauracion") == "space")
        check("get_modelo(moda) == traffic",       get_modelo("moda")        == "traffic")
        check("get_modelo(farmacia) == traffic",   get_modelo("farmacia")    == "traffic")
        check("get_modelo(bar) == space",          get_modelo("bar")         == "space")
    except ImportError:
        print("  [SKIP] get_modelo tests — sector_taxonomy no disponible fuera del contexto Django/FastAPI")

    # ── Resumen ───────────────────────────────────────────────────────────────
    total = 18
    passed = total - failures
    print(f"\n{passed}/{total} tests OK" + (" — TODOS PASARON" if failures == 0 else f" — {failures} FALLOS"))
    if failures:
        raise AssertionError(f"{failures} tests fallaron en financiero/core.py")


if __name__ == "__main__":
    _run_tests()
