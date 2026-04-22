"""
financiero/estimador.py — Estimación automática de todos los parámetros financieros.

Deriva alquiler, ticket, clientes, salarios, reforma y equipamiento sin input del usuario.
Ver comentarios inline para la fuente de cada parámetro.
"""
from __future__ import annotations
import logging, math
from dataclasses import dataclass, field
from typing import Optional
import numpy as np

from db.conexion import get_db

logger = logging.getLogger(__name__)

_SS_EMPRESA          = 0.31   # Seguridad Social empresa ~31% sobre salario bruto
_FACTOR_CONSERVADOR  = 0.60
_FACTOR_OPTIMISTA    = 1.15
_MAX_CAPTURE_RATE    = 0.15   # límite hard: nunca > 15% del flujo peatonal
_DEFAULT_OCCUPANCY   = 0.65   # ocupación conservadora para negocios de cita

# Clasificación del modelo de negocio por sector
_BUSINESS_MODEL_MAP: dict[str, str] = {
    "restauracion":  "restaurant",
    "tatuajes":      "appointment_based",
    "estetica":      "appointment_based",
    "clinica":       "appointment_based",
    "peluqueria":    "appointment_based",
    "shisha_lounge": "hybrid",
    "moda":          "retail_walkin",
    "supermercado":  "retail_walkin",
    "farmacia":      "retail_walkin",
}


def _determinar_modelo_negocio(sector: str, bench: dict) -> str:
    if bench.get("is_appointment_based"):
        return "appointment_based"
    return _BUSINESS_MODEL_MAP.get(sector, "retail_walkin")


# Google Places precio_nivel (1-4) → ticket medio estimado por sector
_TICKET_POR_NIVEL = {
    "restauracion": {1:11.0, 2:19.0, 3:34.0, 4:65.0},
    "moda":         {1:28.0, 2:72.0, 3:190.0, 4:420.0},
    "tatuajes":     {1:80.0, 2:150.0, 3:260.0, 4:450.0},
    "estetica":     {1:30.0, 2:60.0, 3:100.0, 4:160.0},
    "shisha_lounge":{1:25.0, 2:40.0, 3:65.0, 4:90.0},
}

# Pesos de flujo por franja horaria según sector
_PESO_FLUJO = {
    "restauracion": {"manana":2.0,"tarde":2.5,"noche":3.0},
    "moda":         {"manana":1.0,"tarde":3.5,"noche":1.5},
    "shisha_lounge":{"manana":0.2,"tarde":1.0,"noche":4.0},
    "default":      {"manana":1.5,"tarde":2.5,"noche":1.5},
}


@dataclass
class PE:
    """Parámetro Estimado con metadatos de fuente."""
    valor: float
    fuente: str
    confianza: str  # 'alta' | 'media' | 'baja'
    rango_min: float = 0.0
    rango_max: float = 0.0


@dataclass
class ParametrosEstimados:
    ticket_medio:               PE = field(default_factory=lambda: PE(0,"","baja"))
    # ── Demanda: valor único base (el calculador deriva escenarios × 0.6/1.0/1.2)
    clients_per_day:            PE = field(default_factory=lambda: PE(0,"","baja"))
    max_capacity:               float = 0.0
    # Mantenidos para carga desde BD y compatibilidad slider legacy
    clientes_dia_conservador:   PE = field(default_factory=lambda: PE(0,"","baja"))
    clientes_dia_optimista:     PE = field(default_factory=lambda: PE(0,"","baja"))
    dias_apertura_mes:          PE = field(default_factory=lambda: PE(26,"","alta"))
    alquiler_mensual:           PE = field(default_factory=lambda: PE(0,"","baja"))
    salarios_mensual:           PE = field(default_factory=lambda: PE(0,"","media"))
    otros_fijos_mensual:        PE = field(default_factory=lambda: PE(0,"","media"))
    coste_mercancia_pct:        PE = field(default_factory=lambda: PE(0.4,"","alta"))
    reforma_local:              PE = field(default_factory=lambda: PE(0,"","baja"))
    equipamiento:               PE = field(default_factory=lambda: PE(0,"","baja"))
    deposito_fianza:            PE = field(default_factory=lambda: PE(0,"","alta"))
    otros_iniciales:            PE = field(default_factory=lambda: PE(0,"","media"))
    num_empleados:              int = 1
    business_model_type:        str = "retail_walkin"


async def estimar_parametros(zona_id: str, sector: str, perfil: dict) -> ParametrosEstimados:
    import asyncio
    bench, vz, local, comp, precio_nivel_mediana, dias_apertura = await asyncio.gather(
        _get_bench(sector), _get_vz(zona_id), _get_local(zona_id, perfil),
        _get_comp(zona_id, sector), _get_precio_nivel(zona_id, sector),
        _get_dias_apertura(zona_id, sector), return_exceptions=True)

    bench        = bench        if isinstance(bench, dict)  else {}
    vz           = vz           if isinstance(vz, dict)     else {}
    local        = local        if isinstance(local, dict)  else {}
    comp         = comp         if isinstance(comp, dict)   else {}
    precio_nivel_mediana = precio_nivel_mediana if not isinstance(precio_nivel_mediana, Exception) else None
    dias_apertura        = dias_apertura        if not isinstance(dias_apertura, Exception)        else None

    m2 = local.get("m2") or perfil.get("m2_aprox") or 60.0

    p = ParametrosEstimados()
    p.business_model_type = _determinar_modelo_negocio(sector, bench)
    p.dias_apertura_mes = _dias(dias_apertura, bench)
    p.ticket_medio      = _ticket(sector, precio_nivel_mediana, bench)
    p.alquiler_mensual  = _alquiler(local, vz, m2)

    es_cita = bench.get("is_appointment_based", False) or p.business_model_type == "appointment_based"
    pe_base, max_cap = (_clientes_cita_base(m2, bench) if es_cita
                        else _clientes_flujo_base(sector, vz, comp, bench))

    p.clients_per_day = pe_base
    p.max_capacity    = max_cap
    # Derivados para compatibilidad con la tabla de precálculo
    p.clientes_dia_conservador = PE(
        max(1.0, round(pe_base.valor * 0.60, 1)), pe_base.fuente, pe_base.confianza,
        pe_base.rango_min, pe_base.rango_max,
    )
    p.clientes_dia_optimista = PE(
        max(1.0, round(min(pe_base.valor * 1.20, max_cap), 1)), pe_base.fuente, pe_base.confianza,
        pe_base.rango_min, pe_base.rango_max,
    )

    p.num_empleados      = max(1, math.ceil(m2 / (bench.get("empleados_por_m2") or 20)))
    p.salarios_mensual   = _salarios(p.num_empleados, bench)
    margen = bench.get("margen_bruto_tipico", 0.65)
    p.coste_mercancia_pct = PE(round(1-margen,3), f"INE CNAE — margen bruto típico {margen:.0%}",
                                "alta", round(1-margen-0.10,2), round(1-margen+0.10,2))
    p.otros_fijos_mensual = _otros_fijos(m2, bench)
    p.reforma_local       = _reforma(m2, bench)
    p.equipamiento        = _equip(bench)
    p.deposito_fianza     = PE(round(p.alquiler_mensual.valor*2,0),
                                "Art. 36 LAU — 2 meses alquiler", "alta",
                                p.alquiler_mensual.valor, p.alquiler_mensual.valor*3)
    p.otros_iniciales     = _otros_ini(bench)
    return p


# ── Sub-estimadores ────────────────────────────────────────────────────────────

def _ticket(sector, precio_nivel, bench) -> PE:
    bmin = bench.get("ticket_medio_min", 10)
    bmax = bench.get("ticket_medio_max", 50)
    bmid = (bmin+bmax)/2
    if precio_nivel is not None:
        t = _TICKET_POR_NIVEL.get(sector, {}).get(max(1,min(4,round(precio_nivel))), bmid)
        return PE(round(t,2),"Mediana precio_nivel competidores (Google Places)","media",round(bmin),round(bmax))
    return PE(round(bmid,2),"Benchmarks sectoriales INE (sin datos precio zona)","baja",round(bmin),round(bmax))


def _clientes_flujo_base(sector: str, vz: dict, comp: dict, bench: dict) -> tuple[PE, float]:
    """Retail / restaurant / hybrid — demanda basada en flujo peatonal.
    Aplica hard-cap al 15% del flujo total (nunca más).
    Devuelve (PE_base, max_capacity).
    """
    fm = vz.get("flujo_peatonal_manana") or 0
    ft = vz.get("flujo_peatonal_tarde")  or 0
    fn = vz.get("flujo_peatonal_noche")  or 0
    footfall_total = fm + ft + fn

    pesos     = _PESO_FLUJO.get(sector, _PESO_FLUJO["default"])
    ponderado = fm*pesos["manana"]*6 + ft*pesos["tarde"]*6 + fn*pesos["noche"]*3
    peso_total = pesos["manana"]*6 + pesos["tarde"]*6 + pesos["noche"]*3
    flujo_h   = ponderado / peso_total if peso_total else 0
    horas     = bench.get("horas_apertura_dia", 9.0)
    pax       = flujo_h * horas

    conv = (bench.get("conversion_rate_min", 0.005) + bench.get("conversion_rate_max", 0.02)) / 2
    nc   = comp.get("num_competidores", 0) or 0
    share = 1 / (nc + 1)
    sat   = comp.get("score_saturacion", 50) or 50
    if sat > 75:  share *= 0.80
    elif sat < 25: share *= 1.20

    raw_demand = pax * conv * share

    # Hard-cap: nunca superar el 15% del flujo peatonal diario total
    if footfall_total > 0:
        max_realistic = footfall_total * _MAX_CAPTURE_RATE
        raw_demand    = min(raw_demand, max_realistic)

    base = max(1.0, round(raw_demand, 1))
    # Para retail, la capacidad máxima es el techo de demanda (no hay barrera física diferente)
    max_cap = max(base * 2.5, footfall_total * _MAX_CAPTURE_RATE if footfall_total > 0 else base * 2.5, 10.0)

    tiene = footfall_total > 0
    fuente = ("Aforadors Open Data BCN × tasa captación × reparto mercado"
               if tiene else "Benchmarks sectoriales (sin datos de flujo)")
    conf   = "media" if tiene else "baja"
    smax   = max(50.0, max_cap)

    return PE(base, fuente, conf, 1.0, smax), float(max_cap)


def _clientes_cita_base(m2: float, bench: dict) -> tuple[PE, float]:
    """Appointment-based — demanda basada en capacidad del local.
    Ignora flujo peatonal como base principal.
    Devuelve (PE_base, max_capacity).
    """
    e_m2    = bench.get("empleados_por_m2", 15.0) or 15.0
    puestos = max(1, math.floor(m2 / e_m2))
    cmin    = bench.get("clientes_dia_por_puesto_min", 2.0)
    cmax    = bench.get("clientes_dia_por_puesto_max", 5.0)

    max_capacity = float(puestos * cmax)
    # Base conservador con ocupación del 65%
    base = max(1.0, round(puestos * (cmin + cmax) / 2 * _DEFAULT_OCCUPANCY, 1))

    fuente = f"Benchmarks × {puestos} puesto(s) × {_DEFAULT_OCCUPANCY:.0%} ocupación ({e_m2:.0f}m²/puesto)"
    smax   = max(max_capacity, 20.0)

    return PE(base, fuente, "media", 1.0, smax), max_capacity


def _alquiler(local, vz, m2) -> PE:
    real = local.get("alquiler_mensual")
    if real and real > 0:
        return PE(round(real), "Dato real del local (Idealista API)", "alta",
                  round(real*0.8), round(real*1.3))
    pm2 = vz.get("precio_m2")
    if pm2 and pm2 > 0 and m2 > 0:
        e = round(pm2*m2)
        return PE(e, f"Precio zona {pm2:.1f}€/m² × {m2:.0f}m² (Open Data BCN/Idealista)", "media",
                  round(e*0.75), round(e*1.35))
    fb = round(m2*18.0)
    return PE(fb, "Estimación genérica ~18€/m²/mes (CBRE BCN 2024)", "baja",
              round(m2*10), round(m2*35))


def _salarios(n, bench) -> PE:
    sal = bench.get("salario_base_mensual_convenio",1650.0)
    total = round(n * sal * (1+_SS_EMPRESA))
    return PE(total, f"{n} empleado(s) × {sal:.0f}€ × {1+_SS_EMPRESA:.0%} SS — Convenio Cat 2024",
              "media", round(total*0.6), round(total*1.5))


def _otros_fijos(m2, bench) -> PE:
    sumi = round(m2*(bench.get("coste_suministros_por_m2",5.0)))
    seg  = round(bench.get("seguro_rc_mensual",100.0))
    ges  = round(bench.get("coste_gestoria_mensual",185.0))
    mant = round(m2*1.5)
    total = sumi+seg+ges+mant
    return PE(total, f"Suministros {sumi}€ + seguro {seg}€ + gestoría {ges}€ + mant. {mant}€",
              "media", round(total*0.7), round(total*1.5))


def _reforma(m2, bench) -> PE:
    rmin = bench.get("reforma_m2_min",300.0)
    rmax = bench.get("reforma_m2_max",700.0)
    e = round(m2*(rmin+rmax)/2)
    return PE(e, f"m² × media benchmarks {rmin:.0f}–{rmax:.0f}€/m²", "baja",
              round(m2*rmin), round(m2*rmax))


def _equip(bench) -> PE:
    emin = bench.get("equipamiento_base_min",5000.0)
    emax = bench.get("equipamiento_base_max",20000.0)
    e = round((emin+emax)/2)
    return PE(e, f"Benchmarks sector {emin:.0f}–{emax:.0f}€", "baja", emin, emax)


def _dias(dias_comp, bench) -> PE:
    b = bench.get("dias_apertura_mes_tipico",26)
    if dias_comp and dias_comp > 0:
        return PE(round(dias_comp), "Mediana horario competidores zona (Google Places)", "alta", 20, 31)
    return PE(b, "Benchmarks sectoriales", "media", 20, 31)


def _otros_ini(bench) -> PE:
    lic = bench.get("coste_licencias_apertura",2000.0)
    fij = bench.get("otros_iniciales_fijos",2000.0)
    total = round(lic+fij)
    return PE(total, f"Licencias {lic:.0f}€ + constitución SL + gestoría apertura {fij:.0f}€",
              "media", round(total*0.8), round(total*1.4))


# ── Queries ────────────────────────────────────────────────────────────────────

async def _get_bench(sector):
    from db.financiero import get_benchmarks_sector
    return await get_benchmarks_sector(sector)

async def _get_vz(zona_id):
    async with get_db() as conn:
        r = await conn.fetchrow("""
            SELECT vz.*, paz.precio_m2
            FROM variables_zona vz
            LEFT JOIN LATERAL (
                SELECT precio_m2 FROM precios_alquiler_zona WHERE zona_id=$1 ORDER BY fecha DESC LIMIT 1
            ) paz ON TRUE
            WHERE vz.zona_id=$1 ORDER BY vz.fecha DESC LIMIT 1
        """, zona_id)
    return dict(r) if r else {}

async def _get_local(zona_id, perfil):
    m2 = perfil.get("m2_aprox")
    async with get_db() as conn:
        if m2:
            r = await conn.fetchrow(
                "SELECT id,m2,alquiler_mensual FROM locales WHERE zona_id=$1 AND disponible=TRUE AND planta='PB' ORDER BY ABS(m2-$2) LIMIT 1",
                zona_id, float(m2))
        else:
            r = await conn.fetchrow(
                "SELECT id,m2,alquiler_mensual FROM locales WHERE zona_id=$1 AND disponible=TRUE AND planta='PB' ORDER BY alquiler_mensual ASC NULLS LAST LIMIT 1",
                zona_id)
    return dict(r) if r else {}

async def _get_comp(zona_id, sector):
    async with get_db() as conn:
        r = await conn.fetchrow(
            "SELECT num_competidores,score_saturacion FROM competencia_por_local WHERE zona_id=$1 AND sector_codigo=$2 AND radio_m=300 ORDER BY fecha_calculo DESC LIMIT 1",
            zona_id, sector)
    return dict(r) if r else {}

async def _get_precio_nivel(zona_id, sector):
    async with get_db() as conn:
        r = await conn.fetchrow("""
            SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY precio_nivel) AS med,
                   COUNT(*) FILTER (WHERE precio_nivel IS NOT NULL) AS n
            FROM negocios_activos WHERE zona_id=$1 AND sector_codigo=$2 AND activo=TRUE
        """, zona_id, sector)
    if r and (r["n"] or 0) >= 3:
        return r["med"]
    return None

async def _get_dias_apertura(zona_id, sector):
    async with get_db() as conn:
        r = await conn.fetchrow("""
            WITH d AS (
                SELECT id,
                  ((horario->>'lun' IS NOT NULL)::int+(horario->>'mar' IS NOT NULL)::int+
                   (horario->>'mie' IS NOT NULL)::int+(horario->>'jue' IS NOT NULL)::int+
                   (horario->>'vie' IS NOT NULL)::int+(horario->>'sab' IS NOT NULL)::int+
                   (horario->>'dom' IS NOT NULL)::int) AS dias_sem
                FROM negocios_activos
                WHERE zona_id=$1 AND sector_codigo=$2 AND activo=TRUE AND horario IS NOT NULL AND horario!='{}')
            SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY dias_sem)*4.33 AS dias_mes, COUNT(*) AS n FROM d
        """, zona_id, sector)
    if r and (r["n"] or 0) >= 3:
        return r["dias_mes"]
    return None


# Alias para importación desde api/financiero.py
ParametroEstimado = PE
