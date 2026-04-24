"""
api/financiero.py — POST /api/financiero

Análisis financiero v3 — modelo realista de 12 pasos:

  INPUT   → un único clients_per_day (base) + max_capacity
  CALC    → escenarios derivados × 0.60 / × 1.00 / × 1.20 (cap por capacidad)
  SALIDA  → 8 bloques + validation_flags + correcciones_aplicadas

Regla de oro: NUNCA llegan valores irreales al calculador.
              Todo se corrige AQUÍ antes de llamar a calcular_proyeccion().
"""

from __future__ import annotations

import logging
import math
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from schemas.models import (
    ProyeccionMes,
    DecisionBlock, EconomiaBase, EstructuraCostes,
    BreakEvenInfo, BreakEvenPunto, MetricasClave,
    Riesgo, Insight, ModeloDemanda,
    CorreccionAplicada, CapacityModelInfo,
    ValidacionFinanciera, ProblemaDetectado, AjusteRecomendado, ChecksDetallados,
    SensitividadItem,
)
from agente.validador_financiero import validar_financiero
from financiero.estimador import estimar_parametros, ParametrosEstimados, PE
from financiero.calculadora import calcular_proyeccion, MAX_OCCUPANCY
from db.sesiones import get_sesion
from db.financiero import (
    get_parametros_precalculados,
    guardar_analisis_financiero,
    get_benchmarks_sector,
)

logger = logging.getLogger(__name__)
router = APIRouter(tags=["financiero"])

_UMBRAL_ALQUILER_VENTAS   = 0.10   # 10% s/ingresos conservadores para decisión "sí"
_UMBRAL_ALQUILER_DISPLAY  = 0.15   # umbral visual de alerta (sobre ingresos base)
_CAPITAL_ESTIMADO_DEFAULT = 50_000.0

# Tasa de captación máxima realista por sector
# Para negocios de paso: footfall × tasa = clientes potenciales
# Para negocios de cita (appointment_based): no aplica (capacidad física limita)
_CAPTURE_RATE_POR_SECTOR: dict[str, float] = {
    "restauracion":  0.08,  # aforo físico limita; 8% del flujo es ya muy alto para un local
    "moda":          0.10,  # retail moda — compra impulsiva moderada
    "supermercado":  0.15,  # alta frecuencia y necesidad, mayor conversión
    "farmacia":      0.12,  # necesidad, mayor conversión que moda
    "shisha_lounge": 0.05,  # destino, no de paso — baja conversión espontánea
    "_default":      0.10,
}


# ─── Request ──────────────────────────────────────────────────────────────────

class BusinessContext(BaseModel):
    tipo:                str             = "nuevo"  # "nuevo" | "traspaso"
    capital_inicial:     Optional[float] = None
    capacidad_operativa: Optional[int]   = None


class FinancieroRequest(BaseModel):
    """
    overrides válidos:
      clients_per_day, ticket_medio, dias_apertura_mes,
      alquiler_mensual, salarios_mensual, otros_fijos_mensual,
      coste_mercancia_pct, reforma_local, equipamiento,
      deposito_fianza, otros_iniciales
    """
    zona_id:          str
    session_id:       str
    overrides:        dict[str, float]          = Field(default_factory=dict)
    business_context: Optional[BusinessContext] = None


# ─── Response ─────────────────────────────────────────────────────────────────

class ParametroResponse(BaseModel):
    valor_estimado: float
    valor_usado:    float
    es_override:    bool
    fuente:         str
    confianza:      str
    rango_min:      float
    rango_max:      float


class ParametrosResponse(BaseModel):
    ticket_medio:        ParametroResponse
    clients_per_day:     ParametroResponse   # único slider de clientes
    max_capacity:        float               # límite físico
    dias_apertura_mes:   ParametroResponse
    alquiler_mensual:    ParametroResponse
    num_empleados:       int
    salarios_mensual:    ParametroResponse
    otros_fijos_mensual: ParametroResponse
    coste_mercancia_pct: ParametroResponse
    reforma_local:       ParametroResponse
    equipamiento:        ParametroResponse
    deposito_fianza:     ParametroResponse
    otros_iniciales:     ParametroResponse


class DesgloseInversionResponse(BaseModel):
    reforma_local:   float
    equipamiento:    float
    deposito_fianza: float
    otros_iniciales: float


class FinancieroResponse(BaseModel):
    parametros:         ParametrosResponse
    inversion_total:    float
    desglose_inversion: DesgloseInversionResponse
    ingresos_anuales_conservador: float
    ingresos_anuales_base:        float
    ingresos_anuales_optimista:   float
    margen_bruto_pct:             float
    ebitda_anual_conservador:     float
    ebitda_anual_base:            float
    ebitda_anual_optimista:       float
    ebitda_anual_stress:          float = 0.0
    roi_3a_conservador:        float
    roi_3a_base:               float
    roi_3a_optimista:          float
    roi_3a_stress:             float = -1.0
    payback_meses_conservador: int
    payback_meses_base:        int
    payback_meses_optimista:   int
    payback_meses_stress:      int = 999
    breakeven_clientes_dia:    int
    proyeccion:                list[ProyeccionMes]
    margen_sector_tipico:      float
    alquiler_sobre_ventas_pct: float
    alerta_alquiler:           bool
    # Bloques v2
    decision:          DecisionBlock
    economia_base:     EconomiaBase
    estructura_costes: EstructuraCostes
    break_even:        BreakEvenInfo
    metricas_clave:    MetricasClave
    riesgos:           list[Riesgo]
    insights:          list[Insight]
    modelo_demanda:    ModeloDemanda
    # Contexto v3
    business_model_type:    str                        = "retail_walkin"
    correcciones_aplicadas: list[CorreccionAplicada]   = []
    capacity_model:         Optional[CapacityModelInfo] = None
    tipo_negocio:           str                        = "nuevo"
    validation_flags:       list[str]                  = []
    ocupacion_efectiva:     float                      = 0.0
    # Validación LLM — capa crítica (None si el LLM falla)
    validacion_financiera:  Optional[ValidacionFinanciera] = None
    # Análisis v4: sensibilidad + estrés
    sensibilidad:           list[SensitividadItem]     = []


# ─── Endpoint ─────────────────────────────────────────────────────────────────

@router.post(
    "/financiero",
    response_model=FinancieroResponse,
    summary="Análisis financiero v3 — modelo realista 12 pasos",
)
async def financiero(body: FinancieroRequest) -> FinancieroResponse:
    sesion = await get_sesion(body.session_id)
    if sesion is None:
        raise HTTPException(status_code=404, detail="Sesión no encontrada o expirada.")

    sector  = sesion.get("perfil", {}).get("sector", "desconocido")
    perfil  = sesion.get("perfil", {})

    bc           = body.business_context
    tipo_negocio = bc.tipo if bc else "nuevo"
    capital      = float(
        (bc.capital_inicial if bc and bc.capital_inicial else None)
        or perfil.get("capital_disponible")
        or _CAPITAL_ESTIMADO_DEFAULT
    )
    capacidad_operativa = bc.capacidad_operativa if bc else None

    # ── Estimar parámetros ────────────────────────────────────────────────────
    estimados = await _get_o_calcular_estimados(
        zona_id=body.zona_id, sector=sector, perfil=perfil,
    )

    # ── Aplicar overrides (incluyendo clients_per_day) ────────────────────────
    params = _aplicar_overrides(estimados, body.overrides)

    # ── Benchmarks + tipo de modelo ───────────────────────────────────────────
    benchmarks          = await get_benchmarks_sector(sector)
    margen_sector       = benchmarks.get("margen_bruto_tipico", 0.65)
    business_model_type = getattr(estimados, "business_model_type", "retail_walkin")

    # ── Diccionario de trabajo ────────────────────────────────────────────────
    v: dict = {
        "ticket_medio":        params.ticket_medio.valor_usado,
        "clients_per_day":     params.clients_per_day.valor_usado,
        "max_capacity":        params.max_capacity,
        "dias_apertura_mes":   params.dias_apertura_mes.valor_usado,
        "alquiler_mensual":    params.alquiler_mensual.valor_usado,
        "salarios_mensual":    params.salarios_mensual.valor_usado,
        "otros_fijos_mensual": params.otros_fijos_mensual.valor_usado,
        "coste_mercancia_pct": params.coste_mercancia_pct.valor_usado,
        "reforma_local":       params.reforma_local.valor_usado,
        "equipamiento":        params.equipamiento.valor_usado,
        "deposito_fianza":     params.deposito_fianza.valor_usado,
        "otros_iniciales":     params.otros_iniciales.valor_usado,
        # v3: productividad del personal
        "num_empleados":       params.num_empleados,
        "business_model_type": business_model_type,
    }
    flujo_peatonal      = float(perfil.get("flujo_peatonal_dia", v["clients_per_day"] * 12))

    # ── Correcciones hard (CRÍTICO — antes de la calculadora) ─────────────────
    correcciones_raw = _aplicar_correcciones_demanda(
        v=v,
        flujo_peatonal=flujo_peatonal,
        business_model_type=business_model_type,
        capacidad_operativa=capacidad_operativa,
        sector=sector,
    )

    # ── Modelo de capacidad ───────────────────────────────────────────────────
    capacity_model = _build_capacity_model(
        business_model_type=business_model_type,
        sector=sector,
        m2=float(perfil.get("m2_aprox", 60.0)),
        benchmarks=benchmarks,
    )

    # ── Proyección 36 meses ───────────────────────────────────────────────────
    try:
        resultado = await calcular_proyeccion(v, tipo_negocio=tipo_negocio, sector=sector)
    except Exception as exc:
        logger.error("Error calculadora zona=%s: %s", body.zona_id, exc, exc_info=True)
        raise HTTPException(status_code=500, detail="Error calculando la proyección financiera.")

    # ── Métricas derivadas (coherentes con MAX_OCCUPANCY del calculador) ─────────
    cl_base           = resultado["clients_per_day"]
    cl_conservador    = resultado["scenario_clients"]["conservador"]
    ing_estable       = v["ticket_medio"] * cl_base * v["dias_apertura_mes"] * MAX_OCCUPANCY
    ing_conservador   = v["ticket_medio"] * cl_conservador * v["dias_apertura_mes"] * MAX_OCCUPANCY
    margen_bruto      = 1 - v["coste_mercancia_pct"]
    # Alquiler sobre ingresos CONSERVADORES (corrección auditoría: usar escenario malo, no base)
    alquiler_pct      = (v["alquiler_mensual"] / ing_conservador) if ing_conservador > 0 else 1.0
    alquiler_pct_base = (v["alquiler_mensual"] / ing_estable) if ing_estable > 0 else 1.0

    # ── Modelo de demanda (display) ───────────────────────────────────────────
    # Tasa de captación máxima según sector (corregida auditoría: no universal 15%)
    sector_capture_max = _CAPTURE_RATE_POR_SECTOR.get(sector, _CAPTURE_RATE_POR_SECTOR["_default"])
    max_potential = flujo_peatonal * sector_capture_max if flujo_peatonal > 0 else cl_base * 2
    capture_rate  = min((cl_base / max_potential) if max_potential > 0 else 0.05, sector_capture_max)

    modelo_demanda = ModeloDemanda(
        flujo_peatonal_dia=round(flujo_peatonal),
        max_potential_customers=round(max_potential, 1),
        capture_rate=round(capture_rate, 3),
    )

    # ── Validación LLM (capa crítica — no bloquea el flujo si falla) ─────────
    _val_payload = _build_validation_payload(
        zona_id=body.zona_id,
        sector=sector,
        business_model_type=business_model_type,
        tipo_negocio=tipo_negocio,
        params=params,
        v=v,
        resultado=resultado,
        ing_estable=ing_estable,
        margen_bruto=margen_bruto,
        max_potential=max_potential,
        benchmarks=benchmarks,
        correcciones_raw=correcciones_raw,
        has_overrides=bool(body.overrides),
    )
    _val_raw = await validar_financiero(_val_payload, session_id=body.session_id)
    validacion_financiera = _parse_validacion(_val_raw)

    # ── Bloque 1: Decisión ────────────────────────────────────────────────────
    roi_b    = resultado["roi_3a_base"]
    roi_c    = resultado["roi_3a_conservador"]
    pb_b     = resultado["payback_meses_base"]
    ben_mes  = round(ing_estable * margen_bruto - (
        v["alquiler_mensual"] + v["salarios_mensual"] + v["otros_fijos_mensual"]
    ))

    decision = DecisionBlock(
        # alquiler_pct ya usa ingresos conservadores (corregido auditoría)
        recomendacion=_calcular_recomendacion(roi_b, pb_b, alquiler_pct, roi_c),
        beneficio_mensual=ben_mes,
        payback=pb_b,
        capital_necesario=resultado["inversion_total"],
        gap_capital=max(0.0, resultado["inversion_total"] - capital),
    )

    # ── Bloque 2: Economía base ───────────────────────────────────────────────
    economia_base = EconomiaBase(
        ingresos_mensuales=round(ing_estable),
        clientes_dia=cl_base,
        ticket_medio=v["ticket_medio"],
        conversion_pct=round(capture_rate, 3),
        max_potential_customers=round(max_potential, 1),
        ocupacion_efectiva=resultado.get("ocupacion_efectiva", 0.0),
    )

    # ── Bloque 3: Costes ──────────────────────────────────────────────────────
    estructura_costes = EstructuraCostes(**resultado["estructura_costes"])

    # ── Bloque 4: Break-even ─────────────────────────────────────────────────
    be_clientes     = resultado["breakeven_clientes_dia"]
    ing_be          = v["ticket_medio"] * be_clientes * v["dias_apertura_mes"]
    margen_sobre_be = ((cl_base - be_clientes) / be_clientes * 100) if be_clientes > 0 else 0.0
    break_even = BreakEvenInfo(
        clientes_be=be_clientes,
        ingresos_be=round(ing_be),
        clientes_base=cl_base,
        margen_sobre_be_pct=round(margen_sobre_be, 1),
        chart=[BreakEvenPunto(**p) for p in resultado["break_even_chart"]],
    )

    # ── Bloque 6: Métricas ────────────────────────────────────────────────────
    metricas_clave = MetricasClave(
        roi_conservador=resultado["roi_3a_conservador"],
        roi_base=roi_b,
        roi_optimista=resultado["roi_3a_optimista"],
        roi_stress=round(resultado.get("roi_3a_stress", -1.0), 3),
        margen_bruto_pct=round(margen_bruto, 3),
        payback_meses=pb_b,
        payback_stress=resultado.get("payback_meses_stress", 999),
        mes_caja_positiva=pb_b,
    )

    # ── Bloque 7: Riesgos ─────────────────────────────────────────────────────
    riesgos = _generar_riesgos(
        alquiler_pct=alquiler_pct,
        payback_b=pb_b,
        roi_c=resultado["roi_3a_conservador"],
        roi_b=resultado["roi_3a_base"],
        inversion=resultado["inversion_total"],
        capital=capital,
        correcciones=correcciones_raw,
        validation_flags=resultado.get("validation_flags", []),
        ocupacion_efectiva=resultado.get("ocupacion_efectiva", 0.0),
        cf_mes=v["alquiler_mensual"] + v["salarios_mensual"] + v["otros_fijos_mensual"],
        ing_estable=ing_estable,
        margen_bruto=margen_bruto,
    )

    # ── Enriquecer riesgos con problemas de alto impacto detectados por LLM ──
    if validacion_financiera:
        for prob in validacion_financiera.problemas_detectados:
            if prob.impacto == "alto":
                riesgos.append(Riesgo(tipo="warning", mensaje=prob.descripcion))

    # ── Bloque 8: Insights ────────────────────────────────────────────────────
    competition_index = float(perfil.get("competition_index", 0.5))
    insights = _generar_insights(
        capture_rate=capture_rate,
        competition_index=competition_index,
        alquiler_pct=alquiler_pct,
        roi_b=roi_b,
        margen_bruto=margen_bruto,
        payback_b=pb_b,
        business_model_type=business_model_type,
        tipo_negocio=tipo_negocio,
    )

    # ── Guardar en BD ─────────────────────────────────────────────────────────
    try:
        await guardar_analisis_financiero(
            session_id=body.session_id,
            zona_id=body.zona_id,
            params={**v, "overrides": body.overrides, "sector": sector,
                    "tipo_negocio": tipo_negocio, "business_model_type": business_model_type},
            resultado=resultado,
        )
    except Exception as exc:
        logger.warning("No se pudo guardar analisis_financiero: %s", exc)

    return FinancieroResponse(
        parametros=params,
        inversion_total=resultado["inversion_total"],
        desglose_inversion=DesgloseInversionResponse(
            reforma_local=v["reforma_local"],
            equipamiento=v["equipamiento"],
            deposito_fianza=v["deposito_fianza"],
            otros_iniciales=v["otros_iniciales"],
        ),
        ingresos_anuales_conservador=resultado["ingresos_anuales_conservador"],
        ingresos_anuales_base=resultado["ingresos_anuales_base"],
        ingresos_anuales_optimista=resultado["ingresos_anuales_optimista"],
        margen_bruto_pct=round(margen_bruto, 2),
        ebitda_anual_conservador=resultado["ebitda_anual_conservador"],
        ebitda_anual_base=resultado["ebitda_anual_base"],
        ebitda_anual_optimista=resultado["ebitda_anual_optimista"],
        ebitda_anual_stress=resultado.get("ebitda_anual_stress", 0.0),
        roi_3a_conservador=round(resultado["roi_3a_conservador"], 2),
        roi_3a_base=round(roi_b, 2),
        roi_3a_optimista=round(resultado["roi_3a_optimista"], 2),
        roi_3a_stress=round(resultado.get("roi_3a_stress", -1.0), 2),
        payback_meses_conservador=resultado["payback_meses_conservador"],
        payback_meses_base=pb_b,
        payback_meses_optimista=resultado["payback_meses_optimista"],
        payback_meses_stress=resultado.get("payback_meses_stress", 999),
        breakeven_clientes_dia=be_clientes,
        proyeccion=[ProyeccionMes(**m) for m in resultado["proyeccion"]],
        margen_sector_tipico=margen_sector,
        # Display: alquiler sobre ingresos BASE (informativo)
        alquiler_sobre_ventas_pct=round(alquiler_pct_base, 3),
        # Alerta visual: umbral 15% sobre ingresos base
        alerta_alquiler=alquiler_pct_base > _UMBRAL_ALQUILER_DISPLAY,
        decision=decision,
        economia_base=economia_base,
        estructura_costes=estructura_costes,
        break_even=break_even,
        metricas_clave=metricas_clave,
        riesgos=riesgos,
        insights=insights,
        modelo_demanda=modelo_demanda,
        business_model_type=business_model_type,
        correcciones_aplicadas=[CorreccionAplicada(**c) for c in correcciones_raw],
        capacity_model=capacity_model,
        tipo_negocio=tipo_negocio,
        validation_flags=resultado.get("validation_flags", []),
        ocupacion_efectiva=resultado.get("ocupacion_efectiva", 0.0),
        validacion_financiera=validacion_financiera,
        sensibilidad=[SensitividadItem(**s) for s in resultado.get("sensibilidad", [])],
    )


# ─── Correcciones hard ────────────────────────────────────────────────────────

def _aplicar_correcciones_demanda(
    v: dict,
    flujo_peatonal: float,
    business_model_type: str,
    capacidad_operativa: Optional[int],
    sector: str = "_default",
) -> list[dict]:
    """
    Aplica límites duros a clients_per_day en v (in-place).
    Devuelve lista de correcciones para mostrar al usuario.

    Reglas:
      1. Retail/restaurant: clients_per_day ≤ flujo_peatonal × tasa_sector (no universal 15%)
      2. Todos: clients_per_day ≤ max_capacity (del modelo)
      3. Todos: clients_per_day ≤ capacidad_operativa (input usuario, si existe)
    """
    correcciones: list[dict] = []

    # Regla 1: cap por capture_rate según sector (corregido auditoría: no universal 15%)
    sector_rate = _CAPTURE_RATE_POR_SECTOR.get(sector, _CAPTURE_RATE_POR_SECTOR["_default"])
    if flujo_peatonal > 0 and business_model_type not in ("appointment_based",):
        max_realistic = flujo_peatonal * sector_rate
        if v["clients_per_day"] > max_realistic and max_realistic > 0:
            original      = v["clients_per_day"]
            tasa_original = round(original / flujo_peatonal * 100, 1)
            v["clients_per_day"] = round(max_realistic, 1)
            correcciones.append({
                "parametro":       "clientes_dia",
                "valor_original":  original,
                "valor_corregido": v["clients_per_day"],
                "motivo": (
                    f"Captación estimada ({tasa_original}% del flujo peatonal) superaba el "
                    f"{round(sector_rate*100)}% máximo realista para el sector '{sector}'. "
                    f"Ajuste aplicado: {original} → {v['clients_per_day']} clientes/día."
                ),
            })

    # Regla 2: cap por max_capacity del modelo de capacidad
    max_cap = v.get("max_capacity", float("inf"))
    if v["clients_per_day"] > max_cap and math.isfinite(max_cap):
        original = v["clients_per_day"]
        v["clients_per_day"] = round(max_cap, 1)
        correcciones.append({
            "parametro":       "capacidad_maxima",
            "valor_original":  original,
            "valor_corregido": v["clients_per_day"],
            "motivo": (
                f"Demanda estimada ({original}) supera la capacidad máxima del modelo "
                f"({max_cap} clientes/día). Ajuste aplicado."
            ),
        })

    # Regla 3: cap por capacidad_operativa declarada por el usuario
    if capacidad_operativa and v["clients_per_day"] > capacidad_operativa:
        original = v["clients_per_day"]
        v["clients_per_day"] = float(capacidad_operativa)
        v["max_capacity"]    = min(float(capacidad_operativa), v.get("max_capacity", float("inf")))
        correcciones.append({
            "parametro":       "capacidad_operativa",
            "valor_original":  original,
            "valor_corregido": v["clients_per_day"],
            "motivo": (
                f"Demanda estimada ({original} clientes/día) supera la capacidad operativa "
                f"declarada ({capacidad_operativa} clientes/día). El modelo se ajusta a la "
                f"capacidad real del local."
            ),
        })

    v["clients_per_day"] = max(1.0, v["clients_per_day"])
    return correcciones


def _build_capacity_model(
    business_model_type: str,
    sector: str,
    m2: float,
    benchmarks: dict,
) -> Optional[CapacityModelInfo]:
    if business_model_type != "appointment_based":
        return None
    e_m2     = benchmarks.get("empleados_por_m2", 15.0) or 15.0
    units    = max(1, math.floor(m2 / e_m2))
    cmin     = benchmarks.get("clientes_dia_por_puesto_min", 2.0)
    cmax     = benchmarks.get("clientes_dia_por_puesto_max", 5.0)
    sessions = (cmin + cmax) / 2
    tipo_str = {"tatuajes": "artistas", "estetica": "cabinas", "clinica": "consultas",
                "peluqueria": "sillas"}.get(sector, "puestos")
    return CapacityModelInfo(
        tipo=business_model_type,
        descripcion=f"{units} {tipo_str} × {round(sessions, 1)} sesiones/día",
        units=units,
        sessions_per_unit_per_day=round(sessions, 1),
        max_clients_day=round(units * cmax),
    )


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _calcular_recomendacion(
    roi_b: float, payback_b: int, alquiler_pct: float, roi_c: float = 0.0
) -> str:
    """
    Umbrales corregidos (auditoría v4):
    - ROI base ≥ 40% (≈ 12% anual, por encima del coste de oportunidad)
    - Payback ≤ 18 meses (no 24)
    - Alquiler ≤ 10% de ingresos conservadores (no base)
    - El escenario conservador no puede tener ROI negativo para dar "sí"
    """
    if (roi_b >= 0.40
            and payback_b <= 18
            and alquiler_pct <= _UMBRAL_ALQUILER_VENTAS
            and roi_c >= 0.0):
        return "si"
    if roi_b >= 0.0 and payback_b <= 30:
        return "riesgo"
    return "no"


def _generar_riesgos(
    alquiler_pct: float,
    payback_b: int,
    roi_c: float,
    roi_b: float,
    inversion: float,
    capital: float,
    correcciones: list[dict],
    validation_flags: list[str] = None,
    ocupacion_efectiva: float = 0.0,
    cf_mes: float = 0.0,
    ing_estable: float = 0.0,
    margen_bruto: float = 0.65,
) -> list[Riesgo]:
    riesgos: list[Riesgo] = []
    flags = validation_flags or []

    # ── Bloqueos (impiden viabilidad) ──────────────────────────────────────────
    if roi_c < 0:
        riesgos.append(Riesgo(tipo="bloqueo",
            mensaje="ROI negativo en escenario conservador — si los ingresos caen un 40%, "
                    "la inversión no se recupera en 3 años. Para una decisión 'sí' se requiere "
                    "que incluso el escenario malo sea positivo."))

    if payback_b > 30:
        riesgos.append(Riesgo(tipo="bloqueo",
            mensaje=f"Payback de {payback_b} meses supera el límite recomendado de 30 meses."))

    if roi_b < 0.40 and roi_b >= 0:
        riesgos.append(Riesgo(tipo="warning",
            mensaje=f"ROI base del {round(roi_b * 100)}% inferior al 40% mínimo recomendado "
                    f"(≈12% anual). El negocio sería viable pero con retorno por debajo del "
                    f"coste de oportunidad de invertir el capital en otras opciones."))

    # ── Advertencias críticas ─────────────────────────────────────────────────
    if alquiler_pct > _UMBRAL_ALQUILER_VENTAS:
        riesgos.append(Riesgo(tipo="warning",
            mensaje=f"Alquiler: {round(alquiler_pct * 100)}% sobre ventas — "
                    f"supera el umbral del 15%. Con ventas por debajo de lo esperado, "
                    f"el alquiler puede absorber todo el margen."))

    if any(c["parametro"] == "clientes_dia" for c in correcciones):
        riesgos.append(Riesgo(tipo="warning",
            mensaje="Clientes corregidos automáticamente: la estimación inicial superaba "
                    "el 15% de captación del flujo peatonal. El modelo ha aplicado el límite realista."))

    if capital > 0 and inversion > capital * 1.4:
        riesgos.append(Riesgo(tipo="warning",
            mensaje=f"Inversión inicial ({round(inversion):,} €) supera 1.4× el capital disponible "
                    f"({round(capital):,} €). Considera financiación bancaria o socios."))

    # ── Productividad y plantilla ─────────────────────────────────────────────
    if any("sobredimensión" in f.lower() or "sobredimensi" in f.lower() for f in flags):
        riesgos.append(Riesgo(tipo="warning",
            mensaje="Sobredimensión de plantilla detectada: más empleados de los necesarios "
                    "para la demanda estimada. Aumenta la estructura de costes fijos sin incrementar ingresos."))

    if any("capacidad del personal insuficiente" in f.lower() for f in flags):
        riesgos.append(Riesgo(tipo="warning",
            mensaje="Capacidad del personal ajustada: la demanda estimada superaba lo que "
                    "el equipo puede atender. Ampliar plantilla o reducir demanda objetivo."))

    # ── Ocupación y supuestos ─────────────────────────────────────────────────
    if ocupacion_efectiva > 0.75:
        riesgos.append(Riesgo(tipo="warning",
            mensaje=f"El modelo depende de una ocupación efectiva del {round(ocupacion_efectiva * 100)}%. "
                    f"Alta ocupación sostenida requiere excelente operativa y captación constante de clientes."))

    if roi_b > 3.0:
        riesgos.append(Riesgo(tipo="warning",
            mensaje=f"ROI base del {round(roi_b * 100)}% supera el 300% — "
                    f"los supuestos del modelo son muy optimistas. Revisa costes e inversión."))

    # ── Estructura de costes ──────────────────────────────────────────────────
    if ing_estable > 0 and cf_mes > 0:
        cf_pct = cf_mes / ing_estable
        if cf_pct > 0.70:
            riesgos.append(Riesgo(tipo="warning",
                mensaje=f"Costes fijos muy elevados: {round(cf_pct * 100)}% de los ingresos en régimen estable. "
                        f"Ante una caída de demanda del 20%, el negocio entrará en pérdidas."))
        elif cf_pct > 0.50:
            riesgos.append(Riesgo(tipo="warning",
                mensaje=f"Estructura rígida: los costes fijos suponen el {round(cf_pct * 100)}% de los ingresos. "
                        f"Poca flexibilidad ante variaciones estacionales."))

    # ── Riesgo mínimo garantizado (siempre hay algo que evaluar) ──────────────
    if not riesgos:
        if margen_bruto < 0.50:
            riesgos.append(Riesgo(tipo="warning",
                mensaje=f"Margen bruto del {round(margen_bruto * 100)}% — moderado. "
                        f"El coste de mercancía consume más de la mitad de cada euro ingresado."))
        else:
            riesgos.append(Riesgo(tipo="warning",
                mensaje="Perfil financiero sólido, sin alertas críticas detectadas. "
                        "Monitoriza mensualmente ingresos vs. costes fijos durante el primer año."))

    return riesgos


def _generar_insights(
    capture_rate: float,
    competition_index: float,
    alquiler_pct: float,
    roi_b: float,
    margen_bruto: float,
    payback_b: int,
    business_model_type: str,
    tipo_negocio: str,
) -> list[Insight]:
    insights: list[Insight] = []

    if alquiler_pct > 0.18:
        insights.append(Insight(type="risk",
            message=f"Alquiler crítico: {round(alquiler_pct * 100)}% sobre ventas.",
            suggestion="Negocia a la baja el alquiler o busca un local con menor m²."))

    if competition_index > 0.75:
        insights.append(Insight(type="risk",
            message="Alta saturación competidora en la zona (índice >0.75).",
            suggestion="Diferénciate con propuesta de valor única o busca un nicho específico."))

    if business_model_type == "appointment_based" and capture_rate > 0.08:
        insights.append(Insight(type="risk",
            message="Negocio de cita: la demanda estimada es alta para la capacidad disponible.",
            suggestion="Verifica la capacidad operativa real (puestos × sesiones/día)."))

    if roi_b > 0.40 and payback_b <= 20:
        insights.append(Insight(type="opportunity",
            message=f"ROI atractivo ({round(roi_b * 100)}%) con payback de {payback_b} meses.",
            suggestion="Considera apalancar con financiación para acelerar la apertura."))

    if margen_bruto > 0.60:
        insights.append(Insight(type="opportunity",
            message=f"Margen bruto sólido ({round(margen_bruto * 100)}%) — por encima de la media.",
            suggestion="El margen da colchón ante caídas temporales de ventas."))

    if tipo_negocio == "traspaso":
        insights.append(Insight(type="opportunity",
            message="Traspaso: curva de arranque acelerada — cartera de clientes heredada.",
            suggestion="Negocia incluir contratos de proveedores y base de datos de clientes."))

    if capture_rate < 0.04 and competition_index < 0.50:
        insights.append(Insight(type="opportunity",
            message="Baja competencia con flujo peatonal disponible — oportunidad de captación.",
            suggestion="Invierte en visibilidad para capturar más tráfico peatonal."))

    return insights[:5]


def _build_validation_payload(
    zona_id: str,
    sector: str,
    business_model_type: str,
    tipo_negocio: str,
    params: "ParametrosResponse",
    v: dict,
    resultado: dict,
    ing_estable: float,
    margen_bruto: float,
    max_potential: float,
    benchmarks: dict,
    correcciones_raw: list[dict],
    has_overrides: bool,
) -> dict:
    """Construye el dict que recibe validar_financiero() con todos los datos relevantes."""
    costes_est = resultado.get("estructura_costes", {})
    roi_b      = resultado["roi_3a_base"]

    return {
        "zona_id":             zona_id,
        "sector":              sector,
        "business_model_type": business_model_type,
        "tipo_negocio":        tipo_negocio,
        "has_overrides":       has_overrides,
        "parametros":          params.model_dump(),
        "economia_base": {
            "ingresos_mensuales":      round(ing_estable),
            "clientes_dia":            resultado["clients_per_day"],
            "ticket_medio":            v["ticket_medio"],
            "max_potential_customers": round(max_potential, 1),
            "ocupacion_efectiva":      resultado.get("ocupacion_efectiva", 0.0),
        },
        "estructura_costes": costes_est,
        "metricas": {
            "roi_conservador": resultado["roi_3a_conservador"],
            "roi_base":        roi_b,
            "roi_optimista":   resultado["roi_3a_optimista"],
            "margen_bruto_pct": round(margen_bruto, 3),
            "payback_meses":   resultado["payback_meses_base"],
        },
        "inversion_total":    resultado["inversion_total"],
        "max_capacity":       v.get("max_capacity", params.max_capacity),
        "ocupacion_efectiva": resultado.get("ocupacion_efectiva", 0.0),
        "benchmarks_sector":  benchmarks,
        "validation_flags":   resultado.get("validation_flags", []),
        "correcciones_str":   [c.get("motivo", "") for c in correcciones_raw],
    }


def _parse_validacion(raw: dict) -> Optional[ValidacionFinanciera]:
    """Construye ValidacionFinanciera desde el dict del LLM. Nunca lanza excepción."""
    try:
        checks_raw = raw.get("checks_detallados", {})
        return ValidacionFinanciera(
            coherencia_global=raw.get("coherencia_global", "media"),
            veredicto=raw.get("veredicto", "fiable"),
            problemas_detectados=[
                ProblemaDetectado(**p)
                for p in raw.get("problemas_detectados", [])
                if isinstance(p, dict) and "tipo" in p and "descripcion" in p and "impacto" in p
            ],
            ajustes_recomendados=[
                AjusteRecomendado(**a)
                for a in raw.get("ajustes_recomendados", [])
                if isinstance(a, dict) and all(k in a for k in ("variable", "accion", "rango_sugerido", "motivo"))
            ],
            supuestos_peligrosos=[
                s for s in raw.get("supuestos_peligrosos", []) if isinstance(s, str)
            ],
            checks_detallados=ChecksDetallados(
                capacidad=checks_raw.get("capacidad", "ok"),
                costes=checks_raw.get("costes",    "ok"),
                margenes=checks_raw.get("margenes", "ok"),
                roi=checks_raw.get("roi",      "ok"),
                payback=checks_raw.get("payback",  "ok"),
            ),
        )
    except Exception as exc:
        logger.warning("_parse_validacion error: %s", exc)
        return None


async def _get_o_calcular_estimados(
    zona_id: str,
    sector: str,
    perfil: dict,
) -> ParametrosEstimados:
    precalc = await get_parametros_precalculados(zona_id=zona_id, sector=sector)
    if precalc:
        return _row_to_estimados(precalc)
    logger.info("Calculando parámetros en tiempo real zona=%s sector=%s", zona_id, sector)
    return await estimar_parametros(zona_id=zona_id, sector=sector, perfil=perfil)


def _row_to_estimados(p: dict) -> ParametrosEstimados:
    def pe(valor, fuente, confianza, rmin, rmax) -> PE:
        return PE(valor=valor, fuente=fuente, confianza=confianza,
                  rango_min=rmin, rango_max=rmax)

    # Derivar clients_per_day del promedio de conservador/optimista de BD
    cpd_val = (p["clientes_dia_conservador"] + p["clientes_dia_optimista"]) / 2
    max_cap  = p["clientes_dia_optimista"] * 1.5
    slider_max = max(100.0, max_cap)

    return ParametrosEstimados(
        ticket_medio=pe(p["ticket_medio"], p["ticket_fuente"], p["ticket_confianza"],
                        p["ticket_rango_min"], p["ticket_rango_max"]),
        clients_per_day=pe(cpd_val, p["clientes_fuente"], p["clientes_confianza"],
                           1.0, slider_max),
        max_capacity=max_cap,
        clientes_dia_conservador=pe(p["clientes_dia_conservador"], p["clientes_fuente"],
                                    p["clientes_confianza"], 1.0, slider_max),
        clientes_dia_optimista=pe(p["clientes_dia_optimista"], p["clientes_fuente"],
                                  p["clientes_confianza"], 1.0, slider_max),
        dias_apertura_mes=pe(p["dias_apertura_mes"],
                             "Mediana horario competidores / benchmarks sector",
                             "alta", 20, 31),
        alquiler_mensual=pe(p["alquiler_mensual"], p["alquiler_fuente"], p["alquiler_confianza"],
                            round(p["alquiler_mensual"] * 0.75),
                            round(p["alquiler_mensual"] * 1.40)),
        salarios_mensual=pe(p["salarios_mensual"], p["salarios_fuente"], "media",
                            round(p["salarios_mensual"] * 0.50),
                            round(p["salarios_mensual"] * 1.80)),
        otros_fijos_mensual=pe(p["otros_fijos_mensual"],
                               "Suministros + seguro RC + gestoría + mantenimiento", "media",
                               round(p["otros_fijos_mensual"] * 0.70),
                               round(p["otros_fijos_mensual"] * 1.60)),
        coste_mercancia_pct=pe(p["coste_mercancia_pct"],
                               "INE CNAE — margen bruto típico del sector", "alta",
                               max(0.0, p["coste_mercancia_pct"] - 0.10),
                               min(0.95, p["coste_mercancia_pct"] + 0.10)),
        reforma_local=pe(p["reforma_estimada"], "m² × €/m² benchmarks sectoriales", "baja",
                         p["reforma_rango_min"], p["reforma_rango_max"]),
        equipamiento=pe(p["equipamiento_estimado"], "Benchmarks sectoriales", "baja",
                        p["equipamiento_rango_min"], p["equipamiento_rango_max"]),
        deposito_fianza=pe(p["deposito_fianza"],
                           "Art. 36 LAU — 2 meses de alquiler", "alta",
                           p["alquiler_mensual"], p["alquiler_mensual"] * 3),
        otros_iniciales=pe(p["otros_iniciales"],
                           "Licencias apertura + constitución SL + gestoría apertura", "media",
                           round(p["otros_iniciales"] * 0.80),
                           round(p["otros_iniciales"] * 1.40)),
        num_empleados=p["num_empleados"],
    )


def _aplicar_overrides(
    estimados: ParametrosEstimados,
    overrides: dict[str, float],
) -> ParametrosResponse:
    def _build(nombre: str, estimado: PE) -> ParametroResponse:
        if nombre in overrides:
            return ParametroResponse(
                valor_estimado=estimado.valor,
                valor_usado=overrides[nombre],
                es_override=True,
                fuente=estimado.fuente,
                confianza=estimado.confianza,
                rango_min=estimado.rango_min,
                rango_max=estimado.rango_max,
            )
        return ParametroResponse(
            valor_estimado=estimado.valor,
            valor_usado=estimado.valor,
            es_override=False,
            fuente=estimado.fuente,
            confianza=estimado.confianza,
            rango_min=estimado.rango_min,
            rango_max=estimado.rango_max,
        )

    return ParametrosResponse(
        ticket_medio=        _build("ticket_medio",        estimados.ticket_medio),
        clients_per_day=     _build("clients_per_day",     estimados.clients_per_day),
        max_capacity=        estimados.max_capacity,
        dias_apertura_mes=   _build("dias_apertura_mes",   estimados.dias_apertura_mes),
        alquiler_mensual=    _build("alquiler_mensual",    estimados.alquiler_mensual),
        num_empleados=       estimados.num_empleados,
        salarios_mensual=    _build("salarios_mensual",    estimados.salarios_mensual),
        otros_fijos_mensual= _build("otros_fijos_mensual", estimados.otros_fijos_mensual),
        coste_mercancia_pct= _build("coste_mercancia_pct", estimados.coste_mercancia_pct),
        reforma_local=       _build("reforma_local",       estimados.reforma_local),
        equipamiento=        _build("equipamiento",        estimados.equipamiento),
        deposito_fianza=     _build("deposito_fianza",     estimados.deposito_fianza),
        otros_iniciales=     _build("otros_iniciales",     estimados.otros_iniciales),
    )
