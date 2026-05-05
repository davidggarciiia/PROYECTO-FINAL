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

import datetime
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
from financiero.validador_pipeline import run_pipeline, BusinessInput as PipelineInput
from financiero.gatekeeper import run_gatekeeper, GatekeeperInput
from scoring.concepto.taxonomy import SECTOR_PROFILE_DEFAULTS, NEUTRAL_PROFILE
from financiero.estimador import estimar_parametros, ParametrosEstimados, PE, aplicar_subsector
from financiero.calculadora import calcular_proyeccion, get_max_occupancy
from db.sesiones import get_sesion
from db.financiero import (
    get_parametros_precalculados,
    guardar_analisis_financiero,
    get_benchmarks_sector,
)

logger = logging.getLogger(__name__)
router = APIRouter(tags=["financiero"])

_UMBRAL_ALQUILER_VENTAS   = 0.10   # base para decisión "sí" (dinámico por perfil_negocio)

# Ingresos máximos realistas por m² y mes (€) — límite físico duro por tipo de local.
# Superar esto implica rotación imposible o tickets de fantasy. Genera flag "ingresos_irreales".
_MAX_INGRESOS_M2: dict[str, float] = {
    "restauracion":  900.0,  # restaurante excelente BCN ~750; 900 = techo absoluto
    "estetica":      700.0,  # salón ocupado al 100%, 8 sillas productivas
    "tatuajes":      600.0,  # estudio pequeño, citas continuas
    "moda":          550.0,  # retail ropa — rotación limitada por decisión de compra
    "shisha_lounge": 400.0,  # gasto por mesa limitado, sesiones largas
    "farmacia":      950.0,  # alta rotación y ticket bajo pero volumen
    "supermercado":  1200.0, # volumen + frecuencia diaria
    "alimentacion":  700.0,
    "_default":      700.0,
}

# Margen neto máximo creíble por sector (EBITDA / ingresos).
# Por encima → el modelo está generando dinero falso. Genera flag "margen_irreal".
_MARGEN_NETO_MAX: dict[str, float] = {
    "restauracion":  0.22,  # industria: 8–15%; 22% = excelente, encima = irrealista
    "moda":          0.20,
    "estetica":      0.28,  # appointment con COGS bajo — margen algo mayor
    "tatuajes":      0.35,  # mano de obra cualificada pero sin COGS material
    "shisha_lounge": 0.20,
    "farmacia":      0.18,
    "supermercado":  0.12,  # volumen pero márgenes muy ajustados
    "alimentacion":  0.18,
    "_default":      0.25,
}

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
    mes_apertura:     Optional[int]             = None  # 1–12; None → mes actual


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
    margen_sector_tipico:                  float
    alquiler_sobre_ventas_pct:             float
    alquiler_sobre_ventas_pct_conservador: float = 0.0
    alerta_alquiler:                       bool
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
    max_staff_capacity:     float                      = 0.0
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

    sector             = sesion.get("perfil", {}).get("sector", "desconocido")
    perfil             = sesion.get("perfil", {})
    descripcion_original = sesion.get("descripcion_original", "") or ""

    # Subsector desde taxonomía — usar perfil_refinado si existe (re-refinamiento del cuestionario)
    _perfil_refinado = perfil.get("perfil_refinado") or {}
    _concepto       = _perfil_refinado.get("concepto_negocio") or perfil.get("concepto_negocio") or {}
    _base_concepts  = _concepto.get("base_concepts") or []
    _top_id         = _base_concepts[0].get("id", "") if _base_concepts else ""
    subsector = _top_id.split(".")[-1] if _top_id.count(".") >= 2 else ""
    if not subsector:
        subsector = perfil.get("subsector_detectado") or ""

    logger.info(
        "DEBUG financiero — sector=%r subsector=%r descripcion=%r top_concept_id=%r",
        sector, subsector, descripcion_original[:60], _top_id,
    )

    # Dimensiones del perfil de negocio desde taxonomía (fallback: defaults del sector)
    _pn = perfil.get("perfil_negocio") or {}
    _sector_defaults = SECTOR_PROFILE_DEFAULTS.get(sector, NEUTRAL_PROFILE)
    dependencia_flujo     = float(_pn.get("dependencia_flujo",     _sector_defaults.get("dependencia_flujo",     0.5)))
    sensibilidad_alquiler = float(_pn.get("sensibilidad_alquiler", _sector_defaults.get("sensibilidad_alquiler", 0.5)))
    # Umbral de alquiler dinámico: negocios menos sensibles al alquiler toleran una ratio mayor
    _umbral_alquiler_decision = round(0.10 + (1.0 - sensibilidad_alquiler) * 0.08, 4)
    _umbral_alquiler_display  = round(_umbral_alquiler_decision + 0.05, 4)

    bc           = body.business_context
    tipo_negocio = bc.tipo if bc else "nuevo"

    _capital_raw = (
        (bc.capital_inicial if bc and bc.capital_inicial else None)
        or perfil.get("capital_disponible")
    )
    if not _capital_raw:
        raise HTTPException(
            status_code=422,
            detail="capital_inicial es obligatorio. Indica el capital disponible para iniciar la actividad.",
        )
    capital = float(_capital_raw)
    capacidad_operativa = bc.capacidad_operativa if bc else None

    # ── Estimar parámetros ────────────────────────────────────────────────────
    estimados = await _get_o_calcular_estimados(
        zona_id=body.zona_id, sector=sector, perfil=perfil, subsector=subsector,
        descripcion=descripcion_original, session_id=body.session_id,
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
    _m2                 = float(perfil.get("m2_aprox", 60.0))

    # ── Pipeline determinista: clasificación + validaciones físicas ───────────
    _sal_base = benchmarks.get("salario_base_mensual_convenio", 1400.0)
    _dur       = (benchmarks.get("duracion_servicio_min")
                  if business_model_type == "appointment_based" else None)
    _pipeline_result = run_pipeline(PipelineInput(
        sector=sector,
        m2_local=_m2,
        empleados=v["num_empleados"],
        horas_apertura=float(benchmarks.get("horas_apertura_dia", 9.0)),
        clientes_dia=v["clients_per_day"],
        ticket_medio=v["ticket_medio"],
        cogs_pct=v["coste_mercancia_pct"],
        salario_base_mensual=float(_sal_base),
        duracion_servicio=float(_dur) if _dur else None,
    ))
    # Aplicar el ajuste de clientes del pipeline (sólo si es más restrictivo)
    if _pipeline_result.constraints["adjusted_clients"] < v["clients_per_day"]:
        v["clients_per_day"] = _pipeline_result.constraints["adjusted_clients"]
    # CAMBIO 1+7: correcciones con capa explícita para trazabilidad
    _pipeline_corrections: list[dict] = [
        {
            "parametro":       c["field"],
            "valor_original":  float(c["original"]),
            "valor_corregido": float(c["corrected"]),
            "motivo":          c["reason"],
            "capa":            "pipeline",
        }
        for c in _pipeline_result.corrections
    ]

    # ── Gatekeeper: stream decomposition + hard physical constraints ──────────
    _gk_result = run_gatekeeper(GatekeeperInput(
        sector=sector,
        total_m2=_m2,
        hours_open_per_day=float(benchmarks.get("horas_apertura_dia", 9.0)),
        total_staff=v["num_empleados"],
        avg_ticket=v["ticket_medio"],
        flujo_peatonal_dia=flujo_peatonal,
        conversion_rate=float(_CAPTURE_RATE_POR_SECTOR.get(sector, _CAPTURE_RATE_POR_SECTOR["_default"])),
        service_duration_min=float(_dur) if _dur else None,
        seats_or_capacity=float(perfil.get("aforo", 0.0)) or None,
        avg_stay_min=float(benchmarks.get("duracion_media_visita_min", 0.0)) or None,
    ))
    # Cap clients_per_day with gatekeeper's max_capacity (sólo si más restrictivo)
    _gk_max = _gk_result.constraints["max_capacity"]
    if _gk_max > 0 and v["clients_per_day"] > _gk_max:
        _gk_corrections: list[dict] = [{
            "parametro":       "clientes_dia",
            "valor_original":  v["clients_per_day"],
            "valor_corregido": round(_gk_max, 1),
            "motivo":          f"Gatekeeper: demanda supera la capacidad física del stream principal ({round(_gk_max, 1)} clientes/día).",
            "capa":            "gatekeeper",
        }]
        v["clients_per_day"] = round(_gk_max, 1)
    else:
        _gk_corrections = []
    _gk_corrections += [
        {
            "parametro":       c["field"],
            "valor_original":  float(c["original"]),
            "valor_corregido": float(c["corrected"]),
            "motivo":          c["reason"],
            "capa":            "gatekeeper",
        }
        for c in _gk_result.corrections
    ]

    # ── Correcciones hard (CRÍTICO — antes de la calculadora) ─────────────────
    correcciones_raw = _aplicar_correcciones_demanda(
        v=v,
        flujo_peatonal=flujo_peatonal,
        business_model_type=business_model_type,
        capacidad_operativa=capacidad_operativa,
        sector=sector,
        dependencia_flujo=dependencia_flujo,
    )
    correcciones_raw = _pipeline_corrections + _gk_corrections + correcciones_raw

    # ── Modelo de capacidad ───────────────────────────────────────────────────
    capacity_model = _build_capacity_model(
        business_model_type=business_model_type,
        sector=sector,
        m2=_m2,
        benchmarks=benchmarks,
    )

    # ── Caps físicos ANTES de la proyección (corrigen v, no solo avisan) ─────
    _max_occ_est   = get_max_occupancy(sector)
    _max_ing_m2    = _MAX_INGRESOS_M2.get(sector, _MAX_INGRESOS_M2["_default"])
    _ing_techo     = _m2 * _max_ing_m2
    _ing_potencial = (
        v["ticket_medio"] * v["clients_per_day"]
        * v["dias_apertura_mes"] * _max_occ_est
    )

    # 1. Revenue cap: si los ingresos potenciales superan el techo físico → reducir clientes
    if (v["ticket_medio"] > 0 and v["dias_apertura_mes"] > 0
            and _max_occ_est > 0 and _ing_potencial > _ing_techo):
        _clients_cap = _ing_techo / (v["ticket_medio"] * v["dias_apertura_mes"] * _max_occ_est)
        correcciones_raw.append({
            "parametro":       "clientes_dia",
            "valor_original":  v["clients_per_day"],
            "valor_corregido": round(_clients_cap, 1),
            "motivo": (
                f"Revenue cap: ingresos potenciales ({_ing_potencial:,.0f}€/mes) superan el "
                f"techo físico del sector ({_max_ing_m2:.0f}€/m² × {_m2:.0f}m² = {_ing_techo:,.0f}€)"
            ),
            "capa": "caps_fisicos",
        })
        v["clients_per_day"] = round(_clients_cap, 1)

    # 2. Cross-validación modelo de negocio — incoherencia COGS vs tipo operativo
    # Retail no puede tener márgenes de servicio puro; servicio puro no tiene 0% COGS.
    _cogs = v["coste_mercancia_pct"]
    if business_model_type == "retail_walkin" and _cogs < 0.35:
        correcciones_raw.append({
            "parametro":       "coste_mercancia_pct",
            "valor_original":  _cogs,
            "valor_corregido": 0.35,
            "motivo": (
                f"Incoherencia modelo: retail con COGS {_cogs:.0%} — "
                "retail físico no puede operar con márgenes de servicio puro (mínimo 35%)"
            ),
            "capa": "caps_fisicos",
        })
        v["coste_mercancia_pct"] = 0.35
    elif business_model_type == "appointment_based" and _cogs > 0.60:
        correcciones_raw.append({
            "parametro":       "coste_mercancia_pct",
            "valor_original":  _cogs,
            "valor_corregido": 0.60,
            "motivo": (
                f"Incoherencia modelo: servicio por cita con COGS {_cogs:.0%} — "
                "los servicios de cita rara vez superan el 60% de COGS"
            ),
            "capa": "caps_fisicos",
        })
        v["coste_mercancia_pct"] = 0.60

    # ── Jerarquía explícita — resultado final es el mínimo de todos los límites ─
    # Orden de precedencia: gatekeeper (físico duro) > caps_fisicos (económico) > demanda
    # Cada capa solo reduce; esta línea garantiza que ninguna corrección posterior
    # haya superado inadvertidamente el techo del gatekeeper.
    _final_hard_cap = min(
        v.get("max_capacity", float("inf")),
        float(_gk_max) if _gk_max > 0 else float("inf"),
    )
    if math.isfinite(_final_hard_cap) and v["clients_per_day"] > _final_hard_cap:
        v["clients_per_day"] = max(1.0, round(_final_hard_cap, 1))
    v["clients_per_day"] = max(1.0, v["clients_per_day"])

    # ── Proyección 36 meses ───────────────────────────────────────────────────
    # CAMBIO 2: mes de apertura real (default = mes actual del sistema)
    _mes_apertura = body.mes_apertura or datetime.datetime.now().month

    try:
        resultado = await calcular_proyeccion(
            v, tipo_negocio=tipo_negocio, sector=sector, mes_apertura=_mes_apertura,
        )
    except Exception as exc:
        logger.error("Error calculadora zona=%s: %s", body.zona_id, exc, exc_info=True)
        raise HTTPException(status_code=500, detail="Error calculando la proyección financiera.")

    # ── Métricas derivadas ────────────────────────────────────────────────────────
    _max_occ          = resultado.get("max_occupancy_usado", get_max_occupancy(sector))
    cl_base           = resultado["clients_per_day"]
    cl_conservador    = resultado["scenario_clients"]["conservador"]
    ing_estable       = v["ticket_medio"] * cl_base * v["dias_apertura_mes"] * _max_occ
    ing_conservador   = v["ticket_medio"] * cl_conservador * v["dias_apertura_mes"] * _max_occ
    margen_bruto      = 1 - v["coste_mercancia_pct"]
    # CAMBIO 3: alquiler calculado sobre conservador (decisión) y sobre base (display)
    alquiler_pct      = (v["alquiler_mensual"] / ing_conservador) if ing_conservador > 0 else 1.0
    alquiler_pct_base = (v["alquiler_mensual"] / ing_estable) if ing_estable > 0 else 1.0

    # ── Flags residuales post-proyección (solo informativos — no pueden corregirse aquí) ──
    _costes_fijos = v["alquiler_mensual"] + v["salarios_mensual"] + v["otros_fijos_mensual"]
    _extra_flags: list[str] = []

    # Runway: el capital es un input fijo, no se puede corregir — solo avisar
    _meses_runway = (capital / _costes_fijos) if _costes_fijos > 0 else 999
    if _meses_runway < 4:
        _extra_flags.append(
            f"runway_critico: {capital:,.0f}€ capital / {_costes_fijos:,.0f}€/mes costes fijos "
            f"= {_meses_runway:.1f} meses de runway — frágil ante cualquier retraso"
        )

    # Margen neto residual: safety net por si el revenue cap no fue suficiente
    _ebitda_mes  = ing_estable * margen_bruto - _costes_fijos
    _margen_neto = _ebitda_mes / ing_estable if ing_estable > 0 else 0.0
    _max_neto    = _MARGEN_NETO_MAX.get(sector, _MARGEN_NETO_MAX["_default"])
    if _margen_neto > _max_neto:
        _extra_flags.append(
            f"margen_neto_residual: margen neto {_margen_neto:.0%} sigue por encima del "
            f"máximo creíble para {sector} ({_max_neto:.0%}) — revisar costes fijos"
        )

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
        subsector=subsector,
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
        descripcion=descripcion_original,
        m2_aprox=_m2,
        capital_disponible=capital,
        extra_flags=_extra_flags,
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

    _recomendacion = _calcular_recomendacion(
        roi_b, pb_b, alquiler_pct, roi_c, umbral_alquiler=_umbral_alquiler_decision,
    )

    # Runway crítico degrada interpretación — no cambia números, cambia decisión
    # El capital es un input fijo: no se puede corregir, solo avisar y penalizar score.
    if _meses_runway < 2 and _recomendacion != "no":
        _recomendacion = "no"
    elif _meses_runway < 4 and _recomendacion == "si":
        _recomendacion = "riesgo"

    _gap_capital = max(0.0, resultado["inversion_total"] - capital)

    # CAMBIO 5: score calculado en backend, coherente con la decisión
    _viability_score = _calcular_viability_score(
        roi_b=roi_b, payback_b=pb_b, alquiler_pct=alquiler_pct, roi_c=roi_c,
        margen_bruto=margen_bruto, gap_capital=_gap_capital, capital=capital,
        recomendacion=_recomendacion, umbral_alquiler=_umbral_alquiler_decision,
    )
    # Penalización de runway sobre el score (el clamp de recomendacion ya lo limita;
    # esta penalización garantiza que el score refleje la realidad dentro de la banda)
    if _meses_runway < 4:
        _viability_score = max(0, _viability_score - 15)
    if _meses_runway < 2:
        _viability_score = max(0, _viability_score - 15)

    # CAMBIO 6: factor limitante + narrativa completa + recomendaciones accionables
    _explicacion_raw = _calcular_explicacion(
        roi_b=roi_b, payback_b=pb_b, alquiler_pct=alquiler_pct, roi_c=roi_c,
        umbral_alquiler=_umbral_alquiler_decision,
        alquiler_mensual=v["alquiler_mensual"],
        recomendacion=_recomendacion,
        margen_bruto=margen_bruto,
        meses_runway=_meses_runway,
        correcciones_caps=[c for c in correcciones_raw if c.get("capa") == "caps_fisicos"],
    )

    # CAMBIO 4: ordenar correcciones por impacto absoluto y añadir impacto_pct
    for c in correcciones_raw:
        orig = c.get("valor_original", 0.0)
        c["impacto_pct"] = round((c["valor_corregido"] - orig) / orig, 3) if orig != 0 else 0.0
    correcciones_raw.sort(key=lambda x: abs(x["valor_original"] - x["valor_corregido"]), reverse=True)

    from schemas.models import ExplicacionDecision
    decision = DecisionBlock(
        recomendacion=_recomendacion,
        beneficio_mensual=ben_mes,
        payback=pb_b,
        capital_necesario=resultado["inversion_total"],
        gap_capital=_gap_capital,
        viability_score=_viability_score,
        explicacion=ExplicacionDecision(**_explicacion_raw),
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
        validation_flags=resultado.get("validation_flags", []) + _extra_flags,
        ocupacion_efectiva=resultado.get("ocupacion_efectiva", 0.0),
        cf_mes=v["alquiler_mensual"] + v["salarios_mensual"] + v["otros_fijos_mensual"],
        ing_estable=ing_estable,
        margen_bruto=margen_bruto,
        umbral_alquiler=_umbral_alquiler_decision,
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
        # CAMBIO 3: ambos % expuestos — base (display) y conservador (decisión)
        alquiler_sobre_ventas_pct=round(alquiler_pct_base, 3),
        alquiler_sobre_ventas_pct_conservador=round(alquiler_pct, 3),
        # Alerta visual: umbral dinámico sobre ingresos base
        alerta_alquiler=alquiler_pct_base > _umbral_alquiler_display,
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
        validation_flags=list(dict.fromkeys(
            _pipeline_result.flags + resultado.get("validation_flags", []) + _extra_flags
        )),
        ocupacion_efectiva=resultado.get("ocupacion_efectiva", 0.0),
        max_staff_capacity=round(float(resultado.get("max_staff_capacity", 0.0)), 1),
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
    dependencia_flujo: float = 0.5,
) -> list[dict]:
    """
    Aplica límites duros a clients_per_day en v (in-place).
    Devuelve lista de correcciones para mostrar al usuario.

    Reglas:
      1. Retail/restaurant: clients_per_day ≤ flujo_peatonal × tasa_sector × factor_perfil
      2. Todos: clients_per_day ≤ max_capacity (del modelo)
      3. Todos: clients_per_day ≤ capacidad_operativa (input usuario, si existe)
    """
    correcciones: list[dict] = []

    # Regla 1: cap por capture_rate según sector, escalado por dependencia_flujo del perfil
    # Negocios muy dependientes del paso (flujo alto) pueden captar algo más; los de cita menos.
    sector_rate_base = _CAPTURE_RATE_POR_SECTOR.get(sector, _CAPTURE_RATE_POR_SECTOR["_default"])
    sector_rate = min(sector_rate_base * (0.75 + dependencia_flujo * 0.50), 0.20)
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
                    f"{round(sector_rate*100, 1)}% máximo para el sector '{sector}' "
                    f"(dependencia de paso: {round(dependencia_flujo*100)}%). "
                    f"Ajuste aplicado: {original} → {v['clients_per_day']} clientes/día."
                ),
                "capa": "demanda",
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
            "capa": "demanda",
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
            "capa": "demanda",
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
    roi_b: float, payback_b: int, alquiler_pct: float, roi_c: float = 0.0,
    umbral_alquiler: float = _UMBRAL_ALQUILER_VENTAS,
) -> str:
    """
    Umbrales corregidos (auditoría v4):
    - ROI base ≥ 40% (≈ 12% anual, por encima del coste de oportunidad)
    - Payback ≤ 18 meses (no 24)
    - Alquiler ≤ umbral dinámico de ingresos conservadores (ajustado por sensibilidad_alquiler)
    - El escenario conservador no puede tener ROI negativo para dar "sí"
    """
    if (roi_b >= 0.40
            and payback_b <= 18
            and alquiler_pct <= umbral_alquiler
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
    umbral_alquiler: float = _UMBRAL_ALQUILER_VENTAS,
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
    if alquiler_pct > umbral_alquiler:
        riesgos.append(Riesgo(tipo="warning",
            mensaje=f"Alquiler: {round(alquiler_pct * 100)}% sobre ventas — "
                    f"supera el umbral del {round(umbral_alquiler * 100)}% para este tipo de negocio. "
                    f"Con ventas por debajo de lo esperado, el alquiler puede absorber todo el margen."))

    # Correcciones de capacidad → narrativa humana
    for c in correcciones:
        capa = c.get("capa", "")
        param = c.get("parametro", "")
        orig = c.get("valor_original", 0)
        corr = c.get("valor_corregido", 0)

        if capa == "caps_fisicos" and param == "clientes_dia":
            riesgos.append(Riesgo(tipo="warning",
                mensaje=(
                    f"Tu local puede atender como máximo ~{int(corr)} clientes/día dadas sus "
                    f"dimensiones y sector. El modelo inicial asumía {int(orig)} — hemos ajustado "
                    f"la previsión al techo físico real."
                )))
        elif capa == "caps_fisicos" and param == "coste_mercancia_pct":
            modelo = "retail" if corr >= orig else "servicio por cita"
            riesgos.append(Riesgo(tipo="warning",
                mensaje=(
                    f"Incoherencia de modelo detectada: los costes variables estimados "
                    f"({round(orig * 100)}%) no son coherentes con un negocio de tipo {modelo}. "
                    f"Ajustado automáticamente a {round(corr * 100)}%."
                )))
        elif capa in ("gatekeeper", "pipeline") and param in ("clientes_dia", "daily_clients"):
            if orig > corr:
                riesgos.append(Riesgo(tipo="warning",
                    mensaje=(
                        f"La demanda estimada ({int(orig)} clientes/día) superaba la capacidad física "
                        f"del local. Ajustado a {int(corr)} clientes/día para respetar el modelo operativo."
                    )))

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
    subsector: str = "",
    descripcion: str = "",
    m2_aprox: float = 60.0,
    capital_disponible: float = 0.0,
    extra_flags: list[str] | None = None,
) -> dict:
    """Construye el dict que recibe validar_financiero() con todos los datos relevantes."""
    costes_est = resultado.get("estructura_costes", {})
    roi_b      = resultado["roi_3a_base"]

    return {
        "zona_id":             zona_id,
        "sector":              sector,
        "subsector":           subsector,
        "descripcion":         descripcion,
        "m2_aprox":            m2_aprox,
        "capital_disponible":  capital_disponible,
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
        "validation_flags":   resultado.get("validation_flags", []) + (extra_flags or []),
        "correcciones_str":   [c.get("motivo", "") for c in correcciones_raw],
    }


def _parse_validacion(raw: dict) -> Optional[ValidacionFinanciera]:
    """Construye ValidacionFinanciera desde el dict del LLM. Nunca lanza excepción."""
    try:
        checks_raw = raw.get("checks_detallados", {})
        return ValidacionFinanciera(
            coherencia_global=raw.get("coherencia_global", "media"),
            veredicto=raw.get("veredicto", "fiable"),
            subsector_usado=raw.get("subsector_usado", ""),
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
    subsector: str = "",
    descripcion: str = "",
    session_id: str = "",
) -> ParametrosEstimados:
    precalc = await get_parametros_precalculados(zona_id=zona_id, sector=sector)
    if precalc:
        estimados = _row_to_estimados(precalc)
        # Bug 2 fix: aplicar overrides de subsector sobre el caché semanal
        # El pipeline semanal no conoce el subsector del usuario → ajustar ticket, margen y modelo
        await aplicar_subsector(estimados, sector, subsector, descripcion, session_id)
        return estimados
    logger.info(
        "Calculando parámetros en tiempo real zona=%s sector=%s subsector=%s",
        zona_id, sector, subsector,
    )
    return await estimar_parametros(
        zona_id=zona_id, sector=sector, perfil=perfil,
        subsector=subsector, descripcion=descripcion, session_id=session_id,
    )


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


def _calcular_viability_score(
    roi_b: float,
    payback_b: int,
    alquiler_pct: float,
    roi_c: float,
    margen_bruto: float,
    gap_capital: float,
    capital: float,
    recomendacion: str,
    umbral_alquiler: float,
) -> int:
    """CAMBIO 5: score coherente con la decisión backend.
    - recomendacion='si'    → score ≥ 70
    - recomendacion='riesgo'→ 50–69
    - recomendacion='no'    → < 50
    """
    score = 0
    score += 25 if roi_b >= 0.60 else 18 if roi_b >= 0.40 else 10 if roi_b >= 0.20 else 4 if roi_b >= 0 else 0
    score += 20 if payback_b <= 12 else 16 if payback_b <= 18 else 10 if payback_b <= 24 else 5 if payback_b <= 30 else 1 if payback_b < 999 else 0
    score += 20 if alquiler_pct <= umbral_alquiler else 12 if alquiler_pct <= umbral_alquiler * 1.3 else 5 if alquiler_pct <= 0.20 else 0
    score += 15 if roi_c >= 0.20 else 10 if roi_c >= 0 else 0
    score += 10 if margen_bruto >= 0.50 else 6 if margen_bruto >= 0.35 else 2
    score += 10 if gap_capital <= 0 else 5 if capital > 0 and gap_capital < capital * 0.30 else 0
    score = min(100, max(0, score))
    if recomendacion == "si":
        score = max(score, 70)
    elif recomendacion == "riesgo":
        score = max(50, min(69, score))
    else:
        score = min(score, 49)
    return score


def _calcular_explicacion(
    roi_b: float,
    payback_b: int,
    alquiler_pct: float,
    roi_c: float,
    umbral_alquiler: float,
    alquiler_mensual: float,
    recomendacion: str,
    margen_bruto: float = 0.0,
    meses_runway: float = 999.0,
    correcciones_caps: list[dict] | None = None,
) -> dict:
    """Factor limitante + narrativa completa + recomendaciones accionables.
    Incluye contexto de correcciones físicas y runway para mayor transparencia."""
    # ── Factor limitante por magnitud de impacto negativo ──
    exceso_alquiler  = max(0.0, alquiler_pct - umbral_alquiler)
    margen_bajo      = max(0.0, 0.60 - margen_bruto)
    demanda_negativa = 1.0 if roi_c < 0 else 0.0
    payback_exceso   = max(0.0, (payback_b - 18) / 36)  # normalizado

    impactos = {
        "alquiler": exceso_alquiler,
        "costes":   margen_bajo,
        "demanda":  demanda_negativa,
        "payback":  payback_exceso,
    }
    factor = max(impactos, key=impactos.get) if max(impactos.values()) > 0 else "viable"

    # ── Resumen (decisión en una frase, enriquecido por runway y correcciones) ──
    _runway_ok = meses_runway >= 4
    _caps_aplicados = bool(correcciones_caps)

    if recomendacion == "si":
        resumen = "El negocio genera suficiente margen y recupera la inversión en un plazo razonable."
    elif recomendacion == "riesgo":
        if not _runway_ok:
            resumen = (
                f"El negocio podría ser rentable, pero con solo {meses_runway:.1f} meses de "
                f"runway el capital no da margen para el período de arranque."
            )
        elif _caps_aplicados:
            resumen = (
                "El modelo financiero fue ajustado por límites físicos reales. "
                "El negocio puede funcionar, pero con menor capacidad de la inicialmente estimada."
            )
        else:
            resumen = "El negocio puede funcionar, pero tiene riesgos importantes que pueden afectar la rentabilidad."
    else:
        if not _runway_ok and meses_runway < 2:
            resumen = (
                f"Capital insuficiente: con {meses_runway:.1f} meses de runway el negocio "
                f"no sobrevivirá el período de arranque antes de alcanzar el break-even."
            )
        else:
            resumen = "El negocio no es viable en las condiciones actuales."

    # ── Impacto clave (subtítulo concreto) ──
    if factor == "alquiler":
        impacto_clave = (
            f"El alquiler supone el {round(alquiler_pct * 100)}% de los ingresos en el escenario "
            f"desfavorable (límite: {round(umbral_alquiler * 100)}%). Absorbe demasiado margen."
        )
    elif factor == "demanda":
        impacto_clave = (
            f"En el escenario desfavorable el ROI a 3 años es negativo ({round(roi_c * 100)}%). "
            f"La inversión no se recupera si los ingresos caen."
        )
    elif factor == "costes":
        impacto_clave = (
            f"El margen bruto del {round(margen_bruto * 100)}% está por debajo del 60% habitual "
            f"para este tipo de negocio. Los costes variables consumen demasiado."
        )
    elif factor == "payback":
        impacto_clave = (
            f"El payback de {payback_b} meses supera el límite de 18 meses. "
            f"La inversión tarda demasiado en recuperarse."
        )
    else:
        impacto_clave = "Todos los indicadores cumplen los umbrales de viabilidad."

    # ── Razones (bullets explicativos) ──
    razones: list[str] = []
    if roi_b < 0.40:
        razones.append(f"Rentabilidad insuficiente (ROI {round(roi_b * 100)}% — mínimo: 40%)")
    if payback_b > 18:
        razones.append(f"Recuperación de inversión lenta ({payback_b} meses — máximo recomendado: 18)")
    if alquiler_pct > umbral_alquiler:
        razones.append(
            f"Alquiler elevado respecto a los ingresos "
            f"({round(alquiler_pct * 100)}% vs. umbral {round(umbral_alquiler * 100)}%)"
        )
    if margen_bruto < 0.60:
        razones.append(f"Margen operativo bajo ({round(margen_bruto * 100)}%)")
    if roi_c < 0:
        razones.append("El escenario desfavorable no recupera la inversión inicial")
    # Runway como razón explícita
    if meses_runway < 4:
        razones.append(
            f"Capital insuficiente para el arranque: solo {meses_runway:.1f} meses de runway "
            f"antes de necesitar ingresos para cubrir costes fijos"
        )
    # Correcciones físicas aplicadas → explicar qué se limitó y por qué
    for cap in (correcciones_caps or []):
        param = cap.get("parametro", "")
        orig  = cap.get("valor_original", 0)
        corr  = cap.get("valor_corregido", 0)
        if param == "clientes_dia" and orig > corr:
            razones.append(
                f"Capacidad real limitada: el local puede atender ~{int(corr)} clientes/día "
                f"(estimación inicial: {int(orig)})"
            )
        elif param == "coste_mercancia_pct":
            razones.append(
                f"Estructura de costes corregida por incoherencia con el modelo operativo "
                f"({round(orig * 100)}% → {round(corr * 100)}%)"
            )
    if not razones and recomendacion == "si":
        razones.append("ROI, payback y alquiler dentro de los rangos recomendados")

    # ── Recomendaciones accionables ──
    recomendaciones: list[str] = []
    if factor == "alquiler" and alquiler_mensual > 0:
        alq_obj = round(alquiler_mensual * umbral_alquiler / alquiler_pct / 50) * 50
        bajada  = round((1 - alq_obj / alquiler_mensual) * 100)
        recomendaciones.append(
            f"Busca un local con alquiler ≤ {fmt_eur(alq_obj)} €/mes "
            f"(bajada del {bajada}% sobre el actual)"
        )
    if factor in ("demanda", "payback"):
        recomendaciones.append("Revisa la ubicación o la propuesta de valor para atraer más clientes")
    if factor == "costes":
        recomendaciones.append("Negocia mejores condiciones con proveedores o reduce el coste de mercancía")
    if margen_bruto < 0.60:
        recomendaciones.append("Sube el precio medio o reduce los costes variables por cliente")
    if roi_b < 0.40 and factor not in ("alquiler",):
        recomendaciones.append("Reduce la inversión inicial para mejorar el retorno sobre capital")
    if recomendacion == "si" and not recomendaciones:
        recomendaciones.append("Monitoriza mensualmente ingresos vs. costes fijos el primer año")

    return {
        "factor_limitante": factor,
        "resumen":          resumen,
        "impacto_clave":    impacto_clave,
        "razones":          razones,
        "recomendaciones":  recomendaciones,
    }


def fmt_eur(n: float) -> str:
    return f"{int(n):,}".replace(",", ".")


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
        num_empleados=int(overrides["num_empleados"]) if "num_empleados" in overrides else estimados.num_empleados,
        salarios_mensual=    _build("salarios_mensual",    estimados.salarios_mensual),
        otros_fijos_mensual= _build("otros_fijos_mensual", estimados.otros_fijos_mensual),
        coste_mercancia_pct= _build("coste_mercancia_pct", estimados.coste_mercancia_pct),
        reforma_local=       _build("reforma_local",       estimados.reforma_local),
        equipamiento=        _build("equipamiento",        estimados.equipamiento),
        deposito_fianza=     _build("deposito_fianza",     estimados.deposito_fianza),
        otros_iniciales=     _build("otros_iniciales",     estimados.otros_iniciales),
    )
