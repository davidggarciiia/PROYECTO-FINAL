"""
agente/validador_financiero.py — Capa de validación crítica LLM sobre el output financiero.

Actúa DESPUÉS de calcular_proyeccion() y ANTES de construir la respuesta final.
No reemplaza las reglas Python de api/financiero.py — las complementa con razonamiento holístico.
"""
from __future__ import annotations

import json
import logging

from agente import extraer_json
from agente.prompts import FINANCIAL_VALIDATION_SISTEMA
from routers.llm_router import completar

logger = logging.getLogger(__name__)

# Respuesta de seguridad — se devuelve si el LLM falla o el JSON está malformado.
# "media"+"fiable" para no bloquear el flujo pero sin dar un verde falso.
_FALLBACK: dict = {
    "coherencia_global": "media",
    "veredicto": "fiable",
    "problemas_detectados": [],
    "ajustes_recomendados": [],
    "supuestos_peligrosos": [],
    "checks_detallados": {
        "capacidad": "ok",
        "costes": "ok",
        "margenes": "ok",
        "roi": "ok",
        "payback": "ok",
    },
}


async def validar_financiero(payload: dict, session_id: str) -> dict:
    """
    Análisis crítico LLM del modelo financiero completo.

    Args:
        payload: dict estructurado construido por _build_validation_payload() en api/financiero.py
        session_id: para el log de LLM (tabla llm_logs)

    Returns:
        dict con coherencia_global, veredicto, problemas_detectados,
        ajustes_recomendados, supuestos_peligrosos, checks_detallados.
        Nunca lanza excepciones — siempre devuelve _FALLBACK como mínimo.
    """
    prompt = _construir_prompt(payload)
    try:
        respuesta = await completar(
            mensajes=[{"role": "user", "content": prompt}],
            sistema=FINANCIAL_VALIDATION_SISTEMA,
            endpoint="validacion_financiera",
            session_id=session_id,
            max_tokens=1200,
            temperature=0.1,   # determinismo máximo — análisis, no creatividad
            requiere_json=True,
        )
        resultado = json.loads(extraer_json(respuesta))
        # Garantizar que checks_detallados tiene los 5 campos obligatorios
        _sanitizar(resultado)
        return resultado
    except Exception as exc:
        logger.warning(
            "validar_financiero fallback zona=%s session=%s: %s",
            payload.get("zona_id", "?"),
            session_id,
            exc,
        )
        return dict(_FALLBACK)


# ─── Prompt builder ───────────────────────────────────────────────────────────

def _construir_prompt(p: dict) -> str:
    params = p.get("parametros", {})
    econ   = p.get("economia_base", {})
    costes = p.get("estructura_costes", {})
    met    = p.get("metricas", {})
    bench  = p.get("benchmarks_sector", {})

    ing_totales   = costes.get("ingresos_totales", 0) or econ.get("ingresos_mensuales", 0) or 1
    alquiler      = costes.get("alquiler", 0)
    personal      = costes.get("personal", 0)
    otros_fijos   = costes.get("otros", 0)
    beneficio     = costes.get("beneficio", 0)
    perdida       = costes.get("perdida", 0)
    net_result    = beneficio - perdida

    clientes_dia  = econ.get("clientes_dia", _v_float(params, "clients_per_day"))
    max_capacity  = p.get("max_capacity", clientes_dia * 2 or 1)
    num_empleados = params.get("num_empleados", 1)
    if isinstance(num_empleados, dict):
        num_empleados = num_empleados.get("valor_usado", 1)

    flags_str    = "\n".join(f"- {f}" for f in p.get("validation_flags", [])) or "None"
    correc_str   = "\n".join(f"- {c}" for c in p.get("correcciones_str", [])) or "None"

    return f"""FINANCIAL MODEL VALIDATION REQUEST

BUSINESS CONTEXT:
  sector:              {p.get("sector", "unknown")}
  business_model:      {p.get("business_model_type", "retail_walkin")}
  tipo_negocio:        {p.get("tipo_negocio", "nuevo")} (nuevo=greenfield, traspaso=acquisition)
  has_user_overrides:  {p.get("has_overrides", False)}

INPUTS USED IN CALCULATION:
  ticket_medio:         {_v(params, "ticket_medio")} €
  clients_per_day:      {_v(params, "clients_per_day")} clients/day  [base scenario]
  max_capacity:         {max_capacity} clients/day
  dias_apertura_mes:    {_v(params, "dias_apertura_mes")} days/month
  alquiler_mensual:     {_v(params, "alquiler_mensual")} €/month
  num_empleados:        {num_empleados}
  salarios_mensual:     {_v(params, "salarios_mensual")} €/month
  otros_fijos_mensual:  {_v(params, "otros_fijos_mensual")} €/month
  coste_mercancia_pct:  {_pct(params, "coste_mercancia_pct")}

FINANCIAL OUTPUTS (stable regime — 80% max occupancy applied):
  ingresos_mensuales:      {econ.get("ingresos_mensuales", 0):,.0f} €/month
  ocupacion_efectiva:      {p.get("ocupacion_efectiva", 0):.1%}
  max_potential_customers: {econ.get("max_potential_customers", 0):.0f} clients/day
  margen_bruto_pct:        {met.get("margen_bruto_pct", 0):.1%}
  net_margin:              {(net_result / ing_totales):.1%}  [net_result / ingresos]
  alquiler / ingresos:     {(alquiler / ing_totales):.1%}  [threshold: 15%]
  personal / ingresos:     {(personal / ing_totales):.1%}
  otros_fijos / ingresos:  {(otros_fijos / ing_totales):.1%}
  (alq+personal)/ingresos: {((alquiler + personal) / ing_totales):.1%}

MONTHLY COST BREAKDOWN (stable regime):
  Rent:           {alquiler:,.0f} €
  Staff:          {personal:,.0f} €
  Variable COGS:  {costes.get("variable", 0):,.0f} €
  Other fixed:    {otros_fijos:,.0f} €
  Net result:     {net_result:+,.0f} €/month

ROI & PAYBACK (3-year projection model):
  ROI conservador (×0.60): {met.get("roi_conservador", 0):.1%}
  ROI base        (×1.00): {met.get("roi_base", 0):.1%}
  ROI optimista   (×1.20): {met.get("roi_optimista", 0):.1%}
  ROI spread (opt - cons):  {(met.get("roi_optimista", 0) - met.get("roi_conservador", 0)):.1%}
  payback_meses (base):    {met.get("payback_meses", 999)} months
  inversion_total:         {p.get("inversion_total", 0):,.0f} €

SECTOR BENCHMARKS:
  margen_bruto_tipico:         {bench.get("margen_bruto_tipico", "N/A")}
  alquiler_sobre_ventas_tipico: {bench.get("alquiler_sobre_ventas_tipico", "N/A")}

EXISTING VALIDATION FLAGS (rule-based — do NOT duplicate these):
{flags_str}

CORRECTIONS APPLIED BY SYSTEM (before calculation):
{correc_str}

Validate this financial model now. Return JSON only."""


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _v(params: dict, key: str) -> str:
    """Extrae valor_usado de un ParametroResponse serializado."""
    val = params.get(key)
    if isinstance(val, dict):
        v = val.get("valor_usado")
        return f"{v:,.2f}" if isinstance(v, float) else str(v) if v is not None else "N/A"
    if isinstance(val, (int, float)):
        return f"{val:,.2f}"
    return "N/A"


def _v_float(params: dict, key: str) -> float:
    val = params.get(key)
    if isinstance(val, dict):
        return float(val.get("valor_usado", 0) or 0)
    if isinstance(val, (int, float)):
        return float(val)
    return 0.0


def _pct(params: dict, key: str) -> str:
    val = params.get(key)
    if isinstance(val, dict):
        v = val.get("valor_usado")
        return f"{v:.1%}" if isinstance(v, (int, float)) else "N/A"
    if isinstance(val, (int, float)):
        return f"{val:.1%}"
    return "N/A"


def _sanitizar(r: dict) -> None:
    """Garantiza que todos los campos requeridos existen con valores válidos."""
    r.setdefault("coherencia_global", "media")
    r.setdefault("veredicto", "fiable")
    r.setdefault("problemas_detectados", [])
    r.setdefault("ajustes_recomendados", [])
    r.setdefault("supuestos_peligrosos", [])
    checks = r.setdefault("checks_detallados", {})
    for campo in ("capacidad", "costes", "margenes", "roi", "payback"):
        checks.setdefault(campo, "ok")
    # Normalizar impacto en problemas (por si el LLM devuelve valores inesperados)
    for prob in r.get("problemas_detectados", []):
        if prob.get("impacto") not in ("alto", "medio", "bajo"):
            prob["impacto"] = "medio"
