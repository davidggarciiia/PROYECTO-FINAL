"""agente/analizador.py — Genera análisis IA grounded para detalle de zona."""
from __future__ import annotations
import json
import logging
from datetime import date, datetime
from decimal import Decimal

from agente.prompts import ANALISIS_ZONA_SISTEMA
from agente import extraer_json
from routers.llm_router import completar
from scoring.explainability import build_fallback_analysis

logger = logging.getLogger(__name__)


def _json_safe(obj):
    """Default encoder para json.dumps: Decimal->float, date/datetime->isoformat."""
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


async def analizar_zona(zona_data: dict, perfil_negocio: dict,
                         session_id: str) -> dict:
    """
    Genera el análisis IA completo de una zona para un negocio específico.

    Args:
        zona_data: dict con todos los datos de la zona (scores, competidores, alertas, etc.)
        perfil_negocio: dict con el perfil del negocio del usuario
        session_id: para el log de LLM

    Returns:
        dict con resumen_global, puntos_fuertes, puntos_debiles,
        recomendacion_final, razon_recomendacion y explicaciones_dimensiones.
    """
    prompt = _construir_prompt(zona_data, perfil_negocio)

    respuesta = await completar(
        mensajes=[{"role": "user", "content": prompt}],
        sistema=ANALISIS_ZONA_SISTEMA,
        endpoint="analisis_zona",
        session_id=session_id,
        max_tokens=900,
        temperature=0.3,
        requiere_json=True,
    )

    try:
        resultado = json.loads(extraer_json(respuesta))
    except json.JSONDecodeError as e:
        logger.error("JSON inválido en análisis zona=%s: %s", zona_data.get("zona_id"), e)
        return _fallback(zona_data)

    if not isinstance(resultado, dict):
        return _fallback(zona_data)

    resumen = resultado.get("resumen_global") or resultado.get("resumen") or ""
    resultado["resumen_global"] = resumen
    resultado["resumen"] = resumen
    resultado.setdefault("puntos_fuertes", [])
    resultado.setdefault("puntos_debiles", [])
    resultado.setdefault("razon_recomendacion", "")
    resultado.setdefault("recomendacion_final", "Con reservas")
    resultado.setdefault("explicaciones_dimensiones", {})
    return resultado


def _construir_prompt(z: dict, p: dict) -> str:
    """Construye el prompt grounded con el payload estructurado de evidencia."""
    grounding = z.get("llm_grounding") or {}
    prompt = {
        "instruccion": (
            "Redacta la explicación final usando solo la evidencia proporcionada. "
            "No inventes datos y mantén un tono profesional, claro y concreto."
        ),
        "perfil_negocio": p,
        "payload": grounding,
    }
    return json.dumps(prompt, ensure_ascii=False, indent=2, default=_json_safe)


def _fallback(zona_data: dict) -> dict:
    grounding = zona_data.get("llm_grounding") or {}
    return build_fallback_analysis(grounding)
