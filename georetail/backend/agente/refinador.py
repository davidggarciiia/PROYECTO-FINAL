"""
agente/refinador.py — Genera un PerfilRefinado estructurado desde la descripción.

Se llama DESPUÉS de `validar_negocio` y ANTES de scoring. No reemplaza el
perfil numérico (8 dims); añade una capa rica con vocabulario abierto en
`nuances_detected` para preservar el matiz que el perfil numérico pierde.

Si el LLM falla por cualquier motivo (timeout, JSON roto, key ausente), la
función devuelve un `PerfilRefinado()` vacío — nunca propaga excepción para
que el pipeline siga con defaults.
"""
from __future__ import annotations

import json
import logging
from typing import Optional

from agente import extraer_json
from agente.prompts.refinamiento import (
    PREGUNTA_SENAL_SISTEMA,
    REFINADOR_SISTEMA,
    build_refinador_user_message,
)
from routers.llm_router import completar
from schemas.models import (
    Operacion,
    PerfilRefinado,
    PropuestaValor,
    PublicoObjetivo,
    UbicacionIdeal,
)

logger = logging.getLogger(__name__)


def _clip_int(value, lo: int, hi: int) -> Optional[int]:
    """Convierte a int dentro de [lo, hi] o devuelve None si no es numérico."""
    if value is None:
        return None
    try:
        v = int(round(float(value)))
    except (TypeError, ValueError):
        return None
    return max(lo, min(hi, v))


def _clip_float(value, lo: float, hi: float) -> Optional[float]:
    if value is None:
        return None
    try:
        v = float(value)
    except (TypeError, ValueError):
        return None
    return round(max(lo, min(hi, v)), 2)


def _as_str_list(value) -> list[str]:
    """Normaliza una lista de strings: filtra non-str y deduplica preservando orden."""
    if not isinstance(value, list):
        return []
    seen: set[str] = set()
    out: list[str] = []
    for item in value:
        if not isinstance(item, str):
            continue
        clean = item.strip()
        if not clean or clean in seen:
            continue
        seen.add(clean)
        out.append(clean)
    return out


def _pick_literal(value, valid: tuple[str, ...]) -> Optional[str]:
    """Si value está en `valid`, lo devuelve; si no, None. Case-insensitive."""
    if not isinstance(value, str):
        return None
    clean = value.strip().lower()
    for opt in valid:
        if clean == opt.lower():
            return opt
    return None


def _build_perfil(parsed: dict) -> PerfilRefinado:
    """Mapea el dict JSON del LLM a PerfilRefinado con clipping defensivo."""
    publico_raw = parsed.get("publico_objetivo") or {}
    propuesta_raw = parsed.get("propuesta_valor") or {}
    operacion_raw = parsed.get("operacion") or {}
    ubicacion_raw = parsed.get("ubicacion_ideal") or {}

    publico = PublicoObjetivo(
        edad_rango=publico_raw.get("edad_rango") if isinstance(publico_raw.get("edad_rango"), str) else None,
        nivel_socioeconomico=_pick_literal(
            publico_raw.get("nivel_socioeconomico"),
            ("bajo", "medio", "medio-alto", "alto"),
        ),
        estilo_vida=_as_str_list(publico_raw.get("estilo_vida")),
        horarios_pico=_as_str_list(publico_raw.get("horarios_pico")),
    )

    propuesta = PropuestaValor(
        especializacion=propuesta_raw.get("especializacion") if isinstance(propuesta_raw.get("especializacion"), str) else None,
        diferenciadores=_as_str_list(propuesta_raw.get("diferenciadores")),
        calidad_percibida_0_5=_clip_float(propuesta_raw.get("calidad_percibida_0_5"), 0.0, 5.0),
    )

    operacion = Operacion(
        modelo_servicio=_pick_literal(
            operacion_raw.get("modelo_servicio"),
            ("take_away", "mesas", "mixto", "delivery_only"),
        ),
        ticket_tier_p1_p5=_clip_int(operacion_raw.get("ticket_tier_p1_p5"), 1, 5),
        escala_operativa=_pick_literal(
            operacion_raw.get("escala_operativa"),
            ("solo", "micro", "pequeña", "mediana"),
        ),
        horarios_apertura=_as_str_list(operacion_raw.get("horarios_apertura")),
    )

    ubicacion = UbicacionIdeal(
        tipo_calle=ubicacion_raw.get("tipo_calle") if isinstance(ubicacion_raw.get("tipo_calle"), str) else None,
        densidad_preferida=_pick_literal(
            ubicacion_raw.get("densidad_preferida"),
            ("baja", "media", "alta"),
        ),
        flujo_tipo=ubicacion_raw.get("flujo_tipo") if isinstance(ubicacion_raw.get("flujo_tipo"), str) else None,
    )

    signal = _clip_int(parsed.get("signal_preservation_score"), 0, 100)
    return PerfilRefinado(
        publico_objetivo=publico,
        propuesta_valor=propuesta,
        operacion=operacion,
        ubicacion_ideal=ubicacion,
        nuances_detected=_as_str_list(parsed.get("nuances_detected")),
        signal_preservation_score=signal if signal is not None else 100,
    )


async def refinar(
    descripcion: str,
    sector_detectado: str,
    tags_previos: list[str],
    session_id: Optional[str] = None,
) -> PerfilRefinado:
    """Llama al LLM para producir un PerfilRefinado. Idempotente ante errores:
    cualquier fallo (JSON inválido, timeout, sin API keys) devuelve
    `PerfilRefinado()` vacío y registra un warning.
    """
    if not descripcion or not descripcion.strip():
        return PerfilRefinado()

    user_msg = build_refinador_user_message(descripcion, sector_detectado or "desconocido", tags_previos or [])

    try:
        respuesta = await completar(
            mensajes=[{"role": "user", "content": user_msg}],
            sistema=REFINADOR_SISTEMA,
            endpoint="refinador",
            session_id=session_id,
            max_tokens=1200,
            temperature=0.2,
            requiere_json=True,
        )
    except Exception as exc:
        logger.warning("refinar: LLM falló (%s) — devolviendo perfil vacío", exc)
        return PerfilRefinado()

    try:
        parsed = json.loads(extraer_json(respuesta))
        if not isinstance(parsed, dict):
            raise ValueError("respuesta no es un objeto JSON")
        return _build_perfil(parsed)
    except Exception as exc:
        logger.warning("refinar: JSON inválido (%s) — devolviendo perfil vacío", exc)
        return PerfilRefinado()


async def generar_pregunta_senal(
    perfil_refinado: PerfilRefinado,
    descripcion: str,
    session_id: Optional[str] = None,
) -> Optional[str]:
    """Genera 1 pregunta natural que referencia un matiz detectado y apunta al
    gap con más impacto. Devuelve None si el LLM falla.
    """
    nuances = perfil_refinado.nuances_detected or []
    context = {
        "descripcion": descripcion[:600],
        "nuances_detected": nuances[:5],
        "gaps": _detectar_gaps(perfil_refinado),
        "signal_preservation_score": perfil_refinado.signal_preservation_score,
    }
    user_msg = (
        "<profile_context>\n"
        + json.dumps(context, ensure_ascii=False)
        + "\n</profile_context>\n"
        "Produce ONE follow-up question that will maximise signal recovery."
    )
    try:
        respuesta = await completar(
            mensajes=[{"role": "user", "content": user_msg}],
            sistema=PREGUNTA_SENAL_SISTEMA,
            endpoint="refinador_pregunta",
            session_id=session_id,
            max_tokens=200,
            temperature=0.4,
            requiere_json=True,
        )
        parsed = json.loads(extraer_json(respuesta))
        pregunta = parsed.get("pregunta")
        return pregunta.strip() if isinstance(pregunta, str) and pregunta.strip() else None
    except Exception as exc:
        logger.warning("generar_pregunta_senal: falló (%s)", exc)
        return None


def _detectar_gaps(perfil: PerfilRefinado) -> list[str]:
    """Lista de campos sin valor (para orientar la pregunta al hueco más útil)."""
    gaps: list[str] = []
    if perfil.operacion.ticket_tier_p1_p5 is None:
        gaps.append("operacion.ticket_tier_p1_p5")
    if not perfil.operacion.horarios_apertura:
        gaps.append("operacion.horarios_apertura")
    if perfil.publico_objetivo.nivel_socioeconomico is None:
        gaps.append("publico_objetivo.nivel_socioeconomico")
    if not perfil.publico_objetivo.horarios_pico:
        gaps.append("publico_objetivo.horarios_pico")
    if perfil.ubicacion_ideal.flujo_tipo is None:
        gaps.append("ubicacion_ideal.flujo_tipo")
    if perfil.propuesta_valor.especializacion is None:
        gaps.append("propuesta_valor.especializacion")
    return gaps
