"""
api/cuestionario.py — POST /api/cuestionario

Gestiona el cuestionario adaptativo. El LLM va haciendo preguntas hasta tener
suficiente información para buscar zonas. El frontend llama a este endpoint en bucle
hasta recibir trigger_busqueda=True, momento en que relanza POST /api/buscar.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from schemas.models import EstadoBusqueda
from agente.cuestionario import procesar_respuesta
from agente.refinador import refinar
from db.sesiones import (
    get_sesion, actualizar_sesion,
    get_historial_cuestionario, guardar_mensaje,
)

# Mismos umbrales que en api/buscar.py (Fase 3)
_SIGNAL_THRESHOLD = 70
_SIGNAL_MAX_ROUNDS = 3

logger = logging.getLogger(__name__)
router = APIRouter(tags=["cuestionario"])


# ─── Request ──────────────────────────────────────────────────────────────────

class CuestionarioRequest(BaseModel):
    session_id: str = Field(..., description="UUID de la sesión activa")
    respuesta: str = Field(
        ...,
        min_length=1,
        max_length=500,
        description="Respuesta del usuario a la pregunta anterior",
        examples=["Entre 50 y 80 metros cuadrados"],
    )


# ─── Response ─────────────────────────────────────────────────────────────────

class CuestionarioResponse(BaseModel):
    session_id: str
    estado: str = Field(..., description="'continua' si hay más preguntas, 'completo' si terminó")
    pregunta: str | None = Field(None, description="Siguiente pregunta. None si estado='completo'.")
    progreso_pct: int = Field(..., ge=0, le=100, description="% de avance del cuestionario (0-100).")
    trigger_busqueda: bool = Field(
        ...,
        description="True cuando termina. El frontend debe relanzar POST /api/buscar.",
    )


# ─── Endpoint ────────────────────────────────────────────────────────────────

@router.post(
    "/cuestionario",
    response_model=CuestionarioResponse,
    summary="Enviar respuesta a la pregunta actual del cuestionario adaptativo",
)
async def cuestionario(body: CuestionarioRequest) -> CuestionarioResponse:
    """
    Flujo típico del frontend:

        while True:
            res = await POST /api/cuestionario(session_id, respuesta_usuario)
            if res.trigger_busqueda:
                zonas = await POST /api/buscar(session_id, descripcion_original)
                break
            mostrar_pregunta(res.pregunta, res.progreso)
    """
    # ── Verificar sesión ──────────────────────────────────────────────────────
    sesion = await get_sesion(body.session_id)
    if sesion is None:
        raise HTTPException(
            status_code=404,
            detail="Sesión no encontrada o expirada. Vuelve a describir tu negocio.",
        )

    # ── Cargar historial de mensajes ──────────────────────────────────────────
    historial = await get_historial_cuestionario(body.session_id)

    # ── Guardar respuesta del usuario antes de llamar al LLM ─────────────────
    orden_actual = len(historial) + 1
    await guardar_mensaje(
        session_id=body.session_id,
        rol="user",
        texto=body.respuesta,
        orden=orden_actual,
    )

    # ── Procesar con LLM ──────────────────────────────────────────────────────
    # procesar_respuesta devuelve:
    #   { mensaje, variables_extraidas, estado ('continua'|'completo'), progreso_pct }
    try:
        resultado = await procesar_respuesta(
            session_id=body.session_id,
            historial=historial,
            respuesta_usuario=body.respuesta,
            perfil_actual=sesion.get("perfil", {}),
        )
    except Exception as exc:
        logger.error("Error LLM en cuestionario session_id=%s: %s", body.session_id, exc, exc_info=True)
        raise HTTPException(
            status_code=503,
            detail="Error procesando tu respuesta. Inténtalo de nuevo.",
        )

    # ── Actualizar perfil en sesión con variables extraídas ───────────────────
    if resultado.get("variables_extraidas"):
        try:
            await actualizar_sesion(body.session_id, {"perfil": resultado["variables_extraidas"]})
        except Exception as exc:
            logger.warning("No se pudo actualizar perfil en sesión: %s", exc)

    # ── Fase 3: re-refinamiento con respuesta enriquecida ─────────────────────
    # Si había un perfil_refinado en la sesión y todavía no hemos llegado al
    # umbral/rounds, re-corremos el refinador con la descripción + respuesta
    # nueva concatenadas. Así el perfil converge a mayor signal_preservation.
    perfil_sesion = sesion.get("perfil") or {}
    perfil_refinado_prev = perfil_sesion.get("perfil_refinado") or None
    rounds_usados = int(perfil_sesion.get("signal_rounds", 0))
    if perfil_refinado_prev and rounds_usados < _SIGNAL_MAX_ROUNDS:
        prev_score = int((perfil_refinado_prev or {}).get("signal_preservation_score") or 0)
        if prev_score < _SIGNAL_THRESHOLD:
            descripcion_original = sesion.get("descripcion_original") or ""
            descripcion_enriquecida = f"{descripcion_original}\n[respuesta usuario]: {body.respuesta}".strip()
            try:
                nuevo_perfil = await refinar(
                    descripcion=descripcion_enriquecida,
                    sector_detectado=perfil_sesion.get("sector", "desconocido"),
                    tags_previos=perfil_sesion.get("idea_tags") or [],
                    session_id=body.session_id,
                )
                nuevo_dict = nuevo_perfil.model_dump()
                try:
                    await actualizar_sesion(body.session_id, {
                        "perfil": {
                            "perfil_refinado": nuevo_dict,
                            "signal_rounds": rounds_usados + 1,
                        },
                    })
                except Exception as exc:
                    logger.warning("No se pudo guardar perfil_refinado iterado: %s", exc)
            except Exception as exc:
                logger.warning("Re-refinamiento falló: %s", exc)

    # ── Determinar si el cuestionario ha terminado ────────────────────────────
    terminado = resultado["estado"] == "completo"

    # ── Guardar respuesta del asistente si hay pregunta siguiente ─────────────
    if not terminado and resultado.get("mensaje"):
        await guardar_mensaje(
            session_id=body.session_id,
            rol="assistant",
            texto=resultado["mensaje"],
            orden=orden_actual + 1,
        )

    # ── Responder ─────────────────────────────────────────────────────────────
    if terminado:
        return CuestionarioResponse(
            session_id=body.session_id,
            estado="completo",
            pregunta=None,
            progreso_pct=100,
            trigger_busqueda=True,
        )

    return CuestionarioResponse(
        session_id=body.session_id,
        estado="continua",
        pregunta=resultado["mensaje"],
        progreso_pct=resultado.get("progreso_pct", 30),
        trigger_busqueda=False,
    )
