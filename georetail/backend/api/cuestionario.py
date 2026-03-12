"""
api/cuestionario.py — POST /api/cuestionario

Gestiona el cuestionario adaptativo. El LLM va haciendo preguntas hasta tener
suficiente información para buscar zonas. El frontend llama a este endpoint en bucle
hasta recibir trigger_busqueda=True, momento en que relanza POST /api/buscar.

Almacenamiento:
  - Estado de la sesión → Redis (clave: `sesion:{session_id}`, TTL 4h)
  - Historial de mensajes → PostgreSQL tabla `mensajes_cuestionario`
    (necesario para reconstruir el contexto del LLM en cada pregunta)

Módulo de David — agente IA.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from schemas.models import EstadoBusqueda
from agente.cuestionario import procesar_respuesta
from db.sesiones import get_sesion, get_historial_cuestionario, guardar_mensaje_cuestionario

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
    estado: str = Field(
        ...,
        description="'continua' si hay más preguntas, 'completo' si el cuestionario ha terminado",
    )
    pregunta: str | None = Field(
        None,
        description="Siguiente pregunta del cuestionario. None si estado='completo'.",
    )
    progreso: int = Field(
        ...,
        ge=0,
        le=100,
        description="Porcentaje de avance del cuestionario (0-100).",
    )
    trigger_busqueda: bool = Field(
        ...,
        description=(
            "True cuando el cuestionario ha terminado. "
            "El frontend debe relanzar POST /api/buscar con el mismo session_id."
        ),
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

    El LLM (Claude Sonnet via llm_router) recibe el historial completo de mensajes
    en cada llamada para mantener contexto. El historial se carga de PostgreSQL
    tabla `mensajes_cuestionario` ordenado por `orden` ASC.

    Variables mínimas que el cuestionario intenta obtener:
      - sector_especifico (ej: "cafetería de especialidad", "estudio de tatuajes")
      - m2_aprox
      - presupuesto_alquiler_max
      - perfil_cliente (ej: "turistas", "vecinos del barrio", "profesionales")
    """
    # ── Verificar sesión ──────────────────────────────────────────────────────
    # La sesión debe existir (creada en /api/buscar).
    # Si no existe, el usuario ha perdido la sesión o ha pasado más de 4h.
    sesion = await get_sesion(body.session_id)
    if sesion is None:
        raise HTTPException(
            status_code=404,
            detail="Sesión no encontrada o expirada. Vuelve a describir tu negocio.",
        )

    # ── Cargar historial de mensajes ──────────────────────────────────────────
    # El LLM necesita ver toda la conversación anterior para no repetir preguntas
    # y para inferir correctamente qué información falta.
    # Fuente: tabla `mensajes_cuestionario` ordenados por `orden` ASC.
    historial = await get_historial_cuestionario(body.session_id)

    # ── Guardar respuesta del usuario ─────────────────────────────────────────
    # Guardamos antes de llamar al LLM para no perderla si hay un error.
    orden_actual = len(historial) + 1
    await guardar_mensaje_cuestionario(
        session_id=body.session_id,
        rol="user",
        texto=body.respuesta,
        orden=orden_actual,
    )

    # ── Procesar con LLM ──────────────────────────────────────────────────────
    # `procesar_respuesta` en agente/cuestionario.py:
    #   1. Añade la respuesta del usuario al historial
    #   2. Manda todo al LLM con el prompt de cuestionario.txt
    #   3. El LLM decide si tiene suficiente info o necesita otra pregunta
    #   4. Extrae las variables conocidas y actualiza sesion["perfil"] en Redis
    #   5. Devuelve:
    #      {
    #        "trigger_busqueda": bool,
    #        "pregunta": str | None,
    #        "progreso": int (0-100),
    #        "variables_extraidas": dict,  # nuevas variables identificadas
    #      }
    try:
        resultado = await procesar_respuesta(
            session_id=body.session_id,
            respuesta=body.respuesta,
            sesion=sesion,
            historial=historial,
        )
    except Exception as exc:
        logger.error(
            "Error LLM en cuestionario session_id=%s: %s",
            body.session_id, exc, exc_info=True,
        )
        raise HTTPException(
            status_code=503,
            detail="Error procesando tu respuesta. Inténtalo de nuevo.",
        )

    # ── Guardar respuesta del asistente ───────────────────────────────────────
    if not resultado["trigger_busqueda"] and resultado.get("pregunta"):
        await guardar_mensaje_cuestionario(
            session_id=body.session_id,
            rol="assistant",
            texto=resultado["pregunta"],
            orden=orden_actual + 1,
        )

    # ── Responder ─────────────────────────────────────────────────────────────
    if resultado["trigger_busqueda"]:
        return CuestionarioResponse(
            session_id=body.session_id,
            estado="completo",
            pregunta=None,
            progreso=100,
            trigger_busqueda=True,
        )

    return CuestionarioResponse(
        session_id=body.session_id,
        estado="continua",
        pregunta=resultado["pregunta"],
        progreso=resultado["progreso"],
        trigger_busqueda=False,
    )
