"""
api/refinamiento.py — POST /api/refinamiento

Chat post-resultados. El usuario puede ajustar los resultados del mapa
o hacer preguntas sobre las zonas en lenguaje natural.

Casos de uso:
  - "Descarta Eixample, quiero solo zonas con metro a menos de 5 minutos"
  - "¿Cuál de estas zonas tiene más turistas?"
  - "Muéstrame solo las zonas con menos de 3 competidores directos"
  - "Quiero algo más económico" → refiltra por presupuesto reducido

El LLM interpreta el mensaje y decide:
  a) `filtro_aplicado`  → aplica un nuevo filtro a las zonas existentes
  b) `nueva_busqueda`   → la petición implica buscar zonas completamente distintas
  c) `respuesta`        → es una pregunta, no un filtro — solo responde con texto

Módulo de David — agente IA.

Fuentes de datos:
  - `sesiones` (Redis → PostgreSQL) → estado actual de la búsqueda
  - LLM (Claude Sonnet via llm_router) → interpretación del mensaje
  - `zonas` + `scores_zona` (PostgreSQL) → re-filtrado de resultados
"""

from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from schemas.models import ZonaResumen, AccionRefinamiento, ColorZona
from agente.refinamiento import procesar_refinamiento
from db.sesiones import get_sesion

logger = logging.getLogger(__name__)
router = APIRouter(tags=["refinamiento"])


# ─── Request ──────────────────────────────────────────────────────────────────

class RefinamientoRequest(BaseModel):
    session_id: str
    mensaje: str = Field(
        ...,
        min_length=1,
        max_length=500,
        description="Mensaje del usuario en lenguaje natural",
        examples=[
            "Descarta Eixample, quiero solo zonas con metro a menos de 5 minutos",
            "¿Cuál de estas zonas tiene más turistas?",
            "Algo más barato, máximo 1.200€ al mes",
        ],
    )


# ─── Response ─────────────────────────────────────────────────────────────────

class RefinamientoResponse(BaseModel):
    session_id: str
    respuesta_ia: str = Field(
        ...,
        description="Texto de respuesta del asistente (siempre presente, independiente del accion)",
    )
    zonas_actualizadas: Optional[list[ZonaResumen]] = Field(
        None,
        description=(
            "Nuevas zonas si el LLM aplicó un filtro o lanzó una nueva búsqueda. "
            "None si fue solo una pregunta (accion='respuesta')."
        ),
    )
    accion: AccionRefinamiento = Field(
        ...,
        description=(
            "'respuesta'        → solo texto, sin cambios en el mapa\n"
            "'filtro_aplicado'  → zonas actualizadas con el nuevo filtro\n"
            "'nueva_busqueda'   → nueva búsqueda completa, zonas completamente distintas"
        ),
    )


# ─── Endpoint ────────────────────────────────────────────────────────────────

@router.post(
    "/refinamiento",
    response_model=RefinamientoResponse,
    summary="Ajustar resultados del mapa con lenguaje natural (chat post-resultados)",
)
async def refinamiento(body: RefinamientoRequest) -> RefinamientoResponse:
    """
    El LLM (Claude Sonnet via llm_router) recibe:
      - El mensaje del usuario
      - El perfil del negocio de la sesión (sector, variables conocidas)
      - El resumen de las zonas actuales (zona_id, nombre, score, barrio)
      - El historial de refinamientos previos en esta sesión

    Y devuelve una acción estructurada + el texto de respuesta.

    Si la acción es "filtro_aplicado" o "nueva_busqueda", `procesar_refinamiento`
    ya ha aplicado el filtro y devuelve las zonas actualizadas directamente.
    El endpoint NO necesita volver a llamar a la BD para re-filtrar.

    Los filtros se guardan en `sesiones.perfil["refinamientos"]` para mantener
    el historial y permitir "deshacer el último filtro".
    """
    # ── Validar sesión ────────────────────────────────────────────────────────
    sesion = await get_sesion(body.session_id)
    if sesion is None:
        raise HTTPException(
            status_code=404,
            detail="Sesión no encontrada o expirada.",
        )

    # La sesión debe tener zonas — si no, el usuario no ha hecho una búsqueda aún
    if not sesion.get("zonas_actuales"):
        raise HTTPException(
            status_code=400,
            detail=(
                "No hay resultados de búsqueda en esta sesión. "
                "Primero realiza una búsqueda con POST /api/buscar."
            ),
        )

    # ── Procesar con LLM ──────────────────────────────────────────────────────
    # `procesar_refinamiento` en `agente/refinamiento.py`:
    #   1. Construye el prompt con el contexto de la sesión y las zonas actuales
    #   2. Llama al LLM (llm_router → Claude Sonnet)
    #   3. El LLM devuelve JSON con:
    #      { "accion": "filtro_aplicado"|"nueva_busqueda"|"respuesta",
    #        "respuesta_texto": str,
    #        "filtros_nuevos": dict | None,
    #        "nueva_descripcion": str | None }
    #   4. Si hay filtros nuevos, aplica la query PostGIS y devuelve las zonas
    try:
        resultado = await procesar_refinamiento(
            session_id=body.session_id,
            mensaje=body.mensaje,
            sesion=sesion,
        )
    except Exception as exc:
        logger.error(
            "Error LLM en refinamiento session_id=%s: %s",
            body.session_id, exc, exc_info=True,
        )
        raise HTTPException(
            status_code=503,
            detail="Error procesando el refinamiento. Inténtalo de nuevo.",
        )

    # ── Serializar zonas actualizadas ─────────────────────────────────────────
    zonas_actualizadas = None
    if resultado.get("zonas_actualizadas"):
        zonas_actualizadas = [
            ZonaResumen(
                zona_id=z["zona_id"],
                nombre=z["nombre"],
                barrio=z["barrio"],
                distrito=z["distrito"],
                score_global=round(z["score_global"], 1),
                probabilidad_supervivencia_3a=round(z["probabilidad_supervivencia_3a"], 2),
                alquiler_estimado=z["alquiler_estimado"],
                m2_disponibles=z.get("m2_disponibles"),
                color=_score_to_color(z["score_global"]),
                lat=z["lat"],
                lng=z["lng"],
                resumen_ia=z.get("resumen_ia", ""),
            )
            for z in resultado["zonas_actualizadas"]
        ]

    return RefinamientoResponse(
        session_id=body.session_id,
        respuesta_ia=resultado["respuesta_ia"],
        zonas_actualizadas=zonas_actualizadas,
        accion=AccionRefinamiento(resultado["accion"]),
    )


# ─── Helper ───────────────────────────────────────────────────────────────────

def _score_to_color(score: float) -> ColorZona:
    if score > 75:
        return ColorZona.VERDE
    if score >= 50:
        return ColorZona.AMARILLO
    return ColorZona.ROJO
