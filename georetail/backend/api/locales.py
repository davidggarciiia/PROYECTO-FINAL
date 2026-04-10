"""
api/locales.py — GET /api/locales

Vista lista de todos los locales de la sesión con filtros y paginación.

El campo `filtros_disponibles` en la respuesta contiene los rangos REALES de
los datos de la sesión actual. El frontend los usa para construir los sliders
dinámicamente. No hardcodear estos valores en el cliente.

Fuentes de datos:
  - `zonas` + `barrios` + `distritos`  → identificación geográfica (PostGIS)
  - `locales`                           → m2, alquiler, dirección, disponibilidad
                                           (Idealista / Cens Locals BCN / manual)
  - `scores_zona`                       → score_global, probabilidad (XGBoost)
  - `sesiones`                          → recuperar filtros originales de la búsqueda

La query principal usa PostGIS para los filtros espaciales y ARRAY operators
de PostgreSQL para los filtros por distrito.
"""

from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from schemas.models import LocalListItem, FiltrosDisponibles, ColorZona
from api._utils import score_to_color
from db.sesiones import get_sesion
from db.zonas import get_zonas_sesion

logger = logging.getLogger(__name__)
router = APIRouter(tags=["locales"])

_MAX_ZONAS_SESION = 200


# ─── Response ─────────────────────────────────────────────────────────────────

class LocalesResponse(BaseModel):
    total: int = 0
    page: int
    limit: int
    locales: list[LocalListItem]
    filtros_disponibles: FiltrosDisponibles


# ─── Endpoint ────────────────────────────────────────────────────────────────

@router.get(
    "/locales",
    response_model=LocalesResponse,
    summary="Vista lista de locales de la sesión con filtros y paginación",
)
async def get_locales(
    session_id: str = Query(..., description="UUID de la sesión activa"),
    score_min: int = Query(default=0, ge=0, le=100),
    score_max: int = Query(default=100, ge=0, le=100),
    distrito: Optional[str] = Query(default=None),
    m2_min: Optional[int] = Query(default=None, gt=0),
    m2_max: Optional[int] = Query(default=None, gt=0),
    alquiler_max: Optional[int] = Query(default=None, gt=0),
    disponible: Optional[bool] = Query(
        default=None,
        description="True = solo locales disponibles para alquilar",
    ),
    orden: str = Query(
        default="score",
        pattern="^(score|alquiler|m2)$",
        description="Campo por el que ordenar: 'score', 'alquiler' o 'm2'",
    ),
    page: int = Query(default=1, ge=1),
    limit: int = Query(default=20, ge=1, le=50),
) -> LocalesResponse:
    """
    Devuelve todos los locales de la sesión aplicando los filtros indicados.

    La query a PostgreSQL usa:
      - `scores_zona.score_global BETWEEN score_min AND score_max`
      - `barrios.distrito_id = (SELECT id FROM distritos WHERE nombre = $distrito)`
      - `locales.m2 BETWEEN m2_min AND m2_max`
      - `locales.alquiler_mensual <= alquiler_max`
      - `locales.disponible = disponible` (si se especifica)
      - ORDER BY {campo} {ASC|DESC}
      - LIMIT limit OFFSET (page-1)*limit

    `filtros_disponibles` se calcula ANTES de aplicar los filtros del request,
    sobre el universo completo de zonas de la sesión. Esto permite que los sliders
    del frontend siempre muestren los rangos totales, no los filtrados.
    """
    # ── Validar sesión ────────────────────────────────────────────────────────
    sesion = await get_sesion(session_id)
    if sesion is None:
        raise HTTPException(
            status_code=404,
            detail="Sesión no encontrada o expirada.",
        )

    # ── Validar coherencia de filtros ─────────────────────────────────────────
    if m2_min is not None and m2_max is not None and m2_min > m2_max:
        raise HTTPException(
            status_code=400,
            detail="m2_min no puede ser mayor que m2_max.",
        )
    if score_min > score_max:
        raise HTTPException(
            status_code=400,
            detail="score_min no puede ser mayor que score_max.",
        )

    # ── Obtener todas las zonas de la sesión ──────────────────────────────────
    # `get_zonas_sesion` devuelve todas las zonas que salieron en la última
    # búsqueda de esta sesión, con sus scores y datos de localización.
    # Fuente: `scores_zona` JOIN `zonas` JOIN `locales` JOIN `barrios`.
    #
    # Se recuperan TODAS para poder calcular `filtros_disponibles` sobre el
    # universo completo antes de filtrar.
    sector = sesion.get("perfil", {}).get("sector", "desconocido")
    todas_las_zonas = await get_zonas_sesion(session_id=session_id, sector=sector)

    if len(todas_las_zonas) > _MAX_ZONAS_SESION:
        logger.warning(
            "Sesión %s tiene %d zonas (límite: %d). Truncando.",
            session_id, len(todas_las_zonas), _MAX_ZONAS_SESION,
        )
        todas_las_zonas = todas_las_zonas[:_MAX_ZONAS_SESION]

    if not todas_las_zonas:
        # Si no hay zonas (ej: sesión recién creada sin búsqueda completada)
        return LocalesResponse(
            total=0,
            page=page,
            limit=limit,
            locales=[],
            filtros_disponibles=_filtros_vacios(),
        )

    # ── Calcular filtros disponibles (sobre el universo completo) ─────────────
    filtros_disponibles = _calcular_filtros_disponibles(todas_las_zonas)

    # ── Aplicar filtros del request ───────────────────────────────────────────
    zonas_filtradas = todas_las_zonas

    # Filtro por score
    zonas_filtradas = [
        z for z in zonas_filtradas
        if score_min <= z["score_global"] <= score_max
    ]

    # Filtro por distrito
    if distrito:
        zonas_filtradas = [z for z in zonas_filtradas if z["distrito"] == distrito]

    # Filtro por m2
    if m2_min is not None:
        zonas_filtradas = [
            z for z in zonas_filtradas
            if z.get("m2") is not None and z["m2"] >= m2_min
        ]
    if m2_max is not None:
        zonas_filtradas = [
            z for z in zonas_filtradas
            if z.get("m2") is not None and z["m2"] <= m2_max
        ]

    # Filtro por alquiler máximo
    if alquiler_max is not None:
        zonas_filtradas = [
            z for z in zonas_filtradas
            if z.get("alquiler_mensual") is not None and z["alquiler_mensual"] <= alquiler_max
        ]

    # Filtro por disponibilidad
    if disponible is not None:
        zonas_filtradas = [
            z for z in zonas_filtradas
            if z.get("disponible") == disponible
        ]

    # ── Ordenar ───────────────────────────────────────────────────────────────
    # Usamos funciones key separadas en lugar de un dict de lambdas
    # para que Python gestione correctamente los None.
    if orden == "score":
        # Mayor score primero
        zonas_filtradas.sort(key=lambda z: z.get("score_global", 0), reverse=True)
    elif orden == "alquiler":
        # Menor alquiler primero; los sin precio van al final
        zonas_filtradas.sort(
            key=lambda z: z.get("alquiler_mensual") or float("inf")
        )
    elif orden == "m2":
        # Mayor m2 primero; los sin m2 van al final
        zonas_filtradas.sort(
            key=lambda z: z.get("m2") or 0, reverse=True
        )

    # ── Paginar ───────────────────────────────────────────────────────────────
    total = len(zonas_filtradas)
    offset = (page - 1) * limit
    zonas_pagina = zonas_filtradas[offset : offset + limit]

    # ── Serializar ────────────────────────────────────────────────────────────
    items = [
        LocalListItem(
            zona_id=z["zona_id"],
            nombre=z["nombre"],
            direccion=z.get("direccion"),
            barrio=z["barrio"],
            distrito=z["distrito"],
            score_global=round(z["score_global"], 1),
            color=score_to_color(z["score_global"]),
            alquiler_mensual=z.get("alquiler_mensual"),
            m2=z.get("m2"),
            disponible=z.get("disponible"),
            probabilidad_supervivencia_3a=round(z.get("probabilidad_supervivencia_3a", 0.5), 2),
            resumen_ia=z.get("resumen_ia", ""),
        )
        for z in zonas_pagina
    ]

    return LocalesResponse(
        total=total,
        page=page,
        limit=limit,
        locales=items,
        filtros_disponibles=filtros_disponibles,
    )


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _calcular_filtros_disponibles(zonas: list[dict]) -> FiltrosDisponibles:
    """
    Calcula los rangos reales de los datos para construir los sliders del frontend.
    Solo considera zonas con valor no nulo en cada campo.
    """
    distritos = sorted({z["distrito"] for z in zonas if z.get("distrito")})

    scores = [z["score_global"] for z in zonas if z.get("score_global") is not None]
    alquileres = [z["alquiler_mensual"] for z in zonas if z.get("alquiler_mensual")]
    m2s = [z["m2"] for z in zonas if z.get("m2")]

    return FiltrosDisponibles(
        distritos=distritos,
        score_min=round(min(scores), 1) if scores else 0.0,
        score_max=round(max(scores), 1) if scores else 100.0,
        alquiler_min=min(alquileres) if alquileres else 0.0,
        alquiler_max=max(alquileres) if alquileres else 0.0,
        m2_min=min(m2s) if m2s else 0,
        m2_max=max(m2s) if m2s else 0,
    )


def _filtros_vacios() -> FiltrosDisponibles:
    return FiltrosDisponibles(
        distritos=[],
        score_min=0.0, score_max=100.0,
        alquiler_min=0.0, alquiler_max=0.0,
        m2_min=0, m2_max=0,
    )
