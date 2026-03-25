"""
api/admin.py — Endpoints de administración para lanzar pipelines manualmente.

Endpoints:
  GET  /api/admin/pipelines           → lista pipelines disponibles
  GET  /api/admin/pipelines/{nombre}  → última ejecución del pipeline
  POST /api/admin/pipelines/{nombre}  → lanza el pipeline sincrónicamente

Uso típico (primera puesta en marcha):
  1. POST /api/admin/pipelines/transporte
  2. POST /api/admin/pipelines/aforaments
  3. POST /api/admin/pipelines/habitaclia    ← NUEVO: carga locales reales
  4. POST /api/admin/pipelines/resenas
  5. POST /api/admin/pipelines/scores

Para Habitaclia rápido (prueba, ~75 locales, ~5 min):
  POST /api/admin/pipelines/habitaclia
  Body: {"max_paginas": 5}

Para Habitaclia completo (~1.600 locales, ~25 min):
  POST /api/admin/pipelines/habitaclia
  Body: {"max_paginas": 107}
"""
from __future__ import annotations

import importlib
import logging
from typing import Any, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from db.conexion import get_db

logger = logging.getLogger(__name__)
router = APIRouter(tags=["admin"])

# Mapa nombre → módulo Python del pipeline
_PIPELINES: dict[str, str] = {
    "transporte":             "pipelines.transporte",
    "aforaments":             "pipelines.aforaments",
    "resenas":                "pipelines.resenas",
    "scores":                 "pipelines.scores",
    "demografia":             "pipelines.demografia",
    "precios":                "pipelines.precios",
    "parametros_financieros": "pipelines.parametros_financieros",
    "registre_mercantil":     "pipelines.registre_mercantil",
    "mercado_inmobiliario":   "pipelines.mercado_inmobiliario",
    "habitaclia":             "pipelines.mercado_inmobiliario",  # entrada directa
}

# Función a llamar por pipeline (por defecto "ejecutar")
_PIPELINE_FUNCION: dict[str, str] = {
    "habitaclia": "ejecutar_habitaclia",  # función especializada, más rápida
}

# Kwargs por defecto para cada pipeline
_PIPELINE_KWARGS: dict[str, dict] = {
    "habitaclia": {"max_paginas": 107},
}

# Orden recomendado para poblar la BD desde cero
_ORDEN_RECOMENDADO = [
    "transporte",
    "aforaments",
    "demografia",
    "precios",
    "habitaclia",   # ← cargar locales reales antes de scores
    "resenas",
    "scores",
]


class PipelineInfo(BaseModel):
    nombre: str
    modulo: str
    descripcion: Optional[str] = None


class PipelineListResponse(BaseModel):
    pipelines: list[PipelineInfo]
    orden_recomendado: list[str]


class EjecucionResponse(BaseModel):
    id: int
    pipeline: str
    estado: str
    fecha_inicio: str
    fecha_fin: Optional[str]
    registros: Optional[int]
    mensaje_error: Optional[str]


class PipelineRunRequest(BaseModel):
    """Body opcional para parametrizar el pipeline."""
    max_paginas: Optional[int] = None
    modo: Optional[str] = None
    portales: Optional[list[str]] = None


class PipelineRunResponse(BaseModel):
    pipeline: str
    resultado: dict[str, Any]


_DESCRIPCIONES = {
    "habitaclia":  "Scraping Habitaclia (~1.600 locales BCN) + sincronización en tabla locales. ~25 min completo.",
    "scores":      "Recalcular scores XGBoost para todas las zonas.",
    "aforaments":  "Importar datos de flujo peatonal desde sensores CKAN.",
    "transporte":  "Importar líneas y paradas TMB.",
    "resenas":     "Scrapear reseñas Google/Foursquare y generar embeddings NLP.",
    "demografia":  "Importar datos demográficos del padró BCN.",
    "precios":     "Importar precios de alquiler desde Idealista y Open Data BCN.",
}


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.get(
    "/admin/pipelines",
    response_model=PipelineListResponse,
    summary="Lista de pipelines disponibles",
)
async def listar_pipelines() -> PipelineListResponse:
    return PipelineListResponse(
        pipelines=[
            PipelineInfo(
                nombre=n,
                modulo=m,
                descripcion=_DESCRIPCIONES.get(n),
            )
            for n, m in _PIPELINES.items()
        ],
        orden_recomendado=_ORDEN_RECOMENDADO,
    )


@router.get(
    "/admin/pipelines/{nombre}",
    response_model=Optional[EjecucionResponse],
    summary="Última ejecución de un pipeline",
)
async def estado_pipeline(nombre: str) -> Optional[EjecucionResponse]:
    if nombre not in _PIPELINES:
        raise HTTPException(status_code=404, detail=f"Pipeline '{nombre}' no existe")

    async with get_db() as conn:
        row = await conn.fetchrow("""
            SELECT id, pipeline, estado, fecha_inicio, fecha_fin, registros, mensaje_error
            FROM pipeline_ejecuciones
            WHERE pipeline = $1
            ORDER BY fecha_inicio DESC
            LIMIT 1
        """, nombre)

    if not row:
        return None

    return EjecucionResponse(
        id=row["id"],
        pipeline=row["pipeline"],
        estado=row["estado"],
        fecha_inicio=row["fecha_inicio"].isoformat(),
        fecha_fin=row["fecha_fin"].isoformat() if row["fecha_fin"] else None,
        registros=row["registros"],
        mensaje_error=row["mensaje_error"],
    )


@router.post(
    "/admin/pipelines/{nombre}",
    response_model=PipelineRunResponse,
    summary="Lanzar un pipeline manualmente",
)
async def lanzar_pipeline(
    nombre: str,
    body: Optional[PipelineRunRequest] = None,
) -> PipelineRunResponse:
    """
    Ejecuta el pipeline de forma síncrona y devuelve el resultado.

    Para Habitaclia puedes pasar max_paginas en el body:
      {"max_paginas": 5}   → prueba rápida (~75 locales, ~5 min)
      {"max_paginas": 107} → cobertura completa (~1.600 locales, ~25 min)
    """
    if nombre not in _PIPELINES:
        raise HTTPException(status_code=404, detail=f"Pipeline '{nombre}' no existe")

    modulo_path = _PIPELINES[nombre]
    try:
        modulo = importlib.import_module(modulo_path)
    except ImportError as exc:
        raise HTTPException(status_code=500, detail=f"No se pudo importar {modulo_path}: {exc}")

    # Determinar función a llamar
    nombre_funcion = _PIPELINE_FUNCION.get(nombre, "ejecutar")
    if not hasattr(modulo, nombre_funcion):
        raise HTTPException(
            status_code=500,
            detail=f"{modulo_path} no tiene función {nombre_funcion}()",
        )

    funcion = getattr(modulo, nombre_funcion)

    # Construir kwargs: defaults del pipeline + overrides del body
    kwargs = dict(_PIPELINE_KWARGS.get(nombre, {}))
    if body:
        if body.max_paginas is not None:
            kwargs["max_paginas"] = body.max_paginas
        if body.modo is not None:
            kwargs["modo"] = body.modo
        if body.portales is not None:
            kwargs["portales"] = body.portales

    logger.info("Admin: lanzando pipeline '%s' con kwargs=%s", nombre, kwargs)

    try:
        resultado = await funcion(**kwargs)
    except Exception as exc:
        logger.error("Pipeline '%s' terminó con error: %s", nombre, exc, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Pipeline '{nombre}' falló: {exc}")

    logger.info("Admin: pipeline '%s' completado — %s", nombre, resultado)
    return PipelineRunResponse(pipeline=nombre, resultado=resultado or {})
