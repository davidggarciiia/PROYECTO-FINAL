"""
api/admin.py — Endpoints de administración para lanzar pipelines manualmente.

Endpoints:
  GET  /api/admin/pipelines           → lista pipelines disponibles
  GET  /api/admin/pipelines/{nombre}  → última ejecución del pipeline
  POST /api/admin/pipelines/{nombre}  → lanza el pipeline sincrónicamente

Uso típico (primera puesta en marcha):
  1. POST /api/admin/pipelines/transporte
  2. POST /api/admin/pipelines/aforaments
  3. POST /api/admin/pipelines/resenas
  4. POST /api/admin/pipelines/scores
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
}

# Orden recomendado para poblar la BD desde cero
_ORDEN_RECOMENDADO = [
    "transporte",
    "aforaments",
    "demografia",
    "precios",
    "resenas",
    "scores",
]


class PipelineInfo(BaseModel):
    nombre: str
    modulo: str


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


class PipelineRunResponse(BaseModel):
    pipeline: str
    resultado: dict[str, Any]


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.get(
    "/admin/pipelines",
    response_model=PipelineListResponse,
    summary="Lista de pipelines disponibles",
)
async def listar_pipelines() -> PipelineListResponse:
    """Devuelve todos los pipelines disponibles y el orden recomendado de ejecución."""
    return PipelineListResponse(
        pipelines=[PipelineInfo(nombre=n, modulo=m) for n, m in _PIPELINES.items()],
        orden_recomendado=_ORDEN_RECOMENDADO,
    )


@router.get(
    "/admin/pipelines/{nombre}",
    response_model=Optional[EjecucionResponse],
    summary="Última ejecución de un pipeline",
)
async def estado_pipeline(nombre: str) -> Optional[EjecucionResponse]:
    """Devuelve la última ejecución registrada del pipeline indicado."""
    if nombre not in _PIPELINES:
        raise HTTPException(status_code=404, detail=f"Pipeline '{nombre}' no existe")

    async with get_db() as conn:
        row = await conn.fetchrow(
            """
            SELECT id, pipeline, estado, fecha_inicio, fecha_fin, registros, mensaje_error
            FROM pipeline_ejecuciones
            WHERE pipeline = $1
            ORDER BY fecha_inicio DESC
            LIMIT 1
            """,
            nombre,
        )

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
async def lanzar_pipeline(nombre: str) -> PipelineRunResponse:
    """
    Ejecuta el pipeline de forma síncrona y devuelve el resultado.

    El endpoint bloquea hasta que el pipeline termina.
    Para pipelines largos (resenas, mercado_inmobiliario) puede tardar varios minutos.
    """
    if nombre not in _PIPELINES:
        raise HTTPException(status_code=404, detail=f"Pipeline '{nombre}' no existe")

    modulo_path = _PIPELINES[nombre]
    try:
        modulo = importlib.import_module(modulo_path)
    except ImportError as exc:
        raise HTTPException(status_code=500, detail=f"No se pudo importar {modulo_path}: {exc}")

    if not hasattr(modulo, "ejecutar"):
        raise HTTPException(status_code=500, detail=f"{modulo_path} no tiene función ejecutar()")

    logger.info("Admin: lanzando pipeline '%s'", nombre)
    try:
        resultado = await modulo.ejecutar()
    except Exception as exc:
        logger.error("Pipeline '%s' terminó con error: %s", nombre, exc)
        raise HTTPException(status_code=500, detail=f"Pipeline '{nombre}' falló: {exc}")

    logger.info("Admin: pipeline '%s' completado — %s", nombre, resultado)
    return PipelineRunResponse(pipeline=nombre, resultado=resultado or {})
