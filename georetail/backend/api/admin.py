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
import secrets
from typing import Any, Optional

from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel

from config import settings
from db.conexion import get_db


async def _require_admin(x_admin_key: str = Header(..., alias="X-Admin-Key")) -> None:
    if not settings.ADMIN_API_KEY or not secrets.compare_digest(x_admin_key, settings.ADMIN_API_KEY):
        raise HTTPException(status_code=403, detail="Admin key required")

logger = logging.getLogger(__name__)
router = APIRouter(tags=["admin"])

# Mapa nombre → módulo Python del pipeline
_PIPELINES: dict[str, str] = {
    "transporte":             "pipelines.transporte.transporte",
    "transporte_agregado":    "pipelines.transporte.agregado_zona",
    "bicing":                 "pipelines.transporte.bicing",
    "aforaments":             "pipelines.peatonal.aforaments",
    "resenas":                "pipelines.entorno.resenas",
    "scores":                 "pipelines.scores",
    "demografia":             "pipelines.demografia.demografia",
    "precios":                "pipelines.inmobiliario.precios",
    "parametros_financieros": "pipelines.parametros_financieros",
    "registre_mercantil":     "pipelines.comercio.registre_mercantil",
    "mercado_inmobiliario":   "pipelines.inmobiliario.mercado_inmobiliario",
    "competencia":            "pipelines.comercio.competencia",
    "google_maps":            "pipelines.entorno.google_maps",
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
    dependencies=[Depends(_require_admin)],
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


# =============================================================================
# Admin — dimensión Competencia
# =============================================================================

class CoberturaBarrio(BaseModel):
    barrio:                    str
    distrito:                  str
    zonas_totales:             int
    zonas_con_datos_recientes: int
    pct_recientes:             float
    hhi_medio:                 Optional[float] = None
    cadenas_dominantes:        int = 0  # zonas con HHI > 0.25


class CompetenciaAdminResponse(BaseModel):
    total_zonas:          int
    total_filas_vigentes: int
    ultima_ejecucion:     Optional[str] = None
    ultimo_estado:        Optional[str] = None
    por_barrio:           list[CoberturaBarrio]


@router.get(
    "/admin/competencia",
    response_model=CompetenciaAdminResponse,
    summary="Estado de la dimensión Competencia por barrio (auditoría interna)",
)
async def admin_competencia() -> CompetenciaAdminResponse:
    """Agrega por barrio: % de zonas con filas recientes (<=7d) en
    `competencia_detalle_zona`, HHI medio y zonas con cadenas dominantes
    (HHI>0.25). Para detectar rincones de BCN donde el scraper gosom no llega."""
    async with get_db() as conn:
        ejec = await conn.fetchrow("""
            SELECT fecha_inicio, fecha_fin, estado
            FROM pipeline_ejecuciones
            WHERE pipeline = 'competencia'
            ORDER BY fecha_inicio DESC LIMIT 1
        """)

        rows = await conn.fetch("""
            WITH recientes AS (
                SELECT DISTINCT ON (cdz.zona_id, cdz.sector_codigo)
                       cdz.zona_id, cdz.sector_codigo,
                       cdz.hhi_index,
                       (cdz.fecha >= CURRENT_DATE - INTERVAL '7 days') AS reciente
                FROM competencia_detalle_zona cdz
                ORDER BY cdz.zona_id, cdz.sector_codigo, cdz.fecha DESC
            )
            SELECT b.nombre AS barrio,
                   d.nombre AS distrito,
                   COUNT(DISTINCT z.id)::int AS zonas_totales,
                   COUNT(DISTINCT CASE WHEN r.reciente THEN z.id END)::int AS zonas_recientes,
                   AVG(r.hhi_index) FILTER (WHERE r.hhi_index IS NOT NULL)  AS hhi_medio,
                   COUNT(*) FILTER (WHERE r.hhi_index > 0.25)::int           AS cadenas
            FROM zonas z
            JOIN barrios b   ON b.id = z.barrio_id
            JOIN distritos d ON d.id = b.distrito_id
            LEFT JOIN recientes r ON r.zona_id = z.id
            GROUP BY b.nombre, d.nombre
            ORDER BY (1.0 - COALESCE(
                COUNT(DISTINCT CASE WHEN r.reciente THEN z.id END)::float
                  / NULLIF(COUNT(DISTINCT z.id), 0),
                0
            )) DESC
        """)

        total = await conn.fetchval("SELECT COUNT(*) FROM zonas")
        vigentes = await conn.fetchval(
            "SELECT COUNT(*) FROM competencia_detalle_zona "
            "WHERE fecha >= CURRENT_DATE - INTERVAL '7 days'"
        )

    ultima_ejec = None
    ultimo_estado = None
    if ejec:
        fi = ejec["fecha_fin"] or ejec["fecha_inicio"]
        ultima_ejec = fi.isoformat() if fi else None
        ultimo_estado = ejec["estado"]

    por_barrio: list[CoberturaBarrio] = []
    for r in rows:
        total_b = int(r["zonas_totales"] or 0)
        rec_b = int(r["zonas_recientes"] or 0)
        pct = round(100.0 * rec_b / total_b, 1) if total_b else 0.0
        por_barrio.append(CoberturaBarrio(
            barrio=r["barrio"], distrito=r["distrito"],
            zonas_totales=total_b,
            zonas_con_datos_recientes=rec_b,
            pct_recientes=pct,
            hhi_medio=round(float(r["hhi_medio"]), 3) if r["hhi_medio"] is not None else None,
            cadenas_dominantes=int(r["cadenas"] or 0),
        ))

    return CompetenciaAdminResponse(
        total_zonas=int(total or 0),
        total_filas_vigentes=int(vigentes or 0),
        ultima_ejecucion=ultima_ejec,
        ultimo_estado=ultimo_estado,
        por_barrio=por_barrio,
    )
