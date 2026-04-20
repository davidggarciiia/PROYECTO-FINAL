"""
api/health.py — GET /api/health

Estado del sistema. El frontend lo usa para mostrar un banner de aviso
si algún servicio crítico está caído.

Servicios monitorizados:
  - postgres  → SELECT 1 con latencia
  - redis     → PING con latencia
  - celery    → inspect workers (si no responde en 2s → degraded)
  - llm       → verifica qué proveedor está activo en Redis (no llama al LLM)

Estado global:
  "down"     → postgres o llm caídos (la app NO puede funcionar)
  "degraded" → celery o redis caídos (la app funciona pero con limitaciones)
  "ok"       → todos los servicios funcionan
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter
from pydantic import BaseModel

from schemas.models import ServiceStatus, EstadoServicio

logger = logging.getLogger(__name__)
router = APIRouter(tags=["health"])

# Servicios críticos: si caen, la app no puede funcionar
_CRITICOS = {"postgres", "llm"}

# Timeout para checks de servicios lentos (Celery, LLM)
_TIMEOUT_CELERY_S = 2.0
_TIMEOUT_LLM_S = 3.0


# ─── Response ─────────────────────────────────────────────────────────────────

class HealthResponse(BaseModel):
    status: EstadoServicio
    version: str
    timestamp: str
    services: list[ServiceStatus]


# ─── Checks individuales ──────────────────────────────────────────────────────

async def _check_postgres() -> ServiceStatus:
    """
    Verifica que PostgreSQL responde con un SELECT 1.
    Usa el pool de asyncpg inicializado en el startup de main.py.
    """
    from db.conexion import get_db
    try:
        t0 = time.perf_counter()
        async with get_db() as conn:
            await conn.fetchval("SELECT 1")
        latency_ms = round((time.perf_counter() - t0) * 1000, 1)
        return ServiceStatus(name="postgres", status=EstadoServicio.OK, latency_ms=latency_ms)
    except Exception as exc:
        logger.error("Health check postgres FAILED: %s", exc)
        return ServiceStatus(
            name="postgres",
            status=EstadoServicio.DOWN,
            latency_ms=None,
            message="No disponible",
        )


async def _check_redis() -> ServiceStatus:
    """
    Verifica que Redis responde con un PING.
    Usa el cliente redis.asyncio inicializado en el startup de main.py.
    Redis es necesario para las sesiones — sin él el cuestionario no funciona.
    """
    from db.redis_client import get_redis
    try:
        t0 = time.perf_counter()
        r = get_redis()
        await r.ping()
        latency_ms = round((time.perf_counter() - t0) * 1000, 1)
        return ServiceStatus(name="redis", status=EstadoServicio.OK, latency_ms=latency_ms)
    except Exception as exc:
        logger.error("Health check redis FAILED: %s", exc)
        # Redis caído → sesiones no persistirán, pero la app puede responder
        # con datos de PostgreSQL (degraded, no down)
        return ServiceStatus(
            name="redis",
            status=EstadoServicio.DEGRADED,
            latency_ms=None,
            message="No disponible",
        )


async def _check_celery() -> ServiceStatus:
    """
    Verifica que al menos un worker Celery está activo.
    Usa inspect() con timeout de 2s para no bloquear el health check.

    Celery caído → el scoring batch y la exportación de PDF no funcionan,
    pero el cuestionario y la validación LLM sí.
    """
    try:
        from workers.celery_app import celery_app
        t0 = time.perf_counter()

        # inspect() hace broadcast a todos los workers — si no hay respuesta en timeout → degraded
        inspect = celery_app.control.inspect(timeout=_TIMEOUT_CELERY_S)
        stats = await asyncio.wait_for(
            asyncio.get_running_loop().run_in_executor(None, inspect.stats),
            timeout=_TIMEOUT_CELERY_S + 0.5,
        )

        latency_ms = round((time.perf_counter() - t0) * 1000, 1)

        if not stats:
            return ServiceStatus(
                name="celery",
                status=EstadoServicio.DEGRADED,
                latency_ms=latency_ms,
                message="No hay workers activos",
            )

        num_workers = len(stats)
        return ServiceStatus(
            name="celery",
            status=EstadoServicio.OK,
            latency_ms=latency_ms,
            message=f"{num_workers} worker(s) activo(s)",
        )

    except asyncio.TimeoutError:
        return ServiceStatus(
            name="celery",
            status=EstadoServicio.DEGRADED,
            latency_ms=None,
            message=f"Timeout ({_TIMEOUT_CELERY_S}s) — workers no responden",
        )
    except Exception as exc:
        logger.error("Health check celery FAILED: %s", exc)
        return ServiceStatus(
            name="celery",
            status=EstadoServicio.DEGRADED,
            latency_ms=None,
            message="No disponible",
        )


async def _check_llm() -> ServiceStatus:
    """
    Verifica qué proveedor LLM está activo leyendo el estado de Redis.
    NO hace una llamada real al LLM (sería caro y lento para un health check).

    El llm_router.py guarda en Redis la clave `llm:proveedor_activo` con el
    nombre del proveedor actual (claude, openai, groq, gemini, ollama).
    Si la clave no existe, asume Claude (el primero de la cadena de fallback).

    Si ningún proveedor está disponible → DOWN.
    """
    from db.redis_client import get_redis

    proveedores_fallback = ["deepseek", "openai"]

    try:
        t0 = time.perf_counter()
        r = get_redis()

        # Verificar proveedor activo
        proveedor_activo = await r.get("llm:proveedor_activo")
        proveedor_activo = (
            proveedor_activo.decode() if isinstance(proveedor_activo, bytes) else (proveedor_activo or "deepseek")
        )

        # Verificar si todos los proveedores están marcados como exhausted
        exhausted = []
        for p in proveedores_fallback:
            val = await r.get(f"llm:exhausted:{p}")
            if val:
                exhausted.append(p)

        latency_ms = round((time.perf_counter() - t0) * 1000, 1)

        if len(exhausted) >= len(proveedores_fallback):
            # Todos los proveedores agotados — situación crítica
            return ServiceStatus(
                name="llm",
                status=EstadoServicio.DOWN,
                latency_ms=latency_ms,
                message="Todos los proveedores LLM agotados o con error",
            )

        disponibles = [p for p in proveedores_fallback if p not in exhausted]
        return ServiceStatus(
            name="llm",
            status=EstadoServicio.OK,
            latency_ms=latency_ms,
            message=f"Activo: {proveedor_activo} | Disponibles: {', '.join(disponibles)}",
        )

    except Exception as exc:
        # Si Redis no está disponible, asumimos que el LLM está ok (no podemos verificar)
        logger.warning("No se pudo verificar estado LLM desde Redis: %s", exc)
        return ServiceStatus(
            name="llm",
            status=EstadoServicio.DEGRADED,
            latency_ms=None,
            message="No se puede verificar el estado del LLM (Redis no disponible)",
        )


# ─── Endpoint ────────────────────────────────────────────────────────────────

@router.get(
    "/health",
    response_model=HealthResponse,
    summary="Estado del sistema — todos los servicios en paralelo",
)
async def health() -> HealthResponse:
    """
    Ejecuta todos los checks en paralelo y agrega el estado global.

    Lógica de estado global:
      - Si postgres o llm están DOWN → global = DOWN (app no funciona)
      - Si cualquier otro servicio está DEGRADED → global = DEGRADED
      - Si todos están OK → global = OK
    """
    # Ejecutar todos los checks en paralelo — no tienen dependencias entre sí
    services: tuple[ServiceStatus, ...] = await asyncio.gather(
        _check_postgres(),
        _check_redis(),
        _check_celery(),
        _check_llm(),
    )

    services_list = list(services)

    # Determinar estado global
    estados_por_nombre = {s.name: s.status for s in services_list}
    criticos_caidos = [
        nombre for nombre in _CRITICOS
        if estados_por_nombre.get(nombre) == EstadoServicio.DOWN
    ]

    if criticos_caidos:
        status_global = EstadoServicio.DOWN
    elif any(s.status != EstadoServicio.OK for s in services_list):
        status_global = EstadoServicio.DEGRADED
    else:
        status_global = EstadoServicio.OK

    if status_global != EstadoServicio.OK:
        logger.warning(
            "Health check: status=%s | services=%s",
            status_global,
            {s.name: s.status for s in services_list},
        )

    return HealthResponse(
        status=status_global,
        version="0.1.0",
        timestamp=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        services=services_list,
    )


# ─── /api/health/places ──────────────────────────────────────────────────────

class PlacesHealthResponse(BaseModel):
    """Diagnóstico de la dimensión de Competencia.

    El frontend puede pintar un banner de aviso si `pct_cobertura_ninguna` es
    alto (el dato que se muestra a los usuarios entonces está imputado) o si
    el último dump del scraper es antiguo.
    """
    timestamp: str
    pct_cobertura_ninguna: Optional[float] = None  # 0-100
    pct_cobertura_alta: Optional[float] = None
    total_zonas_sector_radio: Optional[int] = None
    ultima_ejecucion_scrape: Optional[str] = None
    ultimo_estado_scrape: Optional[str] = None
    rate_limits_ultimas_24h: dict[str, int] = {}


@router.get(
    "/health/places",
    response_model=PlacesHealthResponse,
    summary="Métricas de la dimensión de Competencia (scraper + rate-limits)",
)
async def health_places() -> PlacesHealthResponse:
    from db.conexion import get_db
    from db.redis_client import get_redis

    pct_ninguna: Optional[float] = None
    pct_alta: Optional[float] = None
    total: Optional[int] = None
    ultima_ejec: Optional[str] = None
    ultimo_estado: Optional[str] = None

    try:
        async with get_db() as conn:
            row = await conn.fetchrow("""
                SELECT
                    COUNT(*) FILTER (WHERE cobertura_competencia = 'ninguna') AS n_ninguna,
                    COUNT(*) FILTER (WHERE cobertura_competencia = 'alta')    AS n_alta,
                    COUNT(*)                                                  AS total
                FROM competencia_por_local
                WHERE fecha_calculo >= CURRENT_DATE - INTERVAL '3 days'
            """)
            if row and row["total"]:
                total = int(row["total"])
                pct_ninguna = round(100.0 * (row["n_ninguna"] or 0) / total, 1)
                pct_alta    = round(100.0 * (row["n_alta"]    or 0) / total, 1)

            ejec = await conn.fetchrow("""
                SELECT fecha_inicio, fecha_fin, estado
                FROM pipeline_ejecuciones
                WHERE pipeline = 'competencia_scrape'
                ORDER BY fecha_inicio DESC
                LIMIT 1
            """)
            if ejec:
                fi = ejec["fecha_fin"] or ejec["fecha_inicio"]
                ultima_ejec = fi.isoformat() if fi else None
                ultimo_estado = ejec["estado"]
    except Exception as exc:
        logger.warning("health_places: BD no disponible: %s", exc)

    # Rate-limits de los proveedores — contadores que pone places_router.
    rate_limits: dict[str, int] = {}
    try:
        r = get_redis()
        hoy = datetime.now(timezone.utc).date().isoformat()
        ayer = (datetime.now(timezone.utc).date().toordinal() - 1)
        ayer_iso = datetime.fromordinal(ayer).date().isoformat()
        for prv in ("google", "foursquare", "yelp", "osm"):
            total_prv = 0
            for dia_iso in (hoy, ayer_iso):
                v = await r.get(f"places:rl:{prv}:{dia_iso}")
                if v:
                    try:
                        total_prv += int(v)
                    except (TypeError, ValueError):
                        pass
            if total_prv:
                rate_limits[prv] = total_prv
    except Exception as exc:
        logger.warning("health_places: Redis no disponible: %s", exc)

    return PlacesHealthResponse(
        timestamp=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        pct_cobertura_ninguna=pct_ninguna,
        pct_cobertura_alta=pct_alta,
        total_zonas_sector_radio=total,
        ultima_ejecucion_scrape=ultima_ejec,
        ultimo_estado_scrape=ultimo_estado,
        rate_limits_ultimas_24h=rate_limits,
    )
