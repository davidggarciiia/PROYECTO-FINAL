"""
main.py — Punto de entrada (entry point) de la aplicación FastAPI.

Arrancar en local:
  uvicorn main:app --reload --port 8000

Arrancar en producción (dentro del contenedor Docker):
  uvicorn main:app --host 0.0.0.0 --port 8000 --workers 2

Docs interactivas (Swagger UI):
  http://localhost:8000/docs
"""
from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from config import settings
from db.conexion import init_db_pool, close_db_pool
from db.redis_client import init_redis, close_redis
from nlp.embeddings import cargar_modelo as cargar_embeddings
from scoring.scorer import cargar_modelo as cargar_xgb
from scoring.motor import verificar_pesos_sectores
from pipelines.scheduler import init_scheduler, stop_scheduler
from routers.llm_router import close_llm_clients

# Endpoints
from api.buscar       import router as router_buscar
from api.cuestionario import router as router_cuestionario
from api.local        import router as router_local
from api.locales      import router as router_locales
from api.legal        import router as router_legal
from api.financiero   import router as router_financiero
from api.refinamiento import router as router_refinamiento
from api.exportar     import router as router_exportar
from api.health       import router as router_health
from api.mercado      import router as router_mercado
from api.admin        import router as router_admin
from api.competencia  import router as router_competencia
from api.transporte   import router as router_transporte

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan (ciclo de vida) de la aplicación.
    El bloque antes del `yield` se ejecuta al arrancar.
    El bloque después del `yield` se ejecuta al apagar.
    """
    logger.info("Arrancando GeoRetail backend...")

    try:
        await init_db_pool()
        logger.info("Pool de PostgreSQL inicializado")
    except Exception as exc:
        logger.warning("PostgreSQL no disponible (modo sin BD): %s", exc)

    try:
        await init_redis()
        logger.info("Redis inicializado")
    except Exception as exc:
        logger.warning("Redis no disponible (modo sin caché): %s", exc)

    try:
        await asyncio.wait_for(asyncio.to_thread(cargar_embeddings), timeout=20)
        logger.info("Modelo de embeddings cargado")
    except asyncio.TimeoutError:
        logger.warning("La carga de embeddings ha excedido 20s; continuando en modo degradado")
    except Exception as exc:
        logger.warning("No se pudo cargar el modelo de embeddings: %s", exc)

    try:
        await cargar_xgb()
        logger.info("Modelo XGBoost cargado")
    except Exception as exc:
        logger.warning("No se pudo cargar el modelo XGBoost: %s", exc)

    try:
        # Chequeo de invariante crítico: peso_* por sector debe sumar 1.0.
        # Solo loguea; no aborta para no bloquear despliegues en caliente.
        await verificar_pesos_sectores()
    except Exception as exc:
        logger.warning("verificar_pesos_sectores falló (no bloqueante): %s", exc)

    init_scheduler()
    logger.info("Scheduler iniciado")

    logger.info("GeoRetail listo en http://localhost:%d", settings.PORT)

    yield

    logger.info("Apagando GeoRetail...")
    stop_scheduler()
    await close_llm_clients()
    await close_db_pool()
    await close_redis()
    logger.info("GeoRetail apagado correctamente")


_is_production = settings.ENVIRONMENT == "production"

app = FastAPI(
    title       = "GeoRetail API",
    description = "API de recomendación de ubicaciones comerciales en Barcelona",
    version     = "1.0.0",
    lifespan    = lifespan,
    # Deshabilitar Swagger/ReDoc en producción para no exponer el contrato de la API
    docs_url    = None if _is_production else "/docs",
    redoc_url   = None if _is_production else "/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins     = settings.CORS_ORIGINS,
    allow_credentials = True,
    allow_methods     = ["GET", "POST"],
    allow_headers     = ["Content-Type", "Authorization", "X-Admin-Key"],
)

# Registrar todos los routers
app.include_router(router_buscar,       prefix="/api", tags=["búsqueda"])
app.include_router(router_cuestionario, prefix="/api", tags=["cuestionario"])
app.include_router(router_local,        prefix="/api", tags=["zonas"])
app.include_router(router_locales,      prefix="/api", tags=["zonas"])
app.include_router(router_legal,        prefix="/api", tags=["legal"])
app.include_router(router_financiero,   prefix="/api", tags=["financiero"])
app.include_router(router_refinamiento, prefix="/api", tags=["búsqueda"])
app.include_router(router_exportar,     prefix="/api", tags=["exportar"])
app.include_router(router_health,       prefix="/api", tags=["sistema"])
app.include_router(router_mercado,      prefix="/api", tags=["mercado"])
app.include_router(router_admin,        prefix="/api", tags=["admin"])
app.include_router(router_competencia,  prefix="/api", tags=["competencia"])
app.include_router(router_transporte,    prefix="/api", tags=["transporte"])
