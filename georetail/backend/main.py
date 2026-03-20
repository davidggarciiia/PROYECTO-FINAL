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

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from config import settings
from db.conexion import init_db_pool, close_db_pool
from db.redis_client import init_redis, close_redis
from nlp.embeddings import cargar_modelo as cargar_embeddings
from scoring.scorer import cargar_modelo as cargar_xgb
from pipelines.scheduler import init_scheduler, stop_scheduler

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

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan (ciclo de vida) de la aplicación.
    El bloque antes del `yield` se ejecuta al arrancar.
    El bloque después del `yield` se ejecuta al apagar.
    """
    logger.info("Arrancando GeoRetail backend...")

    await init_db_pool()
    logger.info("Pool de PostgreSQL inicializado")

    await init_redis()
    logger.info("Redis inicializado")

    try:
        cargar_embeddings()
        logger.info("Modelo de embeddings cargado")
    except Exception as exc:
        logger.warning("No se pudo cargar el modelo de embeddings: %s", exc)

    try:
        await cargar_xgb()
        logger.info("Modelo XGBoost cargado")
    except Exception as exc:
        logger.warning("No se pudo cargar el modelo XGBoost: %s", exc)

    init_scheduler()
    logger.info("Scheduler iniciado")

    logger.info("GeoRetail listo en http://localhost:%d", settings.PORT)

    yield

    logger.info("Apagando GeoRetail...")
    stop_scheduler()
    await close_db_pool()
    await close_redis()
    logger.info("GeoRetail apagado correctamente")


app = FastAPI(
    title       = "GeoRetail API",
    description = "API de recomendación de ubicaciones comerciales en Barcelona",
    version     = "1.0.0",
    lifespan    = lifespan,
    docs_url    = "/docs",
    redoc_url   = "/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins     = settings.CORS_ORIGINS,
    allow_credentials = True,
    allow_methods     = ["*"],
    allow_headers     = ["*"],
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
