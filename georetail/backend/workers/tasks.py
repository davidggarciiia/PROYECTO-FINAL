"""
workers/tasks.py — Tareas Celery asíncronas.

calcular_scores_batch:  scoring de múltiples zonas en background.
generar_pdf_task:       generación de PDF en background.
scrape_mercado_task:    scraping bajo demanda de portales inmobiliarios.
"""
from __future__ import annotations
import asyncio, logging
from workers.celery_app import celery_app

logger = logging.getLogger(__name__)


def _run(coro):
    """Ejecutar coroutine en un nuevo event loop (Celery workers son síncronos)."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.run_until_complete(_cleanup())
        loop.close()


async def _cleanup():
    """Cierra los pools de BD y Redis al finalizar cada tarea Celery."""
    try:
        from db.conexion import close_db_pool
        await close_db_pool()
    except Exception:
        pass
    try:
        from db.redis_client import close_redis
        await close_redis()
    except Exception:
        pass


@celery_app.task(bind=True, max_retries=2, default_retry_delay=30)
def calcular_scores_batch(self, zona_ids: list[str], sector: str, session_id: str):
    """
    Calcula scores para una lista de zonas y los persiste en scores_zona.
    Llamado desde /api/buscar cuando hay más de 5 zonas candidatas.
    """
    try:
        return _run(_calcular_scores(zona_ids, sector, session_id))
    except Exception as exc:
        logger.error("calcular_scores_batch error: %s", exc)
        raise self.retry(exc=exc)


async def _calcular_scores(zona_ids, sector, session_id):
    from db.conexion import init_db_pool
    from db.redis_client import init_redis
    from scoring.scorer import calcular_scores_batch, guardar_scores
    from db.conexion import get_db

    await init_db_pool()
    await init_redis()

    # Obtener sector_id
    async with get_db() as conn:
        sid = await conn.fetchval("SELECT id FROM sectores WHERE codigo=$1", sector)

    resultados = await calcular_scores_batch(zona_ids, sector)

    # Guardar en BD
    for zona_id, scores in resultados.items():
        await guardar_scores(zona_id, sid, scores)

    # Actualizar zonas en sesión con scores calculados
    from db.sesiones import get_sesion, actualizar_sesion
    sesion = await get_sesion(session_id)
    if sesion and sesion.get("zonas_actuales"):
        for z in sesion["zonas_actuales"]:
            if z["zona_id"] in resultados:
                z["score_global"] = resultados[z["zona_id"]].get("score_global")
        await actualizar_sesion(session_id, {"zonas_actuales": sesion["zonas_actuales"]})

    return {"ok": len(resultados), "zona_ids": list(resultados.keys())}


@celery_app.task(bind=True, max_retries=1)
def generar_pdf_task(self, pdf_id: str, session_id: str, zona_ids: list[str],
                     opciones: dict):
    """Genera el PDF en background y actualiza el estado en BD."""
    try:
        return _run(_generar_pdf(pdf_id, session_id, zona_ids, opciones))
    except Exception as exc:
        logger.error("generar_pdf_task error pdf_id=%s: %s", pdf_id, exc)
        _run(_marcar_error(pdf_id, str(exc)))
        raise self.retry(exc=exc)


async def _generar_pdf(pdf_id, session_id, zona_ids, opciones):
    from db.conexion import init_db_pool
    await init_db_pool()
    from exportar.generador import generar
    from db.exportaciones import marcar_completada, marcar_error
    try:
        ruta = await generar(pdf_id=pdf_id, session_id=session_id,
                             zona_ids=zona_ids, opciones=opciones)
        await marcar_completada(pdf_id, ruta)
    except Exception as e:
        await marcar_error(pdf_id, str(e))
        raise
    return {"pdf_id": pdf_id}


async def _marcar_error(pdf_id, msg):
    from db.conexion import init_db_pool
    await init_db_pool()
    from db.exportaciones import marcar_error
    await marcar_error(pdf_id, msg)


# ── Mercado inmobiliario ──────────────────────────────────────────────────────

@celery_app.task(bind=True, max_retries=2, default_retry_delay=60)
def scrape_mercado_task(
    self,
    modo: str = "locales_alquiler",
    zonas: list[str] | None = None,
    max_paginas: int | None = None,
    portales: list[str] | None = None,
):
    """
    Scraping bajo demanda de portales inmobiliarios.

    Args:
        modo:       "locales_alquiler" | "locales_venta" | "viviendas"
        zonas:      zonas de BCN para Idealista. None = todos los distritos.
        max_paginas: límite de páginas por portal/zona. None = sin límite.
        portales:   portales a usar. None = todos.
                    Valores: "idealista", "fotocasa", "habitaclia", "milanuncios", "pisos"

    Ejemplos:
        scrape_mercado_task.delay(modo="locales_alquiler")
        scrape_mercado_task.delay(modo="locales_alquiler", zonas=["Gràcia"], max_paginas=3)
        scrape_mercado_task.delay(modo="locales_alquiler", portales=["fotocasa", "habitaclia"])
    """
    try:
        return _run(_scrape_mercado(modo, zonas, max_paginas, portales))
    except Exception as exc:
        logger.error("scrape_mercado_task error modo=%s: %s", modo, exc)
        raise self.retry(exc=exc)


async def _scrape_mercado(modo, zonas, max_paginas, portales):
    from db.conexion import init_db_pool
    await init_db_pool()
    from pipelines.mercado_inmobiliario import ejecutar
    stats = await ejecutar(modo=modo, zonas=zonas, max_paginas=max_paginas, portales=portales)
    logger.info("scrape_mercado_task completado: %s", stats)
    return stats
