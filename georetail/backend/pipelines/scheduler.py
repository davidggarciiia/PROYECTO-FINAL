"""
pipelines/scheduler.py — APScheduler para todos los pipelines de datos.

Frecuencias (ver arquitectura.md):
  resenas:             Diario 03:00
  aforaments:          Diario 04:00
  precios:             Semanal lunes 05:00
  scores:              Semanal martes 06:00
  demografia:          Mensual día 1, 07:00
  registre_merc:       Mensual día 1, 08:00
  params_fin:          Semanal domingo 03:00
  mercado_locales_alq: Cada 3 días, 02:00
  mercado_locales_vta: Semanal miércoles 02:30
  mercado_viviendas:   Cada 14 días, 01:00
"""
import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

logger = logging.getLogger(__name__)
_scheduler = AsyncIOScheduler(timezone="Europe/Madrid")


def init_scheduler() -> None:
    # ── Pipelines existentes ──────────────────────────────────────────────────
    _scheduler.add_job(_run_resenas,    CronTrigger(hour=3,  minute=0),               id="resenas",     replace_existing=True)
    _scheduler.add_job(_run_aforaments, CronTrigger(hour=4,  minute=0),               id="aforaments",  replace_existing=True)
    _scheduler.add_job(_run_precios,    CronTrigger(day_of_week="mon", hour=5),        id="precios",     replace_existing=True)
    _scheduler.add_job(_run_scores,     CronTrigger(day_of_week="tue", hour=6),        id="scores",      replace_existing=True)
    _scheduler.add_job(_run_demografia, CronTrigger(day=1, hour=7),                    id="demografia",  replace_existing=True)
    _scheduler.add_job(_run_registre,   CronTrigger(day=1, hour=8),                    id="registre",    replace_existing=True)
    _scheduler.add_job(_run_params_fin, CronTrigger(day_of_week="sun", hour=3),        id="params_fin",  replace_existing=True)

    # ── Mercado inmobiliario (multi-portal) ───────────────────────────────────
    _scheduler.add_job(_run_mercado_locales_alq, CronTrigger(day="*/3", hour=2, minute=0),          id="mercado_locales_alq", replace_existing=True)
    _scheduler.add_job(_run_mercado_locales_vta, CronTrigger(day_of_week="wed", hour=2, minute=30), id="mercado_locales_vta", replace_existing=True)
    _scheduler.add_job(_run_mercado_viviendas,   CronTrigger(day="*/14", hour=1, minute=0),         id="mercado_viviendas",   replace_existing=True)

    _scheduler.start()
    logger.info("APScheduler iniciado con %d jobs", len(_scheduler.get_jobs()))


def stop_scheduler() -> None:
    _scheduler.shutdown(wait=False)


# ── Runners existentes ────────────────────────────────────────────────────────

async def _run_resenas():
    try:
        from pipelines.resenas import ejecutar
        await ejecutar()
    except Exception as e:
        logger.error("Pipeline resenas error: %s", e)

async def _run_aforaments():
    try:
        from pipelines.aforaments import ejecutar
        await ejecutar()
    except Exception as e:
        logger.error("Pipeline aforaments error: %s", e)

async def _run_precios():
    try:
        from pipelines.precios import ejecutar
        await ejecutar()
    except Exception as e:
        logger.error("Pipeline precios error: %s", e)

async def _run_scores():
    try:
        from pipelines.scores import ejecutar
        await ejecutar()
    except Exception as e:
        logger.error("Pipeline scores error: %s", e)

async def _run_demografia():
    try:
        from pipelines.demografia import ejecutar
        await ejecutar()
    except Exception as e:
        logger.error("Pipeline demografia error: %s", e)

async def _run_registre():
    try:
        from pipelines.registre_mercantil import ejecutar
        await ejecutar()
    except Exception as e:
        logger.error("Pipeline registre_mercantil error: %s", e)

async def _run_params_fin():
    try:
        from pipelines.parametros_financieros import ejecutar
        await ejecutar()
    except Exception as e:
        logger.error("Pipeline params_fin error: %s", e)


# ── Runners mercado inmobiliario ──────────────────────────────────────────────

async def _run_mercado_locales_alq():
    try:
        from pipelines.mercado_inmobiliario import ejecutar
        stats = await ejecutar(modo="locales_alquiler")
        logger.info("Mercado locales_alquiler — %s", stats)
    except Exception as e:
        logger.error("Pipeline mercado locales_alquiler error: %s", e)

async def _run_mercado_locales_vta():
    try:
        from pipelines.mercado_inmobiliario import ejecutar
        stats = await ejecutar(modo="locales_venta")
        logger.info("Mercado locales_venta — %s", stats)
    except Exception as e:
        logger.error("Pipeline mercado locales_venta error: %s", e)

async def _run_mercado_viviendas():
    try:
        from pipelines.mercado_inmobiliario import ejecutar
        stats = await ejecutar(modo="viviendas")
        logger.info("Mercado viviendas — %s", stats)
    except Exception as e:
        logger.error("Pipeline mercado viviendas error: %s", e)
