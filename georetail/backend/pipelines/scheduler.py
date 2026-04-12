"""
pipelines/scheduler.py — APScheduler para todos los pipelines de datos.

Frecuencias (ver arquitectura.md):
  resenas:             Diario 03:00
  aforaments:          Diario 04:00
  vianants:            Mensual día 10, 05:00  ← peatones reales (sobreescribe aforaments)
  vcity:               Mensual día 12, 04:00  ← flujo peatonal BSC (VCity, anual)
  precios:             Semanal lunes 05:00
  scores:              Semanal martes 06:00
  demografia:          Mensual día 1, 07:00
  registre_merc:       Mensual día 1, 08:00
  params_fin:          Semanal domingo 03:00
  mercado_locales_alq: Cada 3 días, 02:00
  mercado_locales_vta: Semanal miércoles 02:30
  mercado_viviendas:   Cada 14 días, 01:00
  transporte:          Semanal sábado 01:00
  purgar_portales:     Mensual día 15, 00:00
  airbnb:              Mensual día 3, 05:00
  llicencies:          Mensual día 5, 06:00
  venues_ocio:         Mensual día 7, 07:00
  booking:             Semanal jueves 03:00
  google_maps:         Semanal miércoles 02:00
  parques:             Mensual día 2, 06:00  ← parques AMB → m2_zonas_verdes_cercanas

NOTA sobre flujo peatonal:
  aforaments.py mide TRÁFICO RODADO (vehicles + bicis) — proxy impreciso.
  vianants.py mide PERSONAS A PIE — fuente correcta para análisis comercial.
  vcity.py mide PROMEDIO DIARIO ANUAL BSC (móvil+GPS) — fuente de alta precisión.
  Jerarquía de prevalencia: vcity_bsc > vianants_bcn > aforadors_csv_2025.
  El pipeline vianants sobreescribe variables_zona.fuente='vianants_bcn' cuando
  hay datos disponibles, prevaleciendo sobre 'aforadors_csv_2025'.
  El pipeline vcity escribe en la columna dedicada vcity_flujo_peatonal (no
  sobreescribe flujo_peatonal_total — conviven como columnas separadas).
"""
import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

logger = logging.getLogger(__name__)
_scheduler = AsyncIOScheduler(timezone="Europe/Madrid")


_JOB_DEFAULTS = {
    "max_instances": 1,        # evitar solapamiento de ejecuciones
    "misfire_grace_time": 300, # si el scheduler arranca tarde, ejecutar si han pasado <5min
}


def init_scheduler() -> None:
    # ── Pipelines existentes ──────────────────────────────────────────────────
    _scheduler.add_job(_run_resenas,    CronTrigger(hour=3,  minute=0),               id="resenas",     replace_existing=True, **_JOB_DEFAULTS)
    _scheduler.add_job(_run_aforaments, CronTrigger(hour=4,  minute=0),               id="aforaments",  replace_existing=True, **_JOB_DEFAULTS)
    # Peatones reales (mensual día 10, 05:00) — sobreescribe aforaments si hay datos
    _scheduler.add_job(_run_vianants,   CronTrigger(day=10, hour=5, minute=0),        id="vianants",    replace_existing=True, **_JOB_DEFAULTS)
    # VCity BSC flujo peatonal (mensual día 12, 04:00) — datos móvil+GPS, anuales
    _scheduler.add_job(_run_vcity,      CronTrigger(day=12, hour=4, minute=0),        id="vcity",       replace_existing=True, **_JOB_DEFAULTS)
    _scheduler.add_job(_run_precios,    CronTrigger(day_of_week="mon", hour=5),        id="precios",     replace_existing=True, **_JOB_DEFAULTS)
    _scheduler.add_job(_run_scores,     CronTrigger(day_of_week="tue", hour=6),        id="scores",      replace_existing=True, **_JOB_DEFAULTS)
    _scheduler.add_job(_run_demografia, CronTrigger(day=1, hour=7),                    id="demografia",  replace_existing=True, **_JOB_DEFAULTS)
    _scheduler.add_job(_run_registre,   CronTrigger(day=1, hour=8),                    id="registre",    replace_existing=True, **_JOB_DEFAULTS)
    _scheduler.add_job(_run_params_fin, CronTrigger(day_of_week="sun", hour=3),        id="params_fin",  replace_existing=True, **_JOB_DEFAULTS)

    # ── Mercado inmobiliario (multi-portal) ───────────────────────────────────
    _scheduler.add_job(_run_mercado_locales_alq, CronTrigger(day="*/3", hour=2, minute=0),          id="mercado_locales_alq", replace_existing=True, **_JOB_DEFAULTS)
    _scheduler.add_job(_run_mercado_locales_vta, CronTrigger(day_of_week="wed", hour=2, minute=30), id="mercado_locales_vta", replace_existing=True, **_JOB_DEFAULTS)
    _scheduler.add_job(_run_mercado_viviendas,   CronTrigger(day="*/14", hour=1, minute=0),         id="mercado_viviendas",   replace_existing=True, **_JOB_DEFAULTS)

    # ── Transporte público (TMB + Bicing) ────────────────────────────────────
    _scheduler.add_job(_run_transporte, CronTrigger(day_of_week="sat", hour=1, minute=0), id="transporte", replace_existing=True, **_JOB_DEFAULTS)
    _scheduler.add_job(_run_bicing,     CronTrigger(day_of_week="sat", hour=2, minute=0), id="bicing",     replace_existing=True, **_JOB_DEFAULTS)

    # ── Mantenimiento BD ──────────────────────────────────────────────────────
    _scheduler.add_job(_run_purgar_portales, CronTrigger(day=15, hour=0, minute=0), id="purgar_portales", replace_existing=True, **_JOB_DEFAULTS)

    # ── Nuevos pipelines v3: turismo y dinamismo comercial ────────────────────
    _scheduler.add_job(_run_airbnb,      CronTrigger(day=3,              hour=5, minute=0), id="airbnb",      replace_existing=True, **_JOB_DEFAULTS)
    _scheduler.add_job(_run_llicencies,  CronTrigger(day=5,              hour=6, minute=0), id="llicencies",  replace_existing=True, **_JOB_DEFAULTS)
    _scheduler.add_job(_run_venues_ocio, CronTrigger(day=7,              hour=7, minute=0), id="venues_ocio", replace_existing=True, **_JOB_DEFAULTS)
    _scheduler.add_job(_run_booking,     CronTrigger(day_of_week="thu",  hour=3, minute=0), id="booking",     replace_existing=True, **_JOB_DEFAULTS)
    _scheduler.add_job(_run_google_maps, CronTrigger(day_of_week="wed",  hour=2, minute=0), id="google_maps", replace_existing=True, **_JOB_DEFAULTS)

    # ── Parques AMB (v5: m2_zonas_verdes_cercanas) ────────────────────────────
    _scheduler.add_job(_run_parques,     CronTrigger(day=2,              hour=6, minute=0), id="parques",     replace_existing=True, **_JOB_DEFAULTS)

    # ── Seguridad (v7: dimensión multivariable — GU + comisarías + IERMB) ────
    _scheduler.add_job(_run_seguridad,   CronTrigger(day=1,              hour=7, minute=30), id="seguridad",  replace_existing=True, **_JOB_DEFAULTS)

    # ── Entorno comercial (v8: mercats + soroll + equipaments) ────────────────
    _scheduler.add_job(_run_entorno_comercial, CronTrigger(day=4, hour=6, minute=0), id="entorno_comercial", replace_existing=True, **_JOB_DEFAULTS)

    _scheduler.start()
    logger.info("APScheduler iniciado con %d jobs", len(_scheduler.get_jobs()))


def stop_scheduler() -> None:
    _scheduler.shutdown(wait=True)  # esperar a que terminen los jobs en curso


# ── Runners existentes ────────────────────────────────────────────────────────

async def _run_resenas():
    try:
        from pipelines.entorno.resenas import ejecutar
        await ejecutar()
    except Exception as e:
        logger.error("Pipeline resenas error: %s", e)

async def _run_aforaments():
    try:
        from pipelines.peatonal.aforaments import ejecutar
        await ejecutar()
    except Exception as e:
        logger.error("Pipeline aforaments error: %s", e)

async def _run_vianants():
    """
    Mensual día 10, 05:00 — Comptadors de persones vianants (Open Data BCN).
    Sobreescriu flujo_peatonal_* de aforaments.py quan hi ha dades disponibles.
    ADVERTÈNCIA: aforaments.py mesura trànsit de vehicles, NO vianants a peu.
    """
    try:
        from pipelines.peatonal.vianants import ejecutar
        await ejecutar()
    except Exception as e:
        logger.error("Pipeline vianants error: %s", e)

async def _run_vcity():
    """
    Mensual día 12, 04:00 — VCity BSC flujo peatonal por tramo.
    Escribe variables_zona.vcity_flujo_peatonal (columna dedicada, no sobrescribe
    flujo_peatonal_total). Si VCity no está disponible, usa fallback vianants_bcn.
    """
    try:
        from pipelines.peatonal.vcity import ejecutar
        result = await ejecutar()
        logger.info("Pipeline vcity — %s", result)
    except Exception as e:
        logger.error("Pipeline vcity error: %s", e)

async def _run_precios():
    try:
        from pipelines.inmobiliario.precios import ejecutar
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
        from pipelines.demografia.demografia import ejecutar
        await ejecutar()
    except Exception as e:
        logger.error("Pipeline demografia error: %s", e)

async def _run_registre():
    try:
        from pipelines.comercio.registre_mercantil import ejecutar
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
        from pipelines.inmobiliario.mercado_inmobiliario import ejecutar
        stats = await ejecutar(modo="locales_alquiler")
        logger.info("Mercado locales_alquiler — %s", stats)
    except Exception as e:
        logger.error("Pipeline mercado locales_alquiler error: %s", e)

async def _run_mercado_locales_vta():
    try:
        from pipelines.inmobiliario.mercado_inmobiliario import ejecutar
        stats = await ejecutar(modo="locales_venta")
        logger.info("Mercado locales_venta — %s", stats)
    except Exception as e:
        logger.error("Pipeline mercado locales_venta error: %s", e)

async def _run_mercado_viviendas():
    try:
        from pipelines.inmobiliario.mercado_inmobiliario import ejecutar
        stats = await ejecutar(modo="viviendas")
        logger.info("Mercado viviendas — %s", stats)
    except Exception as e:
        logger.error("Pipeline mercado viviendas error: %s", e)

async def _run_transporte():
    try:
        from pipelines.transporte.transporte import ejecutar
        await ejecutar()
    except Exception as e:
        logger.error("Pipeline transporte error: %s", e)

async def _run_bicing():
    """Semanal sábado 02:00 — BSMSA GBFS → estaciones_bicing."""
    try:
        from pipelines.transporte.bicing import ejecutar
        result = await ejecutar()
        logger.info("Pipeline bicing — %s", result)
    except Exception as e:
        logger.error("Pipeline bicing error: %s", e)

async def _run_purgar_portales():
    try:
        from db.conexion import get_db
        async with get_db() as conn:
            deleted = await conn.fetchval("SELECT purgar_portales_antiguos()")
            logger.info("Portales purgados: %d filas eliminadas", deleted or 0)
    except Exception as e:
        logger.error("Mantenimiento purgar_portales error: %s", e)


# ── Runners v3: turismo y dinamismo comercial ─────────────────────────────────

async def _run_airbnb():
    """Mensual día 3, 05:00 — Inside Airbnb → airbnb_density_500m, airbnb_occupancy_est."""
    try:
        from pipelines.turismo.airbnb import ejecutar
        await ejecutar()
    except Exception as e:
        logger.error("Pipeline airbnb error: %s", e)

async def _run_llicencies():
    """Mensual día 5, 06:00 — Open Data BCN llicències d'activitat → licencias_nuevas_1a."""
    try:
        from pipelines.comercio.llicencies import ejecutar
        await ejecutar()
    except Exception as e:
        logger.error("Pipeline llicencies error: %s", e)

async def _run_venues_ocio():
    """Mensual día 7, 07:00 — OSM + Agenda BCN → tabla venues_ocio + eventos_culturales_500m."""
    try:
        from pipelines.entorno.venues_ocio import ejecutar
        await ejecutar()
    except Exception as e:
        logger.error("Pipeline venues_ocio error: %s", e)

async def _run_booking():
    """Semanal jueves 03:00 — Booking.com API → tabla alojamientos_turisticos + booking_hoteles_500m."""
    try:
        from pipelines.turismo.booking import ejecutar
        await ejecutar()
    except Exception as e:
        logger.error("Pipeline booking error: %s", e)

async def _run_google_maps():
    """Semanal miércoles 02:00 — Google Places → negocios_activos.review_count + google_place_id."""
    try:
        from pipelines.entorno.google_maps import ejecutar
        await ejecutar()
    except Exception as e:
        logger.error("Pipeline google_maps error: %s", e)


# ── Runner parques AMB (v5) ───────────────────────────────────────────────────

async def _run_parques():
    """Mensual día 2, 06:00 — Parques AMB → parques_amb + m2_zonas_verdes_cercanas."""
    try:
        from pipelines.entorno.parques import ejecutar
        result = await ejecutar()
        logger.info("Pipeline parques AMB — %s", result)
    except Exception as e:
        logger.error("Pipeline parques error: %s", e)


# ── Runner seguridad (v7) ────────────────────────────────────────────────────

async def _run_seguridad():
    """Mensual día 1, 07:30 — GU incidents + comisarías + IERMB → seguridad multivariable."""
    try:
        from pipelines.entorno.seguridad import ejecutar
        result = await ejecutar()
        logger.info("Pipeline seguridad — %s", result)
    except Exception as e:
        logger.error("Pipeline seguridad error: %s", e)


# ── Runner entorno comercial (v8) ────────────────────────────────────────────

async def _run_entorno_comercial():
    """Mensual día 4, 06:00 — mercats + soroll + equipaments → entorno comercial multivariable."""
    try:
        from pipelines.comercio.entorno_comercial import ejecutar
        result = await ejecutar()
        logger.info("Pipeline entorno_comercial — %s", result)
    except Exception as e:
        logger.error("Pipeline entorno_comercial error: %s", e)
