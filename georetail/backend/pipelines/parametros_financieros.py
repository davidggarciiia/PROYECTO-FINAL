"""
pipelines/parametros_financieros.py — Pre-calcula parámetros financieros semanalmente.
Ejecuta cada domingo 03:00 para las ~200 zonas × 5 sectores.
"""
from __future__ import annotations
import asyncio, logging
from datetime import date
from db.conexion import get_db
from financiero.estimador import estimar_parametros

logger = logging.getLogger(__name__)
_SECTORES = ["restauracion","moda","estetica","tatuajes","shisha_lounge"]
_MAX_CONC = 20


async def ejecutar() -> dict:
    eid = await _init()
    ok = err = 0
    try:
        async with get_db() as conn:
            zonas = await conn.fetch("""
                SELECT DISTINCT zona_id FROM variables_zona
                WHERE fecha >= CURRENT_DATE - INTERVAL '60 days'
            """)
            sector_map = {r["codigo"]: r["id"]
                          for r in await conn.fetch("SELECT id, codigo FROM sectores")}

        zona_ids = [r["zona_id"] for r in zonas]
        hoy = date.today()
        sem = asyncio.Semaphore(_MAX_CONC)

        tareas = [
            _procesar(sem, zid, sector, sector_map.get(sector), hoy)
            for zid in zona_ids
            for sector in _SECTORES
            if sector_map.get(sector)
        ]

        resultados = await asyncio.gather(*tareas, return_exceptions=True)
        for r in resultados:
            if isinstance(r, Exception): err += 1
            else: ok += 1

        await _fin(eid, ok, "ok")
        return {"ok": ok, "errores": err}
    except Exception as e:
        logger.error("Pipeline params_fin error: %s", e)
        await _fin(eid, ok, "error", str(e))
        raise


async def _procesar(sem, zona_id, sector, sector_id, fecha):
    async with sem:
        p = await estimar_parametros(zona_id=zona_id, sector=sector, perfil={})
        async with get_db() as conn:
            await conn.execute("""
                INSERT INTO parametros_financieros_zona (
                    zona_id,sector_id,fecha_calculo,
                    ticket_medio,ticket_fuente,ticket_confianza,ticket_rango_min,ticket_rango_max,
                    clientes_dia_conservador,clientes_dia_optimista,clientes_fuente,clientes_confianza,
                    dias_apertura_mes,
                    alquiler_mensual,alquiler_fuente,alquiler_confianza,
                    num_empleados,salarios_mensual,salarios_fuente,
                    coste_mercancia_pct,otros_fijos_mensual,
                    reforma_estimada,reforma_rango_min,reforma_rango_max,
                    equipamiento_estimado,equipamiento_rango_min,equipamiento_rango_max,
                    deposito_fianza,otros_iniciales
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,
                          $17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29)
                ON CONFLICT (zona_id,sector_id,fecha_calculo) DO UPDATE SET
                    ticket_medio=EXCLUDED.ticket_medio,ticket_fuente=EXCLUDED.ticket_fuente,
                    ticket_confianza=EXCLUDED.ticket_confianza,
                    clientes_dia_conservador=EXCLUDED.clientes_dia_conservador,
                    clientes_dia_optimista=EXCLUDED.clientes_dia_optimista,
                    alquiler_mensual=EXCLUDED.alquiler_mensual,
                    salarios_mensual=EXCLUDED.salarios_mensual,
                    coste_mercancia_pct=EXCLUDED.coste_mercancia_pct,
                    otros_fijos_mensual=EXCLUDED.otros_fijos_mensual,
                    reforma_estimada=EXCLUDED.reforma_estimada,
                    equipamiento_estimado=EXCLUDED.equipamiento_estimado,
                    deposito_fianza=EXCLUDED.deposito_fianza,
                    otros_iniciales=EXCLUDED.otros_iniciales
            """,
            zona_id, sector_id, fecha,
            p.ticket_medio.valor, p.ticket_medio.fuente, p.ticket_medio.confianza,
            p.ticket_medio.rango_min, p.ticket_medio.rango_max,
            p.clientes_dia_conservador.valor, p.clientes_dia_optimista.valor,
            p.clientes_dia_conservador.fuente, p.clientes_dia_conservador.confianza,
            int(p.dias_apertura_mes.valor),
            p.alquiler_mensual.valor, p.alquiler_mensual.fuente, p.alquiler_mensual.confianza,
            p.num_empleados, p.salarios_mensual.valor, p.salarios_mensual.fuente,
            p.coste_mercancia_pct.valor, p.otros_fijos_mensual.valor,
            p.reforma_local.valor, p.reforma_local.rango_min, p.reforma_local.rango_max,
            p.equipamiento.valor, p.equipamiento.rango_min, p.equipamiento.rango_max,
            p.deposito_fianza.valor, p.otros_iniciales.valor)


async def _init():
    async with get_db() as conn:
        return await conn.fetchval(
            "INSERT INTO pipeline_ejecuciones(pipeline,estado) VALUES('parametros_financieros','running') RETURNING id")

async def _fin(eid, reg, estado, msg=None):
    async with get_db() as conn:
        await conn.execute(
            "UPDATE pipeline_ejecuciones SET fecha_fin=NOW(),registros=$1,estado=$2,mensaje_error=$3 WHERE id=$4",
            reg, estado, msg, eid)
