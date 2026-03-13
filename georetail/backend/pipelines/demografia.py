"""
pipelines/demografia.py — Pipeline mensual de datos demográficos.

Fuentes:
  - Padró Municipal BCN (CKAN `pad_mdbas`): población, edad, extranjeros
  - Renda disponible llars BCN (CKAN `renda-disponible-llars-bcn`): renta por barrio
  - INE API: datos complementarios si los BCN no están actualizados
"""
from __future__ import annotations
import logging
import os
import httpx
from db.conexion import get_db

logger = logging.getLogger(__name__)

# ── Open Data Barcelona ───────────────────────────────────────────────────────
# API key opcional — sin key funciona con límite ~1000 req/día por IP.
# Configurar OPEN_DATA_BCN_API_KEY en el .env para límite ~10.000 req/día.
# Registro gratuito: https://opendata-ajuntament.barcelona.cat → Mi cuenta → API Key
_CKAN = "https://opendata-ajuntament.barcelona.cat/data/api/action"
_CKAN_HEADERS: dict = (
    {"Authorization": os.environ.get("OPEN_DATA_BCN_API_KEY", "")}
    if os.environ.get("OPEN_DATA_BCN_API_KEY")
    else {}
)


async def ejecutar() -> dict:
    eid = await _init()
    ok = 0
    try:
        ok += await _poblar_renta()
        ok += await _poblar_padro()
        await _fin(eid, ok, "ok")
        return {"registros": ok}
    except Exception as e:
        logger.error("Pipeline demografia error: %s", e)
        await _fin(eid, ok, "error", str(e))
        raise


async def _poblar_renta() -> int:
    """Carga renta media por hogar por barrio → variables_zona.renta_media_hogar"""
    ok = 0
    try:
        async with httpx.AsyncClient(timeout=30.0, headers=_CKAN_HEADERS) as c:
            sql = """SELECT * FROM "renda-disponible-llars-bcn"
                     WHERE "Codi_Barri" IS NOT NULL ORDER BY "Any" DESC LIMIT 2000"""
            r = await c.get(f"{_CKAN}/datastore_search_sql", params={"sql": sql})
            r.raise_for_status()
            rows = r.json().get("result",{}).get("records",[])

        async with get_db() as conn:
            for row in rows:
                barri_code = str(row.get("Codi_Barri","")).zfill(6)
                renta = float(str(row.get("Import_Ren_Bruta_Mit","0")).replace(".","").replace(",",".") or 0)
                any_ = int(row.get("Any",2022))
                fecha = f"{any_}-06-01"

                if not renta: continue

                # Mapear barrio a zonas
                zona_ids = await conn.fetch("""
                    SELECT z.id FROM zonas z
                    JOIN barrios b ON b.id=z.barrio_id
                    WHERE b.codigo=$1
                """, barri_code)

                for z in zona_ids:
                    await conn.execute("""
                        INSERT INTO variables_zona (zona_id, fecha, renta_media_hogar, fuente)
                        VALUES ($1,$2,$3,'padro_bcn')
                        ON CONFLICT (zona_id,fecha) DO UPDATE
                        SET renta_media_hogar=EXCLUDED.renta_media_hogar
                    """, z["id"], fecha, renta)
                    ok += 1

    except Exception as e:
        logger.warning("_poblar_renta error: %s", e)
    return ok


async def _poblar_padro() -> int:
    """Carga datos del padrón: edad media, % extranjeros, densidad."""
    ok = 0
    try:
        async with httpx.AsyncClient(timeout=30.0, headers=_CKAN_HEADERS) as c:
            sql = """SELECT * FROM "pad_mdbas" LIMIT 2000"""
            r = await c.get(f"{_CKAN}/datastore_search_sql", params={"sql": sql})
            r.raise_for_status()
            rows = r.json().get("result",{}).get("records",[])

        async with get_db() as conn:
            for row in rows:
                barri_code = str(row.get("Codi_Barri","")).zfill(6)
                year = int(row.get("Any",2023))
                fecha = f"{year}-01-01"

                poblacion    = int(row.get("Total","0") or 0)
                pct_estran   = float(row.get("Pct_Estrangers","0").replace(",",".") or 0) / 100
                edad_mediana = float(row.get("Edat_Mediana","42").replace(",",".") or 42)

                zona_ids = await conn.fetch("""
                    SELECT z.id FROM zonas z
                    JOIN barrios b ON b.id=z.barrio_id
                    WHERE b.codigo=$1
                """, barri_code)

                for z in zona_ids:
                    await conn.execute("""
                        INSERT INTO variables_zona (zona_id, fecha, poblacion, pct_extranjeros, edad_media, fuente)
                        VALUES ($1,$2,$3,$4,$5,'padro_bcn')
                        ON CONFLICT (zona_id,fecha) DO UPDATE
                        SET poblacion=EXCLUDED.poblacion,
                            pct_extranjeros=EXCLUDED.pct_extranjeros,
                            edad_media=EXCLUDED.edad_media
                    """, z["id"], fecha, poblacion, pct_estran, edad_mediana)
                    ok += 1

    except Exception as e:
        logger.warning("_poblar_padro error: %s", e)
    return ok


async def _init():
    async with get_db() as conn:
        return await conn.fetchval(
            "INSERT INTO pipeline_ejecuciones(pipeline,estado) VALUES('demografia','running') RETURNING id")

async def _fin(eid, reg, estado, msg=None):
    async with get_db() as conn:
        await conn.execute(
            "UPDATE pipeline_ejecuciones SET fecha_fin=NOW(),registros=$1,estado=$2,mensaje_error=$3 WHERE id=$4",
            reg, estado, msg, eid)
