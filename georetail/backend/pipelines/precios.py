"""
pipelines/precios.py — Pipeline semanal de precios de alquiler comercial.

Fuentes:
  1. Idealista API (primaria) — locales en alquiler por zona
  2. Open Data BCN `locllo-evolucio` (precio/m² mensual por barrio)

Tabla destino: precios_alquiler_zona + locales (actualización)
"""
from __future__ import annotations
import logging
import httpx
from db.conexion import get_db

logger = logging.getLogger(__name__)


async def ejecutar() -> dict:
    eid = await _init()
    ok = 0
    try:
        # 1. Actualizar precios por zona desde Open Data BCN
        ok += await _precios_open_data()

        # 2. Actualizar locales individuales desde Idealista (si hay API key)
        ok += await _locales_idealista()

        await _fin(eid, ok, "ok")
        return {"registros": ok}
    except Exception as e:
        logger.error("Pipeline precios error: %s", e)
        await _fin(eid, ok, "error", str(e))
        raise


async def _precios_open_data() -> int:
    """
    Descarga precios de alquiler de locales comerciales del CKAN de BCN.
    Dataset: locllo-evolucio (evolución de precios de alquiler por barrio)
    """
    import os
    _CKAN_BASE = "https://opendata-ajuntament.barcelona.cat/data/api/action"
    ok = 0
    try:
        async with httpx.AsyncClient(timeout=30.0) as c:
            sql = "SELECT * FROM \"locllo-evolucio\" ORDER BY \"Any\" DESC, \"Trimestre\" DESC LIMIT 1000"
            r = await c.get(f"{_CKAN_BASE}/datastore_search_sql", params={"sql": sql})
            r.raise_for_status()
            rows = r.json().get("result",{}).get("records",[])

        async with get_db() as conn:
            for row in rows:
                # Mapear barrio → zona_id
                barrio_nom = row.get("Nom_Barri","")
                precio_m2 = float(row.get("Index_Preu_Unitari","0").replace(",",".") or 0)
                any_ = int(row.get("Any",2024))
                trim = int(row.get("Trimestre","1") or 1)
                mes = (trim-1)*3 + 2  # mes central del trimestre
                fecha = f"{any_}-{mes:02d}-01"

                if not precio_m2:
                    continue

                zona_id = await conn.fetchval("""
                    SELECT z.id FROM zonas z
                    JOIN barrios b ON b.id=z.barrio_id
                    WHERE b.nombre ILIKE $1
                    LIMIT 1
                """, f"%{barrio_nom}%")

                if not zona_id:
                    continue

                await conn.execute("""
                    INSERT INTO precios_alquiler_zona (zona_id,fecha,precio_m2,fuente)
                    VALUES ($1,$2,$3,'open_data_bcn')
                    ON CONFLICT (zona_id,fecha,fuente) DO UPDATE SET precio_m2=EXCLUDED.precio_m2
                """, zona_id, fecha, precio_m2)
                ok += 1

    except Exception as e:
        logger.warning("_precios_open_data error: %s", e)

    return ok


async def _locales_idealista() -> int:
    """
    Actualiza locales disponibles en alquiler desde Idealista API.
    Requiere IDEALISTA_API_KEY y IDEALISTA_SECRET.
    """
    import os
    import base64

    key = os.environ.get("IDEALISTA_API_KEY","")
    secret = os.environ.get("IDEALISTA_SECRET","")
    if not key or not secret:
        logger.info("Sin credenciales Idealista — skipping")
        return 0

    ok = 0
    try:
        # OAuth2 client_credentials
        creds = base64.b64encode(f"{key}:{secret}".encode()).decode()
        async with httpx.AsyncClient(timeout=15.0) as c:
            tok_r = await c.post(
                "https://api.idealista.com/oauth/accesstoken",
                headers={"Authorization": f"Basic {creds}",
                         "Content-Type": "application/x-www-form-urlencoded"},
                data={"grant_type":"client_credentials","scope":"read"})
            tok_r.raise_for_status()
            token = tok_r.json()["access_token"]

            # Buscar locales en alquiler en Barcelona
            search_r = await c.post(
                "https://api.idealista.com/3.5/es/search",
                headers={"Authorization": f"Bearer {token}"},
                data={
                    "country":"es","operation":"rent","propertyType":"premises",
                    "center":"41.3851,2.1734","distance":5000,
                    "maxItems":50,"numPage":1,
                })
            search_r.raise_for_status()
            data = search_r.json()

        async with get_db() as conn:
            for local in data.get("elementList",[]):
                pid = f"idealista_{local['propertyCode']}"
                lat = local.get("latitude")
                lng = local.get("longitude")
                if not lat or not lng:
                    continue

                zona_id = await conn.fetchval("""
                    SELECT id FROM zonas
                    WHERE ST_Within(ST_SetSRID(ST_MakePoint($1,$2),4326),geometria)
                    LIMIT 1
                """, lng, lat)

                precio = local.get("price")
                m2 = local.get("size")
                direccion = local.get("address","")

                await conn.execute("""
                    INSERT INTO locales (id,zona_id,direccion,lat,lng,
                        geometria,m2,alquiler_mensual,disponible,planta,fuente)
                    VALUES ($1,$2,$3,$4,$5,ST_SetSRID(ST_MakePoint($5,$4),4326),$6,$7,TRUE,'PB','idealista')
                    ON CONFLICT (id) DO UPDATE SET
                        alquiler_mensual=EXCLUDED.alquiler_mensual,
                        disponible=TRUE, updated_at=NOW()
                """, pid, zona_id, direccion, lat, lng, m2, precio)
                ok += 1

                # Actualizar precio/m² de la zona
                if zona_id and precio and m2 and m2 > 0:
                    await conn.execute("""
                        INSERT INTO precios_alquiler_zona (zona_id,fecha,precio_m2,num_muestras,fuente)
                        VALUES ($1,CURRENT_DATE,$2,1,'idealista')
                        ON CONFLICT (zona_id,fecha,fuente) DO UPDATE SET
                            precio_m2=(precios_alquiler_zona.precio_m2*precios_alquiler_zona.num_muestras + EXCLUDED.precio_m2)
                                      /(precios_alquiler_zona.num_muestras+1),
                            num_muestras=precios_alquiler_zona.num_muestras+1
                    """, zona_id, round(precio/m2, 2))

    except Exception as e:
        logger.warning("_locales_idealista error: %s", e)

    return ok


async def _init():
    async with get_db() as conn:
        return await conn.fetchval(
            "INSERT INTO pipeline_ejecuciones(pipeline,estado) VALUES('precios','running') RETURNING id")

async def _fin(eid, reg, estado, msg=None):
    async with get_db() as conn:
        await conn.execute(
            "UPDATE pipeline_ejecuciones SET fecha_fin=NOW(),registros=$1,estado=$2,mensaje_error=$3 WHERE id=$4",
            reg, estado, msg, eid)
