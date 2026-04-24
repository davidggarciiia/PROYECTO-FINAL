"""
pipelines/registre_mercantil.py — Pipeline mensual del Registre Mercantil.

Fuente: datos.gob.es — Estadística Registral Mercantil
Dataset: empresas constituidas y disueltas en Barcelona (municipio 08019)

Tabla destino: negocios_historico
  - Cada empresa del Registre = potencial ejemplo de entrenamiento para XGBoost
  - activo_3_anos = TRUE si no se disolvió en los primeros 3 años
  - features_snapshot = variables_zona en el momento de apertura

Este dataset es el más crítico — sin él no hay training data para XGBoost.
"""
from __future__ import annotations
import logging
from datetime import date, timedelta
import httpx
from routers.geocoding_router import geocodificar
from db.conexion import get_db

logger = logging.getLogger(__name__)

# Mapeo CNAE → sector interno
_CNAE_SECTOR = {
    "5610":"restauracion","5621":"restauracion","5629":"restauracion","5630":"restauracion",
    "4711":"moda","4719":"moda","4771":"moda","4772":"moda","4779":"moda",
    "9602":"estetica","9609":"estetica",
    "8690":"salud","8621":"salud","8622":"salud",
    "9319":"deporte","9313":"deporte",
    "8510":"educacion","8520":"educacion","8531":"educacion",
}


async def ejecutar() -> dict:
    eid = await _init()
    ok = 0
    try:
        async with httpx.AsyncClient(timeout=60.0) as c:
            r = await c.get(
                "https://datos.gob.es/apidata/catalog/dataset/"
                "e04958001-estadistica-registral-mercantil",
                params={"_format":"json","_pageSize":1000})
            r.raise_for_status()
            data = r.json()

        empresas = data.get("result",{}).get("items",[])
        for emp in empresas:
            try:
                ok += await _procesar_empresa(emp)
            except Exception as e:
                logger.debug("Error empresa: %s", e)

        await _fin(eid, ok, "ok")
        return {"registros": ok}
    except Exception as e:
        logger.error("Pipeline registre_mercantil error: %s", e)
        await _fin(eid, ok, "error", str(e))
        raise


async def _procesar_empresa(emp: dict) -> int:
    """Geocodifica, determina zona y guarda en negocios_historico."""
    municipio = str(emp.get("municipio_codigo",""))
    if not municipio.startswith("08019"):
        return 0

    cnae = str(emp.get("cnae",""))[:4]
    sector = _CNAE_SECTOR.get(cnae)
    if not sector:
        return 0

    fecha_ap_str = emp.get("fecha_constitucion","")
    if not fecha_ap_str or len(fecha_ap_str) < 10:
        return 0

    try:
        fecha_ap = date.fromisoformat(fecha_ap_str[:10])
    except ValueError:
        return 0
    fecha_ci_str = emp.get("fecha_disolucion")
    try:
        fecha_ci = date.fromisoformat(fecha_ci_str[:10]) if fecha_ci_str else None
    except ValueError:
        fecha_ci = None

    # activo_3_anos = no se disolvió antes de los 3 años
    hoy = date.today()
    tres_anos = fecha_ap + timedelta(days=1095)
    if tres_anos > hoy:
        return 0  # no ha pasado suficiente tiempo para saber

    activo_3a = fecha_ci is None or fecha_ci > tres_anos

    # Geocodificar dirección
    direccion = emp.get("domicilio_social","")
    coords = await geocodificar(direccion) if direccion else None
    if not coords:
        return 0

    if len(coords) < 2:
        return 0
    lat, lng = coords[0], coords[1]

    # Encontrar zona
    async with get_db() as conn:
        zona_id = await conn.fetchval("""
            SELECT id FROM zonas
            WHERE ST_Within(ST_SetSRID(ST_MakePoint($1,$2),4326), geometria)
            LIMIT 1
        """, lng, lat)

        if not zona_id:
            return 0

        # Features snapshot: variables_zona en el momento de apertura
        snap = await conn.fetchrow("""
            SELECT flujo_peatonal_total, renta_media_hogar, pct_extranjeros
            FROM variables_zona WHERE zona_id=$1
            ORDER BY ABS(EXTRACT(EPOCH FROM (fecha - $2::date))) ASC LIMIT 1
        """, zona_id, str(fecha_ap))

        features = dict(snap) if snap else {}

        await conn.execute("""
            INSERT INTO negocios_historico
                (nombre, sector_codigo, zona_id, fecha_apertura, fecha_cierre,
                 sobrevivio_3a, features_snapshot)
            VALUES ($1,$2,$3,$4,$5,$6,$7)
            ON CONFLICT DO NOTHING
        """, emp.get("nombre_empresa",""), sector, zona_id,
            fecha_ap, fecha_ci, activo_3a,
            str(features) if features else None)

    return 1


async def _init():
    async with get_db() as conn:
        return await conn.fetchval(
            "INSERT INTO pipeline_ejecuciones(pipeline,estado) VALUES('registre_mercantil','running') RETURNING id")

async def _fin(eid, reg, estado, msg=None):
    async with get_db() as conn:
        await conn.execute(
            "UPDATE pipeline_ejecuciones SET fecha_fin=NOW(),registros=$1,estado=$2,mensaje_error=$3 WHERE id=$4",
            reg, estado, msg, eid)
