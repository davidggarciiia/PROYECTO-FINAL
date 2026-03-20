"""scoring/features.py — Vector de 21 features para XGBoost."""
from __future__ import annotations
import logging
from typing import Optional
import numpy as np
from db.conexion import get_db

logger = logging.getLogger(__name__)

FEATURE_NAMES = [
    "flujo_peatonal_total","flujo_manana_pct","flujo_tarde_pct","flujo_noche_pct",
    "renta_media_hogar","edad_media","pct_extranjeros","densidad_hab_km2",
    "num_competidores_300m","rating_medio_competidores","score_saturacion",
    "precio_m2_alquiler","pct_locales_vacios","tasa_rotacion_anual",
    "score_turismo","incidencias_por_1000hab","nivel_ruido_db",
    "score_equipamientos","num_lineas_transporte","num_paradas_500m",
    "m2_zonas_verdes_cercanas",
]

# Medias de imputación calculadas sobre dataset de entrenamiento BCN
_MEDIAS = {
    "flujo_peatonal_total":850.0,"flujo_manana_pct":0.35,"flujo_tarde_pct":0.42,
    "flujo_noche_pct":0.23,"renta_media_hogar":32000.0,"edad_media":42.5,
    "pct_extranjeros":0.22,"densidad_hab_km2":22000.0,"num_competidores_300m":8.0,
    "rating_medio_competidores":3.9,"score_saturacion":50.0,"precio_m2_alquiler":18.0,
    "pct_locales_vacios":0.15,"tasa_rotacion_anual":0.18,"score_turismo":45.0,
    "incidencias_por_1000hab":35.0,"nivel_ruido_db":63.0,"score_equipamientos":55.0,
    "num_lineas_transporte":6.0,"num_paradas_500m":4.0,"m2_zonas_verdes_cercanas":1200.0,
}


async def construir_features(zona_id: str, sector: str) -> np.ndarray:
    import asyncio
    vz, comp, precio, trans = await asyncio.gather(
        _vz(zona_id), _comp(zona_id, sector), _precio(zona_id), _trans(zona_id))
    return _build_array(vz, comp, precio, trans)


async def construir_features_batch(zona_ids: list[str], sector: str):
    import asyncio
    vzs, comps, precios, transs = await asyncio.gather(
        _vzs(zona_ids), _comps(zona_ids,sector), _precios(zona_ids), _transs(zona_ids))
    rows = []
    for zid in zona_ids:
        arr = _build_array(vzs.get(zid,{}), comps.get(zid,{}),
                           precios.get(zid), transs.get(zid,{}))
        rows.append(arr[0])
    return np.array(rows, dtype=np.float32), zona_ids


def _build_array(vz, comp, precio, trans) -> np.ndarray:
    # Distinguir None (sin dato → imputa) de 0 (flujo real cero → score 0)
    _flujo_raw = vz.get("flujo_peatonal_total")
    total = _flujo_raw if _flujo_raw is not None else 0
    raw = {
        "flujo_peatonal_total": _flujo_raw,
        "flujo_manana_pct": (vz.get("flujo_peatonal_manana") or 0)/total if total else None,
        "flujo_tarde_pct":  (vz.get("flujo_peatonal_tarde")  or 0)/total if total else None,
        "flujo_noche_pct":  (vz.get("flujo_peatonal_noche")  or 0)/total if total else None,
        "renta_media_hogar": vz.get("renta_media_hogar"),
        "edad_media": vz.get("edad_media"),
        "pct_extranjeros": vz.get("pct_extranjeros"),
        "densidad_hab_km2": vz.get("densidad_hab_km2"),
        "num_competidores_300m": comp.get("num_competidores"),
        "rating_medio_competidores": comp.get("rating_medio"),
        "score_saturacion": comp.get("score_saturacion"),
        "precio_m2_alquiler": precio,
        "pct_locales_vacios": vz.get("pct_locales_vacios"),
        "tasa_rotacion_anual": vz.get("tasa_rotacion_anual"),
        "score_turismo": vz.get("score_turismo"),
        "incidencias_por_1000hab": vz.get("incidencias_por_1000hab"),
        "nivel_ruido_db": vz.get("nivel_ruido_db"),
        "score_equipamientos": vz.get("score_equipamientos"),
        "num_lineas_transporte": trans.get("num_lineas"),
        "num_paradas_500m": trans.get("num_paradas"),
        "m2_zonas_verdes_cercanas": vz.get("m2_zonas_verdes_cercanas"),
    }
    vec = [float(raw.get(f) if raw.get(f) is not None else _MEDIAS[f]) for f in FEATURE_NAMES]
    return np.array([vec], dtype=np.float32)


async def _vz(zid):
    async with get_db() as conn:
        r = await conn.fetchrow("SELECT * FROM variables_zona WHERE zona_id=$1 ORDER BY fecha DESC LIMIT 1", zid)
    return dict(r) if r else {}

async def _vzs(zids):
    async with get_db() as conn:
        rows = await conn.fetch("SELECT DISTINCT ON(zona_id) * FROM variables_zona WHERE zona_id=ANY($1) ORDER BY zona_id,fecha DESC", zids)
    return {r["zona_id"]: dict(r) for r in rows}

async def _comp(zid, sector):
    async with get_db() as conn:
        r = await conn.fetchrow("SELECT num_competidores,rating_medio,score_saturacion FROM competencia_por_local WHERE zona_id=$1 AND sector_codigo=$2 AND radio_m=300 ORDER BY fecha_calculo DESC LIMIT 1", zid, sector)
    return dict(r) if r else {}

async def _comps(zids, sector):
    async with get_db() as conn:
        rows = await conn.fetch("SELECT DISTINCT ON(zona_id) zona_id,num_competidores,rating_medio,score_saturacion FROM competencia_por_local WHERE zona_id=ANY($1) AND sector_codigo=$2 AND radio_m=300 ORDER BY zona_id,fecha_calculo DESC", zids, sector)
    return {r["zona_id"]: dict(r) for r in rows}

async def _precio(zid):
    async with get_db() as conn:
        v = await conn.fetchval("SELECT precio_m2 FROM precios_alquiler_zona WHERE zona_id=$1 ORDER BY fecha DESC LIMIT 1", zid)
    return float(v) if v else None

async def _precios(zids):
    async with get_db() as conn:
        rows = await conn.fetch("SELECT DISTINCT ON(zona_id) zona_id,precio_m2 FROM precios_alquiler_zona WHERE zona_id=ANY($1) ORDER BY zona_id,fecha DESC", zids)
    return {r["zona_id"]: float(r["precio_m2"]) for r in rows}

async def _trans(zid):
    async with get_db() as conn:
        r = await conn.fetchrow("""
            SELECT COUNT(DISTINCT pl.linea_id)::int AS num_lineas, COUNT(DISTINCT pt.id)::int AS num_paradas
            FROM paradas_transporte pt JOIN paradas_lineas pl ON pl.parada_id=pt.id
            JOIN zonas z ON z.id=$1
            WHERE ST_DWithin(pt.geometria::geography, z.geometria::geography, 500)
        """, zid)
    return {"num_lineas": r["num_lineas"] or 0, "num_paradas": r["num_paradas"] or 0} if r else {}

async def _transs(zids):
    import asyncio
    results = await asyncio.gather(*[_trans(z) for z in zids])
    return dict(zip(zids, results))
