"""scoring/features.py — Vector de 30 features para XGBoost.

Cambio v2: añadidas dos features de granularidad geográfica de nivel zona:
  - dist_playa_m              → distancia PostGIS al litoral de Barcelona (metros)
  - ratio_locales_comerciales → % de locales comerciales en la zona (vs residencial)

Cambio v3: añadidas seis features de turismo y dinamismo comercial al final
(índices 23-28) para mantener compatibilidad de índices con modelos v2:
  - airbnb_density_500m       → listados Airbnb en radio 500m (proxy turismo informal)
  - airbnb_occupancy_est      → ocupación estimada Airbnb (Inside Airbnb)
  - google_review_count_medio → media de reseñas Google de competidores en 300m
  - licencias_nuevas_1a       → nuevas licencias de actividad último año en la zona
  - eventos_culturales_500m   → venues culturales / ocio en radio 500m
  - booking_hoteles_500m      → hoteles Booking en 500m (proxy turismo alojado)

Cambio v4 (flujo_peatonal_score): añadida la feature de fusión ponderada de fuentes
de flujo peatonal al final (índice 29). Calculada por scoring/flujo_peatonal.py a
partir de 4 fuentes con redistribución adaptativa de pesos si falta alguna:
  - flujo_popular_times_score  (35%) — Google Maps Popular Times
  - vcity_flujo_peatonal       (30%) — VCity BSC promedio diario
  - flujo_peatonal_total       (20%) — Sensores vianants BCN Open Data
  - ratio_locales_comerciales  (15%) — proxy estructural (siempre disponible)
flujo_peatonal_total se mantiene en FEATURE_NAMES para compatibilidad con modelos v1-v3.

NOTA: Los modelos entrenados con v1 (21 features), v2 (23 features) o v3 (29 features)
fallarán al recibir 30 features y caerán al scorer manual. Relanzar scoring/train.py
para entrenar en v4.
"""
from __future__ import annotations
import logging
from typing import Optional
import numpy as np

# Importación lazy para evitar dependencias transitivas (asyncpg, pydantic_settings)
# cuando el módulo se importa desde train_synthetic.py o tests sin BD.
def _get_db():  # pragma: no cover
    from db.conexion import get_db  # noqa: PLC0415
    return get_db

logger = logging.getLogger(__name__)

# v1 tenía 21 features; v2 añade dist_playa_m y ratio_locales_comerciales al final
# para mantener compatibilidad de índices con modelos guardados.
# v3 añade 6 features de turismo y dinamismo comercial al final (índices 23-28).
FEATURE_NAMES = [
    "flujo_peatonal_total","flujo_manana_pct","flujo_tarde_pct","flujo_noche_pct",
    "renta_media_hogar","edad_media","pct_extranjeros","densidad_hab_km2",
    "num_competidores_300m","rating_medio_competidores","score_saturacion",
    "precio_m2_alquiler","pct_locales_vacios","tasa_rotacion_anual",
    "score_turismo","incidencias_por_1000hab","nivel_ruido_db",
    "score_equipamientos","num_lineas_transporte","num_paradas_500m",
    "m2_zonas_verdes_cercanas",
    # ── v2: granularidad geográfica real a nivel zona ──────────────────────────
    "dist_playa_m",              # distancia al litoral BCN en metros (PostGIS)
    "ratio_locales_comerciales", # fracción 0-1 de locales comerciales en la zona
    # ── v3: turismo y dinamismo comercial ─────────────────────────────────────
    "airbnb_density_500m",        # densidad Airbnb en 500m
    "airbnb_occupancy_est",       # ocupación estimada Airbnb
    "google_review_count_medio",  # media de reseñas Google de competidores en 300m
    "licencias_nuevas_1a",        # nuevas licencias de actividad último año en la zona
    "eventos_culturales_500m",    # venues culturales/ocio en 500m
    "booking_hoteles_500m",       # hoteles Booking en 500m (proxy turismo alojado)
    # ── v4: fusión ponderada de fuentes de flujo peatonal ─────────────────────
    # Combina Popular Times (35%), VCity BSC (30%), vianants BCN (20%) y
    # ratio_locales (15%) con redistribución adaptativa si falta alguna fuente.
    # Calculado en _build_array() via scoring/flujo_peatonal.calcular_flujo_score().
    "flujo_peatonal_score",       # fusión 0-100 (v4)
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
    # v2
    "dist_playa_m": 3500.0,           # media BCN: ~3.5 km del mar para zona interior
    "ratio_locales_comerciales": 0.22, # ~22% de locales son comerciales en BCN
    # v3 — estimaciones BCN a partir de datos Inside Airbnb 2024 y Open Data BCN
    "airbnb_density_500m": 28.0,       # ~28 listados Airbnb en radio 500m (media BCN)
    "airbnb_occupancy_est": 0.62,      # ~62% ocupación media estimada BCN
    "google_review_count_medio": 145.0, # ~145 reseñas Google por negocio (media competidores)
    "licencias_nuevas_1a": 4.0,        # ~4 nuevas licencias por zona/año (media BCN)
    "eventos_culturales_500m": 3.0,    # ~3 venues culturales/ocio en radio 500m
    "booking_hoteles_500m": 2.0,       # ~2 hoteles Booking en radio 500m (media BCN)
    # v4 — fusión ponderada de fuentes de flujo peatonal (feature #30, índice 29)
    # Calculada en tiempo real por scoring/flujo_peatonal.calcular_flujo_score().
    # Media estimada BCN: ~45 pts (ponderación de las 4 fuentes con cobertura parcial).
    "flujo_peatonal_score": 45.0,      # ~45 pts media BCN
    # Inputs de la fusión v4 — usados por calcular_flujo_score(), no en FEATURE_NAMES:
    "flujo_popular_times_score": 48.0, # ~48 pts media BCN (escala 0-100)
    "vcity_flujo_peatonal": 18_000.0,  # ~18 000 peatones/día media BCN (raw, normalizar)
}


async def construir_features(zona_id: str, sector: str) -> np.ndarray:
    import asyncio
    vz, comp, precio, trans, geo, tur = await asyncio.gather(
        _vz(zona_id), _comp(zona_id, sector), _precio(zona_id), _trans(zona_id),
        _geo(zona_id), _turismo(zona_id))
    return _build_array(vz, comp, precio, trans, geo, tur)


async def construir_features_batch(zona_ids: list[str], sector: str):
    import asyncio
    vzs, comps, precios, transs, geos, turs = await asyncio.gather(
        _vzs(zona_ids), _comps(zona_ids, sector), _precios(zona_ids),
        _transs(zona_ids), _geos(zona_ids), _turismo_batch(zona_ids))
    rows = []
    for zid in zona_ids:
        arr = _build_array(vzs.get(zid, {}), comps.get(zid, {}),
                           precios.get(zid), transs.get(zid, {}),
                           geos.get(zid, {}), turs.get(zid, {}))
        rows.append(arr[0])
    return np.array(rows, dtype=np.float32), zona_ids


def _build_array(vz, comp, precio, trans, geo, tur) -> np.ndarray:
    from scoring.flujo_peatonal import calcular_flujo_score  # noqa: PLC0415

    # Distinguir None (sin dato → imputa) de 0 (flujo real cero → score 0)
    _flujo_raw = vz.get("flujo_peatonal_total")
    total = _flujo_raw if _flujo_raw is not None else 0

    # v4: fusión ponderada de las 4 fuentes de flujo peatonal
    _flujo_score = calcular_flujo_score(
        popular_times_score=vz.get("flujo_popular_times_score"),
        vcity_flujo=vz.get("vcity_flujo_peatonal"),
        vianants_intensitat=_flujo_raw,
        ratio_locales=vz.get("ratio_locales_comerciales"),
    )

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
        # v2: granularidad geográfica de nivel zona
        "dist_playa_m": geo.get("dist_playa_m"),
        "ratio_locales_comerciales": vz.get("ratio_locales_comerciales"),
        # v3: turismo y dinamismo comercial
        "airbnb_density_500m":       tur.get("airbnb_density_500m"),
        "airbnb_occupancy_est":      tur.get("airbnb_occupancy_est"),
        "google_review_count_medio": tur.get("google_review_count_medio"),
        "licencias_nuevas_1a":       tur.get("licencias_nuevas_1a"),
        "eventos_culturales_500m":   tur.get("eventos_culturales_500m"),
        "booking_hoteles_500m":      tur.get("booking_hoteles_500m"),
        # v4: fusión ponderada de flujo peatonal (Popular Times 35% + VCity 30%
        #     + vianants BCN 20% + ratio_locales 15%); pesos adaptativos si falta
        #     alguna fuente. Ver scoring/flujo_peatonal.py para detalles.
        "flujo_peatonal_score":      _flujo_score,
    }
    vec = [float(raw.get(f) if raw.get(f) is not None else _MEDIAS[f]) for f in FEATURE_NAMES]
    return np.array([vec], dtype=np.float32)


async def _vz(zid):
    async with _get_db()() as conn:
        r = await conn.fetchrow("SELECT * FROM v_variables_zona WHERE zona_id=$1 ORDER BY fecha DESC NULLS LAST LIMIT 1", zid)
    return dict(r) if r else {}

async def _vzs(zids):
    async with _get_db()() as conn:
        rows = await conn.fetch("SELECT DISTINCT ON(zona_id) * FROM v_variables_zona WHERE zona_id=ANY($1) ORDER BY zona_id,fecha DESC NULLS LAST", zids)
    return {r["zona_id"]: dict(r) for r in rows}

async def _comp(zid, sector):
    async with _get_db()() as conn:
        r = await conn.fetchrow("SELECT num_competidores,rating_medio,score_saturacion FROM competencia_por_local WHERE zona_id=$1 AND sector_codigo=$2 AND radio_m=300 ORDER BY fecha_calculo DESC LIMIT 1", zid, sector)
    return dict(r) if r else {}

async def _comps(zids, sector):
    async with _get_db()() as conn:
        rows = await conn.fetch("SELECT DISTINCT ON(zona_id) zona_id,num_competidores,rating_medio,score_saturacion FROM competencia_por_local WHERE zona_id=ANY($1) AND sector_codigo=$2 AND radio_m=300 ORDER BY zona_id,fecha_calculo DESC", zids, sector)
    return {r["zona_id"]: dict(r) for r in rows}

async def _precio(zid):
    async with _get_db()() as conn:
        v = await conn.fetchval("SELECT precio_m2 FROM precios_alquiler_zona WHERE zona_id=$1 ORDER BY fecha DESC LIMIT 1", zid)
    return float(v) if v else None

async def _precios(zids):
    async with _get_db()() as conn:
        rows = await conn.fetch("SELECT DISTINCT ON(zona_id) zona_id,precio_m2 FROM precios_alquiler_zona WHERE zona_id=ANY($1) ORDER BY zona_id,fecha DESC", zids)
    return {r["zona_id"]: float(r["precio_m2"]) for r in rows}

async def _trans(zid):
    async with _get_db()() as conn:
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


# ── Queries geográficas de nivel zona (v2) ─────────────────────────────────────
# Usamos el centroide de la zona para calcular distancias absolutas.
# El litoral de Barcelona corre de Barceloneta (~41.379, 2.192) a Diagonal Mar.
# Aproximamos la línea de costa como una serie de puntos del frente marítimo.
_LITORAL_BCN_WKT = (
    "LINESTRING("
    "2.1850 41.3740,"   # Passeig Marítim sud
    "2.1940 41.3792,"   # Barceloneta
    "2.2030 41.3840,"   # Vila Olímpica
    "2.2130 41.3900,"   # Poblenou
    "2.2250 41.3970,"   # Diagonal Mar
    "2.2380 41.4020"    # El Besòs
    ")"
)


async def _geo(zid: str) -> dict:
    """Distancia al litoral BCN y demás features geográficos de nivel zona."""
    async with _get_db()() as conn:
        r = await conn.fetchrow(
            """
            SELECT
                ST_Distance(
                    ST_Centroid(z.geometria)::geography,
                    ST_GeomFromText($2, 4326)::geography
                )::int AS dist_playa_m
            FROM zonas z
            WHERE z.id = $1
            """,
            zid, _LITORAL_BCN_WKT,
        )
    return {"dist_playa_m": r["dist_playa_m"]} if r else {}


async def _geos(zids: list[str]) -> dict:
    """Batch de features geográficos para múltiples zonas."""
    async with _get_db()() as conn:
        rows = await conn.fetch(
            """
            SELECT
                z.id AS zona_id,
                ST_Distance(
                    ST_Centroid(z.geometria)::geography,
                    ST_GeomFromText($2, 4326)::geography
                )::int AS dist_playa_m
            FROM zonas z
            WHERE z.id = ANY($1)
            """,
            zids, _LITORAL_BCN_WKT,
        )
    return {r["zona_id"]: {"dist_playa_m": r["dist_playa_m"]} for r in rows}


# ── Queries de turismo y dinamismo comercial (v3) ─────────────────────────────
# Lee directamente de las nuevas columnas de variables_zona añadidas en la
# migración 006_nuevos_datos.sql, más un join a negocios_activos para la media
# de reseñas Google de los competidores en 300m.

async def _turismo(zona_id: str) -> dict:
    """Features de turismo y dinamismo comercial para una zona (v3).

    Columnas leídas de variables_zona:
      - airbnb_density_500m     → airbnb_density_500m
      - airbnb_occupancy_est    → airbnb_occupancy_est
      - licencias_nuevas_1a     → licencias_nuevas_1a
      - eventos_culturales_500m → eventos_culturales_500m
      - booking_hoteles_500m    → booking_hoteles_500m

    Calculado en tiempo real:
      - google_review_count_medio → AVG(review_count) de negocios_activos en 300m
    """
    async with _get_db()() as conn:
        vz_row = await conn.fetchrow(
            """
            SELECT
                airbnb_density_500m,
                airbnb_occupancy_est,
                licencias_nuevas_1a,
                eventos_culturales_500m,
                booking_hoteles_500m
            FROM v_variables_zona
            WHERE zona_id = $1
            ORDER BY fecha DESC NULLS LAST
            LIMIT 1
            """,
            zona_id,
        )
        reviews_avg = await conn.fetchval(
            """
            SELECT AVG(na.review_count)::float
            FROM negocios_activos na
            JOIN zonas z ON z.id = $1
            WHERE na.activo = TRUE
              AND ST_DWithin(na.geometria::geography, z.geometria::geography, 300)
              AND na.review_count > 0
            """,
            zona_id,
        )

    result: dict = {}
    if vz_row:
        result["airbnb_density_500m"]   = vz_row["airbnb_density_500m"]
        result["airbnb_occupancy_est"]  = vz_row["airbnb_occupancy_est"]
        result["licencias_nuevas_1a"]   = vz_row["licencias_nuevas_1a"]
        result["eventos_culturales_500m"] = vz_row["eventos_culturales_500m"]
        result["booking_hoteles_500m"]  = vz_row["booking_hoteles_500m"]
    result["google_review_count_medio"] = reviews_avg
    return result


async def _turismo_batch(zona_ids: list[str]) -> dict:
    """Batch de features de turismo y dinamismo comercial para múltiples zonas (v3)."""
    async with _get_db()() as conn:
        vz_rows = await conn.fetch(
            """
            SELECT DISTINCT ON (zona_id)
                zona_id,
                airbnb_density_500m,
                airbnb_occupancy_est,
                licencias_nuevas_1a,
                eventos_culturales_500m,
                booking_hoteles_500m
            FROM v_variables_zona
            WHERE zona_id = ANY($1)
            ORDER BY zona_id, fecha DESC NULLS LAST
            """,
            zona_ids,
        )
        reviews_rows = await conn.fetch(
            """
            SELECT
                z.id AS zona_id,
                AVG(na.review_count)::float AS avg_reviews
            FROM zonas z
            JOIN negocios_activos na
                ON na.activo = TRUE
               AND ST_DWithin(na.geometria::geography, z.geometria::geography, 300)
               AND na.review_count > 0
            WHERE z.id = ANY($1)
            GROUP BY z.id
            """,
            zona_ids,
        )

    reviews_by_zone = {r["zona_id"]: r["avg_reviews"] for r in reviews_rows}

    result: dict = {}
    for row in vz_rows:
        zid = row["zona_id"]
        result[zid] = {
            "airbnb_density_500m":    row["airbnb_density_500m"],
            "airbnb_occupancy_est":   row["airbnb_occupancy_est"],
            "licencias_nuevas_1a":    row["licencias_nuevas_1a"],
            "eventos_culturales_500m": row["eventos_culturales_500m"],
            "booking_hoteles_500m":   row["booking_hoteles_500m"],
            "google_review_count_medio": reviews_by_zone.get(zid),
        }
    # Zonas sin fila en variables_zona: devolver dict vacío para que se impute
    for zid in zona_ids:
        if zid not in result:
            result[zid] = {"google_review_count_medio": reviews_by_zone.get(zid)}
    return result
