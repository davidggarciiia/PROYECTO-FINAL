"""
pipelines/turismo/landmarks.py — Ingesta de landmarks turisticos reales.

Motivacion:
  eventos_culturales_500m (poblado por pipelines/entorno/*) mezcla atracciones
  reales (Sagrada Familia, Park Guell) con ruido (bibliotecas de barrio,
  miradores menores). Esta senal era demasiado ruidosa para el scorer.

  Este pipeline carga solo landmarks "reales" usando el filtro de calidad
  tourism=attraction + wikidata=*: un POI que alguien se tomo la molestia de
  enlazar a Wikidata ya cumple un umbral minimo de relevancia.

Fuente:
  Overpass API (OpenStreetMap) — query al bbox de BCN.
  3 mirrors con fallback (mismo patron que pipelines/turismo/booking.py).

Qué actualiza:
  1. Tabla landmarks_turisticos (UPSERT por osm_id).
  2. vz_turismo.dist_landmark_top3_m por zona (distancia media ponderada a los
     3 landmarks mas cercanos, ponderada por peso_turistico).

Peso turistico (peso_turistico):
  Por defecto 1.0. Ruta de mejora documentada pero NO implementada en esta
  primera pasada:
    - Enriquecer con Wikipedia pageviews mensuales via:
      https://en.wikipedia.org/api/rest_v1/metrics/pageviews/per-article/
        en.wikipedia/all-access/all-agents/<article>/monthly/YYYYMM01/YYYYMM01
      Pasos:
        1. Resolver wikidata_id -> enwiki article slug (SPARQL Wikidata o
           wbgetentities API).
        2. Llamar pageviews API por articulo y mes reciente.
        3. Normalizar log(pageviews) a [0.5, 3.0] y asignar a peso_turistico.
    - Alternativa liviana: usar el conteo de claims en Wikidata (itemprop)
      como proxy de relevancia.

Uso:
  python -m pipelines.turismo.landmarks
"""
from __future__ import annotations

import asyncio
import logging
from datetime import date
from typing import Optional

import httpx

from db.conexion import get_db

logger = logging.getLogger(__name__)

# ── Overpass ──────────────────────────────────────────────────────────────────
_OVERPASS_URLS = [
    "https://overpass-api.de/api/interpreter",
    "https://lz4.overpass-api.de/api/interpreter",
    "https://z.overpass-api.de/api/interpreter",
]
_BCN_BBOX  = "41.32,2.07,41.47,2.23"
_TIMEOUT_S = 60


# ═══════════════════════════════════════════════════════════════════════════════
# Punto de entrada
# ═══════════════════════════════════════════════════════════════════════════════

async def ejecutar() -> dict:
    """
    Carga landmarks turisticos desde Overpass y calcula dist_landmark_top3_m por zona.

    Returns:
        dict con claves:
          landmarks_cargados:   int — filas upsertadas en landmarks_turisticos.
          landmarks_con_wikidata: int — filas con wikidata_id no nulo.
          zonas_actualizadas:   int — zonas con dist_landmark_top3_m calculado.
    """
    eid = await _init("landmarks")
    try:
        # 1. Overpass: nodos y ways con tourism=attraction AND wikidata=*
        items = await _overpass_landmarks()
        if not items:
            msg = "Overpass sin landmarks — todos los mirrors fallaron o sin resultados"
            logger.warning(msg)
            await _fin(eid, 0, "ok", msg)
            return {"landmarks_cargados": 0, "landmarks_con_wikidata": 0, "zonas_actualizadas": 0}

        logger.info("Overpass: %d landmarks con wikidata en bbox BCN", len(items))

        # 2. Persistir (UPSERT por osm_id)
        cargados, con_wd = await _persistir_landmarks(items)
        logger.info("Landmarks persistidos: %d (con wikidata_id: %d)", cargados, con_wd)

        # 3. Calcular dist_landmark_top3_m por zona
        zonas_actualizadas = await _actualizar_dist_landmark()
        logger.info("Zonas con dist_landmark_top3_m actualizada: %d", zonas_actualizadas)

        await _fin(eid, cargados, "ok")
        return {
            "landmarks_cargados":     cargados,
            "landmarks_con_wikidata": con_wd,
            "zonas_actualizadas":     zonas_actualizadas,
        }

    except Exception as exc:
        logger.error("Pipeline landmarks ERROR: %s", exc, exc_info=True)
        await _fin(eid, 0, "error", str(exc))
        raise


# ═══════════════════════════════════════════════════════════════════════════════
# 1. Overpass — tourism=attraction AND wikidata=*
# ═══════════════════════════════════════════════════════════════════════════════

async def _overpass_landmarks() -> list[dict]:
    """
    Consulta Overpass por nodos y ways con tourism=attraction + tag wikidata.

    Retry con los 3 mirrors de _OVERPASS_URLS. Si todos fallan, devuelve [].
    Usa `out center tags;` para obtener centroide de los ways sin descargar
    la geometria completa.
    """
    query = f"""
    [out:json][timeout:{_TIMEOUT_S}];
    (
      node["tourism"="attraction"]["wikidata"]({_BCN_BBOX});
      way["tourism"="attraction"]["wikidata"]({_BCN_BBOX});
    );
    out center tags;
    """
    data: Optional[dict] = None
    async with httpx.AsyncClient(timeout=_TIMEOUT_S + 10) as client:
        for url in _OVERPASS_URLS:
            try:
                resp = await client.post(url, data={"data": query})
                if resp.status_code != 200:
                    logger.warning("Overpass %s HTTP %d — probando siguiente mirror", url, resp.status_code)
                    continue
                data = resp.json()
                logger.info("Overpass OK en %s (%d elementos)", url, len(data.get("elements", [])))
                break
            except Exception as exc:  # noqa: BLE001
                logger.warning("Overpass %s fallo: %s — probando siguiente mirror", url, exc)
                continue

    if not data:
        return []

    resultados: list[dict] = []
    for elem in data.get("elements", []):
        tags = elem.get("tags", {})

        # Coordenadas: nodes tienen lat/lon; ways tienen center (por `out center`)
        if elem.get("type") == "node":
            lat = _safe_float(elem.get("lat"))
            lng = _safe_float(elem.get("lon"))
        else:
            center = elem.get("center") or {}
            lat = _safe_float(center.get("lat"))
            lng = _safe_float(center.get("lon"))

        if lat is None or lng is None:
            continue

        osm_id_raw = elem.get("id")
        try:
            osm_id = int(osm_id_raw)
        except (TypeError, ValueError):
            continue

        # Nombre: preferir name (catalan/autoreferenciable), luego es/en
        nombre = (
            tags.get("name") or tags.get("name:ca") or
            tags.get("name:es") or tags.get("name:en") or ""
        ).strip()
        if not nombre:
            continue

        wikidata_id = (tags.get("wikidata") or "").strip() or None
        # Normalizar: "Q9188"
        if wikidata_id and not wikidata_id.upper().startswith("Q"):
            wikidata_id = None

        resultados.append({
            "osm_id":         osm_id,
            "nombre":         nombre[:200],
            "wikidata_id":    (wikidata_id[:20] if wikidata_id else None),
            "peso_turistico": 1.0,   # mejora futura: Wikipedia pageviews
            "lat":            lat,
            "lng":            lng,
        })

    return resultados


def _safe_float(v) -> Optional[float]:
    try:
        if v is None:
            return None
        f = float(v)
        return f if f == f else None  # NaN check
    except (TypeError, ValueError):
        return None


# ═══════════════════════════════════════════════════════════════════════════════
# 2. Persistencia
# ═══════════════════════════════════════════════════════════════════════════════

async def _persistir_landmarks(items: list[dict]) -> tuple[int, int]:
    """UPSERT por osm_id. Devuelve (cargados, con_wikidata)."""
    if not items:
        return 0, 0

    cargados = 0
    con_wd = 0
    async with get_db() as conn:
        for it in items:
            try:
                await conn.execute(
                    """
                    INSERT INTO landmarks_turisticos
                        (osm_id, nombre, wikidata_id, peso_turistico, geom)
                    VALUES
                        ($1, $2, $3, $4,
                         ST_SetSRID(ST_MakePoint($5, $6), 4326))
                    ON CONFLICT (osm_id) DO UPDATE
                    SET nombre         = EXCLUDED.nombre,
                        wikidata_id    = EXCLUDED.wikidata_id,
                        peso_turistico = EXCLUDED.peso_turistico,
                        geom           = EXCLUDED.geom
                    """,
                    it["osm_id"],
                    it["nombre"],
                    it["wikidata_id"],
                    it["peso_turistico"],
                    it["lng"], it["lat"],
                )
                cargados += 1
                if it["wikidata_id"]:
                    con_wd += 1
            except Exception as exc:  # noqa: BLE001
                logger.debug("Error insertando landmark osm_id=%s: %s", it.get("osm_id"), exc)

    return cargados, con_wd


# ═══════════════════════════════════════════════════════════════════════════════
# 3. Actualizacion de vz_turismo.dist_landmark_top3_m
# ═══════════════════════════════════════════════════════════════════════════════

async def _actualizar_dist_landmark() -> int:
    """
    Para cada zona, calcula la distancia media ponderada (por peso_turistico)
    en metros a los 3 landmarks mas cercanos desde el centroide de la zona.

    Formula: sum(d_i * w_i) / sum(w_i) con d_i en metros y w_i = peso_turistico,
    i en los 3 landmarks mas cercanos.

    Si hay menos de 1 landmark en la BD, no se actualiza nada.
    Si hay <3 landmarks totales, se usan los que haya (1 o 2).
    """
    hoy = date.today()
    async with get_db() as conn:
        total_lm = await conn.fetchval("SELECT COUNT(*) FROM landmarks_turisticos")
        if not total_lm:
            logger.warning("No hay landmarks en BD — saltando calculo de dist_landmark_top3_m")
            return 0

        rows = await conn.fetch(
            """
            WITH top3 AS (
                SELECT
                    z.id AS zona_id,
                    l.peso_turistico,
                    ST_Distance(
                        ST_Centroid(z.geometria)::geography,
                        l.geom::geography
                    ) AS dist_m
                FROM zonas z
                CROSS JOIN LATERAL (
                    SELECT peso_turistico, geom
                    FROM landmarks_turisticos
                    ORDER BY ST_Centroid(z.geometria) <-> geom
                    LIMIT 3
                ) l
            )
            SELECT
                zona_id,
                -- media ponderada: sum(d*w) / sum(w)
                SUM(dist_m * peso_turistico) / NULLIF(SUM(peso_turistico), 0)
                    AS dist_pond
            FROM top3
            GROUP BY zona_id
            """
        )

        n = 0
        for row in rows:
            dist_pond = row["dist_pond"]
            if dist_pond is None:
                continue
            try:
                # Anchor en variables_zona
                await conn.execute(
                    """
                    INSERT INTO variables_zona (zona_id, fecha, fuente)
                    VALUES ($1, $2, 'landmarks_osm')
                    ON CONFLICT (zona_id, fecha) DO UPDATE
                    SET fuente = EXCLUDED.fuente, updated_at = NOW()
                    """,
                    row["zona_id"], hoy,
                )
                # Score en vz_turismo
                await conn.execute(
                    """
                    INSERT INTO vz_turismo
                        (zona_id, fecha, dist_landmark_top3_m, fuente)
                    VALUES ($1, $2, $3, 'landmarks_osm')
                    ON CONFLICT (zona_id, fecha) DO UPDATE
                    SET dist_landmark_top3_m = EXCLUDED.dist_landmark_top3_m,
                        fuente               = EXCLUDED.fuente,
                        updated_at           = NOW()
                    """,
                    row["zona_id"], hoy, round(float(dist_pond), 2),
                )
                n += 1
            except Exception as exc:  # noqa: BLE001
                logger.debug("Error actualizando dist_landmark_top3_m zona=%s: %s", row["zona_id"], exc)

    return n


# ═══════════════════════════════════════════════════════════════════════════════
# Helpers BD
# ═══════════════════════════════════════════════════════════════════════════════

async def _init(pipeline: str) -> int:
    async with get_db() as conn:
        return await conn.fetchval(
            "INSERT INTO pipeline_ejecuciones (pipeline, estado) "
            "VALUES ($1,'running') RETURNING id",
            pipeline,
        )


async def _fin(
    eid: int, registros: int, estado: str, mensaje: Optional[str] = None
) -> None:
    async with get_db() as conn:
        await conn.execute(
            "UPDATE pipeline_ejecuciones "
            "SET fecha_fin=NOW(), registros=$1, estado=$2, mensaje_error=$3 "
            "WHERE id=$4",
            registros, estado, mensaje, eid,
        )


# ═══════════════════════════════════════════════════════════════════════════════
# CLI entry
# ═══════════════════════════════════════════════════════════════════════════════

async def _main() -> None:
    from db.conexion import init_db_pool, close_db_pool
    await init_db_pool()
    try:
        resumen = await ejecutar()
    finally:
        await close_db_pool()
    print()
    print("Resumen pipeline landmarks:")
    for k, v in resumen.items():
        print(f"  {k:28s} -> {v}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    asyncio.run(_main())
