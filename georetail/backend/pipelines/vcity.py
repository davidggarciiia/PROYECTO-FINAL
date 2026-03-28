"""
pipelines/vcity.py — Flujo peatonal por zona desde VCity BSC (Martin tileserver).

Fuente: https://martin.vcity.dataviz.bsc.es
Capa:   barcelonapedestrians_100percentage_v2
Método: MVT tiles (Mapbox Vector Tiles) decodificados con mapbox-vector-tile

Para cada zona de GeoRetail:
  1. Obtener el centroide (lat, lng)
  2. Convertir a tile XY en zoom 15 (mercantile)
  3. Descargar tile MVT del Martin tileserver
  4. Decodificar protobuf → extraer features
  5. Calcular media de num_pedestrians de los segmentos en el tile
  6. Guardar en variables_zona.vcity_flujo_peatonal

Campos adicionales guardados en variables_zona (nuevas columnas):
  - vcity_tourist_rate
  - vcity_shopping_rate
  - vcity_resident_rate

Fallback: si el tileserver no responde → usar flujo_peatonal_total (vianants_bcn)

Frecuencia: mensual (día 12, 04:00)
"""
from __future__ import annotations

import logging
import math
from datetime import date
from typing import Optional

import httpx

from db.conexion import get_db

logger = logging.getLogger(__name__)

# ── VCity Martin tileserver ────────────────────────────────────────────────────
_VCITY_BASE    = "https://martin.vcity.dataviz.bsc.es"
_LAYER         = "barcelonapedestrians_100percentage_v2"
_ZOOM          = 15
_TIMEOUT_S     = 15.0
_MAX_CONN      = 5   # be gentle with the tileserver


# ═══════════════════════════════════════════════════════════════════════════════
# Punto de entrada público
# ═══════════════════════════════════════════════════════════════════════════════

async def ejecutar() -> dict:
    """
    Pipeline de flujo peatonal desde VCity BSC (Martin tileserver):
      1. Cargar zonas con centroide lat/lng desde BD
      2. Para cada zona: descargar tile MVT en zoom 15 → decodificar → agregar
      3. Persistir resultados en variables_zona
      4. Si el tileserver falla → fallback desde vianants_bcn
    """
    eid = await _init()
    try:
        zonas = await _get_zonas()
        if not zonas:
            logger.warning("vcity: no hay zonas con centroide definido en la BD")
            await _fin(eid, 0, "ok", "sin zonas con centroide")
            return {"zonas_actualizadas": 0, "fuente": "vcity_mvt", "zonas_fuente": 0}

        zona_data = await _fetch_vcity_data(zonas)

        if not zona_data:
            logger.warning(
                "vcity: tileserver no devolvió datos para ninguna zona — "
                "usando flujo_peatonal_total (vianants_bcn) como proxy"
            )
            n = await _fallback_desde_vianants()
            await _fin(eid, n, "ok", "tileserver sin datos — proxy vianants usado")
            return {
                "zonas_actualizadas": n,
                "fuente": "vianants_proxy",
                "zonas_fuente": 0,
            }

        n = await _persistir(zona_data)
        await _fin(eid, n, "ok")
        logger.info(
            "vcity OK — %d zonas con datos MVT, %d persistidas",
            len(zona_data), n,
        )
        return {
            "zonas_actualizadas": n,
            "fuente": "vcity_mvt",
            "zonas_fuente": len(zona_data),
        }

    except Exception as exc:
        logger.error("Pipeline vcity ERROR: %s", exc, exc_info=True)
        await _fin(eid, 0, "error", str(exc))
        raise


# ═══════════════════════════════════════════════════════════════════════════════
# 1. Tile coordinate conversion
# ═══════════════════════════════════════════════════════════════════════════════

def lat_lng_to_tile(lat: float, lng: float, zoom: int = 15) -> tuple[int, int]:
    """Convert lat/lng (WGS84) to tile x/y at given zoom level."""
    n = 2 ** zoom
    x = int((lng + 180.0) / 360.0 * n)
    lat_rad = math.radians(lat)
    y = int(
        (1.0 - math.log(math.tan(lat_rad) + 1.0 / math.cos(lat_rad)) / math.pi)
        / 2.0
        * n
    )
    return x, y


# ═══════════════════════════════════════════════════════════════════════════════
# 2. Fetch MVT tile from Martin tileserver
# ═══════════════════════════════════════════════════════════════════════════════

async def _fetch_tile(
    session: httpx.AsyncClient, x: int, y: int, zoom: int = 15
) -> bytes | None:
    """
    Download MVT tile bytes from the VCity Martin tileserver.
    Retries once on any failure before giving up.
    """
    url = f"{_VCITY_BASE}/{_LAYER}/{zoom}/{x}/{y}"
    for attempt in range(2):
        try:
            r = await session.get(url, timeout=_TIMEOUT_S)
            if r.status_code == 200 and r.content:
                return r.content
            if r.status_code == 204:
                # Empty tile — valid but no data
                return None
            logger.debug(
                "vcity tile %d/%d/%d → HTTP %d (attempt %d)",
                zoom, x, y, r.status_code, attempt + 1,
            )
        except Exception as exc:
            logger.debug(
                "vcity tile %d/%d/%d fetch error (attempt %d): %s",
                zoom, x, y, attempt + 1, exc,
            )
    return None


# ═══════════════════════════════════════════════════════════════════════════════
# 3. Decode MVT tile
# ═══════════════════════════════════════════════════════════════════════════════

def _decode_tile(tile_bytes: bytes) -> list[dict]:
    """
    Decode MVT (Mapbox Vector Tile) protobuf bytes into a list of feature
    property dicts.

    Primary decoder: mapbox-vector-tile>=2.1.0
    Fallback: raw protobuf parse using vector_tile_pb2 if available,
              or basic struct unpacking for the simplest fields.
    """
    if not tile_bytes:
        return []

    # ── Primary: mapbox_vector_tile ──────────────────────────────────────────
    try:
        import mapbox_vector_tile  # type: ignore

        tile = mapbox_vector_tile.decode(tile_bytes)
        features: list[dict] = []
        for layer_data in tile.values():
            for feature in layer_data.get("features", []):
                props = feature.get("properties", {})
                if props:
                    features.append(props)
        logger.debug("vcity decoded %d features via mapbox_vector_tile", len(features))
        return features
    except ImportError:
        logger.debug("mapbox_vector_tile not available — trying protobuf fallback")
    except Exception as exc:
        logger.debug("mapbox_vector_tile decode error: %s", exc)

    # ── Fallback: vector_tile protobuf (if vt_pb2 / mapbox proto stubs present)
    try:
        # Some environments ship the raw protobuf definition as vector_tile_pb2
        from vector_tile import vector_tile_pb2  # type: ignore

        vt = vector_tile_pb2.Tile()
        vt.ParseFromString(tile_bytes)
        features = []
        for layer in vt.layers:
            keys = list(layer.keys)
            for feat in layer.features:
                props: dict = {}
                for i in range(0, len(feat.tags), 2):
                    k = keys[feat.tags[i]]
                    val_idx = feat.tags[i + 1]
                    v = layer.values[val_idx]
                    if v.HasField("double_value"):
                        props[k] = v.double_value
                    elif v.HasField("float_value"):
                        props[k] = float(v.float_value)
                    elif v.HasField("int_value"):
                        props[k] = v.int_value
                    elif v.HasField("uint_value"):
                        props[k] = v.uint_value
                    elif v.HasField("sint_value"):
                        props[k] = v.sint_value
                    elif v.HasField("bool_value"):
                        props[k] = v.bool_value
                    elif v.HasField("string_value"):
                        props[k] = v.string_value
                if props:
                    features.append(props)
        logger.debug("vcity decoded %d features via vector_tile_pb2", len(features))
        return features
    except (ImportError, ModuleNotFoundError):
        logger.debug("vector_tile_pb2 not available")
    except Exception as exc:
        logger.debug("vector_tile_pb2 decode error: %s", exc)

    # ── Fallback 2: vt (python-vector-tile) ─────────────────────────────────
    try:
        import vt  # type: ignore

        tile_obj = vt.VectorTile(tile_bytes)
        features = []
        for layer in tile_obj.layers:
            for feat in layer.features:
                if feat.properties:
                    features.append(dict(feat.properties))
        if features:
            logger.debug("vcity decoded %d features via vt", len(features))
            return features
    except (ImportError, Exception):
        pass

    logger.warning(
        "vcity: no MVT decoder available — "
        "install mapbox-vector-tile>=2.1.0 (pip install mapbox-vector-tile)"
    )
    return []


# ═══════════════════════════════════════════════════════════════════════════════
# 4. Aggregate tile features into a single zone metric
# ═══════════════════════════════════════════════════════════════════════════════

def _mean_field(features: list[dict], field: str) -> float | None:
    """Return mean of `field` across features that have a non-None value."""
    vals = [
        float(f[field])
        for f in features
        if f.get(field) is not None
    ]
    if not vals:
        return None
    return sum(vals) / len(vals)


def _aggregate_tile_features(features: list[dict]) -> dict | None:
    """
    Aggregate pedestrian metrics from all features in a tile.

    Returns a dict with aggregated metrics, or None if there are no valid
    pedestrian counts.
    """
    pedestrians = [
        float(f["num_pedestrians"])
        for f in features
        if f.get("num_pedestrians") is not None and float(f["num_pedestrians"]) > 0
    ]
    if not pedestrians:
        return None

    return {
        "num_pedestrians": sum(pedestrians) / len(pedestrians),  # daily mean
        "tourist_rate": _mean_field(features, "tourist_rate"),
        "resident_rate": _mean_field(features, "resident_rate"),
        "shopping_rate": _mean_field(features, "shopping_and_leisure_rate"),
    }


# ═══════════════════════════════════════════════════════════════════════════════
# 5. Load zones from DB
# ═══════════════════════════════════════════════════════════════════════════════

async def _get_zonas() -> list[dict]:
    """
    Fetch all zones that have a defined centroid (lat/lng) from the database.

    Returns a list of dicts with: zona_id, nombre, lat, lng.
    """
    async with get_db() as conn:
        rows = await conn.fetch(
            """
            SELECT
                z.id        AS zona_id,
                z.nombre    AS nombre,
                ST_Y(ST_Centroid(z.geometria::geometry)) AS lat,
                ST_X(ST_Centroid(z.geometria::geometry)) AS lng
            FROM zonas z
            WHERE z.geometria IS NOT NULL
            ORDER BY z.nombre
            """
        )
    zonas = []
    for row in rows:
        lat = row["lat"]
        lng = row["lng"]
        if lat is None or lng is None:
            continue
        # Basic sanity check: Barcelona bounding box
        if not (41.0 < lat < 41.8 and 1.8 < lng < 2.5):
            logger.debug(
                "vcity: zona %s centroide fuera de Barcelona (%.4f, %.4f) — omitida",
                row["nombre"], lat, lng,
            )
            continue
        zonas.append({
            "zona_id": str(row["zona_id"]),
            "nombre": row["nombre"],
            "lat": float(lat),
            "lng": float(lng),
        })
    logger.info("vcity: %d zonas con centroide válido", len(zonas))
    return zonas


# ═══════════════════════════════════════════════════════════════════════════════
# 6. Main data-fetch loop: tile per zone
# ═══════════════════════════════════════════════════════════════════════════════

async def _fetch_vcity_data(zonas: list[dict]) -> dict[str, dict]:
    """
    For each zone, determine the tile coordinates, download the MVT tile from
    the VCity Martin tileserver, decode it, and aggregate the pedestrian metrics.

    Returns a dict mapping zona_id → aggregated metrics dict.
    """
    # Deduplicate tiles: multiple zones may share the same tile at zoom 15
    tile_to_zonas: dict[tuple[int, int], list[str]] = {}
    zona_to_tile: dict[str, tuple[int, int]] = {}

    for zona in zonas:
        tx, ty = lat_lng_to_tile(zona["lat"], zona["lng"], zoom=_ZOOM)
        key = (tx, ty)
        zona_to_tile[zona["zona_id"]] = key
        tile_to_zonas.setdefault(key, []).append(zona["zona_id"])

    logger.info(
        "vcity: %d zonas → %d tiles únicos en zoom %d",
        len(zonas), len(tile_to_zonas), _ZOOM,
    )

    # Fetch + decode each unique tile
    tile_cache: dict[tuple[int, int], dict | None] = {}

    limits = httpx.Limits(max_connections=_MAX_CONN, max_keepalive_connections=_MAX_CONN)
    async with httpx.AsyncClient(
        timeout=_TIMEOUT_S,
        limits=limits,
        headers={"User-Agent": "GeoRetail/1.0 (georetail.app - contacto: info@georetail.app)"},
    ) as session:
        for (tx, ty) in tile_to_zonas:
            tile_bytes = await _fetch_tile(session, tx, ty, zoom=_ZOOM)
            if tile_bytes is None:
                tile_cache[(tx, ty)] = None
                continue

            features = _decode_tile(tile_bytes)
            aggregated = _aggregate_tile_features(features)
            tile_cache[(tx, ty)] = aggregated

            if aggregated:
                logger.debug(
                    "vcity tile %d/%d/%d → %.0f ped/día (n=%d features)",
                    _ZOOM, tx, ty, aggregated["num_pedestrians"], len(features),
                )

    # Map results back to zones
    zona_data: dict[str, dict] = {}
    for zona in zonas:
        tile_key = zona_to_tile[zona["zona_id"]]
        result = tile_cache.get(tile_key)
        if result is not None:
            zona_data[zona["zona_id"]] = result

    tiles_ok = sum(1 for v in tile_cache.values() if v is not None)
    logger.info(
        "vcity fetch: %d/%d tiles con datos → %d zonas con métricas",
        tiles_ok, len(tile_to_zonas), len(zona_data),
    )
    return zona_data


# ═══════════════════════════════════════════════════════════════════════════════
# 7. Persist to variables_zona
# ═══════════════════════════════════════════════════════════════════════════════

async def _persistir(zona_data: dict[str, dict]) -> int:
    """
    UPSERT pedestrian metrics per zone into variables_zona.

    Writes: vcity_flujo_peatonal, vcity_tourist_rate, vcity_shopping_rate,
            vcity_resident_rate.

    Returns the number of rows successfully written.
    """
    if not zona_data:
        return 0

    fecha = date.today()
    n = 0

    async with get_db() as conn:
        for zona_id, metrics in zona_data.items():
            try:
                flujo = round(metrics["num_pedestrians"], 2)
                tourist  = metrics.get("tourist_rate")
                shopping = metrics.get("shopping_rate")
                resident = metrics.get("resident_rate")

                await conn.execute(
                    """
                    INSERT INTO variables_zona
                        (zona_id, fecha,
                         vcity_flujo_peatonal,
                         vcity_tourist_rate,
                         vcity_shopping_rate,
                         vcity_resident_rate)
                    VALUES ($1, $2, $3, $4, $5, $6)
                    ON CONFLICT (zona_id, fecha) DO UPDATE
                    SET vcity_flujo_peatonal = EXCLUDED.vcity_flujo_peatonal,
                        vcity_tourist_rate   = EXCLUDED.vcity_tourist_rate,
                        vcity_shopping_rate  = EXCLUDED.vcity_shopping_rate,
                        vcity_resident_rate  = EXCLUDED.vcity_resident_rate
                    """,
                    zona_id, fecha, flujo, tourist, shopping, resident,
                )
                n += 1
            except Exception as exc:
                logger.debug("vcity: error persistiendo zona %s: %s", zona_id, exc)

    logger.info("vcity: %d zonas persistidas con datos MVT", n)
    return n


# ═══════════════════════════════════════════════════════════════════════════════
# 8. Fallback: copy vianants flujo → vcity_flujo_peatonal
# ═══════════════════════════════════════════════════════════════════════════════

async def _fallback_desde_vianants() -> int:
    """
    Copia flujo_peatonal_total → vcity_flujo_peatonal con fuente='vianants_proxy'
    para las zonas que ya tienen datos de vianants.py.

    Devuelve el número de filas actualizadas.
    """
    async with get_db() as conn:
        await conn.execute(
            """
            UPDATE variables_zona
            SET vcity_flujo_peatonal = flujo_peatonal_total
            WHERE flujo_peatonal_total IS NOT NULL
              AND flujo_peatonal_total > 0
            """
        )
        n = await conn.fetchval(
            """
            SELECT COUNT(*) FROM variables_zona
            WHERE vcity_flujo_peatonal IS NOT NULL
            """
        )
    n = int(n or 0)
    logger.info("Proxy vianants → vcity_flujo_peatonal: %d zonas", n)
    return n


# ═══════════════════════════════════════════════════════════════════════════════
# Helpers BD — patrón estándar pipeline_ejecuciones
# ═══════════════════════════════════════════════════════════════════════════════

async def _init() -> int:
    """Registra inicio de ejecución en pipeline_ejecuciones. Devuelve eid."""
    async with get_db() as conn:
        return await conn.fetchval(
            "INSERT INTO pipeline_ejecuciones (pipeline, estado) "
            "VALUES ('vcity', 'running') RETURNING id"
        )


async def _fin(
    eid: int,
    registros: int,
    estado: str,
    mensaje: Optional[str] = None,
) -> None:
    """Registra fin de ejecución en pipeline_ejecuciones."""
    async with get_db() as conn:
        await conn.execute(
            "UPDATE pipeline_ejecuciones "
            "SET fecha_fin=NOW(), registros=$1, estado=$2, mensaje_error=$3 "
            "WHERE id=$4",
            registros, estado, mensaje, eid,
        )
