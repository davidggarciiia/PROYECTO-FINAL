"""
pipelines/hut.py — Actualización de score_turismo_hut desde Habitatges d'Ús Turístic (HUT).

Fuente:
  CSV local: /data/csv/turisme/hut_apartaments_turistics.csv
  10.725 licencias de apartamentos turísticos con coordenadas.

Columnas clave:
  LONGITUD_X, LATITUD_Y — coordenadas (formato español: coma como decimal)
  NUMERO_PLACES          — número de plazas del apartamento
  NOM_DISTRICTE, NOM_BARRI — ubicación administrativa

Metodología:
  Para cada zona, se cuenta el número de apartamentos turísticos dentro de
  un radio de 500m desde el centroide de la zona.

  La densidad turística (aptmtos/km²) se normaliza a [0,100]:
    score_turismo_hut = 100 × (count_zona - min) / (max - min)
  Zonas sin ningún HUT en radio obtienen score_turismo_hut = 0.

  El score se guarda en vz_turismo (tabla satélite post-split mig 017).
  La columna score_turismo_hut existe a partir de la mig 029.

Tabla destino: vz_turismo.score_turismo_hut
"""
from __future__ import annotations

import csv
import logging
import os
from datetime import date
from pathlib import Path
from typing import Optional

from db.conexion import get_db

logger = logging.getLogger(__name__)

# ── Configuración ──────────────────────────────────────────────────────────────
_CSV_DIR  = Path(os.environ.get("CSV_DIR", "/data/csv"))
_CSV_HUT  = _CSV_DIR / "turisme" / "hut_apartaments_turistics.csv"

_RADIO_M  = 500   # Radio de búsqueda de apartamentos turísticos por zona


# ═══════════════════════════════════════════════════════════════════════════════
# Punto de entrada
# ═══════════════════════════════════════════════════════════════════════════════

async def ejecutar() -> dict:
    eid = await _init("hut")
    try:
        # 1. Leer coordenadas del CSV
        hut_coords = _leer_csv_hut()
        if not hut_coords:
            msg = f"CSV HUT vacío o no encontrado: {_CSV_HUT}"
            logger.warning(msg)
            await _fin(eid, 0, "ok", msg)
            return {"zonas_actualizadas": 0}

        logger.info("HUT CSV: %d apartamentos cargados", len(hut_coords))

        # 2. Calcular densidad HUT por zona
        fecha_ref = date.today()
        counts_por_zona = await _calcular_densidad_zonas(hut_coords)
        logger.info("Zonas con HUT cercanos: %d", len(counts_por_zona))

        # 3. Normalizar → score_turismo [0,100]
        scores = _normalizar_scores(counts_por_zona)

        # 4. Actualizar variables_zona
        actualizadas = await _actualizar_scores(scores, fecha_ref)

        await _fin(eid, actualizadas, "ok")
        logger.info("HUT OK — %d zonas actualizadas", actualizadas)
        return {"zonas_actualizadas": actualizadas}

    except Exception as exc:
        logger.error("Pipeline HUT ERROR: %s", exc, exc_info=True)
        await _fin(eid, 0, "error", str(exc))
        raise


# ═══════════════════════════════════════════════════════════════════════════════
# 1. Lectura CSV
# ═══════════════════════════════════════════════════════════════════════════════

def _leer_csv_hut() -> list[tuple[float, float]]:
    """
    Lee el CSV de apartamentos turísticos y devuelve lista de (lng, lat).
    El CSV usa coma como separador decimal (locale español) → reemplazar por punto.
    """
    if not _CSV_HUT.exists():
        logger.warning("CSV HUT no encontrado: %s", _CSV_HUT)
        return []

    coords: list[tuple[float, float]] = []
    with open(_CSV_HUT, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                # Formato: "2,17017206787341" → 2.17017206787341
                lng_raw = str(row.get("LONGITUD_X", "") or "").strip().strip('"').replace(",", ".")
                lat_raw = str(row.get("LATITUD_Y", "") or "").strip().strip('"').replace(",", ".")
                lng = float(lng_raw)
                lat = float(lat_raw)
                if lng is not None and lat is not None and (lng != 0.0 or lat != 0.0):
                    coords.append((lng, lat))
            except (ValueError, TypeError):
                continue

    return coords


# ═══════════════════════════════════════════════════════════════════════════════
# 2. Calcular densidad HUT por zona
# ═══════════════════════════════════════════════════════════════════════════════

async def _calcular_densidad_zonas(
    hut_coords: list[tuple[float, float]],
) -> dict[str, int]:
    """
    Para cada zona, cuenta cuántos apartamentos HUT hay dentro del radio.

    Estrategia: crear una tabla temporal con los puntos HUT y hacer un
    ST_DWithin en bloque con todas las zonas. Mucho más eficiente que
    iterar zona a zona.
    """
    if not hut_coords:
        return {}

    # Separar listas de coordenadas para unnest
    lngs = [c[0] for c in hut_coords]
    lats = [c[1] for c in hut_coords]

    async with get_db() as conn:
        # Usar unnest en lugar de tabla temporal para evitar race conditions
        # en el pool de conexiones (TEMP TABLE es por-sesión pero el pool
        # puede reutilizar la misma sesión desde otra tarea concurrente).
        rows = await conn.fetch(
            f"""
            WITH hut_points AS (
                SELECT ST_SetSRID(ST_MakePoint(lng, lat), 4326)::geography AS geom
                FROM unnest($1::float[], $2::float[]) AS t(lng, lat)
            )
            SELECT z.id AS zona_id, COUNT(h.*) AS hut_count
            FROM zonas z
            CROSS JOIN hut_points h
            WHERE ST_DWithin(z.geometria::geography, h.geom, {_RADIO_M})
            GROUP BY z.id
            """,
            lngs, lats,
        )

    return {row["zona_id"]: int(row["hut_count"]) for row in rows}


# ═══════════════════════════════════════════════════════════════════════════════
# 3. Normalización
# ═══════════════════════════════════════════════════════════════════════════════

def _normalizar_scores(counts: dict[str, int]) -> dict[str, float]:
    """
    Normaliza los conteos a [0,100].
    Las zonas con count=0 (no en el dict) obtienen 0.0.
    """
    if not counts:
        return {}
    max_count = max(counts.values())
    if max_count == 0:
        return {zona_id: 0.0 for zona_id in counts}
    return {
        zona_id: round(100.0 * count / max_count, 2)
        for zona_id, count in counts.items()
    }


# ═══════════════════════════════════════════════════════════════════════════════
# 4. Actualizar variables_zona
# ═══════════════════════════════════════════════════════════════════════════════

async def _actualizar_scores(
    scores: dict[str, float], fecha: date
) -> int:
    """
    Actualiza score_turismo_hut en vz_turismo para todas las zonas.
    - Las zonas con HUT cercanos reciben el score calculado.
    - Las zonas sin HUT a menos de 500m reciben score_turismo_hut = 0.0.
    Hace UPSERT sobre (zona_id, fecha) tanto en variables_zona (anchor)
    como en vz_turismo (datos), siguiendo el patrón post-mig 017.
    """
    async with get_db() as conn:
        todas_zonas = await conn.fetch("SELECT id FROM zonas")

    n = 0
    async with get_db() as conn:
        for zona_row in todas_zonas:
            zona_id = zona_row["id"]
            score = scores.get(zona_id, 0.0)
            # Anchor en variables_zona (tabla coordinadora delgada)
            await conn.execute(
                """
                INSERT INTO variables_zona (zona_id, fecha, fuente)
                VALUES ($1, $2, 'hut_bcn')
                ON CONFLICT (zona_id, fecha) DO UPDATE
                SET fuente = EXCLUDED.fuente, updated_at = NOW()
                """,
                zona_id, fecha,
            )
            # Score HUT en tabla satélite (mig 029: split writers)
            await conn.execute(
                """
                INSERT INTO vz_turismo
                    (zona_id, fecha, score_turismo_hut, fuente)
                VALUES ($1, $2, $3, 'hut_bcn')
                ON CONFLICT (zona_id, fecha) DO UPDATE
                SET score_turismo_hut = EXCLUDED.score_turismo_hut,
                    fuente            = EXCLUDED.fuente,
                    updated_at        = NOW()
                """,
                zona_id, fecha, score,
            )
            n += 1

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
