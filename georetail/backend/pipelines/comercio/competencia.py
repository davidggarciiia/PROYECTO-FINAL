"""
pipelines/competencia.py — Pipeline mensual de análisis de competencia v2.

Fuente principal: Google Maps Places API (New) — scraping de negocios activos
en radio 500m para cada zona × sector. Fallback: Foursquare → OSM.

Proceso:
  1. Para cada (zona, sector): scraping de negocios competidores Y complementarios
     via places_router.buscar_negocios_cercanos() con tipos cross-sector.
  2. Upsert en negocios_activos (actualiza rating, reseñas, precio_nivel).
  3. Consulta espacial PostGIS sobre negocios_activos (radio 500m).
  4. Calcula sub-scores con scoring.competencia.calcular_score_competencia().
  5. Upsert en competencia_detalle_zona.
  6. Actualiza competencia_por_local.score_saturacion para compatibilidad v1.
  7. Registra ejecución en pipeline_ejecuciones.

Diseño:
  - Respeta límites de API: pausa configurable entre zonas.
  - Retry HTTP: 3 intentos con backoff 1s.
  - Sin datos suficientes: score_competencia_v2 = 50.0 (neutro).
  - Radio fijo: 500m (configurable vía _RADIO_M).
"""
from __future__ import annotations

import asyncio
import logging
from datetime import date
from typing import Optional

import httpx

from db.conexion import get_db
from routers.places_router import buscar_negocios_cercanos
from scoring.dimensiones.competencia import (
    NegocioCompetidor,
    SECTORES_COMPETIDORES,
    SECTORES_COMPLEMENTARIOS,
    calcular_score_competencia,
    negocios_desde_rows,
)

logger = logging.getLogger(__name__)

# ── Configuración ──────────────────────────────────────────────────────────────

_RADIO_M       = 500          # radio de análisis en metros
_SECTORES      = ["restauracion", "moda", "estetica", "tatuajes", "shisha_lounge"]
_HTTP_RETRIES  = 3
_HTTP_RETRY_DELAY = 1.0       # segundos entre reintentos
_PAUSA_ENTRE_ZONAS = 0.3      # segundos para respetar límites API
_LIMITE_NEGOCIOS   = 40       # máx negocios por sector por zona vía API

# Mapeo sector → list[sector] a scrapear para cubrir competidores Y complementarios
# (buscar_negocios_cercanos sólo acepta un sector; llamamos varias veces)
_SECTORES_A_SCRAPEAR: dict[str, list[str]] = {
    s: list(set(SECTORES_COMPETIDORES.get(s, [s]) + SECTORES_COMPLEMENTARIOS.get(s, [])))
    for s in _SECTORES
}

_VALID_SECTORES = {
    "restauracion", "moda", "estetica", "tatuajes", "shisha_lounge",
    "supermercado", "farmacia", "electronica", "libreria", "sport",
}


# ── Punto de entrada ───────────────────────────────────────────────────────────

async def ejecutar() -> dict:
    """Ejecuta el pipeline completo de competencia. Llamado por scheduler."""
    eid = await _init()
    ok, errores = 0, 0

    try:
        zonas = await _get_zonas_activas()
        logger.info("Pipeline competencia: %d zonas × %d sectores", len(zonas), len(_SECTORES))

        for zona in zonas:
            for sector in _SECTORES:
                try:
                    n = await _procesar_zona_sector(zona, sector)
                    ok += n
                except Exception as e:
                    logger.warning(
                        "Competencia error zona=%s sector=%s: %s",
                        zona["zona_id"], sector, e
                    )
                    errores += 1
                await asyncio.sleep(_PAUSA_ENTRE_ZONAS)

        await _fin(eid, ok, "ok")
        return {"registros": ok, "errores": errores}

    except Exception as e:
        logger.error("Pipeline competencia error crítico: %s", e, exc_info=True)
        await _fin(eid, ok, "error", str(e))
        raise


# ── Lógica por zona × sector ──────────────────────────────────────────────────

async def _procesar_zona_sector(zona: dict, sector: str) -> int:
    """
    Para una zona+sector:
      1. Scraping de negocios relevantes (competidores + complementarios)
      2. Calcula score con scoring.competencia
      3. Persiste en competencia_detalle_zona
    Devuelve 1 si OK, 0 si no hay datos.
    """
    lat, lng = zona["lat"], zona["lng"]
    zona_id  = zona["zona_id"]

    # 1. Scrapear negocios relevantes (varios sectores en una sola zona)
    sectores_scrapear = _SECTORES_A_SCRAPEAR.get(sector, [sector])
    for sc in sectores_scrapear:
        await _scrapear_y_upsert(lat, lng, sc, zona_id)

    # 2. Leer negocios_activos en radio 500m desde BD (ya actualizados)
    rows = await _get_negocios_radio(zona_id, _RADIO_M)
    if not rows:
        await _upsert_score_neutro(zona_id, sector)
        return 0

    # 3. Calcular score
    negocios = negocios_desde_rows(rows, sector)
    resultado = calcular_score_competencia(negocios, sector)

    # 4. Persistir
    await _upsert_competencia_detalle(zona_id, sector, resultado)
    await _actualizar_competencia_por_local(zona_id, sector, resultado)
    return 1


# ── Scraping ───────────────────────────────────────────────────────────────────

async def _scrapear_y_upsert(
    lat: float, lng: float, sector: str, zona_id: str
) -> None:
    """
    Llama a places_router con retry y hace upsert en negocios_activos.
    No lanza excepción — un fallo de API no detiene el pipeline.
    """
    negocios = []
    last_err: Optional[Exception] = None

    for attempt in range(_HTTP_RETRIES):
        try:
            negocios = await buscar_negocios_cercanos(
                lat=lat, lng=lng,
                sector=sector,
                radio_m=_RADIO_M,
                limite=_LIMITE_NEGOCIOS,
            )
            break
        except Exception as e:
            last_err = e
            if attempt < _HTTP_RETRIES - 1:
                await asyncio.sleep(_HTTP_RETRY_DELAY)

    if not negocios and last_err:
        logger.debug("Scraping %s lat=%.4f lng=%.4f: %s", sector, lat, lng, last_err)
        return

    if negocios:
        await _upsert_negocios(negocios, zona_id)


async def _upsert_negocios(negocios: list[dict], zona_id: str) -> None:
    """Inserta o actualiza negocios en negocios_activos."""
    async with get_db() as conn:
        for n in negocios:
            if not n.get("lat") or not n.get("lng"):
                continue
            sc = n.get("sector_codigo")
            sc = sc if sc in _VALID_SECTORES else None
            await conn.execute("""
                INSERT INTO negocios_activos
                    (id, nombre, sector_codigo, lat, lng,
                     geometria, zona_id, rating, num_resenas,
                     precio_nivel, horario, activo, fuente, updated_at)
                VALUES ($1,$2,$3,$4,$5,
                        ST_SetSRID(ST_MakePoint($5,$4),4326),
                        $6,$7,$8,$9,$10,TRUE,$11,NOW())
                ON CONFLICT (id) DO UPDATE SET
                    rating       = COALESCE(EXCLUDED.rating, negocios_activos.rating),
                    num_resenas  = COALESCE(EXCLUDED.num_resenas, negocios_activos.num_resenas),
                    precio_nivel = COALESCE(EXCLUDED.precio_nivel, negocios_activos.precio_nivel),
                    updated_at   = NOW()
            """,
            n["id"], n["nombre"], sc,
            n["lat"], n["lng"], zona_id,
            n.get("rating"), n.get("num_resenas"),
            n.get("precio_nivel"), n.get("horario"),
            n.get("fuente", "google"))


# ── Queries BD ────────────────────────────────────────────────────────────────

async def _get_zonas_activas() -> list[dict]:
    """Devuelve zonas que tienen datos recientes (últimos 90 días)."""
    async with get_db() as conn:
        rows = await conn.fetch("""
            SELECT
                z.id AS zona_id,
                ST_Y(ST_Centroid(z.geometria)) AS lat,
                ST_X(ST_Centroid(z.geometria)) AS lng
            FROM zonas z
            WHERE EXISTS (
                SELECT 1 FROM variables_zona vz
                WHERE vz.zona_id = z.id
                  AND vz.fecha >= CURRENT_DATE - INTERVAL '90 days'
            )
            ORDER BY z.id
        """)
    return [dict(r) for r in rows]


async def _get_negocios_radio(zona_id: str, radio_m: int) -> list[dict]:
    """
    Devuelve negocios_activos en radio `radio_m` desde el centroide de la zona.
    Incluye distancia_m calculada por PostGIS para el gravity model.
    """
    async with get_db() as conn:
        rows = await conn.fetch("""
            SELECT
                na.sector_codigo,
                na.rating,
                na.num_resenas,
                na.precio_nivel,
                ST_Distance(
                    na.geometria::geography,
                    ST_Centroid(z.geometria)::geography
                )::float AS distancia_m
            FROM negocios_activos na
            JOIN zonas z ON z.id = $1
            WHERE ST_DWithin(
                na.geometria::geography,
                ST_Centroid(z.geometria)::geography,
                $2
            )
              AND na.activo = TRUE
              AND na.sector_codigo IS NOT NULL
            ORDER BY distancia_m ASC
        """, zona_id, float(radio_m))
    return [dict(r) for r in rows]


# ── Escritura BD ──────────────────────────────────────────────────────────────

async def _upsert_competencia_detalle(
    zona_id: str, sector: str, resultado: dict
) -> None:
    """Inserta o actualiza competencia_detalle_zona con el resultado del scoring."""
    hoy = date.today()
    async with get_db() as conn:
        await conn.execute("""
            INSERT INTO competencia_detalle_zona (
                zona_id, sector_codigo, radio_m, fecha,
                num_directos,
                pct_vulnerables,
                hhi_index,
                cluster_score,
                ratio_complementarios,
                num_sectores_complementarios,
                amenaza_incumbentes,
                oportunidad_mercado,
                score_competencia_v2,
                fuente
            ) VALUES (
                $1,$2,$3,$4,
                $5,$6,$7,$8,$9,$10,$11,$12,$13,
                'google_places'
            )
            ON CONFLICT (zona_id, sector_codigo, radio_m, fecha) DO UPDATE SET
                num_directos              = EXCLUDED.num_directos,
                pct_vulnerables           = EXCLUDED.pct_vulnerables,
                hhi_index                 = EXCLUDED.hhi_index,
                cluster_score             = EXCLUDED.cluster_score,
                ratio_complementarios     = EXCLUDED.ratio_complementarios,
                num_sectores_complementarios = EXCLUDED.num_sectores_complementarios,
                amenaza_incumbentes       = EXCLUDED.amenaza_incumbentes,
                oportunidad_mercado       = EXCLUDED.oportunidad_mercado,
                score_competencia_v2      = EXCLUDED.score_competencia_v2,
                updated_at                = NOW()
        """,
        zona_id, sector, _RADIO_M, hoy,
        resultado["num_directos"],
        resultado["pct_vulnerables"],
        resultado["hhi_index"],
        resultado["score_cluster"],
        resultado["ratio_complementarios"],
        len(SECTORES_COMPLEMENTARIOS.get(sector, [])),
        resultado["amenaza_incumbentes"],
        resultado["score_oportunidad"],
        resultado["score_competencia"])


async def _actualizar_competencia_por_local(
    zona_id: str, sector: str, resultado: dict
) -> None:
    """
    Actualiza competencia_por_local con datos v2 para compatibilidad retroactiva.
    Convierte score_competencia_v2 al rango de score_saturacion (invertido).
    """
    hoy = date.today()
    # score_saturacion v1: 0=poca saturación, 100=mucha saturación
    # score_competencia v2: 0=mala competencia, 100=buena competencia
    # Conversión: saturacion = 100 - score_competencia_v2
    score_sat_v2 = max(0.0, min(100.0, 100.0 - resultado["score_competencia"]))

    async with get_db() as conn:
        await conn.execute("""
            INSERT INTO competencia_por_local
                (zona_id, sector_codigo, radio_m,
                 num_competidores, rating_medio, score_saturacion,
                 num_vulnerables, num_complementarios,
                 fecha_calculo, fuente)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'google_places')
            ON CONFLICT (zona_id, sector_codigo, radio_m, fecha_calculo) DO UPDATE SET
                num_competidores  = EXCLUDED.num_competidores,
                rating_medio      = EXCLUDED.rating_medio,
                score_saturacion  = EXCLUDED.score_saturacion,
                num_vulnerables   = EXCLUDED.num_vulnerables,
                num_complementarios = EXCLUDED.num_complementarios,
                fuente            = EXCLUDED.fuente
        """,
        zona_id, sector, _RADIO_M,
        resultado["num_directos"],
        None,          # rating_medio se calcula aparte si se necesita
        score_sat_v2,
        round(resultado["pct_vulnerables"] * resultado["num_directos"]),
        round(resultado["ratio_complementarios"] * len(SECTORES_COMPLEMENTARIOS.get(sector, [])) * 3),
        hoy)


async def _upsert_score_neutro(zona_id: str, sector: str) -> None:
    """Cuando no hay datos de negocios: inserta score neutro 50.0."""
    hoy = date.today()
    async with get_db() as conn:
        await conn.execute("""
            INSERT INTO competencia_detalle_zona
                (zona_id, sector_codigo, radio_m, fecha,
                 num_directos, score_competencia_v2, fuente)
            VALUES ($1,$2,$3,$4, 0, 50.0, 'sin_datos')
            ON CONFLICT (zona_id, sector_codigo, radio_m, fecha) DO NOTHING
        """, zona_id, sector, _RADIO_M, hoy)


# ── Control de ejecución ───────────────────────────────────────────────────────

async def _init() -> int:
    async with get_db() as conn:
        return await conn.fetchval(
            "INSERT INTO pipeline_ejecuciones(pipeline,estado) "
            "VALUES('competencia','running') RETURNING id"
        )


async def _fin(eid: int, registros: int, estado: str, msg: Optional[str] = None) -> None:
    async with get_db() as conn:
        await conn.execute(
            "UPDATE pipeline_ejecuciones "
            "SET fecha_fin=NOW(), registros=$1, estado=$2, mensaje_error=$3 WHERE id=$4",
            registros, estado, msg, eid,
        )
