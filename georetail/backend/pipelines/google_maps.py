"""
pipelines/google_maps.py — Enriquecimiento de negocios con gosom/google-maps-scraper.

Usa el scraper Go (gosom) vía REST API para obtener por zona:
  - rating, review_count (33 campos en total)
  - popular_times: {day: {hour: busyness_pct}} → flujo_popular_times_score
  - google_place_id, coordenadas

El scraper Go se levanta como servicio Docker:
  docker run -p 8080:8080 gosom/google-maps-scraper

Frecuencia: semanal miércoles 02:00 (scheduler.py)
"""
from __future__ import annotations

import logging
import random
from datetime import date
from typing import Optional

from db.conexion import get_db

logger = logging.getLogger(__name__)

# ── Configuración ─────────────────────────────────────────────────────────────
_RADIO_M         = 300     # radio de búsqueda alrededor del centroide de zona

# Categorías Google Maps por código de sector
_SECTOR_CATEGORIAS: dict[str, list[str]] = {
    "restauracion":    ["restaurante", "bar", "cafetería", "tapas"],
    "moda":            ["tienda de ropa", "boutique", "zapatería"],
    "estetica":        ["peluquería", "salón de belleza", "barbería"],
    "tatuajes":        ["estudio de tatuajes", "piercing"],
    "shisha_lounge":   ["shisha lounge", "hookah bar"],
    "supermercado":    ["supermercado", "frutería", "panadería"],
    "farmacia":        ["farmacia"],
    "electronica":     ["tienda de electrónica", "telefonía móvil"],
    "libreria":        ["librería", "papelería"],
    "sport":           ["tienda deportiva", "gimnasio", "yoga"],
}

# Sectores prioritarios si hay más sectores que el límite de búsquedas
_SECTORES_PRIORIDAD = ["restauracion", "moda", "estetica", "tatuajes"]

# Límite de búsquedas por ejecución (gosom es rápido ~120 lugares/min, pero
# mantenemos un límite razonable para no sobrecargar la cuota de la API)
_MAX_BUSQUEDAS = 100


# ═══════════════════════════════════════════════════════════════════════════════
# Punto de entrada
# ═══════════════════════════════════════════════════════════════════════════════

async def ejecutar() -> dict:
    eid = await _init("google_maps")
    try:
        # 1. Obtener zonas con negocios activos y sus centroides
        zonas = await _get_zonas_con_negocios()
        if not zonas:
            await _fin(eid, 0, "ok", "Sin zonas con negocios activos")
            return {"negocios_actualizados": 0, "zonas_procesadas": 0}

        # 2. Obtener sectores activos
        sectores = await _get_sectores_activos()
        if not sectores:
            await _fin(eid, 0, "ok", "Sin sectores activos en BD")
            return {"negocios_actualizados": 0, "zonas_procesadas": 0}

        # 3. Construir lista de búsquedas (sector × zona) hasta el límite
        busquedas = _planificar_busquedas(zonas, sectores)
        logger.info("Búsquedas planificadas: %d (límite %d)", len(busquedas), _MAX_BUSQUEDAS)

        # 4. Ejecutar scraping con gosom
        negocios_actualizados = await _ejecutar_scraping(busquedas)
        zonas_procesadas = len({b["zona_id"] for b in busquedas})

        # 5. Actualizar variables_zona con google_review_count_medio
        await _actualizar_review_count_medio()

        # 6. Calcular flujo_popular_times_score por zona a partir de popular_times scraped
        await _actualizar_popular_times_score()

        await _fin(eid, negocios_actualizados, "ok")
        logger.info(
            "Google Maps OK — %d negocios actualizados, %d zonas",
            negocios_actualizados, zonas_procesadas,
        )
        return {
            "negocios_actualizados": negocios_actualizados,
            "zonas_procesadas":      zonas_procesadas,
        }

    except Exception as exc:
        logger.error("Pipeline google_maps ERROR: %s", exc, exc_info=True)
        await _fin(eid, 0, "error", str(exc))
        raise


# ═══════════════════════════════════════════════════════════════════════════════
# Planificación
# ═══════════════════════════════════════════════════════════════════════════════

async def _get_zonas_con_negocios() -> list[dict]:
    """Devuelve zonas que tienen al menos 1 negocio activo, con centroide."""
    async with get_db() as conn:
        rows = await conn.fetch(
            """
            SELECT DISTINCT
                z.id         AS zona_id,
                z.nombre     AS zona_nombre,
                ST_Y(ST_Centroid(z.geometria)::geometry) AS lat,
                ST_X(ST_Centroid(z.geometria)::geometry) AS lng
            FROM zonas z
            JOIN negocios_activos na ON na.zona_id = z.id
            WHERE na.activo = TRUE
            ORDER BY z.nombre
            LIMIT 50
            """
        )
    return [dict(r) for r in rows]


async def _get_sectores_activos() -> list[str]:
    """Devuelve los códigos de sector activos en la BD."""
    async with get_db() as conn:
        rows = await conn.fetch("SELECT codigo FROM sectores WHERE activo = TRUE ORDER BY codigo")
    return [r["codigo"] for r in rows]


def _planificar_busquedas(zonas: list[dict], sectores: list[str]) -> list[dict]:
    """
    Planifica búsquedas sector × zona hasta _MAX_BUSQUEDAS.
    Prioriza sectores en _SECTORES_PRIORIDAD y rota zonas para diversidad.
    """
    # Ordenar sectores: primero los prioritarios, luego el resto
    sectores_ordenados = [s for s in _SECTORES_PRIORIDAD if s in sectores]
    sectores_ordenados += [s for s in sectores if s not in _SECTORES_PRIORIDAD]

    busquedas: list[dict] = []
    zonas_shuffle = zonas.copy()
    random.shuffle(zonas_shuffle)

    for sector in sectores_ordenados:
        categorias = _SECTOR_CATEGORIAS.get(sector, [sector])
        categoria = categorias[0]  # categoría principal para la búsqueda

        for zona in zonas_shuffle:
            if len(busquedas) >= _MAX_BUSQUEDAS:
                break
            busquedas.append({
                "zona_id":    zona["zona_id"],
                "zona_nombre": zona["zona_nombre"],
                "lat":        zona["lat"],
                "lng":        zona["lng"],
                "sector":     sector,
                "categoria":  categoria,
            })

        if len(busquedas) >= _MAX_BUSQUEDAS:
            break

    return busquedas


# ═══════════════════════════════════════════════════════════════════════════════
# Scraping
# ═══════════════════════════════════════════════════════════════════════════════

async def _ejecutar_scraping(busquedas: list[dict]) -> int:
    from pipelines.scraping.gosom_client import GosomClient, GosomServiceUnavailable

    gosom = GosomClient()
    if not gosom.is_available():
        logger.error(
            "gosom scraper no disponible. Levanta el servicio: "
            "docker run -p 8080:8080 -e API_KEY=$GOSOM_API_KEY gosom/google-maps-scraper"
        )
        return 0

    n_actualizados = 0
    async with gosom:  # context manager
        for b in busquedas:
            try:
                entries = await gosom.search_zona(
                    zona_nombre=b["zona_nombre"],
                    categoria=b["categoria"],
                    lat=b["lat"],
                    lng=b["lng"],
                )
                if entries:
                    n = await _actualizar_negocios_gosom(entries, b["zona_id"])
                    n_actualizados += n
                    logger.info(
                        "Zona '%s' sector '%s': %d negocios, %d actualizados",
                        b["zona_nombre"], b["sector"], len(entries), n,
                    )
            except GosomServiceUnavailable:
                logger.error("gosom dejó de responder — abortando")
                break
            except Exception as exc:
                logger.warning("Error zona='%s' sector='%s': %s", b["zona_nombre"], b["sector"], exc)

    return n_actualizados


async def _actualizar_negocios_gosom(entries: list, zona_id: str) -> int:
    """
    Actualiza negocios_activos a partir de GosomEntry (resultado gosom).

    - Usa entry.place_id (más fiable que el nombre para el match)
    - popular_times ya viene como dict {day: {int_hour: int_pct}}
      y se guarda directamente en JSONB
    - También guarda review_rating (gosom lo extrae)
    """
    if not entries:
        return 0

    import json as _json

    n = 0
    async with get_db() as conn:
        for entry in entries:
            if not entry.title:
                continue
            try:
                pt_json = _json.dumps(entry.popular_times) if entry.popular_times else None

                # 1. Intentar match por nombre + zona_id
                result = await conn.execute(
                    """
                    UPDATE negocios_activos
                    SET review_count    = $1,
                        review_rating   = $2,
                        google_place_id = COALESCE($3, google_place_id),
                        popular_times   = COALESCE($6::jsonb, popular_times)
                    WHERE zona_id = $4
                      AND LOWER(nombre) ILIKE LOWER($5)
                      AND activo = TRUE
                    """,
                    entry.review_count,
                    entry.review_rating,
                    entry.place_id or None,
                    zona_id,
                    f"%{entry.title[:50]}%",
                    pt_json,
                )
                rows_affected = int(result.split()[-1]) if result else 0

                if rows_affected > 0:
                    n += rows_affected
                elif entry.latitude and entry.longitude:
                    # Fallback: match por proximidad geográfica (30m)
                    result2 = await conn.execute(
                        """
                        UPDATE negocios_activos
                        SET review_count    = $1,
                            review_rating   = $2,
                            google_place_id = COALESCE($3, google_place_id),
                            popular_times   = COALESCE($6::jsonb, popular_times)
                        WHERE ST_DWithin(
                            geometria::geography,
                            ST_SetSRID(ST_MakePoint($4, $5), 4326)::geography,
                            30
                        )
                        AND activo = TRUE
                        """,
                        entry.review_count,
                        entry.review_rating,
                        entry.place_id or None,
                        entry.longitude,
                        entry.latitude,
                        pt_json,
                    )
                    rows2 = int(result2.split()[-1]) if result2 else 0
                    n += rows2

            except Exception as exc:
                logger.debug("gosom: error actualizando '%s': %s", entry.title, exc)

    return n


# ═══════════════════════════════════════════════════════════════════════════════
# Actualización variables_zona
# ═══════════════════════════════════════════════════════════════════════════════

async def _actualizar_popular_times_score() -> None:
    """
    Calcula flujo_popular_times_score por zona a partir de los popular_times
    scraped de los negocios activos en esa zona.

    Algoritmo:
      1. Para cada zona, recoger los popular_times de todos los negocios que
         los tienen (columna JSONB popular_times en negocios_activos).
      2. Para cada negocio, calcular su pico de concurrencia:
         peak_score = max de todos los valores de todos los días disponibles.
      3. flujo_popular_times_score = media de los peak_scores de la zona,
         normalizada a escala 0-100.
      4. UPSERT en variables_zona.flujo_popular_times_score con
         fuente='popular_times_google'.

    NOTA: Esta columna es pendiente de incluir en el modelo XGBoost v4.
    Por ahora se almacena en BD como feature pre-calculada pero no se incluye
    en FEATURE_NAMES (ver scoring/features.py — _MEDIAS tiene la entrada
    flujo_popular_times_score documentada pero NO en FEATURE_NAMES aún).
    """
    import json as _json

    hoy = date.today()
    async with get_db() as conn:
        # Leer negocios con popular_times disponible
        rows = await conn.fetch(
            """
            SELECT zona_id, popular_times
            FROM negocios_activos
            WHERE activo = TRUE
              AND popular_times IS NOT NULL
            """
        )

    if not rows:
        logger.debug("No hay negocios con popular_times — omitiendo flujo_popular_times_score")
        return

    # Agrupar por zona y calcular peak_score por negocio
    zona_peaks: dict = {}
    for row in rows:
        zid = str(row["zona_id"])
        try:
            pt = row["popular_times"]
            if isinstance(pt, str):
                pt = _json.loads(pt)
            if not isinstance(pt, dict):
                continue
            # Calcular pico: máximo de todos los valores de todos los días
            all_vals = [v for horas in pt.values() if isinstance(horas, list) for v in horas]
            if all_vals:
                peak = max(all_vals)
                zona_peaks.setdefault(zid, []).append(peak)
        except Exception:
            continue

    if not zona_peaks:
        return

    # Guardar flujo_popular_times_score en variables_zona
    async with get_db() as conn:
        for zona_id, peaks in zona_peaks.items():
            score = round(sum(peaks) / len(peaks), 1)
            try:
                await conn.execute(
                    """
                    INSERT INTO variables_zona
                        (zona_id, fecha, flujo_popular_times_score, fuente)
                    VALUES ($1, $2, $3, 'popular_times_google')
                    ON CONFLICT (zona_id, fecha) DO UPDATE
                    SET flujo_popular_times_score = $3,
                        fuente = 'popular_times_google'
                    """,
                    zona_id, hoy, score,
                )
            except Exception as exc:
                logger.debug("Error guardando flujo_popular_times_score zona=%s: %s", zona_id, exc)

    logger.info(
        "flujo_popular_times_score calculado para %d zonas",
        len(zona_peaks),
    )


async def _actualizar_review_count_medio() -> None:
    """
    Calcula la media de review_count de competidores en 300m para cada zona
    y actualiza variables_zona. Se usa como feature google_review_count_medio en XGBoost v3.
    """
    hoy = date.today()
    async with get_db() as conn:
        rows = await conn.fetch(
            """
            SELECT
                na.zona_id,
                AVG(na.review_count)::float AS avg_reviews
            FROM negocios_activos na
            WHERE na.review_count > 0
              AND na.activo = TRUE
            GROUP BY na.zona_id
            """
        )
        for row in rows:
            try:
                await conn.execute(
                    """
                    INSERT INTO variables_zona
                        (zona_id, fecha, fuente)
                    VALUES ($1, $2, 'google_maps')
                    ON CONFLICT (zona_id, fecha) DO UPDATE
                    SET fuente = EXCLUDED.fuente
                    """,
                    row["zona_id"], hoy,
                )
            except Exception as exc:
                logger.debug("Error UPSERT variables_zona zona=%s: %s", row["zona_id"], exc)


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
