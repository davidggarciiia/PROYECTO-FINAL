"""
pipelines/google_maps.py — Enriquecimiento de negocios activos con datos Google Maps.

Qué hace:
  1. Lee los sectores activos de la BD y las zonas con negocios
  2. Para cada sector × zona usa GoogleMapsScraper para obtener:
     - negocios del sector en esa zona
     - su rating, review_count, google_place_id
  3. Actualiza negocios_activos (match por nombre + zona_id)
  4. Actualiza variables_zona.google_review_count_medio

Estrategia antibot:
  - 1 instancia de GoogleMapsScraper con session warming
  - Delays aleatorios de 20-45 s entre búsquedas
  - Máximo 30 búsquedas por ejecución (≈ 6 sectores × 5 zonas)
    para no sobrecargar la sesión de Playwright
  - Si CAPTCHA → abort de la ejecución, reintentar la semana siguiente

Frecuencia: semanal miércoles 02:00 (scheduler.py)
"""
from __future__ import annotations

import asyncio
import logging
import random
from datetime import date
from typing import Optional

from db.conexion import get_db

logger = logging.getLogger(__name__)

# ── Configuración ─────────────────────────────────────────────────────────────
_MAX_BUSQUEDAS   = 30      # máximo de búsquedas por ejecución (throttle antibot)
_RADIO_M         = 300     # radio de búsqueda alrededor del centroide de zona
_DELAY_MIN_S     = 20.0    # pausa mínima entre búsquedas
_DELAY_MAX_S     = 45.0    # pausa máxima entre búsquedas

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

        # 4. Ejecutar scraping con Playwright
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
    """
    Ejecuta búsquedas Google Maps con una sola instancia de scraper (1 sesión).
    Actualiza negocios_activos en BD para cada negocio encontrado.

    Estrategia:
      - Intenta gosom primero (más datos, popular_times más completo).
        gosom es el scraper Go (docker en localhost:8080) que devuelve
        popular_times estructurado y tiene ~120 lugares/minuto.
      - Si gosom no está disponible, cae al scraper Playwright existente.
    """
    # Intentar gosom primero (más datos, popular_times más completo)
    from pipelines.scraping.gosom_client import GosomClient
    gosom = GosomClient()
    usar_gosom = gosom.is_available()

    if usar_gosom:
        logger.info("gosom disponible en %s — usando scraper Go", gosom.base_url)
        return await _ejecutar_scraping_gosom(gosom, busquedas)

    logger.info("gosom no disponible — usando scraper Playwright")

    try:
        from pipelines.scraping.google_maps_scraper import GoogleMapsScraper
    except ImportError as exc:
        logger.error("GoogleMapsScraper no disponible (¿falta playwright?): %s", exc)
        return 0

    scraper = GoogleMapsScraper(session_id="pipeline_weekly", max_retries=2)
    n_actualizados = 0

    for i, b in enumerate(busquedas):
        try:
            negocios = await scraper.buscar_negocios_por_categoria(
                categoria=b["categoria"],
                zona_nombre=b["zona_nombre"],
                lat=b["lat"],
                lng=b["lng"],
                radio_m=_RADIO_M,
            )
            if negocios:
                n = await _actualizar_negocios(negocios, b["zona_id"])
                n_actualizados += n
                logger.info(
                    "Zona '%s' sector '%s': %d negocios scraped, %d actualizados",
                    b["zona_nombre"], b["sector"], len(negocios), n,
                )

        except Exception as exc:
            msg = str(exc).lower()
            if "captcha" in msg:
                logger.warning("CAPTCHA detectado en búsqueda %d/%d — abortando ejecución", i+1, len(busquedas))
                break
            logger.warning("Error en búsqueda zona='%s' sector='%s': %s", b["zona_nombre"], b["sector"], exc)

        # Delay humano entre búsquedas (excepto la última)
        if i < len(busquedas) - 1:
            delay = random.uniform(_DELAY_MIN_S, _DELAY_MAX_S)
            logger.debug("Esperando %.1f s antes de siguiente búsqueda", delay)
            await asyncio.sleep(delay)

    return n_actualizados


async def _ejecutar_scraping_gosom(gosom, busquedas: list[dict]) -> int:
    """
    Ejecuta las búsquedas usando el cliente gosom (Go scraper REST API).

    A diferencia del scraper Playwright, gosom devuelve GosomEntry con
    popular_times estructurado como {day: {hour: pct}}. Los adaptamos
    al formato esperado por _actualizar_negocios_gosom.

    No se aplican delays humanos: gosom es un servicio controlado internamente
    con su propio rate limiting (~120 lugares/minuto).
    """
    from pipelines.scraping.gosom_client import GosomEntry

    n_actualizados = 0

    async with gosom:
        for i, b in enumerate(busquedas):
            try:
                entries: list[GosomEntry] = await gosom.search_zona(
                    zona_nombre=b["zona_nombre"],
                    categoria=b["categoria"],
                    lat=b["lat"],
                    lng=b["lng"],
                )
                if entries:
                    n = await _actualizar_negocios_gosom(entries, b["zona_id"])
                    n_actualizados += n
                    logger.info(
                        "gosom zona='%s' sector='%s': %d resultados, %d actualizados",
                        b["zona_nombre"], b["sector"], len(entries), n,
                    )

            except Exception as exc:
                logger.warning(
                    "gosom error zona='%s' sector='%s': %s",
                    b["zona_nombre"], b["sector"], exc,
                )

    return n_actualizados


async def _actualizar_negocios_gosom(entries: list, zona_id: str) -> int:
    """
    Actualiza negocios_activos a partir de GosomEntry (resultado gosom).

    Diferencias respecto a _actualizar_negocios (Playwright):
      - Usa entry.place_id (más fiable que el nombre para el match)
      - popular_times ya viene como dict {day: {int_hour: int_pct}}
        y se guarda directamente en JSONB
      - También guarda review_rating (gosom lo extrae, el scraper Playwright no)
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


async def _actualizar_negocios(negocios: list, zona_id: str) -> int:
    """
    Para cada negocio scraped, intenta hacer match en negocios_activos
    por nombre aproximado y zona_id, y actualiza review_count + google_place_id
    + popular_times (si disponible).
    Match: nombre ILIKE '%nombre_scraped%' AND zona_id = zona_id
    """
    if not negocios:
        return 0

    import json as _json

    n = 0
    async with get_db() as conn:
        for neg in negocios:
            if not neg.nombre:
                continue
            try:
                # Serializar popular_times a JSONB si está disponible
                pt_json = _json.dumps(neg.popular_times) if neg.popular_times else None

                result = await conn.execute(
                    """
                    UPDATE negocios_activos
                    SET review_count    = $1,
                        google_place_id = COALESCE($2, google_place_id),
                        popular_times   = COALESCE($5::jsonb, popular_times)
                    WHERE zona_id = $3
                      AND LOWER(nombre) ILIKE LOWER($4)
                      AND activo = TRUE
                    """,
                    neg.review_count,
                    neg.google_place_id,
                    zona_id,
                    f"%{neg.nombre[:50]}%",
                    pt_json,
                )
                # asyncpg devuelve "UPDATE N" como string
                rows_affected = int(result.split()[-1]) if result else 0
                if rows_affected > 0:
                    n += rows_affected
                elif neg.lat and neg.lng:
                    # Si no hay match por nombre, intentar por proximidad geográfica
                    result2 = await conn.execute(
                        """
                        UPDATE negocios_activos
                        SET review_count    = $1,
                            google_place_id = COALESCE($2, google_place_id),
                            popular_times   = COALESCE($5::jsonb, popular_times)
                        WHERE ST_DWithin(
                            geometria::geography,
                            ST_SetSRID(ST_MakePoint($3, $4), 4326)::geography,
                            30
                        )
                        AND activo = TRUE
                        """,
                        neg.review_count, neg.google_place_id,
                        neg.lng, neg.lat,
                        pt_json,
                    )
                    rows2 = int(result2.split()[-1]) if result2 else 0
                    n += rows2
            except Exception as exc:
                logger.debug("Error actualizando negocio '%s': %s", neg.nombre, exc)

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
         peak_score = max de los 24 valores de todos los días disponibles.
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
