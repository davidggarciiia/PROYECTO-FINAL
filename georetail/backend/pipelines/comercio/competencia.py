"""
pipelines/competencia.py — Pipeline mensual de análisis de competencia v2.

Fuente: gosom (google-maps-scraper Go) — scraping de negocios activos en radio
500m para cada zona × sector. Sin API key de Google.

Proceso:
  1. Para cada (zona, sector): scraping vía gosom de negocios relevantes
     (competidores + complementarios).
  2. Upsert en negocios_activos (rating, reseñas, popular_times).
  3. Consulta espacial PostGIS sobre negocios_activos (radio 500m).
  4. Calcula sub-scores con scoring.competencia.calcular_score_competencia().
  5. Upsert en competencia_detalle_zona.
  6. Actualiza competencia_por_local.score_saturacion para compatibilidad v1.
  7. Registra ejecución en pipeline_ejecuciones.
"""
from __future__ import annotations

import asyncio
import json as _json
import logging
from datetime import date
from typing import Optional

from db.conexion import get_db
from scoring.dimensiones.competencia import (
    NegocioCompetidor,
    SECTORES_COMPETIDORES,
    SECTORES_COMPLEMENTARIOS,
    calcular_score_competencia,
    negocios_desde_rows,
)

logger = logging.getLogger(__name__)

# ── Configuración ──────────────────────────────────────────────────────────────

_RADIO_M            = 500
_SECTORES           = ["restauracion", "moda", "estetica", "tatuajes", "shisha_lounge",
                       "deporte", "salud", "educacion", "alimentacion", "servicios"]
_PAUSA_ENTRE_ZONAS  = 0.3

# Términos de búsqueda gosom por sector (se usa el primero como query principal)
_SECTOR_CATEGORIAS: dict[str, list[str]] = {
    "restauracion":  ["restaurante", "bar", "cafetería", "tapas"],
    "moda":          ["tienda de ropa", "boutique", "zapatería"],
    "estetica":      ["peluquería", "salón de belleza", "barbería"],
    "tatuajes":      ["estudio de tatuajes", "piercing"],
    "shisha_lounge": ["shisha lounge", "hookah bar"],
    "alimentacion":  ["supermercado", "frutería", "panadería"],
    "salud":         ["farmacia", "clínica", "fisioterapia"],
    "servicios":     ["tienda de electrónica", "telefonía móvil", "lavandería"],
    "educacion":     ["librería", "academia", "papelería"],
    "deporte":       ["gimnasio", "tienda deportiva", "yoga", "artes marciales"],
}

# Sectores relacionados a scrapear por sector objetivo (competidores + complementarios)
_SECTORES_A_SCRAPEAR: dict[str, list[str]] = {
    s: list(set(SECTORES_COMPETIDORES.get(s, [s]) + SECTORES_COMPLEMENTARIOS.get(s, [])))
    for s in _SECTORES
}

_VALID_SECTORES = {
    "restauracion", "moda", "estetica", "tatuajes", "shisha_lounge",
    "alimentacion", "salud", "servicios", "educacion", "deporte",
}


# ── Punto de entrada ───────────────────────────────────────────────────────────

async def ejecutar() -> dict:
    """Ejecuta el pipeline completo de competencia. Llamado por scheduler."""
    from pipelines.inmobiliario.scraping.gosom_client import GosomClient, GosomServiceUnavailable

    eid = await _init()
    ok, errores = 0, 0

    try:
        zonas = await _get_zonas_activas()
        logger.info("Pipeline competencia: %d zonas × %d sectores", len(zonas), len(_SECTORES))

        gosom = GosomClient()
        if not gosom.is_available():
            logger.error("gosom scraper no disponible — abortando pipeline competencia")
            await _fin(eid, 0, "error", "gosom no disponible")
            return {"registros": 0, "errores": 0, "aviso": "gosom no disponible"}

        async with gosom:
            for zona in zonas:
                for sector in _SECTORES:
                    try:
                        n = await _procesar_zona_sector(zona, sector, gosom)
                        ok += n
                    except GosomServiceUnavailable:
                        logger.error("gosom dejó de responder — abortando")
                        await _fin(eid, ok, "error", "gosom no disponible")
                        return {"registros": ok, "errores": errores}
                    except Exception as e:
                        logger.warning(
                            "Competencia error zona=%s sector=%s: %s",
                            zona["zona_id"], sector, e,
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

async def _procesar_zona_sector(zona: dict, sector: str, gosom) -> int:
    """
    Para una zona+sector:
      1. Scraping de negocios relevantes (competidores + complementarios) vía gosom
      2. Calcula score con scoring.competencia
      3. Persiste en competencia_detalle_zona
    Devuelve 1 si OK, 0 si no hay datos.
    """
    lat, lng    = zona["lat"], zona["lng"]
    zona_id     = zona["zona_id"]
    zona_nombre = zona["zona_nombre"]

    # 1. Scrapear negocios relevantes vía gosom
    sectores_scrapear = _SECTORES_A_SCRAPEAR.get(sector, [sector])
    for sc in sectores_scrapear:
        await _scrapear_gosom_y_upsert(gosom, zona_nombre, lat, lng, sc, zona_id)

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


# ── Scraping vía gosom ─────────────────────────────────────────────────────────

async def _scrapear_gosom_y_upsert(
    gosom,
    zona_nombre: str,
    lat: float,
    lng: float,
    sector: str,
    zona_id: str,
) -> None:
    """
    Busca negocios en zona usando gosom y hace upsert en negocios_activos.
    Propaga GosomServiceUnavailable; silencia el resto de errores.
    """
    from pipelines.inmobiliario.scraping.gosom_client import GosomServiceUnavailable

    categorias = _SECTOR_CATEGORIAS.get(sector, [sector])
    categoria  = categorias[0]

    try:
        entries = await gosom.search_zona(
            zona_nombre=zona_nombre,
            categoria=categoria,
            lat=lat,
            lng=lng,
        )
    except GosomServiceUnavailable:
        raise
    except Exception as e:
        logger.debug("gosom zona='%s' sector=%s: %s", zona_nombre, sector, e)
        return

    if entries:
        await _upsert_gosom_entries(entries, zona_id, sector)


async def _upsert_gosom_entries(entries: list, zona_id: str, sector: str) -> None:
    """Inserta o actualiza negocios en negocios_activos desde resultados GosomEntry.

    Aplica hybrid filter (Opción C):
      - HARD REJECT: sector_inf != None y != sector_target → descartado sin LLM.
      - CLARO: sector_inf == sector_target → aceptado directamente.
      - AMBIGUO: sector_inf == None → acumulado para batch LLM.
    Upsert final = claros + aprobados_llm.
    """
    from scoring.taxonomia import subsector_valido

    claros: list[tuple] = []
    ambiguos: list[tuple] = []

    for entry in entries:
        if not entry.title or entry.latitude is None or entry.longitude is None:
            continue

        sector_inf, subsector_inf = entry.clasificar()

        if sector_inf is not None and sector_inf != sector:
            logger.debug(
                "hybrid filter REJECT '%s': sector_inf=%s != sector=%s",
                entry.title, sector_inf, sector,
            )
            continue

        if subsector_inf and not subsector_valido(sector, subsector_inf):
            subsector_inf = None

        if sector_inf == sector:
            claros.append((entry, subsector_inf))
        else:
            # sector_inf is None → ambiguo
            ambiguos.append((entry, subsector_inf))

    aprobados_llm = await _filtrar_ambiguos_llm(ambiguos, sector) if ambiguos else []

    a_insertar = claros + aprobados_llm
    if not a_insertar:
        return

    async with get_db() as conn:
        for entry, subsector_inf in a_insertar:
            place_id = entry.place_id or f"gosom_{zona_id}_{abs(hash(entry.title))}"
            pt_json  = _json.dumps(entry.popular_times) if entry.popular_times else None

            try:
                await conn.execute(
                    """
                    INSERT INTO negocios_activos
                        (id, nombre, sector_codigo, subsector_codigo,
                         lat, lng, geometria, zona_id,
                         rating, total_resenas, review_count,
                         google_place_id, popular_times,
                         es_activo, fuente, updated_at)
                    VALUES ($1,$2,$3,$4, $5,$6,
                            ST_SetSRID(ST_MakePoint($6,$5),4326),
                            $7, $8,$9,$9, $10,$11::jsonb,
                            TRUE,'gosom',NOW())
                    ON CONFLICT (id) DO UPDATE SET
                        subsector_codigo = COALESCE(EXCLUDED.subsector_codigo, negocios_activos.subsector_codigo),
                        rating           = COALESCE(EXCLUDED.rating,           negocios_activos.rating),
                        total_resenas    = COALESCE(EXCLUDED.total_resenas,    negocios_activos.total_resenas),
                        review_count     = COALESCE(EXCLUDED.review_count,     negocios_activos.review_count),
                        google_place_id  = COALESCE(EXCLUDED.google_place_id,  negocios_activos.google_place_id),
                        popular_times    = COALESCE(EXCLUDED.popular_times,    negocios_activos.popular_times),
                        updated_at       = NOW()
                    """,
                    place_id,
                    entry.title,
                    sector if sector in _VALID_SECTORES else None,
                    subsector_inf,
                    float(entry.latitude),
                    float(entry.longitude),
                    zona_id,
                    entry.review_rating,     # rating
                    entry.review_count,      # total_resenas = review_count ($9 usado dos veces)
                    entry.place_id or None,  # google_place_id
                    pt_json,
                )
            except Exception as exc:
                logger.debug("upsert negocio '%s': %s", entry.title, exc)


async def _filtrar_ambiguos_llm(
    ambiguos: list[tuple], sector: str
) -> list[tuple]:
    """Batch LLM para entries sin sector inferido.

    Construye un prompt numerado con nombre y categoría de cada entry ambigua,
    llama al LLM con temperatura 0 y parsea las respuestas si/no.
    Fallback gracioso: si el LLM falla devuelve todos los ambiguos sin filtrar.
    """
    from routers.llm_router import completar

    if not ambiguos:
        return []

    lineas = [
        f"{i + 1}. '{e.title}' — categoría: '{e.category}'"
        for i, (e, _) in enumerate(ambiguos)
    ]
    prompt_usuario = (
        f"¿Pertenece cada negocio al sector '{sector}'?\n"
        + "\n".join(lineas)
    )
    sistema = (
        "Eres un clasificador estricto de negocios. "
        "Responde ÚNICAMENTE con una lista numerada de 'si' o 'no', "
        "una por línea, en el mismo orden que la entrada. "
        "Sin explicaciones. Sin texto adicional."
    )

    try:
        respuesta = await completar(
            mensajes=[{"role": "user", "content": prompt_usuario}],
            sistema=sistema,
            endpoint="gosom_filter",
            temperature=0.0,
            max_tokens=len(ambiguos) * 8,
        )

        aprobados: list[tuple] = []
        for i, linea in enumerate(respuesta.strip().splitlines()):
            if i >= len(ambiguos):
                break
            if linea.strip().lstrip("0123456789. ").lower().startswith("si"):
                aprobados.append(ambiguos[i])

        logger.debug(
            "hybrid filter LLM sector=%s ambiguos=%d aprobados=%d",
            sector, len(ambiguos), len(aprobados),
        )
        return aprobados

    except Exception as exc:
        logger.debug("hybrid filter LLM fallback (sector=%s): %s", sector, exc)
        return ambiguos


# ── Queries BD ────────────────────────────────────────────────────────────────

async def _get_zonas_activas() -> list[dict]:
    """Devuelve zonas que tienen datos recientes (últimos 90 días)."""
    async with get_db() as conn:
        rows = await conn.fetch("""
            SELECT
                z.id     AS zona_id,
                z.nombre AS zona_nombre,
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
                na.total_resenas AS num_resenas,
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
              AND na.es_activo = TRUE
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
                score_complementarios,
                num_sectores_complementarios,
                amenaza_incumbentes,
                oportunidad_mercado,
                score_competencia_v2,
                fuente
            ) VALUES (
                $1,$2,$3,$4,
                $5,$6,$7,$8,$9,$10,$11,$12,$13,$14,
                'gosom'
            )
            ON CONFLICT (zona_id, sector_codigo, radio_m, fecha) DO UPDATE SET
                num_directos              = EXCLUDED.num_directos,
                pct_vulnerables           = EXCLUDED.pct_vulnerables,
                hhi_index                 = EXCLUDED.hhi_index,
                cluster_score             = EXCLUDED.cluster_score,
                ratio_complementarios     = EXCLUDED.ratio_complementarios,
                score_complementarios     = EXCLUDED.score_complementarios,
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
        resultado["score_complementarios"],
        len(SECTORES_COMPLEMENTARIOS.get(sector, [])),
        resultado["amenaza_incumbentes"],
        resultado["score_oportunidad"],
        resultado["score_competencia"])


async def _actualizar_competencia_por_local(
    zona_id: str, sector: str, resultado: dict
) -> None:
    """Actualiza competencia_por_local con datos v2 para compatibilidad retroactiva."""
    hoy = date.today()
    score_sat_v2 = max(0.0, min(100.0, 100.0 - resultado["score_competencia"]))

    async with get_db() as conn:
        await conn.execute("""
            INSERT INTO competencia_por_local
                (zona_id, sector_codigo, radio_m,
                 num_competidores, rating_medio, score_saturacion,
                 num_vulnerables, num_complementarios,
                 fecha_calculo, fuente)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'gosom')
            ON CONFLICT (zona_id, sector_codigo, radio_m, fecha_calculo) DO UPDATE SET
                num_competidores    = EXCLUDED.num_competidores,
                rating_medio        = EXCLUDED.rating_medio,
                score_saturacion    = EXCLUDED.score_saturacion,
                num_vulnerables     = EXCLUDED.num_vulnerables,
                num_complementarios = EXCLUDED.num_complementarios,
                fuente              = EXCLUDED.fuente
        """,
        zona_id, sector, _RADIO_M,
        resultado["num_directos"],
        None,
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
