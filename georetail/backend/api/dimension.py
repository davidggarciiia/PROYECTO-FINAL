"""
api/dimension.py — Drill-down por dimensión de score.

Cuando el usuario pulsa una barra de score en el panel de detalle, el front
pide a este endpoint los *ingredientes concretos* que componen esa dimensión:
landmarks con nombre, hoteles con estrellas, apartamentos turísticos, venues
culturales cercanos, etc.

La idea es pasar de "Hoteles cercanos: 35" a:
    "Sagrada Família a 254 m"
    "Hotel Casa Camper a 180 m (★4)"
    "Teatre Romea a 340 m"

Radio por defecto: 500 m (coincide con las features *_500m de v_variables_zona).
"""
from __future__ import annotations

import json
import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from db.conexion import get_db
from db.sesiones import get_sesion
from schemas.models import NarrativaDimensionResponse

logger = logging.getLogger(__name__)
router = APIRouter(tags=["dimension"])

_RADIO_M_DEFAULT = 500
_NARRATIVA_TTL_DIAS = 30


# ═══════════════════════════════════════════════════════════════════════════════
# Turismo — drill-down completo
# ═══════════════════════════════════════════════════════════════════════════════


@router.get(
    "/dimension/turismo/{zona_id}",
    summary="Detalle narrativo de la dimensión de turismo",
)
async def detalle_turismo(
    zona_id: str,
    session_id: Optional[str] = Query(None, description="ID de sesión activa"),
    radio_m: int = Query(_RADIO_M_DEFAULT, ge=100, le=1500),
) -> dict:
    """
    Lista los landmarks turísticos, hoteles, apartamentos HUT y venues
    culturales cercanos a la zona, con distancia y nombre real. Usado por
    el drawer del panel de detalle (click en la barra "Turismo").
    """
    if session_id:
        sesion = await get_sesion(session_id)
        if sesion is None:
            logger.info(
                "detalle_turismo zona=%s con session_id expirada; se devuelve detalle zonal",
                zona_id,
            )

    async with get_db() as conn:
        zona = await conn.fetchrow(
            """
            SELECT id, nombre,
                   ST_Y(ST_Centroid(geometria)) AS lat,
                   ST_X(ST_Centroid(geometria)) AS lng
            FROM zonas WHERE id = $1
            """,
            zona_id,
        )
        if zona is None:
            raise HTTPException(status_code=404, detail=f"Zona '{zona_id}' no encontrada.")

        # Features agregadas (densidades y distancias precomputadas).
        # `v_variables_zona` guarda 1 fila por (zona, fecha) — nos quedamos
        # con la más reciente, que es la que tiene las agregaciones
        # actualizadas por los pipelines de turismo.
        features = await conn.fetchrow(
            """
            SELECT vz.score_turismo,
                   vz.score_turismo_hut,
                   vz.score_turismo_airbnb,
                   vz.airbnb_density_500m,
                   vz.airbnb_occupancy_est,
                   vz.booking_hoteles_500m,
                   vz.eventos_culturales_500m,
                   vz.venues_musicales_500m,
                   vz.dist_landmark_top3_m,
                   ROUND(ST_Distance(
                       ST_Centroid(z.geometria)::geography,
                       ST_GeomFromText(
                           'LINESTRING(2.1850 41.3740,2.1940 41.3792,2.2030 41.3840,'
                           '2.2130 41.3900,2.2250 41.3970,2.2380 41.4020)', 4326
                       )::geography
                   )::numeric, 0)::int AS dist_playa_m
            FROM v_variables_zona vz
            JOIN zonas z ON z.id = vz.zona_id
            WHERE vz.zona_id = $1
            ORDER BY vz.fecha DESC
            LIMIT 1
            """,
            zona_id,
        )
        feat = dict(features) if features else {}

        # El score calculado por el scorer vive en scores_zona; el campo
        # vz.score_turismo de variables_zona es el valor histórico que no
        # siempre se reescribe. Preferimos el calculado más reciente.
        score_calc = await conn.fetchval(
            """
            SELECT AVG(score_turismo)::float
            FROM scores_zona
            WHERE zona_id = $1 AND modelo_version <> 'seed_v1'
            """,
            zona_id,
        )
        if score_calc is not None:
            feat["score_turismo"] = round(float(score_calc), 1)

        # Top-8 landmarks turísticos por cercanía. Si una tabla opcional no
        # está disponible, el endpoint sigue devolviendo centro, métricas y mapa.
        landmarks = []
        try:
            landmarks = await conn.fetch(
                """
                SELECT l.nombre,
                       l.wikidata_id,
                       l.peso_turistico,
                       ST_Y(l.geom) AS lat,
                       ST_X(l.geom) AS lng,
                       ROUND(ST_Distance(
                           ST_Centroid(z.geometria)::geography,
                           l.geom::geography
                       )::numeric, 0)::int AS distancia_m
                FROM landmarks_turisticos l
                JOIN zonas z ON z.id = $1
                WHERE ST_DWithin(
                    ST_Centroid(z.geometria)::geography,
                    l.geom::geography,
                    $2
                )
                ORDER BY distancia_m ASC
                LIMIT 12
                """,
                zona_id, radio_m * 3,
            )
        except Exception as exc:
            logger.warning("detalle_turismo landmarks no disponibles zona=%s: %s", zona_id, exc)

        # Top-12 hoteles / alojamientos comerciales cercanos con coords.
        hoteles = []
        try:
            hoteles = await conn.fetch(
                """
                SELECT a.nombre,
                       a.tipo,
                       a.estrellas,
                       a.rating,
                       ST_Y(a.geometria) AS lat,
                       ST_X(a.geometria) AS lng,
                       ROUND(ST_Distance(
                           ST_Centroid(z.geometria)::geography,
                           a.geometria::geography
                       )::numeric, 0)::int AS distancia_m
                FROM alojamientos_turisticos a
                JOIN zonas z ON z.id = $1
                WHERE a.es_activo = TRUE
                  AND ST_DWithin(
                      ST_Centroid(z.geometria)::geography,
                      a.geometria::geography,
                      $2
                  )
                ORDER BY distancia_m ASC
                LIMIT 12
                """,
                zona_id, radio_m,
            )
        except Exception as exc:
            logger.warning("detalle_turismo alojamientos no disponibles zona=%s: %s", zona_id, exc)

        # Venues culturales + musicales cercanos con nombre y coords.
        venues = []
        try:
            venues = await conn.fetch(
                """
                SELECT v.nom AS nombre,
                       v.tipo,
                       v.font AS fuente,
                       ST_Y(v.geometria) AS lat,
                       ST_X(v.geometria) AS lng,
                       ROUND(ST_Distance(
                           ST_Centroid(z.geometria)::geography,
                           v.geometria::geography
                       )::numeric, 0)::int AS distancia_m
                FROM venues_ocio v
                JOIN zonas z ON z.id = $1
                WHERE ST_DWithin(
                    ST_Centroid(z.geometria)::geography,
                    v.geometria::geography,
                    $2
                )
                  AND v.nom IS NOT NULL
                  AND v.nom NOT LIKE 'Venue OSM %'
                ORDER BY distancia_m ASC
                LIMIT 12
                """,
                zona_id, radio_m,
            )
        except Exception as exc:
            logger.warning("detalle_turismo venues no disponibles zona=%s: %s", zona_id, exc)

    # Narrativa de resumen generada deterministamente a partir de los conteos.
    n_land = len(landmarks)
    n_hot  = len(hoteles)
    n_ven  = len(venues)
    dist_playa = feat.get("dist_playa_m")
    n_airbnb = feat.get("airbnb_density_500m")

    resumen_frases: list[str] = []
    if n_land:
        top = landmarks[0]
        resumen_frases.append(
            f"El landmark más cercano es {top['nombre']} a {top['distancia_m']} m. "
            f"Hay {n_land} en un radio de {radio_m * 2} m."
        )
    else:
        resumen_frases.append(
            f"No hay landmarks turísticos destacados en {radio_m * 2} m — "
            "zona poco patrimonial."
        )

    if n_hot:
        resumen_frases.append(
            f"{n_hot} hoteles/alojamientos comerciales a < {radio_m} m."
        )
    else:
        resumen_frases.append(
            "Sin hoteles registrados en el radio — la presión de turismo alojado es baja."
        )

    if n_ven:
        resumen_frases.append(
            f"{n_ven} venues culturales o musicales cercanos (teatros, museos, salas)."
        )

    if n_airbnb is not None and n_airbnb > 0:
        resumen_frases.append(f"{int(n_airbnb)} anuncios de Airbnb activos a < {radio_m} m.")
    if dist_playa is not None:
        if dist_playa < 500:
            resumen_frases.append(f"Playa a {dist_playa} m — efecto playero fuerte.")
        elif dist_playa < 1500:
            resumen_frases.append(f"Playa a {dist_playa} m — influencia litoral moderada.")
        else:
            resumen_frases.append(f"Playa a {dist_playa} m — zona interior.")

    return {
        "zona_id": zona_id,
        "zona_nombre": zona["nombre"],
        "zona_lat": float(zona["lat"]) if zona["lat"] is not None else None,
        "zona_lng": float(zona["lng"]) if zona["lng"] is not None else None,
        "radio_m": radio_m,
        "score_turismo": feat.get("score_turismo"),
        "resumen": " ".join(resumen_frases),
        "hechos": {
            "landmarks_count":      n_land,
            "hoteles_count":        n_hot,
            "venues_count":         n_ven,
            "airbnb_density_500m":  feat.get("airbnb_density_500m"),
            "booking_hoteles_500m": feat.get("booking_hoteles_500m"),
            "dist_playa_m":         dist_playa,
            "dist_landmark_top3_m": feat.get("dist_landmark_top3_m"),
            "score_turismo_airbnb": feat.get("score_turismo_airbnb"),
            "score_turismo_hut":    feat.get("score_turismo_hut"),
        },
        "landmarks": [
            {
                "nombre":      r["nombre"],
                "distancia_m": r["distancia_m"],
                "wikidata_id": r["wikidata_id"],
                "peso":        float(r["peso_turistico"]) if r["peso_turistico"] is not None else None,
                "lat":         float(r["lat"]) if r["lat"] is not None else None,
                "lng":         float(r["lng"]) if r["lng"] is not None else None,
            }
            for r in landmarks
        ],
        "hoteles": [
            {
                "nombre":      r["nombre"],
                "tipo":        r["tipo"],
                "estrellas":   r["estrellas"],
                "rating":      float(r["rating"]) if r["rating"] is not None else None,
                "distancia_m": r["distancia_m"],
                "lat":         float(r["lat"]) if r["lat"] is not None else None,
                "lng":         float(r["lng"]) if r["lng"] is not None else None,
            }
            for r in hoteles
        ],
        "venues": [
            {
                "nombre":      r["nombre"],
                "tipo":        r["tipo"],
                "fuente":      r["fuente"],
                "distancia_m": r["distancia_m"],
                "lat":         float(r["lat"]) if r["lat"] is not None else None,
                "lng":         float(r["lng"]) if r["lng"] is not None else None,
            }
            for r in venues
        ],
    }


# ═══════════════════════════════════════════════════════════════════════════════
# Narrativa LLM por dimensión — botón "Interpretar con IA" en el dossier editorial
# ═══════════════════════════════════════════════════════════════════════════════


@router.post(
    "/dimension/{dim_key}/{zona_id}/narrativa",
    response_model=NarrativaDimensionResponse,
    summary="Lectura interpretativa + 3 decisiones prácticas (LLM) por dimensión",
)
async def narrativa_dimension(
    dim_key: str,
    zona_id: str,
    session_id: str = Query(..., description="ID de sesión activa"),
) -> NarrativaDimensionResponse:
    """Genera (o devuelve cacheado) la lectura LLM + 3 decisiones para una
    dimensión concreta del dossier.

    Llamado al pulsar "Interpretar con IA" en una sección del dossier editorial.
    Cubre 6 dimensiones (flujo_peatonal, demografia, transporte, seguridad,
    turismo, dinamismo). Competencia tiene su propio análisis vía
    /api/competencia/{zona_id}/analisis-profundo y NO entra aquí.

    Cache: 30 días por (zona_id, dim_key, sector, perfil_hash). Re-clicks
    dentro de la misma sesión y mismo perfil devuelven from_cache=True.
    """
    from agente.analisis_dimension import (
        DIMS_SOPORTADAS,
        generar_narrativa_dimension,
        hash_perfil,
    )

    if dim_key not in DIMS_SOPORTADAS:
        raise HTTPException(
            status_code=400,
            detail=(
                f"dim_key '{dim_key}' no soportado. "
                f"Válidos: {sorted(DIMS_SOPORTADAS)}"
            ),
        )

    sesion = await get_sesion(session_id)
    if sesion is None:
        raise HTTPException(status_code=404, detail="Sesión no encontrada o expirada.")

    perfil = sesion.get("perfil") or {}
    sector = perfil.get("sector")
    if not sector:
        raise HTTPException(
            status_code=400,
            detail="Sector no especificado en la sesión.",
        )

    perfil_hash_val = hash_perfil(perfil)

    # 1. Cache check (TTL 30 días)
    async with get_db() as conn:
        cached = await conn.fetchrow(
            f"""
            SELECT lectura, decisiones, generado_at
            FROM narrativa_dimension_cache
            WHERE zona_id       = $1
              AND dim_key       = $2
              AND sector_codigo = $3
              AND perfil_hash   = $4
              AND generado_at >= NOW() - INTERVAL '{_NARRATIVA_TTL_DIAS} days'
            """,
            zona_id, dim_key, sector, perfil_hash_val,
        )

    if cached:
        decisiones_cached = cached["decisiones"]
        if isinstance(decisiones_cached, str):
            decisiones_cached = json.loads(decisiones_cached)
        return NarrativaDimensionResponse(
            zona_id=zona_id,
            dim_key=dim_key,
            sector=sector,
            lectura=cached["lectura"],
            decisiones=list(decisiones_cached or []),
            generado_at=cached["generado_at"].isoformat(),
            from_cache=True,
        )

    # 2. Cargar datos de la zona (vista agregada con la fila más reciente)
    async with get_db() as conn:
        zona_row = await conn.fetchrow(
            """
            SELECT vz.*
            FROM v_variables_zona vz
            WHERE vz.zona_id = $1
            ORDER BY vz.fecha DESC NULLS LAST
            LIMIT 1
            """,
            zona_id,
        )

    if zona_row is None:
        # Si no hay fila en v_variables_zona, comprobamos al menos que la zona
        # exista para devolver 404 honesto.
        async with get_db() as conn:
            existe = await conn.fetchval(
                "SELECT 1 FROM zonas WHERE id = $1",
                zona_id,
            )
        if existe is None:
            raise HTTPException(
                status_code=404,
                detail=f"Zona '{zona_id}' no encontrada.",
            )
        zona_dict: dict = {}
    else:
        zona_dict = dict(zona_row)
        # flujo_peatonal_dia puede venir como JSON string en algunos pipelines
        fpd = zona_dict.get("flujo_peatonal_dia")
        if isinstance(fpd, str):
            try:
                zona_dict["flujo_peatonal_dia"] = json.loads(fpd)
            except (json.JSONDecodeError, TypeError):
                pass

    # 3. LLM
    try:
        result = await generar_narrativa_dimension(
            dim_key=dim_key,
            zona=zona_dict,
            sector=sector,
            subsector=perfil.get("subsector"),
            perfil=perfil,
            session_id=session_id,
        )
    except Exception as exc:
        logger.error(
            "narrativa_dimension dim=%s zona=%s error: %s",
            dim_key, zona_id, exc,
        )
        raise HTTPException(
            status_code=502,
            detail=f"El servicio de análisis no respondió: {exc}",
        ) from exc

    # 4. Persistir y devolver
    async with get_db() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO narrativa_dimension_cache
                (zona_id, dim_key, sector_codigo, perfil_hash,
                 lectura, decisiones, generado_at)
            VALUES ($1, $2, $3, $4, $5, $6::jsonb, NOW())
            ON CONFLICT (zona_id, dim_key, sector_codigo, perfil_hash) DO UPDATE SET
                lectura     = EXCLUDED.lectura,
                decisiones  = EXCLUDED.decisiones,
                generado_at = NOW()
            RETURNING generado_at
            """,
            zona_id, dim_key, sector, perfil_hash_val,
            result["lectura"], json.dumps(result["decisiones"]),
        )

    return NarrativaDimensionResponse(
        zona_id=zona_id,
        dim_key=dim_key,
        sector=sector,
        lectura=result["lectura"],
        decisiones=result["decisiones"],
        generado_at=row["generado_at"].isoformat(),
        from_cache=False,
    )
