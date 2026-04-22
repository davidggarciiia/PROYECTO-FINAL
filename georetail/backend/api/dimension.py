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

import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from db.conexion import get_db
from db.sesiones import get_sesion

logger = logging.getLogger(__name__)
router = APIRouter(tags=["dimension"])

_RADIO_M_DEFAULT = 500


# ═══════════════════════════════════════════════════════════════════════════════
# Turismo — drill-down completo
# ═══════════════════════════════════════════════════════════════════════════════


@router.get(
    "/dimension/turismo/{zona_id}",
    summary="Detalle narrativo de la dimensión de turismo",
)
async def detalle_turismo(
    zona_id: str,
    session_id: str = Query(..., description="ID de sesión activa"),
    radio_m: int = Query(_RADIO_M_DEFAULT, ge=100, le=1500),
) -> dict:
    """
    Lista los landmarks turísticos, hoteles, apartamentos HUT y venues
    culturales cercanos a la zona, con distancia y nombre real. Usado por
    el drawer del panel de detalle (click en la barra "Turismo").
    """
    sesion = await get_sesion(session_id)
    if sesion is None:
        raise HTTPException(status_code=404, detail="Sesión no encontrada o expirada.")

    async with get_db() as conn:
        zona = await conn.fetchrow(
            "SELECT id, nombre FROM zonas WHERE id = $1", zona_id,
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

        # Top-8 landmarks turísticos por cercanía (nombre + distancia + wikidata).
        landmarks = await conn.fetch(
            """
            SELECT l.nombre,
                   l.wikidata_id,
                   l.peso_turistico,
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
            LIMIT 8
            """,
            zona_id, radio_m * 2,  # landmarks ampliamos a 2×radio (son pocos)
        )

        # Top-8 hoteles / alojamientos comerciales cercanos.
        hoteles = await conn.fetch(
            """
            SELECT a.nombre,
                   a.tipo,
                   a.estrellas,
                   a.rating,
                   ROUND(ST_Distance(
                       ST_Centroid(z.geometria)::geography,
                       a.geometria::geography
                   )::numeric, 0)::int AS distancia_m
            FROM alojamientos_turisticos a
            JOIN zonas z ON z.id = $1
            WHERE a.activo = TRUE
              AND ST_DWithin(
                  ST_Centroid(z.geometria)::geography,
                  a.geometria::geography,
                  $2
              )
            ORDER BY distancia_m ASC
            LIMIT 8
            """,
            zona_id, radio_m,
        )

        # Conteo agregado HUT: son 10k y no tiene sentido listarlos por nombre.
        hut_count = await conn.fetchval(
            """
            SELECT COUNT(*)::int
            FROM v_variables_zona vz
            WHERE vz.zona_id = $1
            """,
            zona_id,
        )
        # El conteo real de apartamentos HUT en radio: lo sacamos del CSV-derived
        # score_turismo_hut + total de HUT por barrio usando geom ad-hoc si haría
        # falta. Aquí preferimos devolver la señal que ya hemos persistido.
        hut_apartamentos_en_zona = None
        hut_row = await conn.fetchrow(
            """
            SELECT num_competidores
            FROM competencia_por_local
            WHERE zona_id = $1 LIMIT 1
            """,
            zona_id,
        )
        _ = hut_row  # placeholder — mantenemos señal de cobertura vía score_turismo_hut

        # Venues culturales + musicales cercanos con nombre.
        venues = await conn.fetch(
            """
            SELECT v.nom AS nombre,
                   v.tipo,
                   v.font AS fuente,
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
              AND v.nom NOT LIKE 'Venue OSM %'     -- filtrar IDs sin nombre real
            ORDER BY distancia_m ASC
            LIMIT 8
            """,
            zona_id, radio_m,
        )

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
            "score_turismo_hut":    feat.get("score_turismo_hut"),
        },
        "landmarks": [
            {
                "nombre":      r["nombre"],
                "distancia_m": r["distancia_m"],
                "wikidata_id": r["wikidata_id"],
                "peso":        float(r["peso_turistico"]) if r["peso_turistico"] is not None else None,
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
            }
            for r in hoteles
        ],
        "venues": [
            {
                "nombre":      r["nombre"],
                "tipo":        r["tipo"],
                "fuente":      r["fuente"],
                "distancia_m": r["distancia_m"],
            }
            for r in venues
        ],
    }
