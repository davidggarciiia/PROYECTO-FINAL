"""db/zonas.py — Queries PostGIS para zonas, locales y filtrado espacial."""
from __future__ import annotations
import logging
from typing import Any, Optional
from db.conexion import get_db

logger = logging.getLogger(__name__)

# La zona centro puede devolver cientos de negocios dentro de 500 m. El score se
# calcula con todos, pero el dossier solo necesita los competidores accionables.
_MAX_AMENAZAS_VISIBLES = 24
_MAX_OPORTUNIDADES_VISIBLES = 24
_MAX_SINERGICOS_VISIBLES = 16


_FILTROS_CANDIDATAS = {"distritos", "presupuesto_max", "m2_min", "m2_max"}
_FILTROS_LISTA = {"score_min", "alquiler_max", "distrito"}


async def filtrar_zonas_candidatas(filtros: dict) -> list[dict]:
    """
    Filtra zonas según los criterios del usuario usando PostGIS + SQL.
    Devuelve hasta 100 candidatas sin score (el scoring va en Celery).
    """
    extras = set(filtros.keys()) - _FILTROS_CANDIDATAS
    if extras:
        logger.warning("Filtros candidatas desconocidos ignorados: %s", extras)
    conditions = ["1=1"]
    params: list = []
    i = 1

    if filtros.get("distritos"):
        conditions.append(f"d.nombre = ANY(${i})")
        params.append(filtros["distritos"]); i += 1
    if filtros.get("presupuesto_max"):
        conditions.append(f"l.alquiler_mensual <= ${i}")
        params.append(float(filtros["presupuesto_max"])); i += 1
    if filtros.get("m2_min"):
        conditions.append(f"l.m2 >= ${i}")
        params.append(float(filtros["m2_min"])); i += 1
    if filtros.get("m2_max"):
        conditions.append(f"l.m2 <= ${i}")
        params.append(float(filtros["m2_max"])); i += 1

    where = " AND ".join(conditions)

    async with get_db() as conn:
        rows = await conn.fetch(f"""
            SELECT DISTINCT ON (z.id)
                z.id                           AS zona_id,
                z.nombre,
                b.nombre                       AS barrio,
                d.nombre                       AS distrito,
                ST_X(ST_Centroid(z.geometria)) AS lng,
                ST_Y(ST_Centroid(z.geometria)) AS lat,
                l.m2,
                l.alquiler_mensual,
                l.escaparate_ml,
                l.esta_disponible
            FROM zonas z
            JOIN barrios b      ON b.id = z.barrio_id
            JOIN distritos d    ON d.id = b.distrito_id
            LEFT JOIN locales l ON l.zona_id = z.id AND l.esta_disponible=TRUE AND l.planta='PB'
            WHERE {where}
            ORDER BY z.id, l.alquiler_mensual ASC NULLS LAST
            LIMIT 100
        """, *params)
    return [dict(r) for r in rows]


async def get_zona_preview(zona_id: str, sector: Optional[str]) -> Optional[dict]:
    """Datos mínimos para el tooltip del mapa (< 200ms)."""
    async with get_db() as conn:
        row = await conn.fetchrow("""
            SELECT
                z.id AS zona_id, z.nombre,
                b.nombre AS barrio, d.nombre AS distrito,
                ST_X(ST_Centroid(z.geometria)) AS lng,
                ST_Y(ST_Centroid(z.geometria)) AS lat,
                l.direccion, l.m2, l.alquiler_mensual,
                sz.score_global
            FROM zonas z
            JOIN barrios b      ON b.id = z.barrio_id
            JOIN distritos d    ON d.id = b.distrito_id
            LEFT JOIN LATERAL (
                SELECT direccion, m2, alquiler_mensual
                FROM locales WHERE zona_id=z.id AND planta='PB'
                ORDER BY esta_disponible DESC, alquiler_mensual ASC NULLS LAST LIMIT 1
            ) l ON TRUE
            LEFT JOIN LATERAL (
                SELECT sz2.score_global
                FROM scores_zona sz2
                JOIN sectores s ON s.id=sz2.sector_id
                WHERE sz2.zona_id=z.id AND (s.codigo=$2 OR $2 IS NULL)
                ORDER BY sz2.fecha_calculo DESC LIMIT 1
            ) sz ON TRUE
            WHERE z.id=$1
        """, zona_id, sector)
    return dict(row) if row else None


async def get_zona_completa(zona_id: str, sector: Optional[str]) -> Optional[dict]:
    """Datos completos para el panel de detalle de zona."""
    async with get_db() as conn:
        async with conn.transaction(isolation="repeatable_read", readonly=True):
            row = await conn.fetchrow("""
                SELECT
                    z.id AS zona_id, z.nombre,
                    b.nombre AS barrio, d.nombre AS distrito,
                    ST_X(ST_Centroid(z.geometria)) AS lng,
                    ST_Y(ST_Centroid(z.geometria)) AS lat,
                    l.id AS local_id, l.direccion, l.m2, l.planta,
                    l.escaparate_ml, l.alquiler_mensual, l.esta_disponible,
                    vz.flujo_peatonal_manana, vz.flujo_peatonal_tarde,
                    vz.flujo_peatonal_noche, vz.flujo_peatonal_total,
                    vz.flujo_popular_times_score,
                    vz.vcity_flujo_peatonal,
                    vz.weekend_lift, vz.sunday_lift,
                    vz.weekday_midday_share, vz.weekend_evening_share,
                    vz.late_night_share, vz.holiday_proxy_score,
                    vz.temporal_confianza,
                    vz.seasonality_summer_lift, vz.seasonality_christmas_lift,
                    vz.seasonality_rebajas_lift, vz.seasonality_volatility,
                    vz.seasonality_peak_concentration,
                    -- demographics: COALESCE latest row with best demographics row
                    -- (VCity pipeline 2026 has no demographics; padró 2022 has them)
                    COALESCE(vz.renta_media_hogar,       vz_d.renta_media_hogar)       AS renta_media_hogar,
                    COALESCE(vz.edad_media,              vz_d.edad_media)              AS edad_media,
                    COALESCE(vz.pct_extranjeros,         vz_d.pct_extranjeros)         AS pct_extranjeros,
                    vz.score_turismo, vz.score_turismo_airbnb, vz.score_turismo_hut,
                    vz.eventos_culturales_500m, vz.venues_musicales_500m,
                    vz.dist_landmark_top3_m, vz.vcity_tourist_rate, vz.booking_rating_medio,
                    vz.num_negocios_activos,
                    vz.pct_locales_vacios, vz.nivel_ruido_db,
                    vz.tasa_rotacion_anual, vz.score_equipamientos,
                    -- seguridad v7: desglose por tipo + nocturnidad + policía
                    -- (COALESCE with vz_seg because seguridad pipeline writes a separate row)
                    COALESCE(vz.incidencias_por_1000hab, vz_seg.incidencias_por_1000hab) AS incidencias_por_1000hab,
                    COALESCE(vz.hurtos_por_1000hab,      vz_seg.hurtos_por_1000hab)      AS hurtos_por_1000hab,
                    COALESCE(vz.robatoris_por_1000hab,   vz_seg.robatoris_por_1000hab)   AS robatoris_por_1000hab,
                    COALESCE(vz.danys_por_1000hab,       vz_seg.danys_por_1000hab)       AS danys_por_1000hab,
                    COALESCE(vz.incidencias_noche_pct,   vz_seg.incidencias_noche_pct)   AS incidencias_noche_pct,
                    COALESCE(vz.comisarias_1km,          vz_seg.comisarias_1km)          AS comisarias_1km,
                    COALESCE(vz.dist_comisaria_m,        vz_seg.dist_comisaria_m)        AS dist_comisaria_m,
                    COALESCE(vz.seguridad_barri_score,   vz_seg.seguridad_barri_score)   AS seguridad_barri_score,
                    -- turismo y entorno extra
                    vz.airbnb_density_500m, vz.booking_hoteles_500m,
                    vz.m2_zonas_verdes_cercanas, vz.ratio_locales_comerciales,
                    COALESCE(vz.densidad_hab_km2,        vz_d.densidad_hab_km2)        AS densidad_hab_km2,
                    COALESCE(vz.poblacion,               vz_d.poblacion)               AS poblacion,
                    COALESCE(vz.pct_poblacio_25_44,      vz_d.pct_poblacio_25_44)      AS pct_poblacio_25_44,
                    COALESCE(vz.delta_renta_3a,          vz_d.delta_renta_3a)          AS delta_renta_3a,
                    COALESCE(vz.nivel_estudios_alto_pct, vz_d.nivel_estudios_alto_pct) AS nivel_estudios_alto_pct,
                    COALESCE(vz.gini,                    vz_d.gini)                    AS gini,
                    COALESCE(vz.p80_p20,                 vz_d.p80_p20)                 AS p80_p20,
                    COALESCE(vz.tamano_hogar,            vz_d.tamano_hogar)            AS tamano_hogar,
                    COALESCE(vz.hogares_con_menores,     vz_d.hogares_con_menores)     AS hogares_con_menores,
                    COALESCE(vz.personas_solas,          vz_d.personas_solas)          AS personas_solas,
                    COALESCE(vz.renta_media_uc,          vz_d.renta_media_uc)          AS renta_media_uc,
                    COALESCE(vz.renta_mediana_uc,        vz_d.renta_mediana_uc)        AS renta_mediana_uc,
                    vz.licencias_nuevas_1a, vz.eventos_culturales_500m,
                    vz.mercados_municipales_1km,
                    sz.score_global, sz.score_flujo_peatonal, sz.score_demografia,
                    sz.score_competencia, sz.score_precio_alquiler, sz.score_transporte,
                    sz.score_seguridad, sz.score_turismo AS score_turismo_dim,
                    sz.probabilidad_supervivencia,
                    sz.shap_values, sz.modelo_version,
                    -- dinamismo comercial histórico (pipeline mensual día 6)
                    dz.score_dinamismo, dz.tendencia,
                    dz.tasa_supervivencia_3a, dz.renta_variacion_3a,
                    dz.hhi_sectorial, dz.negocios_historico_count,
                    dz.ratio_apertura_cierre_1a,
                    -- counts crudos + señales infrautilizadas (F2)
                    dz.licencias_abiertas_1a, dz.licencias_cerradas_1a,
                    dz.vacantes_ratio, dz.poblacion_variacion_3a,
                    dz.precio_alquiler_variacion_1a
                FROM zonas z
                JOIN barrios b      ON b.id = z.barrio_id
                JOIN distritos d    ON d.id = b.distrito_id
                LEFT JOIN LATERAL (
                    SELECT * FROM locales
                    WHERE zona_id=z.id AND esta_disponible=TRUE AND planta='PB'
                    ORDER BY alquiler_mensual ASC NULLS LAST LIMIT 1
                ) l ON TRUE
                LEFT JOIN LATERAL (
                    SELECT * FROM v_variables_zona WHERE zona_id=z.id
                    ORDER BY (vcity_tourist_rate IS NOT NULL) DESC,
                             (flujo_peatonal_manana IS NOT NULL) DESC,
                             fecha DESC
                    LIMIT 1
                ) vz ON TRUE
                -- Demographics from a separate row (padró/renta pipelines use older dates)
                LEFT JOIN LATERAL (
                    SELECT renta_media_hogar, edad_media, pct_extranjeros,
                           pct_poblacio_25_44, delta_renta_3a, nivel_estudios_alto_pct,
                           gini, p80_p20, tamano_hogar, hogares_con_menores,
                           personas_solas, renta_media_uc, renta_mediana_uc,
                           densidad_hab_km2, poblacion
                    FROM v_variables_zona WHERE zona_id=z.id AND renta_media_hogar IS NOT NULL
                    ORDER BY fecha DESC LIMIT 1
                ) vz_d ON TRUE
                -- Security from a separate row (seguridad_gu pipeline uses date=today)
                LEFT JOIN LATERAL (
                    SELECT incidencias_por_1000hab, hurtos_por_1000hab,
                           robatoris_por_1000hab, danys_por_1000hab,
                           incidencias_noche_pct, comisarias_1km,
                           dist_comisaria_m, seguridad_barri_score
                    FROM v_variables_zona WHERE zona_id=z.id AND incidencias_por_1000hab IS NOT NULL
                    ORDER BY fecha DESC LIMIT 1
                ) vz_seg ON TRUE
                LEFT JOIN LATERAL (
                    SELECT sz2.*
                    FROM scores_zona sz2
                    JOIN sectores s ON s.id=sz2.sector_id
                    WHERE sz2.zona_id=z.id AND (s.codigo=$2 OR $2 IS NULL)
                    ORDER BY sz2.fecha_calculo DESC LIMIT 1
                ) sz ON TRUE
                LEFT JOIN LATERAL (
                    SELECT
                        dz2.score_dinamismo,
                        dz2.tendencia,
                        dz2.tasa_supervivencia_3a,
                        dz2.renta_variacion_3a,
                        dz2.hhi_sectorial,
                        dz2.negocios_historico_count,
                        dz2.ratio_apertura_cierre_1a,
                        -- counts crudos + señales infrautilizadas (F2)
                        dz2.licencias_abiertas_1a,
                        dz2.licencias_cerradas_1a,
                        dz2.vacantes_ratio,
                        dz2.poblacion_variacion_3a,
                        dz2.precio_alquiler_variacion_1a
                    FROM v_dinamismo_zona dz2
                    WHERE dz2.zona_id = z.id
                ) dz ON TRUE
                WHERE z.id=$1
            """, zona_id, sector)

            if not row:
                return None

            result = dict(row)
            result["flujo_peatonal_dia"] = {
                "manana": int(result.pop("flujo_peatonal_manana") or 0),
                "tarde":  int(result.pop("flujo_peatonal_tarde")  or 0),
                "noche":  int(result.pop("flujo_peatonal_noche")  or 0),
            }

            # Competidores cercanos 500m
            competidores = await conn.fetch("""
                SELECT na.nombre, na.sector_codigo AS sector,
                       round(ST_Distance(na.geometria::geography, z.geometria::geography)::numeric, 0) AS distancia_m,
                       na.rating, na.total_resenas AS num_resenas, na.precio_nivel,
                       COALESCE(na.sector_codigo = $2, FALSE) AS es_competencia_directa
                FROM negocios_activos na
                JOIN zonas z ON z.id=$1
                WHERE ST_DWithin(na.geometria::geography, z.geometria::geography, 500)
                  AND na.es_activo=TRUE
                ORDER BY distancia_m ASC LIMIT 20
            """, zona_id, sector)
            result["competidores_cercanos"] = [dict(c) for c in competidores]

            # Líneas de transporte a 500m
            trans = await conn.fetchrow("""
                SELECT
                    COUNT(DISTINCT pl.linea_id)::int AS num_lineas,
                    COUNT(DISTINCT pt.id)::int       AS num_paradas
                FROM paradas_transporte pt
                JOIN paradas_lineas pl ON pl.parada_id=pt.id
                JOIN zonas z ON z.id=$1
                WHERE ST_DWithin(pt.geometria::geography, z.geometria::geography, 500)
            """, zona_id)
            result["num_lineas_transporte"] = trans["num_lineas"] if trans else 0
            result["num_paradas_transporte"] = trans["num_paradas"] if trans else 0

            lineas = await conn.fetch(
                """
                SELECT
                    lt.codigo,
                    lt.tipo,
                    MIN(ST_Distance(
                        pt.geometria::geography,
                        ST_Centroid(z.geometria)::geography
                    ))::int AS distancia_m,
                    AVG(ft.frecuencia_min)::float AS frecuencia_media_min
                FROM zonas z
                JOIN paradas_transporte pt
                  ON ST_DWithin(pt.geometria::geography, ST_Centroid(z.geometria)::geography, 500)
                JOIN paradas_lineas pl ON pl.parada_id = pt.id
                JOIN lineas_transporte lt ON lt.id = pl.linea_id
                LEFT JOIN frecuencias_transporte ft
                  ON ft.linea_id = lt.id
                 AND ft.dia_tipo = 'laborable'
                 AND ft.franja = 'manana'
                WHERE z.id = $1
                GROUP BY lt.codigo, lt.tipo
                ORDER BY distancia_m ASC, frecuencia_media_min ASC NULLS LAST, lt.codigo
                LIMIT 8
                """,
                zona_id,
            )
            result["transporte_lineas_cercanas"] = [dict(row) for row in lineas]

            try:
                bicing = await conn.fetchval(
                    """
                    SELECT COUNT(*)::int
                    FROM estaciones_bicing eb
                    JOIN zonas z ON z.id = $1
                    WHERE ST_DWithin(
                        eb.geometria::geography,
                        ST_Centroid(z.geometria)::geography,
                        400
                    )
                    """,
                    zona_id,
                )
            except Exception:
                bicing = None
            result["num_bicing_400m"] = bicing

            try:
                carril = await conn.fetchval(
                    """
                    SELECT EXISTS(
                        SELECT 1
                        FROM carriles_bici cb
                        JOIN zonas z ON z.id = $1
                        WHERE ST_DWithin(
                            cb.geometria::geography,
                            ST_Centroid(z.geometria)::geography,
                            200
                        )
                    )::boolean
                    """,
                    zona_id,
                )
            except Exception:
                carril = None
            result["tiene_carril_bici"] = carril

            try:
                festive_ratio = await conn.fetchval(
                    """
                    SELECT
                        AVG(CASE WHEN ft.dia_tipo = 'festivo' THEN 1.0 / NULLIF(ft.frecuencia_min, 0) END)
                        /
                        NULLIF(AVG(CASE WHEN ft.dia_tipo = 'laborable' THEN 1.0 / NULLIF(ft.frecuencia_min, 0) END), 0)
                    FROM zonas z
                    JOIN paradas_transporte pt
                      ON ST_DWithin(pt.geometria::geography, ST_Centroid(z.geometria)::geography, 500)
                    JOIN paradas_lineas pl ON pl.parada_id = pt.id
                    JOIN frecuencias_transporte ft ON ft.linea_id = pl.linea_id
                    WHERE z.id = $1
                    """,
                    zona_id,
                )
            except Exception:
                festive_ratio = None
            result["transporte_festivo_ratio"] = festive_ratio

            # Alertas NLP activas
            alertas = await conn.fetch(
                "SELECT tipo, texto, fuente FROM alertas_zona WHERE zona_id=$1 AND esta_activa=TRUE ORDER BY fecha DESC LIMIT 10",
                zona_id)
            result["alertas"] = [dict(a) for a in alertas]

    return result


async def get_zonas_sesion(session_id: str, sector: str) -> list[dict]:
    """Devuelve las zonas de la última búsqueda de esta sesión."""
    from db.sesiones import get_sesion
    sesion = await get_sesion(session_id)
    if not sesion:
        return []
    zonas = list(sesion.get("zonas_actuales", []))
    for z in zonas:
        if "probabilidad_supervivencia_3a" not in z:
            z["probabilidad_supervivencia_3a"] = z.pop("probabilidad_supervivencia", None) or 0.5
    return zonas


async def get_zonas_lista(filtros: dict, sector: str,
                          pagina: int = 1, por_pagina: int = 20) -> dict:
    """Vista lista con filtros, paginación y filtros disponibles."""
    extras = set(filtros.keys()) - _FILTROS_LISTA
    if extras:
        logger.warning("Filtros lista desconocidos ignorados: %s", extras)
    offset = (pagina - 1) * por_pagina
    conditions = ["sz.score_global IS NOT NULL"]
    params: list = [sector]
    i = 2

    if filtros.get("score_min"):
        conditions.append(f"sz.score_global >= ${i}"); params.append(float(filtros["score_min"])); i += 1
    if filtros.get("alquiler_max"):
        conditions.append(f"l.alquiler_mensual <= ${i}"); params.append(float(filtros["alquiler_max"])); i += 1
    if filtros.get("distrito"):
        conditions.append(f"d.nombre = ${i}"); params.append(filtros["distrito"]); i += 1

    where = " AND ".join(conditions)

    async with get_db() as conn:
        total = await conn.fetchval(f"""
            SELECT COUNT(DISTINCT z.id)
            FROM zonas z
            JOIN barrios b ON b.id=z.barrio_id
            JOIN distritos d ON d.id=b.distrito_id
            LEFT JOIN LATERAL (
                SELECT * FROM locales WHERE zona_id=z.id AND planta='PB'
                ORDER BY alquiler_mensual ASC NULLS LAST LIMIT 1
            ) l ON TRUE
            LEFT JOIN LATERAL (
                SELECT sz2.* FROM scores_zona sz2
                JOIN sectores s ON s.id=sz2.sector_id
                WHERE sz2.zona_id=z.id AND s.codigo=$1
                ORDER BY sz2.fecha_calculo DESC LIMIT 1
            ) sz ON TRUE
            WHERE {where}
        """, *params)

        rows = await conn.fetch(f"""
            SELECT DISTINCT ON (z.id)
                z.id AS zona_id, z.nombre,
                b.nombre AS barrio, d.nombre AS distrito,
                ST_X(ST_Centroid(z.geometria)) AS lng,
                ST_Y(ST_Centroid(z.geometria)) AS lat,
                l.m2, l.alquiler_mensual, l.esta_disponible,
                sz.score_global, sz.probabilidad_supervivencia
            FROM zonas z
            JOIN barrios b ON b.id=z.barrio_id
            JOIN distritos d ON d.id=b.distrito_id
            LEFT JOIN LATERAL (
                SELECT * FROM locales WHERE zona_id=z.id AND planta='PB'
                ORDER BY alquiler_mensual ASC NULLS LAST LIMIT 1
            ) l ON TRUE
            LEFT JOIN LATERAL (
                SELECT sz2.* FROM scores_zona sz2
                JOIN sectores s ON s.id=sz2.sector_id
                WHERE sz2.zona_id=z.id AND s.codigo=$1
                ORDER BY sz2.fecha_calculo DESC LIMIT 1
            ) sz ON TRUE
            WHERE {where}
            ORDER BY z.id, sz.score_global DESC NULLS LAST
            LIMIT ${i} OFFSET ${i+1}
        """, *params, por_pagina, offset)

        # Filtros disponibles (sobre universo completo, no filtrado)
        distritos = await conn.fetch("SELECT DISTINCT d.nombre FROM distritos d ORDER BY d.nombre")

    return {
        "total": total or 0,
        "pagina": pagina,
        "por_pagina": por_pagina,
        "zonas": [dict(r) for r in rows],
        "filtros_disponibles": {
            "distritos": [r["nombre"] for r in distritos],
        },
    }


async def get_competencia_zona(zona_id: str, sector: str, radio_m: int = 500,
                                subsector_usuario: Optional[str] = None) -> Optional[dict]:
    """
    Devuelve los datos de competencia completos para la zona.

    1. Intenta leer scores pre-calculados de competencia_detalle_zona.
    2. Lee negocios_activos en radio 500m con datos individuales completos.
    3. Clasifica competidores usando scoring.competencia.
    4. Calcula amenaza_score individual por competidor.
    5. Analiza gap de precio del segmento.

    Si `subsector_usuario` se pasa, cada competidor que comparta subsector
    exacto con el usuario llevará `es_competencia_directa_subsector=True`.
    Esa señal alimenta el FIFA-link púrpura del mini-mapa.

    Returns dict compatible con CompetenciaDetalle, or None if zona not found.
    """
    from scoring.dimensiones.competencia import (
        NegocioCompetidor,
        negocios_desde_rows,
        calcular_score_competencia,
        amenaza_score_individual,
        analizar_precio_segmento,
        _es_vulnerable,
        SECTORES_COMPETIDORES,
        SECTORES_COMPLEMENTARIOS,
        SINERGIA_MAX_M,
    )

    async with get_db() as conn:
        # Verificar que la zona existe
        zona_exists = await conn.fetchval("SELECT 1 FROM zonas WHERE id=$1", zona_id)
        if not zona_exists:
            return None

        # 1. Intentar leer scores pre-calculados (pipeline mensual de competencia)
        scores_row = await conn.fetchrow("""
            SELECT score_competencia_v2, cluster_score, amenaza_incumbentes,
                   oportunidad_mercado, ratio_complementarios, score_complementarios,
                   num_directos, pct_vulnerables, hhi_index, fuente
            FROM competencia_detalle_zona
            WHERE zona_id=$1 AND sector_codigo=$2 AND radio_m=$3
            ORDER BY fecha DESC LIMIT 1
        """, zona_id, sector, radio_m)

        # 2. Leer negocios_activos en radio con todos los campos
        # (incluye lat/lng y subsector_codigo para el mini-mapa del frontend)
        negocios_rows = await conn.fetch("""
            SELECT
                na.id,
                na.nombre,
                na.sector_codigo,
                na.subsector_codigo,
                na.lat,
                na.lng,
                na.rating,
                na.total_resenas AS num_resenas,
                na.precio_nivel,
                round(ST_Distance(
                    na.geometria::geography,
                    ST_Centroid(z.geometria)::geography
                )::numeric, 0)::float AS distancia_m
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

    rows = [dict(r) for r in negocios_rows]

    # 3. Construir NegocioCompetidor para cálculo de scores
    # negocios_desde_rows filtra por sectores relevantes
    negocios = negocios_desde_rows(rows, sector)

    # 4. Si no hay scores pre-calculados, calcularlos al vuelo
    if scores_row:
        scores = {
            "score_competencia":     float(scores_row["score_competencia_v2"] or 50.0),
            "score_cluster":         float(scores_row["cluster_score"] or 50.0),
            "amenaza_incumbentes":   float(scores_row["amenaza_incumbentes"] or 50.0),
            "oportunidad_mercado":   float(scores_row["oportunidad_mercado"] or 50.0),
            # score_complementarios: use the exact value stored by the pipeline when
            # available (migration 028+). For older rows where the column is NULL,
            # fall back to the ratio approximation (ratio/1.5*100 ≈ ratio*66.67).
            # The *100 approximation used previously overestimated by 1.5× vs the
            # fly-calc path which calls _score_complementarios() directly.
            "score_complementarios": round(
                float(scores_row["score_complementarios"])
                if scores_row["score_complementarios"] is not None
                else min(100.0, float(scores_row["ratio_complementarios"] or 0.0) * 66.67),
                1,
            ),
            "num_directos":          int(scores_row["num_directos"] or 0),
            "pct_vulnerables":       float(scores_row["pct_vulnerables"] or 0.0),
            "hhi_index":             float(scores_row["hhi_index"] or 0.0),
            "ratio_complementarios": float(scores_row["ratio_complementarios"] or 0.0),
            "fuente":                scores_row["fuente"] or "google_places",
            "datos_calculados":      False,
        }
    else:
        calc = calcular_score_competencia(negocios, sector)
        scores = {
            "score_competencia":     calc["score_competencia"],
            "score_cluster":         calc["score_cluster"],
            "amenaza_incumbentes":   calc["amenaza_incumbentes"],
            "oportunidad_mercado":   calc["score_oportunidad"],
            "score_complementarios": calc["score_complementarios"],
            "num_directos":          calc["num_directos"],
            "pct_vulnerables":       calc["pct_vulnerables"],
            "hhi_index":             calc["hhi_index"],
            "ratio_complementarios": calc["ratio_complementarios"],
            "fuente":                "calculado_al_vuelo",
            "datos_calculados":      True,
        }

    # 5. Clasificar competidores en 3 grupos construyendo NegocioCompetidor desde rows
    sectores_comp  = SECTORES_COMPETIDORES.get(sector, [sector])
    sectores_compl = SECTORES_COMPLEMENTARIOS.get(sector, [])

    amenaza_list     = []
    oportunidad_list = []
    sinergicos_list  = []

    for row in rows:
        sc = row.get("sector_codigo")
        if sc not in sectores_comp and sc not in sectores_compl:
            continue

        neg = NegocioCompetidor(
            sector_codigo=sc,
            distancia_m=float(row.get("distancia_m") or 500),
            rating=row.get("rating"),
            num_resenas=row.get("num_resenas") or 0,
            precio_nivel=row.get("precio_nivel"),
        )
        es_directo = sc in sectores_comp
        # Sinergia real sólo si el complementario está lo bastante cerca
        # como para formar eje comercial compacto con la zona objetivo.
        es_compl   = (
            sc in sectores_compl
            and not es_directo
            and neg.distancia_m <= SINERGIA_MAX_M
        )
        vulnerable = _es_vulnerable(neg)

        row_sub = row.get("subsector_codigo")
        es_directo_sub = bool(
            es_directo and subsector_usuario
            and row_sub and row_sub == subsector_usuario
        )

        entry = {
            "id":                     str(row.get("id")) if row.get("id") is not None else None,
            "nombre":                 row.get("nombre") or "Sin nombre",
            "sector":                 sc,
            "subsector":              row_sub,
            "lat":                    row.get("lat"),
            "lng":                    row.get("lng"),
            "distancia_m":            neg.distancia_m,
            "rating":                 neg.rating,
            "num_resenas":            neg.num_resenas,
            "precio_nivel":           neg.precio_nivel,
            "es_competencia_directa": es_directo,
            "es_competencia_directa_subsector": es_directo_sub,
            "es_complementario":      es_compl,
            "es_vulnerable":          vulnerable,
            "amenaza_score":          amenaza_score_individual(neg) if es_directo else None,
            "resenas_resumen":        _empty_resenas()["resumen"],
            "resenas_destacadas":     [],
        }

        if es_directo and not vulnerable:
            amenaza_list.append(entry)
        elif es_directo and vulnerable:
            oportunidad_list.append(entry)
        elif es_compl:
            sinergicos_list.append(entry)

    # Ordenar listas
    amenaza_list.sort(key=lambda x: x.get("amenaza_score") or 0, reverse=True)
    oportunidad_list.sort(key=lambda x: x.get("distancia_m") or 9999)
    sinergicos_list.sort(key=lambda x: x.get("distancia_m") or 9999)

    # 6. Análisis de precio del segmento (sólo con directos)
    directos = [n for n in negocios if n.sector_codigo in sectores_comp]
    precio_seg = analizar_precio_segmento(directos, precio_objetivo=2)

    # 7. Limitar el payload visual y anexar reseñas solo para lo que se pinta.
    amenaza_list = amenaza_list[:_MAX_AMENAZAS_VISIBLES]
    oportunidad_list = oportunidad_list[:_MAX_OPORTUNIDADES_VISIBLES]
    sinergicos_list = sinergicos_list[:_MAX_SINERGICOS_VISIBLES]
    entries_visibles = amenaza_list + oportunidad_list + sinergicos_list
    resenas_por_negocio = await _get_resenas_por_negocio(
        [str(e["id"]) for e in entries_visibles if e.get("id")]
    )
    for entry in entries_visibles:
        review_data = resenas_por_negocio.get(str(entry.get("id")), _empty_resenas())
        entry["resenas_resumen"] = review_data["resumen"]
        entry["resenas_destacadas"] = review_data["destacadas"]

    return {
        "zona_id": zona_id,
        "sector":  sector,
        "radio_m": radio_m,
        **scores,
        "precio_segmento": precio_seg,
        "resenas_cobertura": _build_resenas_cobertura(entries_visibles),
        "amenaza":     amenaza_list,
        "oportunidad": oportunidad_list,
        "sinergicos":  sinergicos_list,
    }


def _empty_resenas() -> dict[str, Any]:
    return {
        "resumen": {
            "total": 0,
            "positivas": 0,
            "negativas": 0,
            "neutras": 0,
            "rating_medio": None,
            "temas_top": [],
            "ultima_fecha": None,
        },
        "destacadas": [],
    }


def _build_resenas_cobertura(entries: list[dict]) -> dict[str, Any]:
    negocios_total = len(entries)
    resenas_total = sum(int((e.get("resenas_resumen") or {}).get("total") or 0) for e in entries)
    negocios_con_resenas = sum(
        1 for e in entries if int((e.get("resenas_resumen") or {}).get("total") or 0) > 0
    )
    return {
        "negocios_total": negocios_total,
        "negocios_con_resenas": negocios_con_resenas,
        "resenas_total": resenas_total,
        "cobertura_pct": round(negocios_con_resenas / negocios_total * 100, 1) if negocios_total else 0.0,
    }


async def _get_resenas_por_negocio(negocio_ids: list[str]) -> dict[str, dict[str, Any]]:
    if not negocio_ids:
        return {}

    result = {nid: _empty_resenas() for nid in negocio_ids}
    async with get_db() as conn:
        resumen_rows = await conn.fetch(
            """
            SELECT
                negocio_id,
                COUNT(*)::int AS total,
                COUNT(*) FILTER (WHERE sentimiento = 'positivo')::int AS positivas,
                COUNT(*) FILTER (WHERE sentimiento = 'negativo')::int AS negativas,
                COUNT(*) FILTER (WHERE sentimiento = 'neutro' OR sentimiento IS NULL)::int AS neutras,
                AVG(rating)::float AS rating_medio,
                MAX(fecha)::text AS ultima_fecha
            FROM resenas
            WHERE negocio_id = ANY($1::varchar[])
            GROUP BY negocio_id
            """,
            negocio_ids,
        )

        destacadas_rows = await conn.fetch(
            """
            SELECT negocio_id, texto, rating::float AS rating, fecha::text AS fecha,
                   sentimiento, categoria, fuente
            FROM (
                SELECT
                    negocio_id, texto, rating, fecha, sentimiento, categoria, fuente, created_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY negocio_id
                        ORDER BY
                            CASE
                                WHEN sentimiento = 'negativo' THEN 0
                                WHEN sentimiento = 'positivo' THEN 1
                                ELSE 2
                            END,
                            fecha DESC NULLS LAST,
                            created_at DESC
                    ) AS rn
                FROM resenas
                WHERE negocio_id = ANY($1::varchar[])
                  AND texto IS NOT NULL
                  AND LENGTH(TRIM(texto)) > 0
            ) ranked
            WHERE rn <= 3
            ORDER BY negocio_id, rn
            """,
            negocio_ids,
        )

        temas_rows = await conn.fetch(
            """
            SELECT negocio_id, categoria, COUNT(*)::int AS n
            FROM resenas
            WHERE negocio_id = ANY($1::varchar[])
              AND categoria IS NOT NULL
            GROUP BY negocio_id, categoria
            ORDER BY negocio_id, n DESC
            """,
            negocio_ids,
        )

    for row in resumen_rows:
        nid = row["negocio_id"]
        result.setdefault(nid, _empty_resenas())
        result[nid]["resumen"].update({
            "total": int(row["total"] or 0),
            "positivas": int(row["positivas"] or 0),
            "negativas": int(row["negativas"] or 0),
            "neutras": int(row["neutras"] or 0),
            "rating_medio": round(float(row["rating_medio"]), 2) if row["rating_medio"] is not None else None,
            "ultima_fecha": _iso_or_none(row["ultima_fecha"]),
        })

    temas_por_negocio: dict[str, list[str]] = {}
    for row in temas_rows:
        nid = row["negocio_id"]
        temas_por_negocio.setdefault(nid, [])
        if len(temas_por_negocio[nid]) < 5 and row["categoria"]:
            temas_por_negocio[nid].append(str(row["categoria"]))
    for nid, temas in temas_por_negocio.items():
        result.setdefault(nid, _empty_resenas())
        result[nid]["resumen"]["temas_top"] = temas

    for row in destacadas_rows:
        nid = row["negocio_id"]
        result.setdefault(nid, _empty_resenas())
        result[nid]["destacadas"].append({
            "texto": row["texto"],
            "rating": float(row["rating"]) if row["rating"] is not None else None,
            "fecha": _iso_or_none(row["fecha"]),
            "sentimiento": row["sentimiento"],
            "categoria": row["categoria"],
            "fuente": row["fuente"] or "google_scrape",
        })

    return result


def _iso_or_none(value: Any) -> Optional[str]:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)
