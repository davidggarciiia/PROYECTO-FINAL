"""db/zonas.py — Queries PostGIS para zonas, locales y filtrado espacial."""
from __future__ import annotations
import logging
from typing import Optional
from db.conexion import get_db

logger = logging.getLogger(__name__)


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
                l.disponible
            FROM zonas z
            JOIN barrios b      ON b.id = z.barrio_id
            JOIN distritos d    ON d.id = b.distrito_id
            LEFT JOIN locales l ON l.zona_id = z.id AND l.disponible=TRUE AND l.planta='PB'
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
                ORDER BY disponible DESC, alquiler_mensual ASC NULLS LAST LIMIT 1
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
                    l.escaparate_ml, l.alquiler_mensual, l.disponible,
                    vz.flujo_peatonal_manana, vz.flujo_peatonal_tarde,
                    vz.flujo_peatonal_noche, vz.flujo_peatonal_total,
                    vz.flujo_popular_times_score,
                    vz.vcity_flujo_peatonal,
                    vz.renta_media_hogar, vz.edad_media, vz.pct_extranjeros,
                    vz.score_turismo, vz.num_negocios_activos,
                    vz.pct_locales_vacios, vz.nivel_ruido_db,
                    vz.tasa_rotacion_anual, vz.score_equipamientos,
                    -- seguridad v7: desglose por tipo + nocturnidad + policía
                    vz.incidencias_por_1000hab,
                    vz.hurtos_por_1000hab, vz.robatoris_por_1000hab,
                    vz.danys_por_1000hab, vz.incidencias_noche_pct,
                    vz.comisarias_1km, vz.dist_comisaria_m,
                    vz.seguridad_barri_score,
                    -- turismo y entorno extra
                    vz.airbnb_density_500m, vz.booking_hoteles_500m,
                    vz.m2_zonas_verdes_cercanas, vz.ratio_locales_comerciales,
                    vz.densidad_hab_km2, vz.poblacion,
                    vz.pct_poblacio_25_44, vz.delta_renta_3a,
                    vz.nivel_estudios_alto_pct,
                    vz.licencias_nuevas_1a, vz.eventos_culturales_500m,
                    vz.mercados_municipales_1km,
                    sz.score_global, sz.score_flujo_peatonal, sz.score_demografia,
                    sz.score_competencia, sz.score_precio_alquiler, sz.score_transporte,
                    sz.score_seguridad, sz.score_turismo AS score_turismo_dim,
                    sz.score_entorno_comercial, sz.probabilidad_supervivencia,
                    sz.shap_values, sz.modelo_version
                FROM zonas z
                JOIN barrios b      ON b.id = z.barrio_id
                JOIN distritos d    ON d.id = b.distrito_id
                LEFT JOIN LATERAL (
                    SELECT * FROM locales
                    WHERE zona_id=z.id AND disponible=TRUE AND planta='PB'
                    ORDER BY alquiler_mensual ASC NULLS LAST LIMIT 1
                ) l ON TRUE
                LEFT JOIN LATERAL (
                    SELECT * FROM v_variables_zona WHERE zona_id=z.id
                    ORDER BY fecha DESC LIMIT 1
                ) vz ON TRUE
                LEFT JOIN LATERAL (
                    SELECT sz2.*
                    FROM scores_zona sz2
                    JOIN sectores s ON s.id=sz2.sector_id
                    WHERE sz2.zona_id=z.id AND (s.codigo=$2 OR $2 IS NULL)
                    ORDER BY sz2.fecha_calculo DESC LIMIT 1
                ) sz ON TRUE
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
                       na.rating, na.num_resenas, na.precio_nivel,
                       (na.sector_codigo = $2) AS es_competencia_directa
                FROM negocios_activos na
                JOIN zonas z ON z.id=$1
                WHERE ST_DWithin(na.geometria::geography, z.geometria::geography, 500)
                  AND na.activo=TRUE
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

            # Alertas NLP activas
            alertas = await conn.fetch(
                "SELECT tipo, texto, fuente FROM alertas_zona WHERE zona_id=$1 AND activa=TRUE ORDER BY fecha DESC LIMIT 10",
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
                l.m2, l.alquiler_mensual, l.disponible,
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


async def get_competencia_zona(zona_id: str, sector: str, radio_m: int = 500) -> Optional[dict]:
    """
    Devuelve los datos de competencia completos para la zona.

    1. Intenta leer scores pre-calculados de competencia_detalle_zona.
    2. Lee negocios_activos en radio 500m con datos individuales completos.
    3. Clasifica competidores usando scoring.competencia.
    4. Calcula amenaza_score individual por competidor.
    5. Analiza gap de precio del segmento.

    Returns dict compatible con CompetenciaDetalle, or None if zona not found.
    """
    from scoring.competencia import (
        NegocioCompetidor,
        negocios_desde_rows,
        calcular_score_competencia,
        amenaza_score_individual,
        analizar_precio_segmento,
        _es_vulnerable,
        SECTORES_COMPETIDORES,
        SECTORES_COMPLEMENTARIOS,
    )

    async with get_db() as conn:
        # Verificar que la zona existe
        zona_exists = await conn.fetchval("SELECT 1 FROM zonas WHERE id=$1", zona_id)
        if not zona_exists:
            return None

        # 1. Intentar leer scores pre-calculados (pipeline mensual de competencia)
        scores_row = await conn.fetchrow("""
            SELECT score_competencia_v2, cluster_score, amenaza_incumbentes,
                   oportunidad_mercado, ratio_complementarios,
                   num_directos, pct_vulnerables, hhi_index, fuente
            FROM competencia_detalle_zona
            WHERE zona_id=$1 AND sector_codigo=$2 AND radio_m=$3
            ORDER BY fecha DESC LIMIT 1
        """, zona_id, sector, radio_m)

        # 2. Leer negocios_activos en radio con todos los campos
        negocios_rows = await conn.fetch("""
            SELECT
                na.nombre,
                na.sector_codigo,
                na.rating,
                na.num_resenas,
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
              AND na.activo = TRUE
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
            "score_complementarios": round(float(scores_row["ratio_complementarios"] or 0.0) * 100.0, 1),
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
        es_compl   = sc in sectores_compl and not es_directo
        vulnerable = _es_vulnerable(neg)

        entry = {
            "nombre":                 row.get("nombre") or "Sin nombre",
            "sector":                 sc,
            "distancia_m":            neg.distancia_m,
            "rating":                 neg.rating,
            "num_resenas":            neg.num_resenas,
            "precio_nivel":           neg.precio_nivel,
            "es_competencia_directa": es_directo,
            "es_complementario":      es_compl,
            "es_vulnerable":          vulnerable,
            "amenaza_score":          amenaza_score_individual(neg) if es_directo else None,
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

    return {
        "zona_id": zona_id,
        "sector":  sector,
        "radio_m": radio_m,
        **scores,
        "precio_segmento": precio_seg,
        "amenaza":     amenaza_list,
        "oportunidad": oportunidad_list,
        "sinergicos":  sinergicos_list,
    }
