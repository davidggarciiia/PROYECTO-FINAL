"""db/zonas.py — Queries PostGIS para zonas, locales y filtrado espacial."""
from __future__ import annotations
import logging
from typing import Optional
from db.conexion import get_db
from db.sesiones import get_sesion

logger = logging.getLogger(__name__)


async def get_zonas_sesion(session_id: str, sector: str) -> list[dict]:
    """
    Devuelve todas las zonas de la última búsqueda de una sesión.
    Los datos se leen directamente de `zonas_actuales` en la sesión Redis/PG.
    """
    sesion = await get_sesion(session_id)
    if not sesion:
        return []
    zonas = sesion.get("zonas_actuales", [])
    # Normalizar nombres de campos para compatibilidad con LocalListItem
    result = []
    for z in zonas:
        result.append({
            "zona_id":   z.get("zona_id", ""),
            "nombre":    z.get("nombre", ""),
            "direccion": z.get("direccion") or z.get("calle"),
            "barrio":    z.get("barrio", ""),
            "distrito":  z.get("distrito", ""),
            "score_global": float(z.get("score_global", 50)),
            "alquiler_mensual": z.get("alquiler_estimado") or z.get("alquiler_mensual"),
            "m2":        z.get("m2_disponibles") or z.get("m2"),
            "disponible": z.get("disponible"),
            "probabilidad_supervivencia_3a": float(z.get("probabilidad_supervivencia_3a", 0.5)),
            "resumen_ia": z.get("resumen_ia", ""),
        })
    return result


async def filtrar_zonas_candidatas(filtros: dict) -> list[dict]:
    """
    Filtra zonas según los criterios del usuario usando PostGIS + SQL.
    Devuelve hasta 100 candidatas sin score (el scoring va en Celery).
    """
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
                vz.renta_media_hogar, vz.edad_media, vz.pct_extranjeros,
                vz.score_turismo, vz.num_negocios_activos,
                vz.pct_locales_vacios, vz.nivel_ruido_db,
                vz.tasa_rotacion_anual, vz.score_equipamientos,
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
                SELECT * FROM variables_zona WHERE zona_id=z.id
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

        # Competidores cercanos 300m
        competidores = await conn.fetch("""
            SELECT na.nombre, na.sector_codigo AS sector,
                   round(ST_Distance(na.geometria::geography, z.geometria::geography)::numeric, 0) AS distancia_m,
                   na.rating, na.precio_nivel,
                   (na.sector_codigo = $2) AS es_competencia_directa
            FROM negocios_activos na
            JOIN zonas z ON z.id=$1
            WHERE ST_DWithin(na.geometria::geography, z.geometria::geography, 300)
              AND na.activo=TRUE
            ORDER BY distancia_m ASC LIMIT 15
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


async def get_zonas_lista(filtros: dict, sector: str,
                          pagina: int = 1, por_pagina: int = 20) -> dict:
    """Vista lista con filtros, paginación y filtros disponibles."""
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
