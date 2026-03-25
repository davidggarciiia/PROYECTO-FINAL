"""
api/mercado.py — Endpoints de datos de mercado inmobiliario.

GET /api/mercado/barrio/{nombre}             → estadísticas de un barrio
GET /api/mercado/distrito/{nombre}           → estadísticas de un distrito
GET /api/mercado/resumen                     → todos los barrios con datos
GET /api/mercado/zona/{zona_id}/precio-referencia → precio/m² para scoring

Consume la vista v_mercado_zona (agregado multi-portal de inmuebles_portales).
"""
from __future__ import annotations

import logging
from typing import Literal, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from db.conexion import get_db

logger = logging.getLogger(__name__)
router = APIRouter(tags=["mercado"])


# ── Response models ───────────────────────────────────────────────────────────

class MercadoZona(BaseModel):
    zona:               str
    nivel:              str
    tipo_operacion:     str
    tipo_inmueble:      str
    n_anuncios:         int
    num_portales:       Optional[int]  = None
    precio_m2_mediana:  Optional[float] = None
    precio_m2_media:    Optional[float] = None
    precio_m2_p25:      Optional[float] = None
    precio_m2_p75:      Optional[float] = None
    precio_m2_min:      Optional[float] = None
    precio_m2_max:      Optional[float] = None
    precio_mediano:     Optional[float] = None
    superficie_mediana: Optional[float] = None
    superficie_media:   Optional[float] = None
    dias_mercado_medio: Optional[float] = None
    pct_particular:     Optional[float] = None
    pct_esquina:        Optional[float] = None
    pct_escaparate:     Optional[float] = None
    ultimo_scraping:    Optional[str]   = None


class ResumenMercadoResponse(BaseModel):
    total_zonas:    int
    zonas:          list[MercadoZona]
    tipo_operacion: str
    tipo_inmueble:  str


def _row_to_mercado(row: dict) -> MercadoZona:
    def f(k): return float(row[k]) if row.get(k) is not None else None
    return MercadoZona(
        zona=row["zona"],
        nivel=row["nivel"],
        tipo_operacion=row["tipo_operacion"],
        tipo_inmueble=row["tipo_inmueble"],
        n_anuncios=row["n_anuncios"],
        num_portales=row.get("num_portales"),
        precio_m2_mediana=f("precio_m2_mediana"),
        precio_m2_media=f("precio_m2_media"),
        precio_m2_p25=f("precio_m2_p25"),
        precio_m2_p75=f("precio_m2_p75"),
        precio_m2_min=f("precio_m2_min"),
        precio_m2_max=f("precio_m2_max"),
        precio_mediano=f("precio_mediano"),
        superficie_mediana=f("superficie_mediana"),
        superficie_media=f("superficie_media"),
        dias_mercado_medio=f("dias_mercado_medio"),
        pct_particular=f("pct_particular"),
        pct_esquina=f("pct_esquina"),
        pct_escaparate=f("pct_escaparate"),
        ultimo_scraping=str(row["ultimo_scraping"]) if row.get("ultimo_scraping") else None,
    )


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.get("/mercado/barrio/{nombre}", response_model=list[MercadoZona])
async def mercado_barrio(
    nombre: str,
    tipo_operacion: Literal["alquiler-locales", "venta-locales"] = Query(default="alquiler-locales"),
    tipo_inmueble:  Literal["local", "nave", "oficina"] = Query(default="local"),
) -> list[MercadoZona]:
    """Estadísticas de precio/m² y rotación para un barrio concreto."""
    async with get_db() as conn:
        rows = await conn.fetch("""
            SELECT * FROM v_mercado_zona
            WHERE LOWER(zona) = LOWER($1)
              AND tipo_operacion = $2
              AND tipo_inmueble  = $3
            ORDER BY n_anuncios DESC
        """, nombre, tipo_operacion, tipo_inmueble)

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"Sin datos para '{nombre}' ({tipo_operacion}). "
                   "El pipeline puede no haber scrapeado esta zona aún.",
        )
    return [_row_to_mercado(dict(r)) for r in rows]


@router.get("/mercado/distrito/{nombre}", response_model=list[MercadoZona])
async def mercado_distrito(
    nombre: str,
    tipo_operacion: Literal["alquiler-locales", "venta-locales"] = Query(default="alquiler-locales"),
    tipo_inmueble:  Literal["local", "nave", "oficina"] = Query(default="local"),
) -> list[MercadoZona]:
    """Estadísticas de todos los barrios de un distrito."""
    async with get_db() as conn:
        rows = await conn.fetch("""
            SELECT * FROM v_mercado_zona
            WHERE nivel = 'barrio'
              AND tipo_operacion = $1
              AND tipo_inmueble  = $2
              AND zona IN (
                  SELECT b.nombre FROM barrios b
                  JOIN distritos d ON d.id = b.distrito_id
                  WHERE LOWER(d.nombre) = LOWER($3)
              )
            ORDER BY precio_m2_mediana DESC NULLS LAST
        """, tipo_operacion, tipo_inmueble, nombre)

    if not rows:
        raise HTTPException(status_code=404, detail=f"Sin datos para el distrito '{nombre}'.")
    return [_row_to_mercado(dict(r)) for r in rows]


@router.get("/mercado/resumen", response_model=ResumenMercadoResponse)
async def mercado_resumen(
    tipo_operacion: Literal["alquiler-locales", "venta-locales"] = Query(default="alquiler-locales"),
    tipo_inmueble:  Literal["local", "nave", "oficina"] = Query(default="local"),
    nivel:          Literal["barrio", "distrito"] = Query(default="barrio"),
) -> ResumenMercadoResponse:
    """Todos los barrios con datos, ordenados por precio/m² desc. Útil para mapa de calor."""
    async with get_db() as conn:
        rows = await conn.fetch("""
            SELECT * FROM v_mercado_zona
            WHERE tipo_operacion = $1
              AND tipo_inmueble  = $2
              AND nivel          = $3
            ORDER BY precio_m2_mediana DESC NULLS LAST
        """, tipo_operacion, tipo_inmueble, nivel)

    zonas = [_row_to_mercado(dict(r)) for r in rows]
    return ResumenMercadoResponse(
        total_zonas=len(zonas), zonas=zonas,
        tipo_operacion=tipo_operacion, tipo_inmueble=tipo_inmueble,
    )


@router.get("/mercado/zona/{zona_id}/precio-referencia", response_model=dict)
async def precio_referencia_zona(
    zona_id: str,
    tipo_operacion: Literal["alquiler-locales", "venta-locales"] = Query(default="alquiler-locales"),
) -> dict:
    """
    Precio/m² de referencia para una zona de GeoRetail.
    Puente entre el scoring (zona_id) y los datos de portales (nombre de barrio).
    Consumido internamente por scoring/features.py como fallback de precio.
    """
    async with get_db() as conn:
        barrio = await conn.fetchval("""
            SELECT b.nombre FROM zonas z
            JOIN barrios b ON b.id = z.barrio_id
            WHERE z.id = $1
        """, zona_id)

        if not barrio:
            raise HTTPException(status_code=404, detail=f"Zona {zona_id} no encontrada")

        precio = await conn.fetchval("""
            SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY precio_m2)
            FROM inmuebles_portales
            WHERE LOWER(barrio) = LOWER($1)
              AND tipo_operacion = $2
              AND precio_m2 IS NOT NULL
              AND precio_m2 BETWEEN 3 AND 300
              AND fecha_scraping > NOW() - INTERVAL '90 days'
        """, barrio, tipo_operacion)

    return {
        "zona_id":        zona_id,
        "barrio":         barrio,
        "precio_m2":      float(precio) if precio else None,
        "tipo_operacion": tipo_operacion,
        "fuente":         "scraping_portales" if precio else None,
    }
