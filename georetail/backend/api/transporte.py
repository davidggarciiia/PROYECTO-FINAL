"""
api/transporte.py — GET /api/transporte/{zona_id}

Detalle de líneas y paradas de transporte público cercanas a una zona.
Consumido por el frontend (pestaña "Transporte" del panel de detalle).

Fuente de datos: lineas_transporte, paradas_transporte, paradas_lineas (pobladas
por pipelines/transporte/transporte.py desde TMB API).

La consulta espacial se hace sobre el centroide de la zona con PostGIS
ST_DWithin (geografía), radio configurable (default 500m).
"""
from __future__ import annotations

import json
import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from db.conexion import get_db
from schemas.models import (
    LineaCercana,
    ParadaCercana,
    TransporteDetalleZona,
)

logger = logging.getLogger(__name__)
router = APIRouter(tags=["transporte"])

_RADIO_MIN = 50
_RADIO_MAX = 2000
_TOP_PARADAS = 3

# Tipos válidos según schemas.models.TipoTransporte
_TIPOS_VALIDOS = {"metro", "bus", "tram", "fgc", "rodalies"}

# Color por defecto si lineas_transporte.color_hex es NULL o vacío
_COLORES_DEFAULT = {
    "metro":    "#D03324",
    "bus":      "#E3000F",
    "tram":     "#007F3B",
    "fgc":      "#9B2743",
    "rodalies": "#9B2743",
}


@router.get(
    "/transporte/{zona_id}",
    response_model=TransporteDetalleZona,
    summary="Líneas y paradas de transporte público cercanas a una zona",
)
async def transporte_zona(
    zona_id: str,
    radio_m: int = Query(500, ge=_RADIO_MIN, le=_RADIO_MAX, description="Radio de búsqueda en metros"),
) -> TransporteDetalleZona:
    """
    Devuelve, para la zona indicada, las líneas de transporte público cuyas
    paradas caen dentro del radio dado. Cada línea incluye su distancia mínima
    y las 3 paradas más cercanas.

    - 200: TransporteDetalleZona
    - 404: zona no existe o no tiene geometría
    """
    async with get_db() as conn:
        # Verificar existencia de zona (y que tenga centroide)
        zona = await conn.fetchrow(
            "SELECT id FROM zonas WHERE id = $1 AND geometria IS NOT NULL",
            zona_id,
        )
        if zona is None:
            raise HTTPException(status_code=404, detail=f"Zona '{zona_id}' no encontrada")

        rows = await conn.fetch(
            """
            WITH zc AS (
                -- Usamos el polígono completo de la zona (no el centroide)
                -- para que ST_Distance devuelva la distancia al borde más
                -- cercano. Así una parada que caiga dentro de la zona
                -- obtiene distancia 0 y no queda fuera del radio.
                SELECT geometria::geography AS poly
                FROM zonas WHERE id = $1
            )
            SELECT l.tipo,
                   l.codigo,
                   l.nombre,
                   l.color_hex AS color,
                   MIN(ST_Distance(p.geometria::geography, zc.poly)) AS dist_min_m,
                   JSONB_AGG(
                       JSONB_BUILD_OBJECT(
                           'nombre',      p.nombre,
                           'distancia_m', ST_Distance(p.geometria::geography, zc.poly),
                           'tipo',        l.tipo
                       )
                       ORDER BY ST_Distance(p.geometria::geography, zc.poly) ASC
                   ) AS paradas
            FROM zc, lineas_transporte l
            JOIN paradas_lineas pl      ON pl.linea_id = l.id
            JOIN paradas_transporte p   ON p.id        = pl.parada_id
            WHERE ST_DWithin(p.geometria::geography, zc.poly, $2)
            GROUP BY l.id, l.tipo, l.codigo, l.nombre, l.color_hex
            ORDER BY dist_min_m ASC
            """,
            zona_id, radio_m,
        )

    lineas: list[LineaCercana] = []
    total_paradas_ids: set[str] = set()

    for r in rows:
        tipo_raw = (r["tipo"] or "").lower()
        if tipo_raw not in _TIPOS_VALIDOS:
            # Saltar líneas con tipo no soportado por el schema (p.ej. funicular legado)
            continue

        color = r["color"] or _COLORES_DEFAULT.get(tipo_raw, "#666666")

        paradas_raw = r["paradas"]
        if isinstance(paradas_raw, str):
            paradas_raw = json.loads(paradas_raw)
        paradas_raw = paradas_raw or []

        paradas: list[ParadaCercana] = []
        for p in paradas_raw[:_TOP_PARADAS]:
            paradas.append(ParadaCercana(
                nombre      = str(p.get("nombre") or ""),
                distancia_m = round(float(p.get("distancia_m") or 0.0), 1),
                tipo        = tipo_raw,  # type: ignore[arg-type]
            ))
        # Para el total_paradas contamos todas las paradas únicas, no solo top-3
        for p in paradas_raw:
            nombre_p = p.get("nombre")
            if nombre_p:
                total_paradas_ids.add(f"{tipo_raw}:{nombre_p}")

        lineas.append(LineaCercana(
            tipo              = tipo_raw,  # type: ignore[arg-type]
            codigo            = str(r["codigo"] or ""),
            nombre            = r["nombre"],
            color             = color,
            dist_min_m        = round(float(r["dist_min_m"] or 0.0), 1),
            paradas_cercanas  = paradas,
        ))

    return TransporteDetalleZona(
        zona_id       = zona_id,
        radio_m       = radio_m,
        total_lineas  = len(lineas),
        total_paradas = len(total_paradas_ids),
        lineas        = lineas,
    )
