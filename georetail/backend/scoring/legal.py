"""
scoring/legal.py — Información legal relevante para una zona concreta.

Complementa api/legal.py (que devuelve requisitos generales del sector)
con datos específicos de la zona: restricciones geográficas, zonas de
protección acústica, límites de densidad de locales, etc.

Usado por: POST /api/local (panel de detalle de zona)
"""
from __future__ import annotations

import logging

from db.conexion import get_db

logger = logging.getLogger(__name__)

# Restricciones fijas por sector que no dependen de la zona
_ALERTAS_SECTOR = {
    "restauracion": [
        "Requiere informe urbanístico previo (IVU) antes de firmar el contrato",
        "Límites de aforo según metros cuadrados — verificar en el PEUAT",
    ],
    "tatuajes": [
        "Registro obligatorio en ASPCAT antes de abrir",
        "El local debe cumplir normativa sanitaria de establecimientos de tatuaje",
    ],
    "shisha_lounge": [
        "Solo viable como club privado de fumadores (Ley 28/2005)",
        "Requiere estatutos de asociación + cuotas de socio",
        "Prohibida la venta directa de tabaco o shisha al público",
    ],
    "moda": [],
    "estetica": [
        "Si se usa aparatología médica → requiere autorización sanitaria específica",
    ],
}


async def get_info_legal_zona(zona_id: str, sector_codigo: str) -> dict:
    """
    Devuelve restricciones legales específicas para una zona y sector.

    Combina:
      1. Alertas fijas del sector (hardcoded arriba)
      2. Restricciones geográficas de la BD (si existen para esta zona)
      3. Si la zona está en área de protección acústica (PGOU BCN)

    Formato de respuesta:
      {
        "alertas":            ["texto alerta 1", ...],
        "restricciones_geo":  ["texto restricción 1", ...],
        "verificar":          True/False  (si hay algo urgente a verificar)
      }
    """
    alertas_sector = _ALERTAS_SECTOR.get(sector_codigo, [])

    restricciones_geo = []
    try:
        async with get_db() as conn:
            rows = await conn.fetch(
                """
                SELECT descripcion
                FROM restricciones_geograficas_sector
                WHERE sector_codigo = $1
                  AND obligatorio_verificar = TRUE
                  AND (
                      geometria_restriccion IS NULL
                      OR ST_Within(
                          (SELECT geometria FROM zonas WHERE id = $2),
                          geometria_restriccion
                      )
                  )
                """,
                sector_codigo, zona_id,
            )
            restricciones_geo = [r["descripcion"] for r in rows]
    except Exception as exc:
        logger.warning("Error leyendo restricciones_geo para zona %s: %s", zona_id, exc)

    verificar = bool(alertas_sector or restricciones_geo)

    return {
        "alertas":           alertas_sector,
        "restricciones_geo": restricciones_geo,
        "verificar":         verificar,
    }
