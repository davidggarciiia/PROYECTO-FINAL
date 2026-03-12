"""
scoring/legal.py — Consulta de requisitos legales aplicables a una zona+sector.

Por ahora devuelve los datos del dict estático de api/legal.py.
En el futuro verificará restricciones de densidad vía PostGIS
(tablas `requisitos_legales_sector` y `restricciones_geograficas_sector`).
"""
from __future__ import annotations
import logging

logger = logging.getLogger(__name__)


async def get_info_legal_zona(zona_id: str, sector: str) -> dict:
    """
    Devuelve los requisitos legales relevantes para zona + sector en el formato
    que espera api/local.py:
      {
        "viabilidad":           str,
        "alerta":               str | None,
        "licencias_necesarias": list[dict],
        "restriccion_zona":     None,          # TODO: verificar PostGIS
        "requisitos_local":     list[str],
      }

    Args:
        zona_id: ID de la zona (reservado para futuras consultas PostGIS)
        sector:  código de sector ('restauracion', 'moda', etc.)
    """
    try:
        # Importación diferida para evitar imports circulares durante el startup
        from api.legal import _SECTORES
        sector_data = _SECTORES.get(sector, {})
    except Exception as exc:
        logger.warning("No se pudo cargar datos legales para sector=%s: %s", sector, exc)
        sector_data = {}

    return {
        "viabilidad":           sector_data.get("viabilidad", "viable"),
        "alerta":               sector_data.get("alerta"),
        "licencias_necesarias": sector_data.get("licencias_necesarias", []),
        "restriccion_zona":     None,   # TODO: verificar densidad PostGIS
        "requisitos_local":     sector_data.get("requisitos_local", []),
    }
