"""nlp/alertas.py — Lectura de alertas NLP activas para una zona."""
from __future__ import annotations
import logging
from db.conexion import get_db

logger = logging.getLogger(__name__)


async def get_alertas_zona(zona_id: str) -> list[dict]:
    """
    Devuelve las alertas NLP activas de una zona ordenadas por fecha descendente.

    Las alertas las genera el pipeline diario (`pipelines/resenas.py`) usando
    sentence-transformers para clasificar reseñas de Google Places / Foursquare / Yelp.

    Returns list de dicts con: tipo ('positiva'|'negativa'|'neutral'), texto, fuente.
    """
    try:
        async with get_db() as conn:
            rows = await conn.fetch(
                "SELECT tipo, texto, fuente FROM alertas_zona "
                "WHERE zona_id=$1 AND activa=TRUE "
                "ORDER BY fecha DESC LIMIT 10",
                zona_id,
            )
        return [dict(r) for r in rows]
    except Exception as exc:
        logger.warning("get_alertas_zona zona=%s: %s", zona_id, exc)
        return []
