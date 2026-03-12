"""
nlp/alertas.py — Alertas NLP (procesamiento de lenguaje natural) por zona.

Las alertas resumen lo que dicen las reseñas de la zona en frases cortas.
Ejemplos:
  ✅ "Alta valoración del ambiente y decoración"
  ⚠️  "Quejas recurrentes sobre el tiempo de espera"
  ❌ "Zona con alta rotación de negocios del sector"

Fuentes:
  - Tabla `alertas_zona` (generadas por pipelines/resenas.py)
  - Si no hay alertas recientes, se generan en tiempo real desde las reseñas

Usado por: POST /api/local (panel de detalle de zona)
"""
from __future__ import annotations

import logging

from db.conexion import get_db

logger = logging.getLogger(__name__)


async def get_alertas_zona(
    zona_id: str,
    sector_codigo: str,
    limite: int = 5,
) -> list[dict]:
    """
    Devuelve las alertas activas de una zona para un sector concreto.

    Formato de cada alerta:
      {
        "tipo":      "positiva" | "negativa" | "neutral",
        "texto":     "Alta valoración del ambiente",
        "fuente":    "resenas",
        "confianza": 0.87
      }
    """
    async with get_db() as conn:
        rows = await conn.fetch(
            """
            SELECT tipo, texto, fuente, confianza
            FROM alertas_zona
            WHERE zona_id = $1
              AND activa  = TRUE
              AND fecha   >= CURRENT_DATE - INTERVAL '90 days'
            ORDER BY
              CASE tipo
                WHEN 'negativa' THEN 1
                WHEN 'neutral'  THEN 2
                WHEN 'positiva' THEN 3
              END,
              confianza DESC
            LIMIT $2
            """,
            zona_id, limite,
        )

    if rows:
        return [dict(r) for r in rows]

    # Sin alertas en BD — generar dinámicamente desde reseñas recientes
    return await _generar_alertas_dinamicas(zona_id, sector_codigo, limite)


async def _generar_alertas_dinamicas(
    zona_id: str,
    sector_codigo: str,
    limite: int,
) -> list[dict]:
    """
    Genera alertas básicas en tiempo real si no hay datos en caché.
    Usa las estadísticas de reseñas directamente (sin LLM — es más rápido).
    """
    try:
        async with get_db() as conn:
            stats = await conn.fetchrow(
                """
                SELECT
                    COUNT(*)                                        AS total,
                    AVG(rating)                                     AS rating_medio,
                    SUM(CASE WHEN sentimiento='positivo' THEN 1 ELSE 0 END) AS positivas,
                    SUM(CASE WHEN sentimiento='negativo' THEN 1 ELSE 0 END) AS negativas,
                    -- Categoría más mencionada negativamente
                    MODE() WITHIN GROUP (
                        ORDER BY categoria
                    ) FILTER (WHERE sentimiento='negativo')        AS categoria_negativa
                FROM resenas
                WHERE zona_id = $1
                  AND fecha   >= CURRENT_DATE - INTERVAL '180 days'
                """,
                zona_id,
            )

        if not stats or not stats["total"]:
            return []

        alertas = []
        total   = stats["total"]
        rating  = float(stats["rating_medio"] or 0)
        pct_neg = (stats["negativas"] or 0) / max(total, 1)

        if rating >= 4.2:
            alertas.append({
                "tipo":      "positiva",
                "texto":     f"Valoración media alta ({rating:.1f}/5) entre {total} reseñas",
                "fuente":    "resenas",
                "confianza": 0.90,
            })
        elif rating <= 3.5:
            alertas.append({
                "tipo":      "negativa",
                "texto":     f"Valoración media baja ({rating:.1f}/5) — investigar causas",
                "fuente":    "resenas",
                "confianza": 0.80,
            })

        if pct_neg > 0.30:
            cat = stats["categoria_negativa"] or "servicio"
            alertas.append({
                "tipo":      "negativa",
                "texto":     f"Quejas frecuentes sobre {cat} en la zona ({int(pct_neg*100)}% de reseñas)",
                "fuente":    "resenas",
                "confianza": 0.75,
            })

        return alertas[:limite]

    except Exception as exc:
        logger.warning("Error generando alertas dinámicas para %s: %s", zona_id, exc)
        return []
