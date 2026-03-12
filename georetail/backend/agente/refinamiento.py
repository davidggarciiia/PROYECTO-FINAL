"""
agente/refinamiento.py — Refinamiento de resultados con lenguaje natural.

Permite al usuario filtrar las zonas ya mostradas usando frases como:
  "solo las del Eixample"
  "quita las que tengan mucha competencia"
  "solo las que tengan score >70"

El LLM interpreta la frase y devuelve una acción estructurada.
Usado por: POST /api/refinamiento
"""
from __future__ import annotations

import json
import logging
from typing import Optional

from routers.llm_router import llamar_llm

logger = logging.getLogger(__name__)

_PROMPT_REFINAMIENTO = """Eres el motor de filtros de una app de recomendación de locales comerciales.

El usuario tiene una lista de zonas y quiere filtrarla con esta frase:
"{instruccion}"

Zonas actuales (ids disponibles): {zona_ids}

Devuelve SOLO un JSON con esta estructura, sin texto adicional:
{{
  "accion":        "filtrar" | "ordenar" | "destacar" | "sin_cambio",
  "zona_ids":      ["id1", "id2"],   // zonas que quedan tras el filtro (todas si sin_cambio)
  "criterio":      "descripción corta del criterio aplicado",
  "mensaje":       "frase corta para mostrar al usuario explicando qué se hizo"
}}

Criterios de filtrado reconocibles:
- Barrio o zona geográfica: filtra por nombre
- Score mínimo: filtra por score_global
- Competencia baja/alta: filtra por score_competencia
- Precio: filtra por score_precio_alquiler
- Transporte: filtra por score_transporte
- Si no entiendes la instrucción → accion="sin_cambio", devuelve todas las zona_ids"""


async def procesar_refinamiento(
    instruccion: str,
    zona_ids: list[str],
    scores: dict[str, dict],
) -> dict:
    """
    Interpreta la instrucción del usuario y devuelve qué zonas mostrar.

    Parámetros:
      instruccion  → frase del usuario ("solo las del Eixample")
      zona_ids     → IDs de las zonas actuales en el mapa
      scores       → dict {zona_id: {score_global, barrio, ...}} para contexto

    Devuelve:
      {
        "accion":   "filtrar",
        "zona_ids": ["bcn_eixample_01", ...],
        "criterio": "barrio Eixample",
        "mensaje":  "Mostrando zonas del Eixample"
      }
    """
    # Construir contexto con nombre de barrio y score para que el LLM pueda filtrar
    contexto = {
        zid: {
            "barrio":       scores.get(zid, {}).get("barrio", ""),
            "score_global": scores.get(zid, {}).get("score_global", 50),
        }
        for zid in zona_ids
    }

    prompt = _PROMPT_REFINAMIENTO.format(
        instruccion = instruccion,
        zona_ids    = json.dumps(contexto, ensure_ascii=False),
    )

    try:
        respuesta = await llamar_llm(
            prompt     = prompt,
            endpoint   = "refinamiento",
            max_tokens = 300,
        )
        # Limpiar posibles backticks (```json ... ```) que añaden algunos modelos
        texto = respuesta.strip().removeprefix("```json").removeprefix("```").removesuffix("```").strip()
        return json.loads(texto)

    except Exception as exc:
        logger.warning("Error procesando refinamiento '%s': %s", instruccion, exc)
        return {
            "accion":   "sin_cambio",
            "zona_ids": zona_ids,
            "criterio": "",
            "mensaje":  "No se pudo aplicar el filtro. Mostrando todos los resultados.",
        }
