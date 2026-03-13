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
from agente.traductor import traducir

logger = logging.getLogger(__name__)

_PROMPT_REFINAMIENTO = """You are the filter engine of a commercial premises recommendation app.

The user has a list of zones and wants to filter it with this instruction:
"{instruccion}"

Current zones (available ids): {zona_ids}

Return ONLY a JSON with this structure, no additional text:
{{
  "accion":    "filtrar" | "ordenar" | "destacar" | "sin_cambio",
  "zona_ids":  ["id1", "id2"],   // zones remaining after the filter (all if sin_cambio)
  "criterio":  "short description of the applied criterion",
  "mensaje":   "short phrase explaining what was done (in English)"
}}

Recognisable filtering criteria:
- Neighbourhood or geographic area: filter by name
- Minimum score: filter by score_global
- Low/high competition: filter by score_competencia
- Price: filter by score_precio_alquiler
- Transport: filter by score_transporte
- If you cannot understand the instruction → accion="sin_cambio", return all zona_ids"""


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
        "mensaje":  "Mostrando zonas del Eixample"   ← ya en español
      }
    """
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
        texto = respuesta.strip().removeprefix("```json").removeprefix("```").removesuffix("```").strip()
        resultado = json.loads(texto)

        # Traducir "mensaje" al español antes de devolver al frontend
        mensaje_en = resultado.get("mensaje", "")
        if mensaje_en:
            resultado["mensaje"] = await traducir(mensaje_en)

        return resultado

    except Exception as exc:
        logger.warning("Error procesando refinamiento '%s': %s", instruccion, exc)
        return {
            "accion":   "sin_cambio",
            "zona_ids": zona_ids,
            "criterio": "",
            "mensaje":  "No se pudo aplicar el filtro. Mostrando todos los resultados.",
        }
