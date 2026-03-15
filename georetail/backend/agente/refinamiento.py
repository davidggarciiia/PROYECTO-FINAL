"""
agente/refinamiento.py — Refinamiento de resultados con lenguaje natural.

Permite al usuario filtrar las zonas ya mostradas usando frases como:
  "solo las del Eixample"
  "quita las que tengan mucha competencia"
  "solo las que tengan score >70"
  "máximo 1200€ de alquiler"

El LLM interpreta la frase y devuelve filtros estructurados.
Los filtros se aplican en memoria sobre la lista de zonas de la sesión.
Usado por: POST /api/refinamiento
"""
from __future__ import annotations

import json
import logging
from typing import Optional

from routers.llm_router import completar
from agente.traductor import traducir

logger = logging.getLogger(__name__)

_SISTEMA = """You are the filter engine of a commercial premises recommendation app in Barcelona.

The user wants to refine their current search results. Interpret the instruction and return ONLY valid JSON:
{
  "accion":         "filtro_aplicado" | "respuesta",
  "filtros": {
    "score_min":    null | number 0-100,
    "alquiler_max": null | number (euros/month),
    "barrio":       null | string (neighbourhood or district name),
    "m2_min":       null | number,
    "m2_max":       null | number
  },
  "respuesta_texto": "short explanation of what was done, or answer to the question (in English)"
}

Rules:
- Filter request  → accion="filtro_aplicado", populate relevant filtros fields, leave others null
- Question        → accion="respuesta", set all filtros to null, answer in respuesta_texto
- "quitar filtros" / "resetear" → accion="filtro_aplicado" with ALL filtros null (returns all zones)
- Always write respuesta_texto in English"""


async def procesar_refinamiento(
    session_id: str,
    mensaje: str,
    sesion: dict,
) -> dict:
    """
    Interpreta el mensaje del usuario y devuelve zonas actualizadas o respuesta de texto.

    Parámetros:
      session_id  → ID de sesión (para logs LLM)
      mensaje     → frase del usuario ("solo las del Eixample")
      sesion      → dict de sesión con zonas_actuales y perfil

    Devuelve:
      {
        "accion":             "filtro_aplicado" | "respuesta",
        "respuesta_ia":       str   ← texto al usuario (en español)
        "zonas_actualizadas": list[dict] | None  ← None si accion="respuesta"
      }
    """
    zonas_actuales: list[dict] = sesion.get("zonas_actuales", [])

    # Contexto resumido de zonas para el LLM (limitado para no saturar el contexto)
    contexto_zonas = [
        {
            "zona_id":          z.get("zona_id"),
            "barrio":           z.get("barrio", ""),
            "distrito":         z.get("distrito", ""),
            "score_global":     z.get("score_global", 50),
            "alquiler_estimado": z.get("alquiler_estimado") or z.get("alquiler_mensual"),
            "m2":               z.get("m2") or z.get("m2_disponibles"),
        }
        for z in zonas_actuales[:20]
    ]

    prompt = (
        f"User instruction: {mensaje}\n\n"
        f"Current zones ({len(zonas_actuales)} total):\n"
        f"{json.dumps(contexto_zonas, ensure_ascii=False)}"
    )

    try:
        respuesta = await completar(
            mensajes=[{"role": "user", "content": prompt}],
            sistema=_SISTEMA,
            endpoint="refinamiento",
            session_id=session_id,
            max_tokens=300,
            temperature=0.2,
            requiere_json=True,
        )

        limpio = respuesta.strip()
        if limpio.startswith("```"):
            lines = limpio.split("\n")[1:]
            while lines and lines[-1].strip() in ("```", ""):
                lines.pop()
            limpio = "\n".join(lines)

        resultado = json.loads(limpio)

        accion = resultado.get("accion", "respuesta")
        filtros = resultado.get("filtros") or {}
        respuesta_texto_en = resultado.get("respuesta_texto", "")

        # Traducir respuesta al español
        respuesta_es = (
            await traducir(respuesta_texto_en, session_id)
            if respuesta_texto_en
            else ""
        )

        if accion == "filtro_aplicado":
            zonas_filtradas = _aplicar_filtros(zonas_actuales, filtros)
            return {
                "accion":             "filtro_aplicado",
                "respuesta_ia":       respuesta_es,
                "zonas_actualizadas": zonas_filtradas,
            }
        else:
            return {
                "accion":             "respuesta",
                "respuesta_ia":       respuesta_es,
                "zonas_actualizadas": None,
            }

    except Exception as exc:
        logger.warning("Error procesando refinamiento session=%s: %s", session_id, exc)
        return {
            "accion":             "respuesta",
            "respuesta_ia":       "No se pudo aplicar el filtro. Mostrando todos los resultados.",
            "zonas_actualizadas": None,
        }


def _aplicar_filtros(zonas: list[dict], filtros: dict) -> list[dict]:
    """Aplica filtros del LLM a la lista de zonas en memoria (sin BD)."""
    resultado = list(zonas)

    score_min = filtros.get("score_min")
    if score_min is not None:
        resultado = [z for z in resultado if (z.get("score_global") or 0) >= score_min]

    alquiler_max = filtros.get("alquiler_max")
    if alquiler_max is not None:
        resultado = [
            z for z in resultado
            if (z.get("alquiler_estimado") or z.get("alquiler_mensual") or 0) <= alquiler_max
        ]

    barrio = filtros.get("barrio")
    if barrio:
        barrio_lower = barrio.lower()
        resultado = [
            z for z in resultado
            if barrio_lower in (z.get("barrio") or "").lower()
            or barrio_lower in (z.get("distrito") or "").lower()
        ]

    m2_min = filtros.get("m2_min")
    if m2_min is not None:
        resultado = [
            z for z in resultado
            if (z.get("m2") or z.get("m2_disponibles") or 0) >= m2_min
        ]

    m2_max = filtros.get("m2_max")
    if m2_max is not None:
        resultado = [
            z for z in resultado
            if (z.get("m2") or z.get("m2_disponibles") or float("inf")) <= m2_max
        ]

    return resultado
