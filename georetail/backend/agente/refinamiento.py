"""
agente/refinamiento.py — Refinamiento de resultados con lenguaje natural.

Permite al usuario filtrar o reordenar las zonas ya mostradas usando frases como:
  "solo las del Eixample"
  "quita las que tengan mucha competencia"
  "solo las que tengan score >70"
  "¿cuál tiene más turismo?"

El LLM interpreta la frase y devuelve una acción estructurada.
Usado por: POST /api/refinamiento
"""
from __future__ import annotations

import json
import logging
from typing import Optional

from routers.llm_router import completar
from agente.prompts import REFINAMIENTO_SISTEMA
from agente.traductor import traducir
from agente import extraer_json

logger = logging.getLogger(__name__)


async def procesar_refinamiento(
    session_id: str,
    mensaje: str,
    sesion: dict,
) -> dict:
    """
    Interpreta el mensaje del usuario en el contexto de las zonas actuales
    y devuelve una acción estructurada con la respuesta y las zonas actualizadas.

    Args:
        session_id: ID de sesión (para el log de LLM)
        mensaje:    Mensaje del usuario en español
        sesion:     Dict completo de la sesión (perfil, zonas_actuales, etc.)

    Returns dict con:
        - respuesta_ia:       texto de respuesta al usuario (en español)
        - accion:             "respuesta" | "filtro_aplicado" | "nueva_busqueda"
        - zonas_actualizadas: list[dict] | None — zonas tras aplicar el filtro
    """
    zonas = sesion.get("zonas_actuales", [])
    perfil = sesion.get("perfil", {})

    # Construir resumen de las zonas actuales para el contexto del LLM
    resumen_zonas = [
        {
            "zona_id": z.get("zona_id", ""),
            "nombre":  z.get("nombre", ""),
            "barrio":  z.get("barrio", ""),
            "distrito": z.get("distrito", ""),
            "score_global": z.get("score_global", 50),
            "alquiler_estimado": z.get("alquiler_estimado") or z.get("alquiler_mensual"),
            "m2_disponibles": z.get("m2_disponibles") or z.get("m2"),
        }
        for z in zonas
    ]

    contexto = (
        f"User instruction: {mensaje}\n\n"
        f"Business profile: {perfil.get('sector', 'unknown')} — "
        f"{perfil.get('descripcion', '')}\n\n"
        f"Current zones ({len(resumen_zonas)} total):\n"
        f"{json.dumps(resumen_zonas, ensure_ascii=False)}"
    )

    try:
        respuesta_llm = await completar(
            mensajes=[{"role": "user", "content": contexto}],
            sistema=REFINAMIENTO_SISTEMA,
            endpoint="refinamiento",
            session_id=session_id,
            max_tokens=400,
            temperature=0.2,
            requiere_json=True,
        )

        parsed = json.loads(extraer_json(respuesta_llm))
        accion_llm = parsed.get("accion", "sin_cambio")

        # Aplicar filtros o reordenamiento según la acción del LLM
        filtros = parsed.get("filtros", {}) or {}
        ordenar_por = parsed.get("ordenar_por")
        mensaje_confirmacion_en = parsed.get("mensaje_confirmacion", "")

        if accion_llm == "resetear":
            # Devolver todas las zonas sin filtrar
            zonas_resultado = list(zonas)
            accion_salida = "filtro_aplicado"
        elif accion_llm in ("filtrar", "ordenar") and zonas:
            zonas_resultado = _aplicar_filtros(zonas, filtros)
            if ordenar_por:
                zonas_resultado = _ordenar_zonas(zonas_resultado, ordenar_por)
            accion_salida = "filtro_aplicado"
        else:
            # "sin_cambio" o acción no reconocida → respuesta de texto sin cambios
            zonas_resultado = None
            accion_salida = "respuesta"

        # Traducir el mensaje de confirmación del LLM (inglés → español)
        if mensaje_confirmacion_en:
            respuesta_ia = await traducir(mensaje_confirmacion_en, session_id)
        else:
            respuesta_ia = "Filtro aplicado." if accion_salida == "filtro_aplicado" else mensaje

        return {
            "respuesta_ia":      respuesta_ia,
            "accion":            accion_salida,
            "zonas_actualizadas": zonas_resultado,
        }

    except Exception as exc:
        logger.warning("Error procesando refinamiento '%s': %s", mensaje, exc)
        return {
            "respuesta_ia":      "No se pudo aplicar el filtro. Mostrando todos los resultados.",
            "accion":            "respuesta",
            "zonas_actualizadas": None,
        }


def _aplicar_filtros(zonas: list[dict], filtros: dict) -> list[dict]:
    """Aplica los filtros numéricos y textuales devueltos por el LLM."""
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

    distrito = filtros.get("distrito")
    if distrito:
        distrito_lower = distrito.lower()
        resultado = [
            z for z in resultado
            if distrito_lower in (z.get("distrito") or "").lower()
            or distrito_lower in (z.get("barrio") or "").lower()
        ]

    m2_min = filtros.get("m2_min")
    if m2_min is not None:
        resultado = [
            z for z in resultado
            if (z.get("m2_disponibles") or z.get("m2") or 0) >= m2_min
        ]

    m2_max = filtros.get("m2_max")
    if m2_max is not None:
        resultado = [
            z for z in resultado
            if (z.get("m2_disponibles") or z.get("m2") or float("inf")) <= m2_max
        ]

    return resultado


def _ordenar_zonas(zonas: list[dict], criterio: str) -> list[dict]:
    """Ordena las zonas según el criterio indicado por el LLM."""
    if criterio == "score":
        return sorted(zonas, key=lambda z: z.get("score_global") or 0, reverse=True)
    if criterio == "alquiler":
        return sorted(
            zonas,
            key=lambda z: z.get("alquiler_estimado") or z.get("alquiler_mensual") or float("inf"),
        )
    if criterio == "m2":
        return sorted(
            zonas,
            key=lambda z: z.get("m2_disponibles") or z.get("m2") or 0,
            reverse=True,
        )
    return zonas
