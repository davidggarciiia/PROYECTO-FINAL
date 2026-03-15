"""
agente/traductor.py — Traduce respuestas del LLM (inglés) a español para el usuario.

El LLM genera respuestas en inglés para reducir el consumo de tokens (~25% menos
tokens de salida). Este módulo traduce el texto visible al usuario de vuelta a
español justo antes de devolverlo al frontend.

Qué se traduce:
  - cuestionario   → campo "mensaje"
  - análisis zona  → resumen, puntos_fuertes, puntos_debiles, oportunidad,
                     riesgos, razon_recomendacion
  - validador      → motivo_rechazo (solo si hay rechazo)
  - refinamiento   → campo "mensaje"

Qué NO se traduce:
  - Claves JSON (son código interno)
  - Valores enumerados ("ok", "completo", "filtrar", etc.)
  - Números, booleanos
  - Nombres de barrios/distritos (vienen de la BD, ya están en español)
  - _PATRONES del clasificador (vectores semánticos sobre reseñas en español)
"""
from __future__ import annotations

import json
import logging

from routers.llm_router import completar
from agente import extraer_json

logger = logging.getLogger(__name__)

# Prompt mínimo para la traducción — sin explicaciones, solo el resultado
_SISTEMA = (
    "Translate from English to Spanish. "
    "Return ONLY the translated content with the exact same format."
)

# Mismo prompt pero dejando claro que la salida debe ser JSON válido
_SISTEMA_JSON = (
    "Translate all string values from English to Spanish. "
    "Return ONLY valid JSON with identical keys and structure."
)


async def traducir(texto: str, session_id: str | None = None) -> str:
    """
    Traduce un string de inglés a español.
    Si falla, devuelve el texto original sin lanzar excepción.
    """
    if not texto or not texto.strip():
        return texto
    try:
        return await completar(
            mensajes=[{"role": "user", "content": texto}],
            sistema=_SISTEMA,
            endpoint="traduccion",
            session_id=session_id,
            max_tokens=min(int(len(texto.split()) * 2) + 50, 1000),
            temperature=0.1,
        )
    except Exception as e:
        logger.warning("Translation failed, returning original: %s", e)
        return texto


async def traducir_dict(
    data: dict,
    campos: list[str],
    session_id: str | None = None,
) -> dict:
    """
    Traduce campos de texto específicos de un dict en UNA sola llamada LLM.
    Más eficiente que llamar a traducir() por cada campo por separado.

    Maneja tanto campos string como campos list[str].
    """
    subset = {k: data[k] for k in campos if k in data and data[k]}
    if not subset:
        return data

    # Estimar max_tokens: ~1.8 palabras por token, con margen
    total_words = sum(
        len(" ".join(v).split()) if isinstance(v, list) else len(str(v).split())
        for v in subset.values()
    )

    try:
        respuesta = await completar(
            mensajes=[{"role": "user", "content": json.dumps(subset, ensure_ascii=False)}],
            sistema=_SISTEMA_JSON,
            endpoint="traduccion",
            session_id=session_id,
            max_tokens=min(int(total_words * 2) + 100, 2000),
            temperature=0.1,
            requiere_json=True,
        )
        traducidos = json.loads(extraer_json(respuesta))
        return {**data, **traducidos}
    except Exception as e:
        logger.warning("Dict translation failed, returning original: %s", e)
        return data
