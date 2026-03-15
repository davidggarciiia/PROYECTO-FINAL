"""agente/__init__.py — Utilidades compartidas para los módulos del agente."""
from __future__ import annotations
import re


def extraer_json(texto: str) -> str:
    """
    Extrae JSON limpio de una respuesta LLM, eliminando bloques markdown y
    texto extra que pueda rodear el JSON.

    Casos que maneja:
      - ```json\\n{...}\\n```
      - ```\\n{...}\\n```
      - Texto libre antes/después del JSON
      - Respuesta ya limpia (la devuelve tal cual)
    """
    texto = texto.strip()

    # Caso 1: bloque markdown ```json...``` o ```...```
    m = re.search(r"```(?:json)?\s*\n?([\s\S]*?)\n?```", texto)
    if m:
        return m.group(1).strip()

    # Caso 2: sin bloque markdown — extraer primer objeto JSON ({ }) o array ([ ])
    m = re.search(r"(\{[\s\S]*\}|\[[\s\S]*\])", texto)
    if m:
        return m.group(1).strip()

    # Caso 3: ya es JSON limpio o no hay nada que extraer
    return texto
