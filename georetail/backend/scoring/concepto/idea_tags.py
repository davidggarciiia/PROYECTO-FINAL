"""Compatibilidad de idea_tags sobre la taxonomia conceptual unificada."""
from __future__ import annotations

import logging

from scoring.concepto.taxonomy import (
    VISIBLE_TAGS,
    aplicar_pesos_a_sector,
    compilar_concepto_negocio,
    idea_tags_visibles_desde_texto,
)

logger = logging.getLogger(__name__)


TAGS_VALIDOS = set(VISIBLE_TAGS)


def _renormalizar_pesos(pesos: dict) -> dict:
    campos = [k for k in pesos if k.startswith("peso_") and not k.startswith("peso_id")]
    total = sum(float(pesos.get(k) or 0.0) for k in campos)
    if total <= 0:
        return pesos
    return {**pesos, **{k: round(float(pesos[k]) / total, 4) for k in campos}}


def _aplicar_invariantes_legacy(pesos: dict, tags: list[str]) -> dict:
    ajustados = dict(pesos)

    if "orientado_turismo" in (tags or []):
        peso_demo = float(ajustados.get("peso_demo", 0.0) or 0.0)
        peso_turismo = float(ajustados.get("peso_turismo", 0.0) or 0.0)
        if peso_turismo <= peso_demo:
            ajustados["peso_turismo"] = round(peso_demo + 0.01, 4)
            ajustados["peso_demo"] = round(max(0.01, peso_demo - 0.01), 4)
            ajustados = _renormalizar_pesos(ajustados)

    return ajustados


def aplicar_idea_tags(pesos_sector: dict, tags: list[str]) -> dict:
    """Compatibilidad legacy: deriva pesos desde `idea_tags` visibles."""
    if not tags:
        return pesos_sector
    concepto = compilar_concepto_negocio(idea_tags=tags)
    pesos = aplicar_pesos_a_sector(pesos_sector, concepto)
    return _aplicar_invariantes_legacy(pesos, tags)


def extraer_tags_de_descripcion(descripcion_llm: str | None) -> list[str]:
    """Fallback local: usa aliases de la taxonomia y devuelve tags visibles."""
    if not descripcion_llm:
        return []
    return idea_tags_visibles_desde_texto(descripcion_llm)

