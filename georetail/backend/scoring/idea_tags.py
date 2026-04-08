"""Compatibilidad de idea_tags sobre la taxonomia conceptual unificada."""
from __future__ import annotations

import logging

from scoring.concept_taxonomy import (
    VISIBLE_TAGS,
    aplicar_pesos_a_sector,
    compilar_concepto_negocio,
    idea_tags_visibles_desde_texto,
)

logger = logging.getLogger(__name__)


TAGS_VALIDOS = set(VISIBLE_TAGS)


def aplicar_idea_tags(pesos_sector: dict, tags: list[str]) -> dict:
    """Compatibilidad legacy: deriva pesos desde `idea_tags` visibles."""
    if not tags:
        return pesos_sector
    concepto = compilar_concepto_negocio(idea_tags=tags)
    return aplicar_pesos_a_sector(pesos_sector, concepto)


def extraer_tags_de_descripcion(descripcion_llm: str | None) -> list[str]:
    """Fallback local: usa aliases de la taxonomia y devuelve tags visibles."""
    if not descripcion_llm:
        return []
    return idea_tags_visibles_desde_texto(descripcion_llm)

