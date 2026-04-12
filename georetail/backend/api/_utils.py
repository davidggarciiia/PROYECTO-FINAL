"""api/_utils.py — Utilidades compartidas por los endpoints."""
from __future__ import annotations

from schemas.models import ColorZona


def score_to_color(score: float) -> ColorZona:
    if score > 75:
        return ColorZona.VERDE
    if score >= 50:
        return ColorZona.AMARILLO
    return ColorZona.ROJO
