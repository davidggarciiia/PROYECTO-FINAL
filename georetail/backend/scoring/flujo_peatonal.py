"""
scoring/flujo_peatonal.py — Fusión ponderada de fuentes de flujo peatonal.

Combina 4 fuentes con pesos adaptativos (si falta una, redistribuye):
  1. popular_times_peak    (35%) — Google Maps Popular Times
  2. vcity_peatones        (30%) — VCity BSC promedio diario
  3. vianants_bcn          (20%) — Sensores BCN Open Data
  4. ratio_locales         (15%) — proxy estructural (siempre disponible)

Output: flujo_peatonal_score [0-100] listo para usar en XGBoost v4.

Nota sobre normalización:
  - popular_times_score ya está en escala 0-100 (viene de gosom/google-maps-scraper vía GosomClient)
  - vcity_flujo_peatonal es el promedio diario de peatones (peatones/día); se
    normaliza dividiendo por vcity_max_barcelona (default: 50 000)
  - flujo_peatonal_total (vianants BCN) es intensidad diaria (personas/día); se
    normaliza dividiendo por vianants_max_barcelona (default: 15 000)
  - ratio_locales_comerciales es fracción 0-1; se multiplica por 100 para obtener
    una puntuación coherente con las demás fuentes
"""
from __future__ import annotations

from typing import Optional


# Pesos base por fuente
PESOS_BASE: dict[str, float] = {
    "popular_times": 0.35,
    "vcity":         0.30,
    "vianants":      0.20,
    "ratio_locales": 0.15,
}

# Valores de referencia para normalización (percentil 99 estimado en BCN)
_VCITY_MAX_BCN:    float = 50_000.0  # peatones/día zona muy transitada
_VIANANTS_MAX_BCN: float = 15_000.0  # personas/día sensor máximo BCN Open Data


# ---------------------------------------------------------------------------
# Función principal
# ---------------------------------------------------------------------------

def calcular_flujo_score(
    popular_times_score: Optional[float],   # 0-100
    vcity_flujo: Optional[float],           # peatones/día (raw, normalizar)
    vianants_intensitat: Optional[float],   # personas/día (raw, normalizar)
    ratio_locales: Optional[float],         # 0.0-1.0
    vcity_max_barcelona: float = _VCITY_MAX_BCN,
    vianants_max_barcelona: float = _VIANANTS_MAX_BCN,
) -> float:
    """
    Devuelve flujo_peatonal_score [0-100].

    Si una fuente es None, redistribuye su peso proporcionalmente entre las
    fuentes disponibles. ratio_locales es el proxy estructural y siempre se
    trata como disponible cuando es un float (puede ser 0.0 legítimo).

    Args:
        popular_times_score: pico de popular times Google Maps (0-100).
        vcity_flujo: promedio diario de peatones de VCity BSC (peatones/día).
        vianants_intensitat: intensidad de los sensores BCN Open Data (personas/día).
        ratio_locales: fracción de locales comerciales en la zona (0.0-1.0).
        vcity_max_barcelona: valor de referencia para normalizar vcity_flujo.
        vianants_max_barcelona: valor de referencia para normalizar vianants_intensitat.

    Returns:
        Puntuación de flujo peatonal en escala 0-100.
    """
    # ── Normalizar cada fuente a escala 0-100 ────────────────────────────────
    scores: dict[str, Optional[float]] = {}

    # 1. popular_times: ya en 0-100
    scores["popular_times"] = (
        float(min(100.0, max(0.0, popular_times_score)))
        if popular_times_score is not None
        else None
    )

    # 2. vcity: peatones/día → 0-100
    scores["vcity"] = (
        float(min(100.0, max(0.0, vcity_flujo / vcity_max_barcelona * 100.0)))
        if vcity_flujo is not None
        else None
    )

    # 3. vianants BCN: personas/día → 0-100
    scores["vianants"] = (
        float(min(100.0, max(0.0, vianants_intensitat / vianants_max_barcelona * 100.0)))
        if vianants_intensitat is not None
        else None
    )

    # 4. ratio_locales: fracción 0-1 → 0-100 (proxy estructural)
    #    None solo si no existe el dato; 0.0 es un valor legítimo
    scores["ratio_locales"] = (
        float(min(100.0, max(0.0, ratio_locales * 100.0)))
        if ratio_locales is not None
        else None
    )

    # ── Redistribución adaptativa de pesos ───────────────────────────────────
    disponibles = {k: v for k, v in scores.items() if v is not None}

    if not disponibles:
        # Sin datos en absoluto: fallback conservador
        return 30.0

    peso_disponible = sum(PESOS_BASE[k] for k in disponibles)
    if peso_disponible == 0.0:
        return 30.0

    # Normalizar pesos para que sumen 1.0 entre los disponibles
    pesos_adj = {k: PESOS_BASE[k] / peso_disponible for k in disponibles}

    resultado = sum(pesos_adj[k] * disponibles[k] for k in disponibles)
    return round(float(min(100.0, max(0.0, resultado))), 2)


# ---------------------------------------------------------------------------
# Versión batch
# ---------------------------------------------------------------------------

def calcular_flujo_score_batch(rows: list[dict]) -> list[float]:
    """
    Versión batch para múltiples zonas.

    Args:
        rows: lista de dicts con keys:
              - popular_times_score      (float | None)
              - vcity_flujo_peatonal     (float | None)
              - flujo_peatonal_total     (float | None)   — vianants BCN
              - ratio_locales_comerciales (float | None)

    Returns:
        Lista de flujo_peatonal_score [0-100], una entrada por fila.
    """
    return [
        calcular_flujo_score(
            popular_times_score=row.get("popular_times_score")
                or row.get("flujo_popular_times_score"),
            vcity_flujo=row.get("vcity_flujo_peatonal"),
            vianants_intensitat=row.get("flujo_peatonal_total"),
            ratio_locales=row.get("ratio_locales_comerciales"),
        )
        for row in rows
    ]


# ---------------------------------------------------------------------------
# Introspección de fuentes disponibles
# ---------------------------------------------------------------------------

def fuentes_disponibles(row: dict) -> list[str]:
    """
    Devuelve qué fuentes tienen datos para una zona dada.

    Args:
        row: dict con claves del esquema variables_zona:
             flujo_popular_times_score, vcity_flujo_peatonal,
             flujo_peatonal_total, ratio_locales_comerciales.

    Returns:
        Lista de nombres de fuente presentes (subconjunto de PESOS_BASE.keys()).
    """
    fuentes: list[str] = []

    pt = row.get("flujo_popular_times_score") or row.get("popular_times_score")
    if pt is not None:
        fuentes.append("popular_times")

    if row.get("vcity_flujo_peatonal") is not None:
        fuentes.append("vcity")

    if row.get("flujo_peatonal_total") is not None:
        fuentes.append("vianants")

    if row.get("ratio_locales_comerciales") is not None:
        fuentes.append("ratio_locales")

    return fuentes
