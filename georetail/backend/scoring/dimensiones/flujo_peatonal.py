"""
scoring/flujo_peatonal.py — Fusión ponderada de fuentes de flujo peatonal.

Combina 4 fuentes con pesos adaptativos (si falta una, redistribuye):
  1. popular_times  (35%) — Google Maps Popular Times (ya en 0-100)
  2. vcity          (30%) — VCity BSC promedio diario (peatones/día, normalizar)
  3. vianants       (20%) — Sensores BCN Open Data (personas/día, normalizar)
  4. ratio_locales  (15%) — proxy estructural (fracción 0-1, siempre disponible)

Output: flujo_peatonal_score [0-100] listo para usar en XGBoost v4.

Fórmula de fusión:
  1. Normalizar cada fuente disponible a escala 0-100.
  2. Calcular peso_disponible = suma de PESOS_BASE de las fuentes presentes.
  3. Redistribuir proporcionalmente: peso_adj[k] = PESOS_BASE[k] / peso_disponible.
  4. score = Σ(peso_adj[k] × valor_norm[k]), acotado a [0, 100].
  Si no hay ninguna fuente disponible, devuelve el fallback conservador (30.0).

Normalización por fuente:
  - popular_times_score: ya en escala 0-100 (sin transformación).
  - vcity_flujo_peatonal: peatones/día ÷ VCITY_MAX_BARCELONA × 100.
  - flujo_peatonal_total (vianants): personas/día ÷ VIANANTS_MAX_BARCELONA × 100.
  - ratio_locales_comerciales: fracción 0-1 × 100.

Constantes de normalización configurables a nivel de módulo:
  VCITY_MAX_BARCELONA    = 50 000  peatones/día (percentil 99 estimado BCN)
  VIANANTS_MAX_BARCELONA = 15 000  personas/día (sensor máximo BCN Open Data)
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

# Valores de referencia para normalización (percentil 99 estimado en BCN).
# Modificar aquí para ajustar la escala global sin tocar las funciones.
VCITY_MAX_BARCELONA:    float = 50_000.0  # peatones/día zona muy transitada
VIANANTS_MAX_BARCELONA: float = 15_000.0  # personas/día sensor máximo BCN Open Data

# Alias privados mantenidos por compatibilidad interna
_VCITY_MAX_BCN    = VCITY_MAX_BARCELONA
_VIANANTS_MAX_BCN = VIANANTS_MAX_BARCELONA


# ---------------------------------------------------------------------------
# Función principal
# ---------------------------------------------------------------------------

def calcular_flujo_score(
    popular_times_score: Optional[float],   # 0-100
    vcity_flujo: Optional[float],           # peatones/día (raw, normalizar)
    vianants_intensitat: Optional[float],   # personas/día (raw, normalizar)
    ratio_locales: Optional[float],         # 0.0-1.0
    vcity_max_barcelona: float = VCITY_MAX_BARCELONA,
    vianants_max_barcelona: float = VIANANTS_MAX_BARCELONA,
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


# ---------------------------------------------------------------------------
# Explicación detallada de la fusión (debugging y frontend)
# ---------------------------------------------------------------------------

def flujo_peatonal_explain(
    row: dict,
    vcity_max_barcelona: float = VCITY_MAX_BARCELONA,
    vianants_max_barcelona: float = VIANANTS_MAX_BARCELONA,
) -> dict:
    """
    Devuelve un desglose detallado de la fusión ponderada para una zona.

    Útil para debugging y para que el frontend muestre qué fuentes
    contribuyeron al flujo_peatonal_score y en qué medida.

    Args:
        row: dict con claves del esquema variables_zona:
             flujo_popular_times_score (o popular_times_score),
             vcity_flujo_peatonal, flujo_peatonal_total,
             ratio_locales_comerciales.
        vcity_max_barcelona: referencia de normalización para VCity.
        vianants_max_barcelona: referencia de normalización para vianants.

    Returns:
        Dict con estructura:
        {
          "score": float,            # flujo_peatonal_score final [0-100]
          "sources": {
            "popular_times": {
              "value": float | None, # valor normalizado 0-100, o None si ausente
              "weight": float,       # peso efectivo aplicado (0.0 si ausente)
              "contribution": float, # weight × value
              "missing": bool,       # True si la fuente no tenía dato
            },
            "vcity": { ... },
            "vianants": { ... },
            "ratio_locales": { ... },
          },
          "sources_available": int,  # número de fuentes con dato
        }
    """
    # ── Resolver valores raw desde el dict ───────────────────────────────────
    pt_raw = row.get("flujo_popular_times_score") or row.get("popular_times_score")
    vc_raw = row.get("vcity_flujo_peatonal")
    vi_raw = row.get("flujo_peatonal_total")
    rl_raw = row.get("ratio_locales_comerciales")

    # ── Normalizar a 0-100 ────────────────────────────────────────────────────
    def _norm_pt(v: Optional[float]) -> Optional[float]:
        return float(min(100.0, max(0.0, v))) if v is not None else None

    def _norm_vc(v: Optional[float]) -> Optional[float]:
        return float(min(100.0, max(0.0, v / vcity_max_barcelona * 100.0))) if v is not None else None

    def _norm_vi(v: Optional[float]) -> Optional[float]:
        return float(min(100.0, max(0.0, v / vianants_max_barcelona * 100.0))) if v is not None else None

    def _norm_rl(v: Optional[float]) -> Optional[float]:
        return float(min(100.0, max(0.0, v * 100.0))) if v is not None else None

    norm_values: dict[str, Optional[float]] = {
        "popular_times": _norm_pt(pt_raw),
        "vcity":         _norm_vc(vc_raw),
        "vianants":      _norm_vi(vi_raw),
        "ratio_locales": _norm_rl(rl_raw),
    }

    # ── Redistribución adaptativa de pesos ───────────────────────────────────
    disponibles = {k: v for k, v in norm_values.items() if v is not None}
    n_disponibles = len(disponibles)

    if not disponibles:
        # Fallback conservador: sin datos
        sources = {
            k: {"value": None, "weight": 0.0, "contribution": 0.0, "missing": True}
            for k in PESOS_BASE
        }
        return {"score": 30.0, "sources": sources, "sources_available": 0}

    peso_disponible = sum(PESOS_BASE[k] for k in disponibles)
    pesos_adj: dict[str, float] = {
        k: (PESOS_BASE[k] / peso_disponible if k in disponibles else 0.0)
        for k in PESOS_BASE
    }

    score = round(float(min(100.0, max(0.0, sum(
        pesos_adj[k] * disponibles[k] for k in disponibles
    )))), 2)

    sources = {}
    for k in PESOS_BASE:
        val = norm_values[k]
        w   = pesos_adj[k]
        sources[k] = {
            "value":        round(val, 4) if val is not None else None,
            "weight":       round(w, 4),
            "contribution": round(w * val, 4) if val is not None else 0.0,
            "missing":      val is None,
        }

    return {
        "score":             score,
        "sources":           sources,
        "sources_available": n_disponibles,
    }
