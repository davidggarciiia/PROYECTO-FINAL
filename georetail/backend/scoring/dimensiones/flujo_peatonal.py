"""
scoring/flujo_peatonal.py — Score de flujo peatonal basado en VCity + gosom popular_times.

Fuentes:
  1. vcity          (70%) — VCity BSC promedio diario (peatones/día). Fuente base.
  2. popular_times  (30%) — Picos de afluencia de Google Maps vía gosom scraper (0-100).

Lógica:
  - vcity es la fuente principal. Sin vcity → fallback 40.0.
  - popular_times captura CUÁNDO hay gente (picos horarios/diarios). Modifica el score base.
  - Si falta popular_times, se usa solo vcity al 100%.
  - Si falta vcity pero hay popular_times → se usa popular_times al 100% (fallback degradado).

Normalización:
  - vcity_flujo_peatonal: peatones/día ÷ VCITY_MAX_BARCELONA × 100.
  - popular_times_score: ya en escala 0-100 (gosom lo normaliza internamente).

Constante de normalización:
  VCITY_MAX_BARCELONA = 50 000  peatones/día (percentil 99 estimado BCN)
"""
from __future__ import annotations

from typing import Optional


# Pesos base por fuente (vcity es la fuente principal, popular_times el modificador de picos).
# Vianants y ratio_locales se eliminaron: el usuario prefiere estimación VCity frente a
# mezclar con sensores Vianants puntuales, que sólo cubren algunas calles emblemáticas.
PESOS_BASE: dict[str, float] = {
    "vcity":         0.70,
    "popular_times": 0.30,
}

# Valor de referencia para normalización vcity (percentil 99 estimado en BCN).
VCITY_MAX_BARCELONA: float = 50_000.0  # peatones/día zona muy transitada

# Mantenido por compatibilidad de import histórico (algunos pipelines lo
# consumen todavía). Ya no participa en el scoring de flujo peatonal.
VIANANTS_MAX_BARCELONA: float = 15_000.0
_VCITY_MAX_BCN    = VCITY_MAX_BARCELONA
_VIANANTS_MAX_BCN = VIANANTS_MAX_BARCELONA


# ---------------------------------------------------------------------------
# Función principal
# ---------------------------------------------------------------------------

def calcular_flujo_score(
    popular_times_score: Optional[float],   # 0-100, de gosom
    vcity_flujo: Optional[float],           # peatones/día (raw, normalizar con VCITY_MAX)
    vianants_intensitat: Optional[float] = None,  # IGNORADO — mantenido por compat de firma
    ratio_locales: Optional[float] = None,        # IGNORADO — mantenido por compat de firma
    vcity_max_barcelona: float = VCITY_MAX_BARCELONA,
    vianants_max_barcelona: float = VIANANTS_MAX_BARCELONA,  # IGNORADO
    vcity_shopping_rate: Optional[float] = None,   # mig 035: calidad del flujo (0..1)
) -> float:
    """
    Devuelve flujo_peatonal_score [0-100].

    Fuentes efectivas:
      - vcity_flujo (70%): peatones/día normalizado contra VCITY_MAX_BARCELONA.
      - popular_times_score (30%): picos Google Popular Times (ya en 0-100).

    Los parámetros `vianants_intensitat` y `ratio_locales` se aceptan por
    compatibilidad con callers antiguos pero se ignoran: la decisión de
    diseño es preferir la estimación VCity homogénea a los sensores puntuales
    de Vianants.

    Si no hay vcity ni popular_times → fallback 40.0.
    Si no hay vcity pero sí popular_times → usa popular_times al 100%.
    Si no hay popular_times → solo vcity al 100%.

    Modulador de calidad (mig 035): vcity_shopping_rate (0..1) reescala el
    resultado final. `quality_mult = 1 + 0.5 * (rate - 0.20)`, cap [0.85, 1.25].
    Neutral (1.0) si la señal es None.
    """
    # Normalizar vcity a 0-100
    s_vcity: Optional[float] = (
        float(min(100.0, max(0.0, vcity_flujo / vcity_max_barcelona * 100.0)))
        if vcity_flujo is not None
        else None
    )

    # popular_times ya viene en 0-100 del pipeline gosom
    s_pt: Optional[float] = (
        float(min(100.0, max(0.0, popular_times_score)))
        if popular_times_score is not None
        else None
    )

    # Modulador de calidad por shopping_rate (mig 035)
    quality_mult = 1.0
    if vcity_shopping_rate is not None:
        try:
            rate_f = float(vcity_shopping_rate)
            quality_mult = 1.0 + 0.5 * (rate_f - 0.20)
            quality_mult = max(0.85, min(1.25, quality_mult))
        except (TypeError, ValueError):
            quality_mult = 1.0

    if s_vcity is None and s_pt is None:
        return 40.0  # sin ninguna fuente: fallback conservador

    if s_vcity is None:
        resultado = float(s_pt) * quality_mult  # type: ignore[arg-type]
        return round(float(min(100.0, max(0.0, resultado))), 2)

    if s_pt is None:
        resultado = float(s_vcity) * quality_mult
        return round(float(min(100.0, max(0.0, resultado))), 2)

    # Fusión ponderada: vcity 70% + popular_times 30%
    resultado = (s_vcity * 0.70 + s_pt * 0.30) * quality_mult
    return round(float(min(100.0, max(0.0, resultado))), 2)


# ---------------------------------------------------------------------------
# Versión batch
# ---------------------------------------------------------------------------

def calcular_flujo_score_batch(rows: list[dict]) -> list[float]:
    """
    Versión batch para múltiples zonas.

    Args:
        rows: lista de dicts con keys:
              - vcity_flujo_peatonal      (float | None)  — fuente principal
              - popular_times_score / flujo_popular_times_score  (float | None)  — picos gosom

    Returns:
        Lista de flujo_peatonal_score [0-100], una entrada por fila.
    """
    return [
        calcular_flujo_score(
            popular_times_score=row.get("popular_times_score")
                or row.get("flujo_popular_times_score"),
            vcity_flujo=row.get("vcity_flujo_peatonal"),
            vcity_shopping_rate=row.get("vcity_shopping_rate"),
        )
        for row in rows
    ]


# ---------------------------------------------------------------------------
# Introspección de fuentes disponibles
# ---------------------------------------------------------------------------

def fuentes_disponibles(row: dict) -> list[str]:
    """
    Devuelve qué fuentes tienen datos para una zona dada.

    Sólo se consideran las fuentes activas del scorer (vcity + popular_times).
    Se mantiene soporte de lectura de `flujo_peatonal_total` y
    `ratio_locales_comerciales` por compatibilidad con callers antiguos que
    inspeccionan legacy; se reportan como `vianants` / `ratio_locales`.
    """
    fuentes: list[str] = []
    if row.get("vcity_flujo_peatonal") is not None:
        fuentes.append("vcity")
    pt = row.get("flujo_popular_times_score") or row.get("popular_times_score")
    if pt is not None:
        fuentes.append("popular_times")
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
    vianants_max_barcelona: float = VIANANTS_MAX_BARCELONA,  # ignorado
) -> dict:
    """
    Devuelve un desglose de la fusión ponderada vcity + popular_times.

    Se mantiene la forma del dict de salida con 4 claves en `sources`
    (popular_times, vcity, vianants, ratio_locales) para compatibilidad con
    código downstream (frontend debug panel, tests). Las dos últimas están
    marcadas `missing=True` con peso 0: el scoring ya no las usa.
    """
    pt_raw = row.get("flujo_popular_times_score") or row.get("popular_times_score")
    vc_raw = row.get("vcity_flujo_peatonal")
    vi_raw = row.get("flujo_peatonal_total")
    rl_raw = row.get("ratio_locales_comerciales")

    def _norm_pt(v: Optional[float]) -> Optional[float]:
        return float(min(100.0, max(0.0, v))) if v is not None else None

    def _norm_vc(v: Optional[float]) -> Optional[float]:
        return float(min(100.0, max(0.0, v / vcity_max_barcelona * 100.0))) if v is not None else None

    _PESOS_EXPLAIN: dict[str, float] = {
        "vcity":         0.70,
        "popular_times": 0.30,
    }

    norm_values: dict[str, Optional[float]] = {
        "vcity":         _norm_vc(vc_raw),
        "popular_times": _norm_pt(pt_raw),
    }

    disponibles = {k: v for k, v in norm_values.items() if v is not None}
    n_disponibles = len(disponibles)

    # Placeholder visible en sources para mantener el shape del API
    _LEGACY_KEYS = ["vianants", "ratio_locales"]

    if not disponibles:
        sources = {
            k: {"value": None, "weight": 0.0, "contribution": 0.0, "missing": True}
            for k in ("popular_times", "vcity", *_LEGACY_KEYS)
        }
        return {"score": 30.0, "sources": sources, "sources_available": 0}

    peso_disponible = sum(_PESOS_EXPLAIN[k] for k in disponibles)
    pesos_adj: dict[str, float] = {
        k: (_PESOS_EXPLAIN[k] / peso_disponible if k in disponibles else 0.0)
        for k in _PESOS_EXPLAIN
    }

    score = round(float(min(100.0, max(0.0, sum(
        pesos_adj[k] * disponibles[k] for k in disponibles
    )))), 2)

    sources: dict[str, dict] = {}
    for k in ("popular_times", "vcity"):
        val = norm_values[k]
        w   = pesos_adj[k]
        sources[k] = {
            "value":        round(val, 4) if val is not None else None,
            "weight":       round(w, 4),
            "contribution": round(w * val, 4) if val is not None else 0.0,
            "missing":      val is None,
        }

    # Legacy (vianants, ratio_locales): conservan el shape pero no aportan.
    for legacy_key, legacy_raw in (("vianants", vi_raw), ("ratio_locales", rl_raw)):
        present = legacy_raw is not None
        sources[legacy_key] = {
            "value":        float(legacy_raw) if present else None,
            "weight":       0.0,
            "contribution": 0.0,
            "missing":      True,  # no participa en el score por diseño
        }

    return {
        "score":             score,
        "sources":           sources,
        "sources_available": n_disponibles,
    }


def calcular_fit_temporal(
    row: dict,
    idea_tags: Optional[list[str]] = None,
    perfil_negocio: Optional[dict] = None,
) -> dict:
    """
    Ajuste temporal explicable para la dimensión de flujo.

    No sustituye al flujo base; mide si la zona encaja con el patrón temporal
    que necesita el negocio: oficina, fin de semana, noche o actividad estable.
    """
    tags = set(idea_tags or [])
    perfil = perfil_negocio or {}

    weekend_lift = _as_float(row.get("weekend_lift"))
    weekday_midday_share = _as_float(row.get("weekday_midday_share"))
    weekend_evening_share = _as_float(row.get("weekend_evening_share"))
    late_night_share = _as_float(row.get("late_night_share"))
    holiday_proxy_score = _as_float(row.get("holiday_proxy_score"))
    volatility = _as_float(row.get("seasonality_volatility"))
    summer_lift = _as_float(row.get("seasonality_summer_lift"))
    christmas_lift = _as_float(row.get("seasonality_christmas_lift"))
    confidence = _as_float(row.get("temporal_confianza")) or 0.0

    if confidence <= 0:
        return {"score_fit_temporal": 50.0, "confianza": 0.0, "modo": "sin_cobertura"}

    wants_weekend = bool(tags & {"weekend_peak", "orientado_turismo", "seasonal_peak"})
    wants_office = bool(tags & {"office_peak", "horario_diurno_comercial", "weekday_peak"})
    wants_night = bool(tags & {"late_night", "horario_nocturno"})
    wants_stability = float(perfil.get("clientela_vecindario", 0.0) or 0.0) >= 0.7

    components: list[tuple[float, float]] = []
    mode = "estable"

    if wants_weekend:
        mode = "weekend_peak"
        if weekend_lift is not None:
            components.append((0.45, _clip01((weekend_lift - 0.70) / 0.80)))
        if weekend_evening_share is not None:
            components.append((0.25, _clip01(weekend_evening_share / 0.35)))
        if holiday_proxy_score is not None:
            components.append((0.20, _clip01(holiday_proxy_score / 100.0)))
        if volatility is not None:
            components.append((0.10, 1.0 - _clip01(volatility / 0.45)))
    elif wants_office:
        mode = "office_peak"
        if weekday_midday_share is not None:
            components.append((0.55, _clip01(weekday_midday_share / 0.30)))
        if weekend_lift is not None:
            components.append((0.20, 1.0 - _clip01((weekend_lift - 0.85) / 0.60)))
        if volatility is not None:
            components.append((0.25, 1.0 - _clip01(volatility / 0.40)))
    elif wants_night:
        mode = "late_night"
        if late_night_share is not None:
            components.append((0.45, _clip01(late_night_share / 0.18)))
        if weekend_evening_share is not None:
            components.append((0.30, _clip01(weekend_evening_share / 0.35)))
        if holiday_proxy_score is not None:
            components.append((0.25, _clip01(holiday_proxy_score / 100.0)))
    else:
        mode = "estable" if wants_stability else "generalista"
        if volatility is not None:
            components.append((0.40, 1.0 - _clip01(volatility / 0.45)))
        if weekend_lift is not None:
            components.append((0.20, _clip01((weekend_lift - 0.60) / 0.80)))
        if summer_lift is not None:
            components.append((0.20, _clip01(summer_lift / 1.20)))
        if christmas_lift is not None:
            components.append((0.20, _clip01(christmas_lift / 1.20)))

    if not components:
        return {"score_fit_temporal": 50.0, "confianza": confidence, "modo": mode}

    total_weight = sum(weight for weight, _ in components)
    score = sum(weight * value for weight, value in components) / max(total_weight, 1e-6)
    return {
        "score_fit_temporal": round(score * 100.0, 1),
        "confianza": round(confidence, 3),
        "modo": mode,
    }


def calcular_flujo_con_temporalidad(
    *,
    row: dict,
    idea_tags: Optional[list[str]] = None,
    perfil_negocio: Optional[dict] = None,
    popular_times_score: Optional[float],
    vcity_flujo: Optional[float],
    vianants_intensitat: Optional[float],
    ratio_locales: Optional[float],
    vcity_max_barcelona: float = VCITY_MAX_BARCELONA,
    vianants_max_barcelona: float = VIANANTS_MAX_BARCELONA,
) -> dict:
    """
    Devuelve el score final de flujo integrando la capa temporal cuando existe.
    """
    score_base = calcular_flujo_score(
        popular_times_score=popular_times_score,
        vcity_flujo=vcity_flujo,
        vianants_intensitat=vianants_intensitat,
        ratio_locales=ratio_locales,
        vcity_max_barcelona=vcity_max_barcelona,
        vianants_max_barcelona=vianants_max_barcelona,
        vcity_shopping_rate=row.get("vcity_shopping_rate"),  # mig 035
    )
    temporal = calcular_fit_temporal(row, idea_tags=idea_tags, perfil_negocio=perfil_negocio)
    confianza = temporal["confianza"]
    if confianza < 0.35:
        score_final = score_base
    else:
        score_final = score_base * 0.75 + temporal["score_fit_temporal"] * 0.25
    return {
        "score_flujo": round(float(min(100.0, max(0.0, score_final))), 1),
        "score_flujo_base": round(float(score_base), 1),
        "score_fit_temporal": temporal["score_fit_temporal"],
        "temporal_confianza": confianza,
        "temporal_mode": temporal["modo"],
    }


def _as_float(value: object) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _clip01(value: float) -> float:
    return max(0.0, min(1.0, float(value)))
