"""
scoring/dimensiones/demografia.py — Scoring demográfico multivariable para GeoRetail BCN.

Calcula un score 0-100 que responde a: "¿Es la demografía de esta zona adecuada
para el tipo de negocio que quiere abrir el usuario?"

Cinco sub-scores ponderados, con pesos adaptativos por tipo de negocio:
  s_renta      — poder adquisitivo (renta_media_hogar)
  s_densidad   — volumen de clientela potencial (densidad_hab_km2)
  s_consumo    — segmento de mayor gasto activo (pct_poblacio_25_44)
  s_capital    — capital humano / sensibilidad a propuestas diferenciadas (nivel_estudios_alto_pct)
  s_dinamismo  — trayectoria del barrio en los últimos 3 años (delta_renta_3a)

Adicionalmente:
  potencial_consumo  — índice compuesto (densidad × consumo × renta), 0-100
  perfil_demografico — etiqueta cualitativa de la zona
  fit_demografico    — bonus/penalización según afinidad idea_tags ↔ perfil, -15/+15

Constantes de referencia BCN 2022-2024:
  Renta:    17k € (mín) · 37k € (media) · 60k € (máx)
  Densidad: 30k hab/km² zona muy densa · 16k media
  Pct 25-44: 42% máximo práctico
  Estudios:  70% máximo práctico (universitarios)
"""
from __future__ import annotations

import logging
from typing import Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constantes de referencia BCN
# ---------------------------------------------------------------------------

# Actualizado 2026: mediana BCN ~42k € (INE/Idescat). Desplazamos el rango
# [min, max] arriba para que zonas típicas no caigan en "renta baja" de forma
# sistemática. Un hogar con 42k € ahora queda cerca del centro del rango.
_RENTA_MIN_BCN     = 20_000.0
_RENTA_MAX_BCN     = 65_000.0
_RENTA_MEDIA_BCN   = 42_000.0
_DENSIDAD_MAX_BCN  = 35_000.0   # hab/km² — umbral para score máximo
_DENSIDAD_MED_BCN  = 16_000.0   # media Barcelona
_PCT_25_44_MAX_BCN = 0.42
_NIVEL_EST_MAX_BCN = 0.70
_DELTA_RENTA_MAX   = 0.15       # +15% en 3 años → score dinamismo máximo

PERFILES_DEMOGRAFICOS: list[str] = [
    "joven_dinamico",
    "profesional_asentado",
    "familiar_consolidado",
    "envejecido_estable",
    "en_transicion",
    "mixto",
]


# ---------------------------------------------------------------------------
# Pesos por tipo de negocio
# ---------------------------------------------------------------------------

# Default: retail generalista. Densidad pesa casi tanto como renta porque
# para la mayoría de negocios el volumen de clientela es tan crítico como
# el poder adquisitivo medio.
_PESOS_DEFAULT: dict[str, float] = {
    "renta":    0.35,
    "densidad": 0.25,
    "consumo":  0.20,
    "capital":  0.10,
    "dinamismo": 0.10,
}

_PESOS_POR_TAG: dict[str, dict[str, float]] = {
    # Premium / alto ticket — renta crítica, densidad moderada
    "alta_renta":           {"renta": 0.55, "densidad": 0.15, "consumo": 0.10, "capital": 0.15, "dinamismo": 0.05},
    "gastronomico_premium": {"renta": 0.55, "densidad": 0.15, "consumo": 0.10, "capital": 0.15, "dinamismo": 0.05},
    "fitness_boutique":     {"renta": 0.45, "densidad": 0.20, "consumo": 0.20, "capital": 0.10, "dinamismo": 0.05},

    # Volumen / alta rotación — densidad y consumo activo son lo que importa
    "low_cost":             {"renta": 0.20, "densidad": 0.35, "consumo": 0.30, "capital": 0.05, "dinamismo": 0.10},
    "street_food":          {"renta": 0.15, "densidad": 0.40, "consumo": 0.30, "capital": 0.05, "dinamismo": 0.10},
    "specialty_coffee":     {"renta": 0.30, "densidad": 0.30, "consumo": 0.25, "capital": 0.10, "dinamismo": 0.05},

    # Segmento joven — consumo activo + dinamismo de barrio
    "clientela_joven":      {"renta": 0.20, "densidad": 0.25, "consumo": 0.35, "capital": 0.10, "dinamismo": 0.10},
    "vegano_organico":      {"renta": 0.25, "densidad": 0.25, "consumo": 0.25, "capital": 0.15, "dinamismo": 0.10},
    "ecologico_sostenible": {"renta": 0.25, "densidad": 0.20, "consumo": 0.25, "capital": 0.20, "dinamismo": 0.10},

    # Segmento profesional — equilibrio renta + capital humano
    "clientela_profesional":{"renta": 0.40, "densidad": 0.20, "consumo": 0.15, "capital": 0.20, "dinamismo": 0.05},
    "health_wellness":      {"renta": 0.38, "densidad": 0.20, "consumo": 0.20, "capital": 0.17, "dinamismo": 0.05},

    # Familiar — densidad alta + renta media
    "clientela_familiar":   {"renta": 0.30, "densidad": 0.30, "consumo": 0.20, "capital": 0.10, "dinamismo": 0.10},
    "kids_activity":        {"renta": 0.30, "densidad": 0.30, "consumo": 0.20, "capital": 0.10, "dinamismo": 0.10},

    # Tercera edad — renta estable + densidad residencial
    "salud_tercera_edad":   {"renta": 0.35, "densidad": 0.30, "consumo": 0.05, "capital": 0.25, "dinamismo": 0.05},

    # Turismo — densidad de paso y dinamismo, renta menos relevante
    "orientado_turismo":    {"renta": 0.20, "densidad": 0.35, "consumo": 0.15, "capital": 0.05, "dinamismo": 0.25},
}


# ---------------------------------------------------------------------------
# Fit demográfico: bonus/penalización por afinidad tag × perfil
# ---------------------------------------------------------------------------

_FIT_MATRIX: dict[str, dict[str, float]] = {
    "alta_renta":            {"profesional_asentado": 13, "joven_dinamico": 6,  "en_transicion": 8,  "envejecido_estable": -6},
    "gastronomico_premium":  {"profesional_asentado": 13, "joven_dinamico": 5,  "familiar_consolidado": -3, "envejecido_estable": -9},
    "low_cost":              {"familiar_consolidado": 9,  "envejecido_estable": 5, "profesional_asentado": -6, "en_transicion": -2},
    "street_food":           {"joven_dinamico": 9,        "familiar_consolidado": 6, "envejecido_estable": -6},
    "clientela_joven":       {"joven_dinamico": 13,       "en_transicion": 8,  "profesional_asentado": 3,  "envejecido_estable": -12},
    "clientela_familiar":    {"familiar_consolidado": 13, "profesional_asentado": 4, "joven_dinamico": -3},
    "kids_activity":         {"familiar_consolidado": 13, "envejecido_estable": -10},
    "specialty_coffee":      {"joven_dinamico": 11,       "profesional_asentado": 8, "en_transicion": 6, "envejecido_estable": -6},
    "fitness_boutique":      {"joven_dinamico": 11,       "profesional_asentado": 8, "familiar_consolidado": 3},
    "salud_tercera_edad":    {"envejecido_estable": 15,   "familiar_consolidado": 5, "joven_dinamico": -9},
    "clientela_profesional": {"profesional_asentado": 13, "joven_dinamico": 5,  "envejecido_estable": -6},
    "vegano_organico":       {"joven_dinamico": 9,        "profesional_asentado": 7, "en_transicion": 6, "envejecido_estable": -4},
    "ecologico_sostenible":  {"joven_dinamico": 9,        "en_transicion": 8,  "profesional_asentado": 5},
    "health_wellness":       {"profesional_asentado": 10, "joven_dinamico": 7,  "en_transicion": 5, "envejecido_estable": -4},
    "orientado_turismo":     {"en_transicion": 7,         "joven_dinamico": 5,  "profesional_asentado": 4},
}


# ---------------------------------------------------------------------------
# Función pública principal
# ---------------------------------------------------------------------------

def calcular_score_demografia(
    datos: dict,
    idea_tags: Optional[list[str]] = None,
    perfil_negocio: Optional[dict] = None,
) -> dict:
    """
    Score demográfico multivariable (0-100) para una zona de Barcelona.

    Claves esperadas en `datos`:
      renta_media_hogar, densidad_hab_km2, pct_poblacio_25_44,
      nivel_estudios_alto_pct, delta_renta_3a, edad_media, pct_extranjeros.
    Las ausentes se sustituyen por medias de Barcelona.
    """
    tags = idea_tags or []

    # ── Extraer variables con fallback a medias BCN ───────────────────────────
    renta      = _get(datos, "renta_media_hogar",       _RENTA_MEDIA_BCN)
    densidad   = _get(datos, "densidad_hab_km2",        _DENSIDAD_MED_BCN)
    pct_25_44  = _get(datos, "pct_poblacio_25_44",      0.28)
    nivel_est  = _get(datos, "nivel_estudios_alto_pct", 0.35)
    delta      = _get(datos, "delta_renta_3a",          0.0)

    # ── Sub-scores individuales (0-100) ───────────────────────────────────────

    # Poder adquisitivo: escala lineal 17k→0 / 60k→100
    s_renta = min(100.0, max(0.0,
        (renta - _RENTA_MIN_BCN) / (_RENTA_MAX_BCN - _RENTA_MIN_BCN) * 100.0
    ))

    # Volumen de clientela potencial: escala raíz cuadrada para suavizar extremos
    # Zonas densas (>35k hab/km²) no son marginalmente mejores que las de 25k
    s_densidad = min(100.0, (densidad / _DENSIDAD_MAX_BCN) ** 0.6 * 100.0)

    # Segmento consumo activo: 25-44 años (0% → 0, 42% → 100)
    s_consumo = min(100.0, (pct_25_44 / _PCT_25_44_MAX_BCN) * 100.0)

    # Capital humano / educación (0% → 0, 70% → 100)
    s_capital = min(100.0, (nivel_est / _NIVEL_EST_MAX_BCN) * 100.0)

    # Dinamismo / trayectoria: -15% → 0 · 0% → 50 · +15% → 100
    s_dinamismo = min(100.0, max(0.0,
        (delta + _DELTA_RENTA_MAX) / (2 * _DELTA_RENTA_MAX) * 100.0
    ))

    # ── Pesos adaptativos por tipo de negocio ─────────────────────────────────
    pesos = _PESOS_DEFAULT.copy()
    for tag in tags:
        if tag in _PESOS_POR_TAG:
            pesos = _PESOS_POR_TAG[tag]
            break

    # ── Métricas adicionales ──────────────────────────────────────────────────
    potencial = calcular_potencial_consumo(densidad, pct_25_44, renta)
    perfil    = detectar_perfil_demografico(datos)
    fit       = calcular_fit_demografico(datos, tags, perfil)

    # ── Score final ───────────────────────────────────────────────────────────
    score_base = (
        s_renta     * pesos["renta"]     +
        s_densidad  * pesos["densidad"]  +
        s_consumo   * pesos["consumo"]   +
        s_capital   * pesos["capital"]   +
        s_dinamismo * pesos["dinamismo"]
    )

    # ── Shift por vcity_resident_rate (mig 035) ──────────────────────────────
    # Proporción de peatones residentes (0..1). Aporta un shift aditivo de
    # ±5 puntos respecto del baseline 0.50 (β = 10, cap [-5, +5]).
    # Para negocios fuertemente turísticos (clientela_turismo > 0.6) se
    # escala por 0.5: los residentes no son el cliente ideal.
    # Neutral (0) si la señal es None.
    resident_rate = datos.get("vcity_resident_rate")
    resident_shift = 0.0
    if resident_rate is not None:
        try:
            rate_f = float(resident_rate)
            resident_shift = 10.0 * (rate_f - 0.50)
            resident_shift = max(-5.0, min(5.0, resident_shift))
            if perfil_negocio:
                try:
                    clientela_tur = float(perfil_negocio.get("clientela_turismo", 0.0) or 0.0)
                    if clientela_tur > 0.6:
                        resident_shift *= 0.5
                except (TypeError, ValueError):
                    pass
        except (TypeError, ValueError):
            resident_shift = 0.0

    score_demografia = min(100.0, max(0.0, score_base + fit + resident_shift))

    logger.debug(
        "demografia zona: s_renta=%.1f s_dens=%.1f s_cons=%.1f s_cap=%.1f "
        "s_din=%.1f fit=%.1f perfil=%s → %.1f",
        s_renta, s_densidad, s_consumo, s_capital, s_dinamismo, fit,
        perfil, score_demografia,
    )

    return {
        "score_demografia":   round(score_demografia, 1),
        "s_renta":            round(s_renta, 1),
        "s_densidad":         round(s_densidad, 1),
        "s_consumo":          round(s_consumo, 1),
        "s_capital":          round(s_capital, 1),
        "s_dinamismo":        round(s_dinamismo, 1),
        "potencial_consumo":  potencial,
        "perfil_demografico": perfil,
        "fit_demografico":    fit,
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get(datos: dict, key: str, default: float) -> float:
    v = datos.get(key)
    return float(v) if v is not None else default


# ---------------------------------------------------------------------------
# Índice de potencial de consumo
# ---------------------------------------------------------------------------

def calcular_potencial_consumo(densidad: float, pct_25_44: float, renta: float) -> float:
    """
    Índice compuesto (0-100): "¿cuántos consumidores activos con poder de compra hay?"

    Media geométrica ponderada para penalizar déficits en cualquier factor:
      densidad 40% · consumo-activo 35% · renta 25%
    """
    f_densidad = min(1.0, densidad / _DENSIDAD_MAX_BCN)
    f_consumo  = min(1.0, pct_25_44 / _PCT_25_44_MAX_BCN)
    f_renta    = min(1.0, max(0.0,
        (renta - _RENTA_MIN_BCN) / (_RENTA_MAX_BCN - _RENTA_MIN_BCN)
    ))

    potencial = (f_densidad ** 0.40) * (f_consumo ** 0.35) * (f_renta ** 0.25)
    return round(min(100.0, potencial * 100.0), 1)


# ---------------------------------------------------------------------------
# Perfil demográfico
# ---------------------------------------------------------------------------

def detectar_perfil_demografico(datos: dict) -> str:
    """
    Clasifica la zona en un perfil demográfico categórico.

    Las reglas se evalúan en orden de prioridad; la primera que se cumpla gana.
    Valores ausentes se sustituyen por medias de Barcelona.
    """
    renta     = _get(datos, "renta_media_hogar",       30_000.0)
    edad      = _get(datos, "edad_media",              43.0)
    pct_25_44 = _get(datos, "pct_poblacio_25_44",      0.28)
    densidad  = _get(datos, "densidad_hab_km2",        16_000.0)
    delta     = _get(datos, "delta_renta_3a",          0.08)
    estudios  = _get(datos, "nivel_estudios_alto_pct", 0.35)

    # Gentrificación activa
    if delta > 0.15:
        return "en_transicion"

    # Joven y dinámico
    if pct_25_44 > 0.33 and edad < 40 and delta > 0.03:
        return "joven_dinamico"

    # Profesional asentado: renta alta + estudios altos + edad laboral media
    if renta > 42_000 and estudios > 0.45 and 36 <= edad <= 47:
        return "profesional_asentado"

    # Familiar consolidado: densidad alta, renta media, población joven-adulta
    if densidad > 18_000 and 26_000 < renta < 46_000 and pct_25_44 > 0.22:
        return "familiar_consolidado"

    # Envejecido estable
    if edad > 46 and pct_25_44 < 0.22:
        return "envejecido_estable"

    return "mixto"


# ---------------------------------------------------------------------------
# Fit demográfico por idea_tags
# ---------------------------------------------------------------------------

def calcular_fit_demografico(datos: dict, idea_tags: list[str], perfil: str) -> float:
    """
    Bonus/penalización por afinidad entre el tipo de negocio y el perfil demográfico.

    Devuelve el valor más extremo (en valor absoluto) de todos los tags para evitar
    que bonificaciones pequeñas de múltiples tags anulen una penalización fuerte.
    Rango: -15 a +15 puntos.
    """
    if not idea_tags:
        return 0.0

    fits: list[float] = [
        _FIT_MATRIX[tag].get(perfil, 0.0)
        for tag in idea_tags
        if tag in _FIT_MATRIX and _FIT_MATRIX[tag].get(perfil, 0.0) != 0.0
    ]

    return max(fits, key=abs) if fits else 0.0
