"""
scoring/demografia_score.py — Scoring demográfico multivariable para GeoRetail BCN.

Integra las 7 variables demográficas disponibles en `v_variables_zona` en un
score de dimensión 0-100. Sustituye el cálculo simplificado de `scorer.py` que
usaba únicamente `renta_media_hogar`.

Dimensiones calculadas:
  - s_renta      (escala lineal BCN 17k-60k €/año)
  - s_dinamismo  (delta_renta_3a normalizado sobre ±10%)
  - s_consumo    (pct_poblacio_25_44, segmento de mayor consumo activo)
  - s_capital    (nivel_estudios_alto_pct, capital humano de la zona)

Métricas adicionales exportadas:
  - potencial_consumo  (índice compuesto densidad × segmento × renta, 0-100)
  - perfil_demografico (categoría cualitativa de la zona)
  - fit_demografico    (bonus/penalización por afinidad con idea_tags, -15/+15)

El módulo NO realiza queries a BD — opera exclusivamente sobre el dict `datos`
que recibe como argumento. Esto lo hace testeable de forma aislada con datos
sintéticos y reutilizable como pipeline de features XGBoost.

Constantes de referencia basadas en datos BCN 2022-2024:
  Renta:      17.000 € (mín) · 37.000 € (media) · 60.000 € (máx)
  Densidad:   30.000 hab/km² marca zona densa para BCN
  Pct 25-44:  42% máximo práctico en BCN
  Estudios:   70% máximo práctico de universitarios en BCN
"""
from __future__ import annotations

import logging
from typing import Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constantes de referencia BCN
# ---------------------------------------------------------------------------

_RENTA_MIN_BCN = 17000.0
_RENTA_MAX_BCN = 60000.0
_RENTA_MEDIA_BCN = 37000.0
_DENSIDAD_ALTA_BCN = 30000.0       # hab/km² zona densa
_PCT_25_44_MAX_BCN = 0.42          # máximo práctico en BCN
_NIVEL_ESTUDIOS_MAX_BCN = 0.70     # máximo práctico en BCN

# Lista exportada de perfiles posibles
PERFILES_DEMOGRAFICOS: list[str] = [
    "joven_dinamico",
    "profesional_asentado",
    "familiar_consolidado",
    "envejecido_estable",
    "en_transicion",
    "mixto",
]


# ---------------------------------------------------------------------------
# Pesos adaptativos por idea_tag
# ---------------------------------------------------------------------------

_PESOS_DEFAULT: dict[str, float] = {
    "renta": 0.50,
    "dinamismo": 0.20,
    "consumo": 0.15,
    "capital": 0.15,
}

_PESOS_POR_TAG: dict[str, dict[str, float]] = {
    "alta_renta":            {"renta": 0.65, "dinamismo": 0.15, "consumo": 0.05, "capital": 0.15},
    "gastronomico_premium":  {"renta": 0.65, "dinamismo": 0.15, "consumo": 0.05, "capital": 0.15},
    "low_cost":              {"renta": 0.30, "dinamismo": 0.20, "consumo": 0.30, "capital": 0.20},
    "street_food":           {"renta": 0.25, "dinamismo": 0.20, "consumo": 0.35, "capital": 0.20},
    "clientela_joven":       {"renta": 0.30, "dinamismo": 0.25, "consumo": 0.35, "capital": 0.10},
    "clientela_profesional": {"renta": 0.40, "dinamismo": 0.20, "consumo": 0.15, "capital": 0.25},
    "fitness_boutique":      {"renta": 0.45, "dinamismo": 0.20, "consumo": 0.20, "capital": 0.15},
    "health_wellness":       {"renta": 0.40, "dinamismo": 0.20, "consumo": 0.15, "capital": 0.25},
    "specialty_coffee":      {"renta": 0.40, "dinamismo": 0.25, "consumo": 0.20, "capital": 0.15},
    "clientela_familiar":    {"renta": 0.40, "dinamismo": 0.15, "consumo": 0.20, "capital": 0.25},
    "kids_activity":         {"renta": 0.40, "dinamismo": 0.15, "consumo": 0.20, "capital": 0.25},
    "vegano_organico":       {"renta": 0.35, "dinamismo": 0.25, "consumo": 0.20, "capital": 0.20},
    "ecologico_sostenible":  {"renta": 0.35, "dinamismo": 0.25, "consumo": 0.20, "capital": 0.20},
    "salud_tercera_edad":    {"renta": 0.40, "dinamismo": 0.10, "consumo": 0.10, "capital": 0.40},
    "orientado_turismo":     {"renta": 0.50, "dinamismo": 0.30, "consumo": 0.10, "capital": 0.10},
}


# ---------------------------------------------------------------------------
# Matriz de fit demográfico (tag × perfil → bonus)
# ---------------------------------------------------------------------------

_FIT_MATRIX: dict[str, dict[str, float]] = {
    "alta_renta":            {"profesional_asentado": 12, "joven_dinamico": 6,  "en_transicion": 8,   "envejecido_estable": -5},
    "gastronomico_premium":  {"profesional_asentado": 12, "joven_dinamico": 5,  "familiar_consolidado": -3, "envejecido_estable": -8},
    "low_cost":              {"familiar_consolidado": 8,  "envejecido_estable": 5, "profesional_asentado": -5, "en_transicion": -3},
    "clientela_joven":       {"joven_dinamico": 12,       "en_transicion": 8,   "profesional_asentado": 3, "envejecido_estable": -10},
    "clientela_familiar":    {"familiar_consolidado": 12, "profesional_asentado": 5, "joven_dinamico": -3},
    "kids_activity":         {"familiar_consolidado": 12, "envejecido_estable": -8},
    "specialty_coffee":      {"joven_dinamico": 10,       "profesional_asentado": 8, "en_transicion": 6, "envejecido_estable": -5},
    "fitness_boutique":      {"joven_dinamico": 10,       "profesional_asentado": 8, "familiar_consolidado": 3},
    "salud_tercera_edad":    {"envejecido_estable": 15,   "familiar_consolidado": 5, "joven_dinamico": -8},
    "clientela_profesional": {"profesional_asentado": 12, "joven_dinamico": 6,  "envejecido_estable": -5},
    "vegano_organico":       {"joven_dinamico": 8,        "profesional_asentado": 8, "en_transicion": 6, "envejecido_estable": -3},
    "ecologico_sostenible":  {"joven_dinamico": 8,        "en_transicion": 8,   "profesional_asentado": 5},
    "street_food":           {"joven_dinamico": 8,        "familiar_consolidado": 6, "envejecido_estable": -5},
    "orientado_turismo":     {"en_transicion": 6,         "joven_dinamico": 5},
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
    Calcula el score demográfico multivariable para una zona de Barcelona.

    No realiza accesos a BD — opera únicamente sobre el dict `datos`.
    Los valores ausentes se sustituyen por medias/valores neutros de BCN.

    Args:
        datos:     Dict con variables de `v_variables_zona`. Claves esperadas:
                   renta_media_hogar, edad_media, pct_extranjeros,
                   densidad_hab_km2, pct_poblacio_25_44, delta_renta_3a,
                   nivel_estudios_alto_pct.
        idea_tags: Tags semánticos del negocio (pueden ser None).
                   Determinan los pesos de cada sub-score y el fit demográfico.

    Returns:
        Dict con:
          score_demografia   (float 0-100): score final de la dimensión.
          s_renta            (float 0-100): sub-score renta.
          s_dinamismo        (float 0-100): sub-score tendencia renta 3 años.
          s_consumo          (float 0-100): sub-score segmento 25-44 años.
          s_capital          (float 0-100): sub-score capital humano.
          potencial_consumo  (float 0-100): índice compuesto por zona.
          perfil_demografico (str):         categoría cualitativa detectada.
          fit_demografico    (float):       bonus/penalización por tags (-15/+15).
    """
    tags = idea_tags or []

    # ── Extraer variables con fallback a medias BCN ───────────────────────────
    renta      = datos.get("renta_media_hogar") if datos.get("renta_media_hogar") is not None else _RENTA_MEDIA_BCN
    delta      = datos.get("delta_renta_3a")    if datos.get("delta_renta_3a")    is not None else 0.0
    pct_25_44  = datos.get("pct_poblacio_25_44") if datos.get("pct_poblacio_25_44") is not None else 0.28
    nivel_est  = datos.get("nivel_estudios_alto_pct") if datos.get("nivel_estudios_alto_pct") is not None else 0.35
    densidad   = datos.get("densidad_hab_km2")  if datos.get("densidad_hab_km2")  is not None else 16000.0

    # ── Sub-scores (0-100) ────────────────────────────────────────────────────

    # Renta: escala lineal BCN (17k → 0, 60k → 100)
    # (60000 - 17000) / 100 = 430 €/punto
    s_renta = min(100.0, max(0.0, (renta - _RENTA_MIN_BCN) / 430.0))

    # Dinamismo: delta_renta_3a normalizado
    # -10% → 0 · 0% → 50 · +10% → 100
    # (delta + 0.10) / 0.0020 = (delta + 0.10) * 500
    s_dinamismo = min(100.0, max(0.0, (delta + 0.10) / 0.0020))

    # Consumo activo: % población 25-44 (0% → 0, 42% → 100)
    s_consumo = min(100.0, (pct_25_44 / _PCT_25_44_MAX_BCN) * 100.0)

    # Capital humano: % estudios universitarios (0% → 0, 70% → 100)
    s_capital = min(100.0, (nivel_est / _NIVEL_ESTUDIOS_MAX_BCN) * 100.0)

    # ── Pesos adaptativos ─────────────────────────────────────────────────────
    pesos = _PESOS_DEFAULT.copy()
    for tag in tags:
        if tag in _PESOS_POR_TAG:
            pesos = _PESOS_POR_TAG[tag]
            break

    # ── Métricas adicionales ──────────────────────────────────────────────────
    potencial = calcular_potencial_consumo(densidad, pct_25_44, renta)
    perfil = detectar_perfil_demografico(datos)
    fit = calcular_fit_demografico(datos, tags, perfil)

    # ── Score final ───────────────────────────────────────────────────────────
    score_base = (
        s_renta     * pesos["renta"]     +
        s_dinamismo * pesos["dinamismo"] +
        s_consumo   * pesos["consumo"]   +
        s_capital   * pesos["capital"]
    )
    score_demografia = min(100.0, max(0.0, score_base + fit))

    logger.debug(
        "demografia_score zona: s_renta=%.1f s_din=%.1f s_cons=%.1f s_cap=%.1f "
        "fit=%.1f perfil=%s → score=%.1f",
        s_renta, s_dinamismo, s_consumo, s_capital, fit, perfil, score_demografia,
    )

    return {
        "score_demografia":   round(score_demografia, 1),
        "s_renta":            round(s_renta, 1),
        "s_dinamismo":        round(s_dinamismo, 1),
        "s_consumo":          round(s_consumo, 1),
        "s_capital":          round(s_capital, 1),
        "potencial_consumo":  potencial,
        "perfil_demografico": perfil,
        "fit_demografico":    fit,
    }


# ---------------------------------------------------------------------------
# Índice de potencial de consumo por zona
# ---------------------------------------------------------------------------

def calcular_potencial_consumo(densidad: float, pct_25_44: float, renta: float) -> float:
    """
    Índice de potencial de consumo por zona (0-100).

    Combina densidad de población, segmento activo y poder adquisitivo.
    Útil como feature XGBoost independiente del score demográfico principal.

    Responde a: "¿cuántos consumidores activos con poder de compra hay por km²?"

    La media geométrica ponderada penaliza de forma multiplicativa los déficits
    en cualquiera de los tres factores, a diferencia de una media aritmética
    que permitiría compensar un factor muy bajo con otro muy alto.

    Args:
        densidad:  Densidad de población en hab/km².
        pct_25_44: Fracción de población de 25 a 44 años (0-1).
        renta:     Renta media del hogar en €/año.

    Returns:
        Índice 0-100 redondeado a 1 decimal.
    """
    # Normalizar cada componente a 0-1
    factor_densidad = min(1.0, densidad / _DENSIDAD_ALTA_BCN)
    factor_consumo  = min(1.0, pct_25_44 / _PCT_25_44_MAX_BCN)
    factor_renta    = min(1.0, max(0.0, (renta - _RENTA_MIN_BCN) / (_RENTA_MAX_BCN - _RENTA_MIN_BCN)))

    # Media geométrica ponderada: densidad 40%, consumo 35%, renta 25%
    potencial = (
        (factor_densidad ** 0.40) *
        (factor_consumo  ** 0.35) *
        (factor_renta    ** 0.25)
    )
    return round(min(100.0, potencial * 100.0), 1)


# ---------------------------------------------------------------------------
# Clasificación del perfil demográfico de la zona
# ---------------------------------------------------------------------------

def detectar_perfil_demografico(datos: dict) -> str:
    """
    Clasifica la zona en un perfil demográfico categórico.

    Las reglas se evalúan en orden de prioridad; la primera que se cumpla
    determina el perfil. Los valores ausentes se sustituyen por las medias
    de Barcelona para garantizar una clasificación razonable.

    Perfiles posibles (ver también PERFILES_DEMOGRAFICOS):
      "joven_dinamico"        — pct_25_44 alta + edad_media baja + delta positivo
      "profesional_asentado"  — renta alta + nivel_estudios alto + edad media laboral
      "familiar_consolidado"  — densidad alta + renta media + pct_25_44 moderada
      "envejecido_estable"    — edad_media alta + pct_25_44 baja + delta bajo
      "en_transicion"         — delta_renta muy alto (gentrificación activa)
      "mixto"                 — no encaja claramente en ningún perfil

    Args:
        datos: Dict con variables demográficas de la zona.

    Returns:
        Nombre del perfil detectado (siempre una de las cadenas de PERFILES_DEMOGRAFICOS).
    """
    renta     = datos.get("renta_media_hogar")        if datos.get("renta_media_hogar")        is not None else 30000.0
    edad      = datos.get("edad_media")               if datos.get("edad_media")               is not None else 43.0
    pct_25_44 = datos.get("pct_poblacio_25_44")       if datos.get("pct_poblacio_25_44")       is not None else 0.28
    densidad  = datos.get("densidad_hab_km2")         if datos.get("densidad_hab_km2")         is not None else 16000.0
    delta     = datos.get("delta_renta_3a")           if datos.get("delta_renta_3a")           is not None else 0.08
    estudios  = datos.get("nivel_estudios_alto_pct")  if datos.get("nivel_estudios_alto_pct")  is not None else 0.35

    # Gentrificación activa: crecimiento de renta > 15%
    if delta > 0.15:
        return "en_transicion"

    # Joven y dinámico: mucha población 25-44 y baja edad media
    if pct_25_44 > 0.33 and edad < 40 and delta > 0.05:
        return "joven_dinamico"

    # Profesional asentado: renta alta + estudios altos + edad media laboral
    if renta > 42000 and estudios > 0.45 and 37 <= edad <= 46:
        return "profesional_asentado"

    # Familiar consolidado: alta densidad, renta media, presencia de familias
    if densidad > 20000 and 28000 < renta < 45000 and pct_25_44 > 0.22:
        return "familiar_consolidado"

    # Envejecido: edad media alta y poca población joven
    if edad > 46 and pct_25_44 < 0.22:
        return "envejecido_estable"

    return "mixto"


# ---------------------------------------------------------------------------
# Cálculo del fit demográfico según idea_tags
# ---------------------------------------------------------------------------

def calcular_fit_demografico(datos: dict, idea_tags: list[str], perfil: str) -> float:
    """
    Calcula el bonus o penalización demográfica por afinidad entre la idea
    de negocio y el perfil de la zona.

    Itera todos los tags presentes en idea_tags y consulta la _FIT_MATRIX.
    Si un tag produce un bonus distinto de cero para el perfil dado, lo añade
    a la lista de candidatos. Devuelve el valor más extremo (en valor absoluto)
    para evitar que bonificaciones pequeñas de múltiples tags anulen una
    penalización fuerte de un tag crítico.

    Args:
        datos:     Dict de variables de la zona (no utilizado directamente,
                   incluido por consistencia de firma con el resto del módulo).
        idea_tags: Lista de tags semánticos del negocio.
        perfil:    Perfil demográfico ya calculado para la zona.

    Returns:
        Bonus/penalización en puntos (-15 a +15). 0.0 si no hay tags o
        ningún tag tiene entrada en la matriz para el perfil dado.
    """
    if not idea_tags:
        return 0.0

    fits: list[float] = []
    for tag in idea_tags:
        if tag in _FIT_MATRIX:
            bonus = _FIT_MATRIX[tag].get(perfil, 0.0)
            if bonus != 0.0:
                fits.append(bonus)

    if not fits:
        return 0.0

    # Usar el fit más extremo (positivo o negativo)
    return max(fits, key=abs)
