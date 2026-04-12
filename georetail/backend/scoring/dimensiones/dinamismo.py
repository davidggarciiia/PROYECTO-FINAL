"""
scoring/dimensiones/dinamismo.py — Score de dinamismo comercial (0-100).

Dimensión nueva que captura la TRAYECTORIA de una zona, no su estado actual.
Opera sobre datos de v_dinamismo_zona (calculados mensualmente por
pipelines/comercio/dinamismo.py).

Variables de entrada:
  score_dinamismo          — pre-calculado por el pipeline (base, 0-10)
  tendencia                — 'emergente' | 'estable' | 'saturado' | 'declive' | 'sin_datos'
  ratio_apertura_cierre_1a — apertura/cierre de licencias último año
  tasa_supervivencia_3a    — % negocios supervivieron ≥3 años en la zona
  renta_variacion_3a       — % variación de renta últimos 3 años
  hhi_sectorial            — diversidad sectorial (0=diverso, 1=monopolio)
  negocios_historico_count — muestra disponible (si <5 → 'sin_datos')

Sin I/O. No accede a BD. Testeable con datos sintéticos.
"""
from __future__ import annotations

import logging
from typing import Optional

logger = logging.getLogger(__name__)

# Ajuste aditivo por tendencia al score base (en unidades 0-100)
_AJUSTE_TENDENCIA: dict[str, float] = {
    "emergente":  5.0,
    "estable":    0.0,
    "saturado":  -5.0,
    "declive":  -10.0,
    "sin_datos":  0.0,
}

_FALLBACK_SCORE = 50.0   # valor neutro cuando no hay datos suficientes
_MUESTRA_MINIMA = 5      # negocios_historico_count mínimo para calcular


def _get(datos: dict, key: str, fallback: float) -> float:
    val = datos.get(key)
    if val is None:
        return fallback
    try:
        return float(val)
    except (TypeError, ValueError):
        return fallback


# ---------------------------------------------------------------------------
# API pública
# ---------------------------------------------------------------------------

def calcular_dinamismo(
    datos: dict,
    perfil_negocio: Optional[dict] = None,
) -> dict:
    """
    Calcula el score de dinamismo comercial para una zona.

    Args:
        datos:          Dict con campos de v_dinamismo_zona.
        perfil_negocio: Dict opcional con dimensiones del negocio.
                        Clave relevante: 'crecimiento' (0-1) — negocios
                        en expansión valoran más zonas emergentes.

    Returns:
        Dict con:
          score_dinamismo  (float 0-100): score final.
          tendencia        (str): clasificación de la zona.
          interpretacion   (str): texto legible para el frontend.
          confianza        (str): 'alta' | 'media' | 'baja' según muestra.
    """
    try:
        count = int(datos.get("negocios_historico_count") or 0)
        tendencia = str(datos.get("tendencia") or "sin_datos")

        # Sin muestra suficiente → fallback neutro
        if count < _MUESTRA_MINIMA or tendencia == "sin_datos":
            return {
                "score_dinamismo": _FALLBACK_SCORE,
                "tendencia":       "sin_datos",
                "interpretacion":  interpretar_tendencia("sin_datos"),
                "confianza":       "baja",
            }

        # Score base del pipeline (0-10) → escalar a 0-100
        base_pipeline = _get(datos, "score_dinamismo", 5.0)
        score = base_pipeline * 10.0

        # Ajuste por tendencia
        score += _AJUSTE_TENDENCIA.get(tendencia, 0.0)

        # Ajuste fino: renta creciendo es señal de gentrificación positiva
        # NOTA: renta_variacion_3a se almacena como fracción decimal (0.04 = 4%)
        renta_var = _get(datos, "renta_variacion_3a", 0.0)
        if renta_var > 0.10:
            score += 3.0
        elif renta_var > 0.05:
            score += 1.5
        elif renta_var < -0.05:
            score -= 2.0

        # Ajuste fino: supervivencia histórica como señal de confianza del mercado
        supervivencia = _get(datos, "tasa_supervivencia_3a", 0.5)
        if supervivencia > 0.70:
            score += 3.0
        elif supervivencia > 0.55:
            score += 1.0
        elif supervivencia < 0.35:
            score -= 3.0

        # Ajuste fino: diversidad sectorial (demasiada concentración = riesgo)
        hhi = _get(datos, "hhi_sectorial", 0.3)
        if hhi > 0.60:
            score -= 2.0
        elif hhi < 0.20:
            score += 1.0

        # Ajuste por perfil: negocios de crecimiento rápido valoran más emergente
        perfil = perfil_negocio or {}
        crecimiento = float(perfil.get("crecimiento", 0.0))
        if crecimiento > 0.7 and tendencia == "emergente":
            score += 4.0

        score = round(min(100.0, max(0.0, score)), 1)

        confianza = "alta" if count >= 20 else "media" if count >= 10 else "baja"

        logger.debug(
            "dinamismo_score: base=%.1f tendencia=%s renta_var=%.2f "
            "supervivencia=%.2f hhi=%.2f → score=%.1f",
            base_pipeline, tendencia, renta_var, supervivencia, hhi, score,
        )

        return {
            "score_dinamismo": score,
            "tendencia":       tendencia,
            "interpretacion":  interpretar_tendencia(tendencia),
            "confianza":       confianza,
        }

    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "dinamismo_score: fallo en cálculo, devolviendo fallback. Error: %s",
            exc,
            exc_info=True,
        )
        return {
            "score_dinamismo": _FALLBACK_SCORE,
            "tendencia":       "sin_datos",
            "interpretacion":  interpretar_tendencia("sin_datos"),
            "confianza":       "baja",
        }


def calcular_dinamismo_batch(registros: list[dict]) -> list[float]:
    """Versión batch — devuelve solo el score para cada registro."""
    return [calcular_dinamismo(r)["score_dinamismo"] for r in registros]


def interpretar_tendencia(tendencia: str) -> str:
    """Descripción legible para el frontend."""
    _TEXTOS = {
        "emergente":  "Zona en crecimiento comercial activo",
        "estable":    "Actividad comercial consolidada y estable",
        "saturado":   "Mercado concentrado, alta competencia sectorial",
        "declive":    "Descenso de actividad comercial en los últimos años",
        "sin_datos":  "Datos históricos insuficientes para esta zona",
    }
    return _TEXTOS.get(tendencia, "Estado desconocido")
