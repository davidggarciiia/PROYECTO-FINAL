"""
scoring/entorno_score.py — Cálculo enriquecido de la dimensión entorno comercial.

Variables de entrada (de v_variables_zona):
  pct_locales_vacios       — % locales vacíos (existente)
  tasa_rotacion_anual      — tasa de rotación de comercios (existente)
  licencias_nuevas_1a      — nuevas licencias último año (v3)
  eventos_culturales_500m  — venues culturales/ocio en 500m (v3)
  score_aglomeracion       — efecto cluster sectorial (v6)
  ratio_complementarios    — negocios sinérgicos / máx esperado (v6)
  mercados_municipales_1km — mercados municipales en 1km (v8)
  m2_zonas_verdes_cercanas — zonas verdes en metros cuadrados (existente)

Ajuste por perfil_negocio:
  experiencial > 0.7  → aumentar peso ecosistema y aglomeración
  clientela_vecindario > 0.7 → penalizar más si vacios alto

El módulo NO realiza queries a BD — opera exclusivamente sobre el dict `datos`
que recibe como argumento. Esto lo hace testeable de forma aislada con datos
sintéticos y reutilizable como pipeline de features XGBoost.
"""
from __future__ import annotations

import logging
from typing import Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _get_float(datos: dict, key: str, fallback: float) -> float:
    """Extrae un float del dict con fallback seguro ante None y errores de tipo."""
    val = datos.get(key)
    if val is None:
        return fallback
    try:
        return float(val)
    except (TypeError, ValueError):
        return fallback


# ---------------------------------------------------------------------------
# Pesos base
# ---------------------------------------------------------------------------

_PESOS_BASE: dict[str, float] = {
    "vacios":    0.30,
    "rotacion":  0.20,
    "dinamismo": 0.20,
    "ecosistema": 0.20,
    "sinergia":  0.10,
}


# ---------------------------------------------------------------------------
# Función pública principal
# ---------------------------------------------------------------------------

def calcular_score_entorno(
    datos: dict,
    perfil_negocio: Optional[dict] = None,
) -> dict:
    """
    Calcula el score de entorno comercial para una zona de Barcelona.

    No realiza accesos a BD — opera únicamente sobre el dict `datos`.
    Los valores ausentes se sustituyen por valores neutros o medias de referencia BCN.

    Args:
        datos:           Dict con variables de `v_variables_zona`. Claves esperadas:
                         pct_locales_vacios, tasa_rotacion_anual, licencias_nuevas_1a,
                         eventos_culturales_500m, score_aglomeracion,
                         ratio_complementarios, mercados_municipales_1km,
                         m2_zonas_verdes_cercanas.
        perfil_negocio:  Dict opcional con dimensiones del perfil del negocio.
                         Claves relevantes: "experiencial", "clientela_vecindario".

    Returns:
        Dict con:
          score_entorno   (float 0-100): score final de la dimensión.
          s_vacios        (float 0-100): sub-score vacíos comerciales.
          s_rotacion      (float 0-100): sub-score rotación de comercios.
          s_dinamismo     (float 0-100): sub-score dinamismo de licencias.
          s_ecosistema    (float 0-100): sub-score ecosistema de entorno.
          s_sinergia      (float 0-100): sub-score sinergia sectorial.
          pesos_efectivos (dict):        pesos finales aplicados tras ajuste de perfil.
    """
    try:
        perfil = perfil_negocio or {}

        # ── Extraer variables con fallback ────────────────────────────────────
        pct_vacios       = _get_float(datos, "pct_locales_vacios",       0.15)
        tasa_rotacion    = _get_float(datos, "tasa_rotacion_anual",       0.30)
        licencias        = _get_float(datos, "licencias_nuevas_1a",       -1.0)  # -1 = ausente
        eventos          = _get_float(datos, "eventos_culturales_500m",   3.0)
        score_aglom      = _get_float(datos, "score_aglomeracion",        -1.0)  # -1 = ausente
        ratio_compl      = _get_float(datos, "ratio_complementarios",     -1.0)  # -1 = ausente
        mercados         = _get_float(datos, "mercados_municipales_1km",  1.0)
        zonas_verdes     = _get_float(datos, "m2_zonas_verdes_cercanas",  1200.0)

        # ── Sub-scores (0-100) ────────────────────────────────────────────────

        # 1. Vacíos: curva menos punitiva. Antes 100 − vacios·200 (25% → 50,
        #    50% → 0). Ahora 100 − vacios·150 (25% → 62, 50% → 25). Razonable
        #    para zonas residenciales periféricas donde 20-25% de vacíos es
        #    habitual sin implicar declive comercial.
        s_vacios = max(0.0, 100.0 - pct_vacios * 150.0)

        # 2. Rotación: >100% rotación anual = 0; 0% = 100
        s_rotacion = max(0.0, 100.0 - tasa_rotacion * 100.0)

        # 3. Dinamismo: licencias_nuevas_1a normalizada sobre media BCN (~8/año → 100)
        #    min(100, licencias * 12.5). Si no hay dato (marcado con -1), valor neutro-bajo 40.
        if licencias < 0.0:
            s_dinamismo = 40.0
        else:
            s_dinamismo = min(100.0, licencias * 12.5)

        # 4. Ecosistema: media ponderada de eventos (50%), mercados (30%), zonas verdes (20%)
        #    Eventos: 0=0, 8+=100  →  min(100, eventos * 12.5)
        s_eventos = min(100.0, eventos * 12.5)

        #    Mercados: 0→0, 1→50, 2→80, 3+→100
        if mercados <= 0:
            s_mercados = 0.0
        elif mercados == 1:
            s_mercados = 50.0
        elif mercados == 2:
            s_mercados = 80.0
        else:
            s_mercados = 100.0

        #    Zonas verdes: 0→20, 5000+→80  →  20 + min(60, zonas_verdes/5000*60)
        s_verdes = 20.0 + min(60.0, zonas_verdes / 5000.0 * 60.0)

        s_ecosistema = (
            s_eventos  * 0.50 +
            s_mercados * 0.30 +
            s_verdes   * 0.20
        )

        # 5. Sinergia: score_aglomeracion (60%) + ratio_complementarios×100 (40%)
        #    Si alguno no está disponible (marcado con -1), usar valor neutro 50 en su lugar.
        aglom_val = score_aglom if score_aglom >= 0.0 else 50.0
        compl_val = (ratio_compl * 100.0) if ratio_compl >= 0.0 else 50.0
        s_sinergia = aglom_val * 0.60 + compl_val * 0.40

        # ── Pesos efectivos con ajuste por perfil_negocio ─────────────────────
        pesos = _PESOS_BASE.copy()

        experiencial        = float(perfil.get("experiencial", 0.0))
        clientela_vecindario = float(perfil.get("clientela_vecindario", 0.0))

        if experiencial > 0.7:
            pesos["ecosistema"] = 0.30
            pesos["sinergia"]   = 0.15
            pesos["vacios"]     = 0.20
            pesos["rotacion"]   = 0.15
            pesos["dinamismo"]  = 0.20

        if clientela_vecindario > 0.7:
            pesos["vacios"]   = 0.35
            pesos["sinergia"] = 0.05

        # Renormalizar para que sumen 1.0
        total = sum(pesos.values())
        if total > 0.0:
            pesos = {k: round(v / total, 6) for k, v in pesos.items()}

        # ── Score final ───────────────────────────────────────────────────────
        score_entorno = (
            s_vacios     * pesos["vacios"]    +
            s_rotacion   * pesos["rotacion"]  +
            s_dinamismo  * pesos["dinamismo"] +
            s_ecosistema * pesos["ecosistema"] +
            s_sinergia   * pesos["sinergia"]
        )
        score_entorno = min(100.0, max(0.0, score_entorno))

        logger.debug(
            "entorno_score: s_vacios=%.1f s_rot=%.1f s_din=%.1f "
            "s_eco=%.1f s_sin=%.1f pesos=%s → score=%.1f",
            s_vacios, s_rotacion, s_dinamismo, s_ecosistema, s_sinergia,
            pesos, score_entorno,
        )

        return {
            "score_entorno":   round(score_entorno, 1),
            "s_vacios":        round(s_vacios, 1),
            "s_rotacion":      round(s_rotacion, 1),
            "s_dinamismo":     round(s_dinamismo, 1),
            "s_ecosistema":    round(s_ecosistema, 1),
            "s_sinergia":      round(s_sinergia, 1),
            "pesos_efectivos": pesos,
        }

    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "entorno_score: fallo en el cálculo, devolviendo fallback. Error: %s",
            exc,
            exc_info=True,
        )
        return {"score_entorno": 45.0}
