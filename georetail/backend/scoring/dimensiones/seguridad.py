"""
scoring/seguridad_score.py — Cálculo enriquecido de la dimensión seguridad.

Variables de entrada (de v_variables_zona / vz_entorno):
  incidencias_por_1000hab  — incidencias totales GU (base existente)
  hurtos_por_1000hab       — carterismo/hurto (v7, pipeline seguridad.py)
  robatoris_por_1000hab    — robo con fuerza/violencia (v7)
  danys_por_1000hab        — daños a la propiedad/vandalismo (v7)
  incidencias_noche_pct    — fracción nocturna 20:00-06:00 (v7)
  comisarias_1km           — comisarías en radio 1km (v7)
  nivel_ruido_db           — proxy entorno nocturno (ya en BD)
  pct_locales_vacios       — "ojos en la calle" (Jane Jacobs)

Ajuste por perfil_negocio:
  horario_nocturno > 0.7  → amplificar peso incidencias nocturnas
  clientela_vecindario > 0.7 → aumentar peso percepción

El módulo NO realiza queries a BD — opera exclusivamente sobre el dict `datos`
que recibe como argumento. Esto lo hace testeable de forma aislada con datos
sintéticos y reutilizable como pipeline de features XGBoost.

Constantes de referencia basadas en datos BCN 2022-2024:
  incidencias_por_1000hab: 5 (mín) · 35 (media) · 120 (máx)
  hurtos_por_1000hab:      media BCN 18
  robatoris_por_1000hab:   media BCN 8
  danys_por_1000hab:       media BCN 5
  incidencias_noche_pct:   rango 0.15-0.60
  nivel_ruido_db:          >70 dB penaliza
"""
from __future__ import annotations

import logging
from typing import Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constantes de referencia BCN
# ---------------------------------------------------------------------------

_INCIDENCIAS_MIN_BCN = 5.0
_INCIDENCIAS_MAX_BCN = 120.0
_INCIDENCIAS_DIVISOR = 1.15        # (120 - 5) / 100 ≈ 1.15

_HURTOS_MEDIA_BCN    = 18.0
_ROBATORIS_MEDIA_BCN = 8.0
_DANYS_MEDIA_BCN     = 5.0

_NOCHE_PCT_MIN = 0.15
_NOCHE_PCT_MAX = 0.60
_RUIDO_UMBRAL_DB = 70.0


# ---------------------------------------------------------------------------
# Helper interno
# ---------------------------------------------------------------------------

def _get_float(datos: dict, key: str, default: Optional[float] = None) -> Optional[float]:
    """Extrae un valor numérico del dict con fallback; devuelve None si ausente y no hay default."""
    val = datos.get(key)
    if val is None:
        return default
    try:
        return float(val)
    except (TypeError, ValueError):
        logger.warning("seguridad_score: valor no numérico para '%s': %r", key, val)
        return default


# ---------------------------------------------------------------------------
# Función pública principal
# ---------------------------------------------------------------------------

def calcular_score_seguridad(
    datos: dict,
    perfil_negocio: Optional[dict] = None,
) -> dict:
    """
    Calcula el score de seguridad multivariable para una zona de Barcelona.

    No realiza accesos a BD — opera únicamente sobre el dict `datos`.
    Los valores ausentes se sustituyen por valores neutros o medias BCN.

    Args:
        datos:           Dict con variables de `v_variables_zona` / `vz_entorno`.
                         Claves esperadas: incidencias_por_1000hab, hurtos_por_1000hab,
                         robatoris_por_1000hab, danys_por_1000hab, incidencias_noche_pct,
                         comisarias_1km, nivel_ruido_db, pct_locales_vacios.
        perfil_negocio:  Dict opcional con rasgos del negocio:
                         horario_nocturno (0-1), clientela_vecindario (0-1).

    Returns:
        Dict con:
          score_seguridad    (float 0-100): score final de la dimensión.
          s_tipo_delito      (float 0-100): sub-score composición delictiva.
          s_entorno_nocturno (float 0-100): sub-score riesgo nocturno.
          s_vigilancia       (float 0-100): sub-score presencia policial.
          s_vitalidad        (float 0-100): sub-score "ojos en la calle".
          tiene_datos_v7     (bool):        True si hay datos desagregados v7.
          pesos_efectivos    (dict):        pesos finales usados para el cálculo.

    Raises:
        No lanza excepciones — cualquier error retorna el dict de fallback.
    """
    perfil_negocio = perfil_negocio or {}

    try:
        # ── 1. s_tipo_delito ─────────────────────────────────────────────────
        incidencias = _get_float(datos, "incidencias_por_1000hab", default=35.0)
        s_incidencias_total = min(100.0, max(0.0, (120.0 - incidencias) / _INCIDENCIAS_DIVISOR))

        hurtos    = _get_float(datos, "hurtos_por_1000hab")
        robatoris = _get_float(datos, "robatoris_por_1000hab")
        danys     = _get_float(datos, "danys_por_1000hab")

        tiene_datos_v7 = hurtos is not None and robatoris is not None and danys is not None

        if tiene_datos_v7:
            # Índice ponderado normalizado sobre medias BCN; invertido a 0-100.
            # Damos más peso a delitos violentos (robatoris) que a hurtos/carterismo:
            # 0.5·hurtos + 1.2·robatoris + 0.4·danys. Así zonas como Raval con
            # muchos hurtos pero pocos robos no caen al suelo injustamente.
            indice_raw = (
                (hurtos    / _HURTOS_MEDIA_BCN)    * 0.5 +
                (robatoris / _ROBATORIS_MEDIA_BCN) * 1.2 +
                (danys     / _DANYS_MEDIA_BCN)     * 0.4
            )
            # indice_raw == 1.0 ≈ media BCN (score ~50).
            # Divisor 2.8 (antes 2.2) suaviza la caída: indice_raw=2.0 → ~28
            # en lugar de ~9, menos punitivo para zonas comercialmente viables.
            s_tipo_delito = min(100.0, max(0.0, 100.0 - (indice_raw / 2.8) * 100.0))
        else:
            s_tipo_delito = s_incidencias_total

        # ── 2. s_entorno_nocturno ─────────────────────────────────────────────
        noche_pct = _get_float(datos, "incidencias_noche_pct")
        ruido_db  = _get_float(datos, "nivel_ruido_db")

        if noche_pct is None and ruido_db is None:
            s_entorno_nocturno = 50.0
        else:
            # Sub-componente fracción nocturna: 0.15 → 100, 0.60 → 0
            if noche_pct is not None:
                rango = _NOCHE_PCT_MAX - _NOCHE_PCT_MIN          # 0.45
                s_noche = min(100.0, max(0.0, (1.0 - (noche_pct - _NOCHE_PCT_MIN) / rango) * 100.0))
            else:
                s_noche = 50.0

            # Sub-componente ruido: >70 dB penaliza linealmente (−2 puntos/dB)
            if ruido_db is not None:
                penalizacion_ruido = max(0.0, (ruido_db - _RUIDO_UMBRAL_DB) * 2.0)
                s_ruido = min(100.0, max(0.0, 100.0 - penalizacion_ruido))
            else:
                s_ruido = 50.0

            # Combinar: noche 65%, ruido 35%
            s_entorno_nocturno = s_noche * 0.65 + s_ruido * 0.35

        # ── 3. s_vigilancia ──────────────────────────────────────────────────
        comisarias = _get_float(datos, "comisarias_1km")
        if comisarias is None:
            s_vigilancia = 60.0
        else:
            _tabla_comisarias = {0: 40.0, 1: 65.0, 2: 80.0}
            n = int(min(comisarias, 3))
            s_vigilancia = _tabla_comisarias.get(n, 100.0)

        # ── 4. s_vitalidad ────────────────────────────────────────────────────
        vacios = _get_float(datos, "pct_locales_vacios")
        if vacios is None:
            s_vitalidad = 55.0
        else:
            s_vitalidad = max(0.0, 100.0 - vacios * 150.0)

        # ── Pesos base ────────────────────────────────────────────────────────
        pesos: dict[str, float] = {
            "tipo_delito":      0.45,
            "entorno_nocturno": 0.25,
            "vigilancia":       0.15,
            "vitalidad":        0.15,
        }

        # ── Ajuste por perfil_negocio ─────────────────────────────────────────
        horario_nocturno     = float(perfil_negocio.get("horario_nocturno", 0.0))
        clientela_vecindario = float(perfil_negocio.get("clientela_vecindario", 0.0))

        if horario_nocturno > 0.7:
            pesos["entorno_nocturno"] = 0.40
            pesos["vitalidad"]        = 0.05
            # tipo_delito y vigilancia no cambian → renormalizar

        if clientela_vecindario > 0.7:
            pesos["vitalidad"]    = 0.25
            pesos["tipo_delito"]  = 0.35
            # entorno_nocturno y vigilancia no cambian → renormalizar

        # Renormalizar para que los pesos sumen exactamente 1.0
        total = sum(pesos.values())
        pesos = {k: round(v / total, 6) for k, v in pesos.items()}

        # ── Score final ───────────────────────────────────────────────────────
        score_seguridad = (
            s_tipo_delito      * pesos["tipo_delito"]      +
            s_entorno_nocturno * pesos["entorno_nocturno"] +
            s_vigilancia       * pesos["vigilancia"]       +
            s_vitalidad        * pesos["vitalidad"]
        )
        score_seguridad = min(100.0, max(0.0, score_seguridad))

        logger.debug(
            "seguridad_score: s_tipo=%.1f s_noche=%.1f s_vig=%.1f s_vit=%.1f "
            "v7=%s → score=%.1f",
            s_tipo_delito, s_entorno_nocturno, s_vigilancia, s_vitalidad,
            tiene_datos_v7, score_seguridad,
        )

        return {
            "score_seguridad":    round(score_seguridad, 1),
            "s_tipo_delito":      round(s_tipo_delito, 1),
            "s_entorno_nocturno": round(s_entorno_nocturno, 1),
            "s_vigilancia":       round(s_vigilancia, 1),
            "s_vitalidad":        round(s_vitalidad, 1),
            "tiene_datos_v7":     tiene_datos_v7,
            "pesos_efectivos":    pesos,
        }

    except Exception as exc:  # noqa: BLE001
        logger.warning("seguridad_score: error en cálculo, usando fallback. %s: %s", type(exc).__name__, exc)
        return {"score_seguridad": 45.0}
