"""
scoring/dimensiones/turismo.py — Score de presencia turística (0-100).

Combina las distintas señales de turismo de una zona en un único score
interpretable, sustituyendo la lógica primitiva inline que vivía en
scorer.py:343-356 (sólo proximidad al litoral + valor pre-calculado
inconsistente entre pipelines).

Inputs (campos del dict `datos` proveniente de v_variables_zona):
  score_turismo_airbnb     — score 0-100 de listings Airbnb 500m (pipeline airbnb.py)
  score_turismo_hut        — score 0-100 de licencias HUT 500m (pipeline hut.py)
  airbnb_density_500m      — count crudo de listings Airbnb a 500m
  airbnb_occupancy_est     — ocupación estimada 0-1 (presión turística real)
  booking_hoteles_500m     — count hoteles/hostels/guest_houses a 500m
  booking_rating_medio     — rating medio Booking 0-10 (puede ser NULL)
  eventos_culturales_500m  — POIs culturales (museos, teatros, atracciones)
  venues_musicales_500m    — salas de concierto/teatros musicales (señal opcional)
  dist_playa_m             — distancia geodésica al frente marítimo (computed runtime)
  dist_landmark_top3_m     — distancia media ponderada a los 3 landmarks turísticos
                              reales más cercanos (OSM+Wikidata, pipeline landmarks.py)
  seasonality_summer_lift  — lift estacional verano vs media (modulador opcional)

Sin I/O. No accede a BD. Testeable con datos sintéticos.
"""
from __future__ import annotations

import logging
import math
from typing import Optional

logger = logging.getLogger(__name__)

# ── Constantes de calibración ─────────────────────────────────────────────────

_FALLBACK_SCORE = 50.0       # neutro real, no pesimista
_MUESTRA_MINIMA_PONDERADA = 2.0   # suma ponderada mínima de stocks para no fallback

# Saturadores log: el valor que mapea a normalizado=1.0
_SATURACION_AIRBNB   = 60.0   # 60+ listings a 500m → señal saturada
_SATURACION_HOTELES  = 15.0   # 15+ hoteles a 500m → señal saturada
_SATURACION_POIS     = 8.0    # 8+ POIs culturales+0.5*musicales → señal saturada

# Pesos de la base (suman 1.0)
_PESO_AIRBNB  = 0.40
_PESO_HOTELES = 0.35
_PESO_POIS    = 0.25


def _get(datos: dict, key: str, fallback: float) -> float:
    val = datos.get(key)
    if val is None:
        return fallback
    try:
        return float(val)
    except (TypeError, ValueError):
        return fallback


def _log_norm(valor: float, saturacion: float) -> float:
    """Normaliza un count a [0, 1] con curva logarítmica saturada."""
    if valor <= 0 or saturacion <= 0:
        return 0.0
    return min(1.0, math.log1p(valor) / math.log1p(saturacion))


def _perfil_dominante(
    airbnb: float, hoteles: float, pois: float, dist_playa: Optional[float]
) -> str:
    """Etiqueta el perfil turístico según la señal dominante."""
    if airbnb < 0.05 and hoteles < 0.05 and pois < 0.05:
        if dist_playa is not None and dist_playa < 700:
            return "playa"
        return "residencial"
    if dist_playa is not None and dist_playa < 300 and (hoteles > 0.3 or airbnb > 0.3):
        return "playa"
    señales = {"hotelero": hoteles, "cultural": pois, "hotelero_informal": airbnb}
    top, top_val = max(señales.items(), key=lambda kv: kv[1])
    segundo = sorted(señales.values(), reverse=True)[1]
    if segundo > 0.7 * top_val:
        return "mixto"
    return "hotelero" if top == "hotelero_informal" else top


# ---------------------------------------------------------------------------
# API pública
# ---------------------------------------------------------------------------

def calcular_turismo(
    datos: dict,
    perfil_negocio: Optional[dict] = None,
) -> dict:
    """
    Calcula el score de presencia turística para una zona.

    Args:
        datos:          Dict con campos de v_variables_zona.
        perfil_negocio: Dict opcional con dimensiones del negocio.
                        Clave relevante: 'turismo_dependencia' (0-1) — negocios
                        muy dependientes del turista valoran más zonas turísticas;
                        negocios de barrio se penalizan en zonas-trampa.

    Returns:
        Dict con:
          score_turismo    (float 0-100): score combinado.
          perfil_turistico (str): 'playa' | 'hotelero' | 'cultural' |
                                  'mixto' | 'residencial' | 'sin_datos'.
          interpretacion   (str): texto legible para el frontend.
          confianza        (str): 'alta' | 'media' | 'baja' según cobertura de datos.
    """
    try:
        # ── Stocks crudos (lo que escriben los pipelines) ───────────────────
        airbnb_count = _get(datos, "airbnb_density_500m", 0.0)
        hoteles      = _get(datos, "booking_hoteles_500m", 0.0)
        pois_cult    = _get(datos, "eventos_culturales_500m", 0.0)
        pois_mus     = _get(datos, "venues_musicales_500m", 0.0)
        pois         = pois_cult + 0.5 * pois_mus

        # Stocks no-null (para confianza)
        stocks_no_null = sum([
            datos.get("airbnb_density_500m") is not None,
            datos.get("booking_hoteles_500m") is not None,
            datos.get("eventos_culturales_500m") is not None,
        ])

        # Suma ponderada de actividad (para detectar zonas demasiado vacías)
        suma_ponderada = airbnb_count + 5.0 * hoteles + 3.0 * pois_cult

        # ── Fallback duro: ningún stock o muestra ínfima ────────────────────
        dist_playa = datos.get("dist_playa_m")
        dist_playa_f: Optional[float] = float(dist_playa) if dist_playa is not None else None

        if stocks_no_null == 0 or suma_ponderada < _MUESTRA_MINIMA_PONDERADA:
            score = _FALLBACK_SCORE
            # Solo levantar si hay proximidad al mar como proxy débil
            if dist_playa_f is not None:
                if dist_playa_f < 300:
                    score = max(score, 75.0)
                elif dist_playa_f < 700:
                    score = max(score, 62.0)
                elif dist_playa_f < 1500:
                    score = max(score, 55.0)
            # Tambien levantar si hay un landmark turistico real muy cerca
            # (zona patrimonial aunque no haya Airbnb/hoteles registrados)
            dist_lm_fb = datos.get("dist_landmark_top3_m")
            if dist_lm_fb is not None:
                try:
                    dist_lm_fb_f = float(dist_lm_fb)
                    if dist_lm_fb_f < 300:
                        score = max(score, 70.0)
                    elif dist_lm_fb_f < 700:
                        score = max(score, 60.0)
                except (TypeError, ValueError):
                    pass
            perfil_fb = "sin_datos"
            if dist_playa_f is not None and dist_playa_f < 1500:
                perfil_fb = "playa"
            elif dist_lm_fb is not None:
                try:
                    if float(dist_lm_fb) < 700:
                        perfil_fb = "cultural"
                except (TypeError, ValueError):
                    pass
            return {
                "score_turismo":    round(score, 1),
                "perfil_turistico": perfil_fb,
                "interpretacion":   interpretar_perfil(perfil_fb),
                "confianza":        "baja",
            }

        # ── Base log-escalada combinada ─────────────────────────────────────
        n_airbnb  = _log_norm(airbnb_count, _SATURACION_AIRBNB)
        n_hoteles = _log_norm(hoteles,      _SATURACION_HOTELES)
        n_pois    = _log_norm(pois,         _SATURACION_POIS)

        base = 100.0 * (
            _PESO_AIRBNB  * n_airbnb +
            _PESO_HOTELES * n_hoteles +
            _PESO_POIS    * n_pois
        )
        score = base

        # ── Ajuste por ocupación real Airbnb (no licencias dormidas) ────────
        ocupacion = datos.get("airbnb_occupancy_est")
        if ocupacion is not None:
            try:
                oc = float(ocupacion)
                if oc > 0.75:
                    score += 6.0
                elif oc < 0.30 and airbnb_count > 5:
                    score -= 4.0
            except (TypeError, ValueError):
                pass

        # ── Ajuste por proximidad al litoral (sustituye max() inline) ───────
        if dist_playa_f is not None:
            if dist_playa_f < 300:
                score += 12.0
            elif dist_playa_f < 700:
                score += 7.0
            elif dist_playa_f < 1500:
                score += 3.0

        # ── Ajuste por proximidad a landmark turístico real (OSM+Wikidata) ──
        # Se aplica DESPUÉS del ajuste de playa para que los efectos compongan
        # (una zona costera con la Sagrada Família a 200m puede recibir ambos).
        dist_lm = datos.get("dist_landmark_top3_m")
        if dist_lm is not None:
            try:
                dist_lm_f = float(dist_lm)
                if dist_lm_f < 300:
                    score += 8.0
                elif dist_lm_f < 700:
                    score += 4.0
            except (TypeError, ValueError):
                pass

        # ── Ajuste por estacionalidad estival (zonas hiperestacionales) ─────
        season = datos.get("seasonality_summer_lift")
        if season is not None:
            try:
                if float(season) > 1.20:
                    score += 3.0
            except (TypeError, ValueError):
                pass

        # ── Ajuste por perfil del negocio ───────────────────────────────────
        # turismo_dependencia: 0 = negocio de barrio, 1 = depende del turista
        perfil = perfil_negocio or {}
        try:
            dep = float(perfil.get("turismo_dependencia", 0.0))
        except (TypeError, ValueError):
            dep = 0.0
        if dep > 0.6 and base > 60.0:
            score += 4.0   # premia match positivo
        elif dep < 0.2 and base > 80.0:
            score -= 5.0   # penaliza zona-trampa para negocio de barrio

        score = round(min(100.0, max(0.0, score)), 1)

        # ── Confianza ──────────────────────────────────────────────────────
        if stocks_no_null >= 2 and suma_ponderada > 20.0:
            confianza = "alta"
        elif stocks_no_null >= 1 and dist_playa_f is not None:
            confianza = "media"
        else:
            confianza = "baja"

        perfil_t = _perfil_dominante(n_airbnb, n_hoteles, n_pois, dist_playa_f)

        logger.debug(
            "turismo_score: airbnb_n=%.2f hoteles_n=%.2f pois_n=%.2f "
            "dist_playa=%s ocup=%s dep=%.2f → base=%.1f score=%.1f perfil=%s",
            n_airbnb, n_hoteles, n_pois,
            dist_playa_f, ocupacion, dep, base, score, perfil_t,
        )

        return {
            "score_turismo":    score,
            "perfil_turistico": perfil_t,
            "interpretacion":   interpretar_perfil(perfil_t),
            "confianza":        confianza,
        }

    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "turismo_score: fallo en cálculo, devolviendo fallback. Error: %s",
            exc,
            exc_info=True,
        )
        return {
            "score_turismo":    _FALLBACK_SCORE,
            "perfil_turistico": "sin_datos",
            "interpretacion":   interpretar_perfil("sin_datos"),
            "confianza":        "baja",
        }


def calcular_turismo_batch(registros: list[dict]) -> list[float]:
    """Versión batch — devuelve solo el score para cada registro."""
    return [calcular_turismo(r)["score_turismo"] for r in registros]


def interpretar_perfil(perfil: str) -> str:
    """Descripción legible para el frontend."""
    _TEXTOS = {
        "playa":       "Zona litoral con afluencia turística estacional",
        "hotelero":    "Zona con concentración de alojamiento turístico",
        "cultural":    "Zona con atractivos culturales y eventos turísticos",
        "mixto":       "Zona turística diversificada (alojamiento + cultura)",
        "residencial": "Zona principalmente residencial, baja presencia turística",
        "sin_datos":   "Datos turísticos insuficientes para esta zona",
    }
    return _TEXTOS.get(perfil, "Perfil turístico desconocido")
