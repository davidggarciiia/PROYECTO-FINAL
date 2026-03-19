"""
api/local.py — POST /api/local/preview  y  POST /api/local

Dos endpoints con responsabilidades muy distintas:

  /api/local/preview  → Llamada rápida al hacer click en el mapa (<200ms).
                         Solo los datos mínimos para el tooltip/popup.
                         Fuente: datos pre-calculados en PostgreSQL.

  /api/local          → Llamada pesada al pulsar "Ver detalle" (~1-3s).
                         Análisis completo con asyncio.gather en paralelo.
                         El análisis financiero NO se incluye aquí — se carga
                         solo cuando el usuario pulsa "Análisis financiero".

Fuentes de datos (detalle completo):
  - Zona y local físico     → tablas `zonas`, `locales` (PostgreSQL/PostGIS)
  - Scores y SHAP           → tabla `scores_zona` (XGBoost, calculado por pipeline semanal)
  - Variables del entorno   → tabla `variables_zona` (aforadors, renta, etc.)
  - Competidores cercanos   → tabla `negocios_activos` (Google Places + Foursquare + OSM)
  - Alertas NLP             → tabla `alertas_zona` (sentence-transformers sobre reseñas)
  - Transporte              → tablas `paradas_transporte`, `lineas_transporte` (TMB API + GTFS)
  - Análisis IA             → Claude Sonnet en tiempo real (llm_router)
  - Info legal por zona     → tablas `requisitos_legales_sector`, `restricciones_geograficas_sector`
"""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from schemas.models import (
    LocalDetalleResponse, ZonaDetalle, ScoresDimensiones, AnalisisIA,
    CompetidorCercano, AlertaZona, InfoLegal, LicenciaNecesaria,
    RestriccionZona, ColorZona, ViabilidadLegal,
)
from db.sesiones import get_sesion
from db.zonas import get_zona_completa
from scoring.motor import get_scores_zona
from nlp.alertas import get_alertas_zona
from agente.analisis import generar_analisis_zona
from scoring.legal import get_info_legal_zona

logger = logging.getLogger(__name__)
router = APIRouter(tags=["local"])


# ─── Preview ──────────────────────────────────────────────────────────────────

class PreviewRequest(BaseModel):
    zona_id: str
    session_id: str


class PreviewResponse(BaseModel):
    """
    Respuesta mínima para el tooltip del mapa.

    Todos los campos salen de datos ya calculados en PostgreSQL —
    no hay llamadas a LLM ni cálculos pesados en este endpoint.
    """

    zona_id: str
    nombre: str
    direccion: Optional[str] = None  # Tabla `locales`, columna `direccion`
    score_global: float
    color: ColorZona
    alquiler_mensual: Optional[float] = None  # Tabla `locales`, columna `alquiler_mensual`
    m2: Optional[int] = None                  # Tabla `locales`, columna `m2`


@router.post(
    "/local/preview",
    response_model=PreviewResponse,
    summary="Datos rápidos de una zona para el tooltip del mapa (<200ms)",
)
async def local_preview(body: PreviewRequest) -> PreviewResponse:
    """
    Se llama cada vez que el usuario hace click en un marcador del mapa.
    Debe ser sub-200ms — solo lee datos pre-calculados de PostgreSQL.

    El score_global viene de `scores_zona` calculado en el último pipeline semanal.
    Si no hay score calculado para la sesión actual, usa el más reciente disponible.
    """
    # Verificar sesión — permite que get_zona_completa use el perfil para escoger
    # el score correcto (el modelo puede tener scores distintos por sector).
    sesion = await get_sesion(body.session_id)
    if sesion is None:
        raise HTTPException(
            status_code=404,
            detail="Sesión no encontrada o expirada.",
        )

    # `get_zona_completa` hace un JOIN entre `zonas`, `locales` y `scores_zona`.
    # Devuelve el local más representativo de la zona (planta baja, disponible).
    # Si hay múltiples locales, usa el de m2 más cercano al perfil del usuario.
    zona = await get_zona_completa(
        zona_id=body.zona_id,
        sector=sesion.get("perfil", {}).get("sector"),
    )
    if zona is None:
        raise HTTPException(
            status_code=404,
            detail=f"Zona '{body.zona_id}' no encontrada.",
        )

    return PreviewResponse(
        zona_id=zona["zona_id"],
        nombre=zona["nombre"],
        direccion=zona.get("direccion"),
        score_global=round(zona.get("score_global") or 50.0, 1),
        color=_score_to_color(zona.get("score_global") or 50.0),
        alquiler_mensual=zona.get("alquiler_mensual"),
        m2=zona.get("m2"),
    )


# ─── Detalle completo ─────────────────────────────────────────────────────────

class DetalleRequest(BaseModel):
    zona_id: str
    local_id: Optional[str] = None  # Para anclar el análisis a un local concreto
    session_id: str


@router.post(
    "/local",
    response_model=LocalDetalleResponse,
    summary="Análisis completo de una zona (llamada pesada, ~1-3s)",
)
async def local_detalle(body: DetalleRequest) -> LocalDetalleResponse:
    """
    Se llama al pulsar "Ver detalle" en el popup del mapa.
    Usa asyncio.gather para lanzar todas las consultas en paralelo.

    Mostrar skeleton en el frontend mientras carga (~1-3s según la carga del servidor).

    El análisis financiero NO se carga aquí para no penalizar el tiempo de carga
    del panel de detalle. Solo se carga cuando el usuario pulsa "Análisis financiero".
    """
    # ── Validar sesión y zona ─────────────────────────────────────────────────
    sesion = await get_sesion(body.session_id)
    if sesion is None:
        raise HTTPException(
            status_code=404,
            detail="Sesión no encontrada o expirada.",
        )

    sector = sesion.get("perfil", {}).get("sector", "desconocido")

    zona = await get_zona_completa(zona_id=body.zona_id, sector=sector)
    if zona is None:
        raise HTTPException(
            status_code=404,
            detail=f"Zona '{body.zona_id}' no encontrada.",
        )

    # ── Gather paralelo ───────────────────────────────────────────────────────
    # Todas las fuentes de datos se consultan en paralelo para minimizar latencia.
    # Estimación individual:
    #   - get_scores_zona:        ~10ms (PostgreSQL)
    #   - get_alertas_zona:       ~15ms (PostgreSQL)
    #   - generar_analisis_zona:  ~800-1200ms (LLM — el cuello de botella)
    #   - get_info_legal_zona:    ~20ms (PostgreSQL)
    #
    # Tiempo total ≈ max(latencias) ≈ 1-1.5s (no suma, es paralelo)
    (
        scores_data,
        alertas_raw,
        analisis_data,
        legal_data,
    ) = await asyncio.gather(
        # Fuente: `scores_zona` JSONB con shap_values y scores por dimensión
        # Calculado por pipeline semanal (`pipelines/scores.py`) con XGBoost v1
        get_scores_zona(zona_id=body.zona_id, sector=sector),

        # Fuente: `alertas_zona` generadas por `nlp/alertas.py`
        # Pipeline diario: embeddings de reseñas (Google/Foursquare/Yelp) → clasificación
        get_alertas_zona(zona_id=body.zona_id),

        # Fuente: Claude Sonnet (llm_router) en tiempo real
        # Prompt en `agente/prompts/analisis_zona.txt`
        # Incluye datos de zona, scores y perfil del usuario para personalizar
        generar_analisis_zona(zona=zona, perfil=sesion.get("perfil", {})),

        # Fuente: `requisitos_legales_sector` + `restricciones_geograficas_sector`
        # Verifica con PostGIS si la zona supera el máximo de establecimientos
        # (restricciones de los Planes de Usos de Barcelona)
        get_info_legal_zona(zona_id=body.zona_id, sector=sector),

        return_exceptions=True,  # No cancelar todo si una falla
    )

    # ── Manejar errores parciales ─────────────────────────────────────────────
    # Si alguna tarea falla, usamos valores por defecto en lugar de 500 al usuario.
    if isinstance(scores_data, Exception):
        logger.error("Error get_scores_zona %s: %s", body.zona_id, scores_data)
        scores_data = _scores_fallback(zona)

    if isinstance(alertas_raw, Exception):
        logger.error("Error get_alertas_zona %s: %s", body.zona_id, alertas_raw)
        alertas_raw = []

    if isinstance(analisis_data, Exception):
        logger.error("Error generar_analisis_zona %s: %s", body.zona_id, analisis_data)
        analisis_data = {
            "texto": "Análisis no disponible temporalmente.",
            "pros": [],
            "contras": [],
        }

    if isinstance(legal_data, Exception):
        logger.error("Error get_info_legal_zona %s: %s", body.zona_id, legal_data)
        legal_data = _legal_fallback()

    # ── Construir respuesta ───────────────────────────────────────────────────
    score_global = scores_data.get("score_global", zona.get("score_global", 50.0))
    prob = scores_data.get("probabilidad_supervivencia_3a")

    zona_detalle = ZonaDetalle(
        zona_id=zona["zona_id"],
        nombre=zona["nombre"],
        barrio=zona["barrio"],
        distrito=zona["distrito"],
        lat=zona["lat"],
        lng=zona["lng"],

        # Datos físicos
        direccion=zona.get("direccion"),
        m2=zona.get("m2"),
        alquiler_mensual=zona.get("alquiler_mensual"),
        disponible=zona.get("disponible", True),

        # Scoring
        score_global=round(score_global, 1),
        scores_dimensiones=ScoresDimensiones(**scores_data["scores_dimension"]),
        probabilidad_supervivencia=round(prob, 3) if prob is not None else None,

        # Variables demográficas y entorno
        flujo_peatonal_dia=zona.get("flujo_peatonal_dia"),
        renta_media_hogar=zona.get("renta_media_hogar"),
        edad_media=zona.get("edad_media"),
        pct_extranjeros=zona.get("pct_extranjeros"),
        num_negocios_activos=zona.get("num_negocios_activos"),
        pct_locales_vacios=zona.get("pct_locales_vacios"),
        num_lineas_transporte=zona.get("num_lineas_transporte"),
        num_paradas_transporte=zona.get("num_paradas_transporte"),

        # Competidores
        competidores_cercanos=[
            CompetidorCercano(**c) for c in zona.get("competidores_cercanos", [])
        ],

        # Alertas NLP
        alertas=[AlertaZona(**a) for a in alertas_raw],

        # Análisis IA — Claude Sonnet en tiempo real
        analisis_ia=AnalisisIA(
            resumen=analisis_data.get("texto", ""),
            puntos_fuertes=analisis_data.get("pros", []),
            puntos_debiles=analisis_data.get("contras", []),
        ),
    )

    return LocalDetalleResponse(zona=zona_detalle)


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _score_to_color(score: float) -> ColorZona:
    if score > 75:
        return ColorZona.VERDE
    if score >= 50:
        return ColorZona.AMARILLO
    return ColorZona.ROJO


def _scores_fallback(zona: dict) -> dict:
    """
    Valores de fallback si la consulta a `scores_zona` falla.
    Usa el score global pre-calculado que viene en el dict de zona
    y rellena las dimensiones con 50 (neutro).
    """
    score = zona.get("score_global", 50.0)
    return {
        "score_global": score,
        "probabilidad_supervivencia_3a": 0.5,
        "scores_dimension": {
            "flujo_peatonal": 50.0,
            "demografia": 50.0,
            "competencia": 50.0,
            "precio_alquiler": 50.0,
            "transporte": 50.0,
            "seguridad": 50.0,
            "turismo": 50.0,
            "entorno_comercial": 50.0,
        },
        "explicaciones_shap": [],
    }


def _legal_fallback() -> dict:
    """
    Fallback legal genérico cuando falla la consulta a BD.
    Indica que hay que verificar manualmente.
    """
    return {
        "viabilidad": "viable",
        "alerta": "No se pudieron cargar los requisitos legales. Verifica con el Ayuntamiento.",
        "licencias_necesarias": [],
        "restriccion_zona": None,
        "requisitos_local": [],
    }
