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
    LocalDetalleResponse, ZonaDetalle, ScoresDimensiones, AnalisisIADetallado,
    CompetidorCercano, AlertaZona, ColorZona, SeguridadDetalle,
    EntornoComercialDetalle, PerfilRefinado,
)
from api._utils import score_to_color
from db.sesiones import get_sesion
from db.zonas import get_zona_completa
from scoring.motor import get_scores_zona
from scoring.explainability import build_llm_grounding_payload, build_fallback_analysis
from nlp.alertas import get_alertas_zona
from agente.analisis import generar_analisis_zona

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
        color=score_to_color(zona.get("score_global") or 50.0),
        alquiler_mensual=zona.get("alquiler_mensual"),
        m2=zona.get("m2"),
    )


# ─── Detalle completo ─────────────────────────────────────────────────────────

class DetalleRequest(BaseModel):
    zona_id: str
    local_id: Optional[str] = None  # Para anclar el análisis a un local concreto
    session_id: str
    dev: bool = False  # Si True, incluir datos crudos para DevPanel


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
    ) = await asyncio.gather(
        # Fuente: `scores_zona` JSONB con shap_values y scores por dimensión
        # Calculado por pipeline semanal (`pipelines/scores.py`) con XGBoost v1
        get_scores_zona(
            zona_id=body.zona_id,
            sector_codigo=sector,
            idea_tags=sesion.get("perfil", {}).get("idea_tags") or [],
            descripcion_negocio=sesion.get("descripcion_original"),
            perfil_negocio=sesion.get("perfil", {}).get("perfil_negocio") or {},
            concepto_negocio=sesion.get("perfil", {}).get("concepto_negocio") or {},
            perfil_refinado=sesion.get("perfil", {}).get("perfil_refinado") or None,
        ),

        # Fuente: `alertas_zona` generadas por `nlp/alertas.py`
        # Pipeline diario: embeddings de reseñas (Google/Foursquare/Yelp) → clasificación
        get_alertas_zona(zona_id=body.zona_id),

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

    perfil = sesion.get("perfil", {}) or {}
    zona["alertas"] = alertas_raw
    llm_grounding = build_llm_grounding_payload(zona, scores_data, perfil)
    zona_llm = {
        **zona,
        "llm_grounding": llm_grounding,
    }

    try:
        analisis_data = await generar_analisis_zona(
            zona=zona_llm,
            perfil=perfil,
            session_id=body.session_id,
        )
    except Exception as exc:
        logger.error("Error generar_analisis_zona %s: %s", body.zona_id, exc)
        # Fallback determinista con hechos_clave reales desde llm_grounding.
        fallback = build_fallback_analysis(llm_grounding)
        analisis_data = {
            "texto": fallback.get("resumen_global") or "Análisis no disponible temporalmente.",
            "pros": fallback.get("puntos_fuertes", []),
            "contras": fallback.get("puntos_debiles", []),
            "resumen_global": fallback.get("resumen_global", "Análisis no disponible temporalmente."),
            "puntos_fuertes": fallback.get("puntos_fuertes", []),
            "puntos_debiles": fallback.get("puntos_debiles", []),
            "razon_recomendacion": fallback.get("razon_recomendacion", "Análisis determinista en base a variables observadas."),
            "recomendacion_final": fallback.get("recomendacion_final", "Con reservas"),
            "explicaciones_dimensiones": fallback.get("explicaciones_dimensiones", {}),
        }

    # Guardrail final: si por la razón que sea explicaciones_dimensiones
    # sigue vacío, rellenamos desde grounding. Mejor algo trazable que un
    # panel en blanco para el usuario.
    if not analisis_data.get("explicaciones_dimensiones"):
        try:
            fallback_merge = build_fallback_analysis(llm_grounding)
            analisis_data["explicaciones_dimensiones"] = (
                fallback_merge.get("explicaciones_dimensiones", {})
            )
        except Exception as exc:
            logger.warning("merge fallback explicaciones falló zona=%s: %s", body.zona_id, exc)

    # ── Pesos del modelo para esta idea (sector clasificado) ──────────────────
    # Permite al frontend mostrar "esta dimension pesa X% para tu idea".
    pesos_dimensiones: dict[str, float] = {}
    try:
        from db.conexion import get_db
        async with get_db() as conn:
            sec_row = await conn.fetchrow(
                "SELECT * FROM sectores WHERE codigo=$1", sector,
            )
        if sec_row:
            sec = dict(sec_row)
            pesos_dimensiones = {
                "flujo_peatonal": float(sec.get("peso_flujo")    or 0.25),
                "demografia":     float(sec.get("peso_demo")     or 0.20),
                "competencia":    float(sec.get("peso_competencia") or 0.15),
                "transporte":     float(sec.get("peso_transporte") or 0.15),
                "entorno":        float(sec.get("peso_entorno")  or 0.10),
                "dinamismo":      float(sec.get("peso_dinamismo") or 0.05),
                "seguridad":      float(sec.get("peso_seguridad") or 0.05),
                "turismo":        float(sec.get("peso_turismo")  or 0.05),
                "precio_alquiler": float(sec.get("peso_precio")  or 0.0),
            }
    except Exception as exc:
        logger.warning("No se pudieron cargar pesos del sector %s: %s", sector, exc)

    # ── PerfilRefinado + pesos modulados (Fase 1-2) ───────────────────────────
    # El perfil refinado viene de la sesión (lo cacheamos en /api/buscar). Los
    # pesos modulados vienen del scorer — son los pesos del sector ajustados
    # por las reglas declarativas del refinador. Si faltan, dejamos None y el
    # frontend cae al fallback pesos_dimensiones estático.
    perfil_refinado_obj: Optional[PerfilRefinado] = None
    raw_perfil_refinado = (sesion.get("perfil") or {}).get("perfil_refinado")
    if raw_perfil_refinado:
        try:
            perfil_refinado_obj = PerfilRefinado(**raw_perfil_refinado)
        except Exception as exc:
            logger.warning("perfil_refinado inválido en sesión: %s", exc)

    pesos_modulados_out = scores_data.get("pesos_modulados") or None

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
        disponible=zona.get("esta_disponible", zona.get("disponible", True)),

        # Scoring
        score_global=round(score_global, 1),
        scores_dimensiones=ScoresDimensiones(**scores_data["scores_dimension"]),
        probabilidad_supervivencia=round(prob, 3) if prob is not None else None,
        shap_values=scores_data.get("shap_values"),
        modelo_version=scores_data.get("modelo_version"),
        explicaciones_dimensiones=analisis_data.get("explicaciones_dimensiones", {}),
        impacto_modelo_por_dimension=scores_data.get("impacto_modelo_por_dimension", {}),
        resumen_global_llm=analisis_data.get("resumen_global") or analisis_data.get("texto"),

        # Pesos del modelo para el sector clasificado de la busqueda.
        sector_codigo=sector,
        pesos_dimensiones=pesos_dimensiones,

        # Capa de preservación de señal (Fase 1-2).
        perfil_refinado=perfil_refinado_obj,
        pesos_modulados=pesos_modulados_out,

        # Variables demográficas y entorno
        flujo_peatonal_dia=zona.get("flujo_peatonal_dia"),
        renta_media_hogar=zona.get("renta_media_hogar"),
        edad_media=zona.get("edad_media"),
        pct_extranjeros=zona.get("pct_extranjeros"),
        num_negocios_activos=zona.get("num_negocios_activos"),
        pct_locales_vacios=zona.get("pct_locales_vacios"),
        num_lineas_transporte=zona.get("num_lineas_transporte"),
        num_paradas_transporte=zona.get("num_paradas_transporte"),

        # Seguridad v7 — desglose granular
        seguridad_detalle=SeguridadDetalle(
            incidencias_por_1000hab=zona.get("incidencias_por_1000hab"),
            hurtos_por_1000hab=zona.get("hurtos_por_1000hab"),
            robatoris_por_1000hab=zona.get("robatoris_por_1000hab"),
            danys_por_1000hab=zona.get("danys_por_1000hab"),
            incidencias_noche_pct=zona.get("incidencias_noche_pct"),
            comisarias_1km=zona.get("comisarias_1km"),
            dist_comisaria_m=zona.get("dist_comisaria_m"),
            seguridad_barri_score=zona.get("seguridad_barri_score"),
        ),

        # Entorno comercial v8 — desglose granular
        entorno_detalle=EntornoComercialDetalle(
            pct_locales_vacios=zona.get("pct_locales_vacios"),
            tasa_rotacion_anual=zona.get("tasa_rotacion_anual"),
            licencias_nuevas_1a=zona.get("licencias_nuevas_1a"),
            ratio_locales_comerciales=zona.get("ratio_locales_comerciales"),
            nivel_ruido_db=zona.get("nivel_ruido_db"),
            score_equipamientos=zona.get("score_equipamientos"),
            m2_zonas_verdes_cercanas=zona.get("m2_zonas_verdes_cercanas"),
            mercados_municipales_1km=zona.get("mercados_municipales_1km"),
            eventos_culturales_500m=zona.get("eventos_culturales_500m"),
        ),

        # Competidores
        competidores_cercanos=[
            CompetidorCercano(**c) for c in zona.get("competidores_cercanos", [])
        ],

        # Alertas NLP
        alertas=[AlertaZona(**a) for a in alertas_raw],

        # Análisis IA — Claude Sonnet en tiempo real
        analisis_ia=AnalisisIADetallado(
            resumen=analisis_data.get("texto", ""),
            resumen_global=analisis_data.get("resumen_global", analisis_data.get("texto", "")),
            puntos_fuertes=analisis_data.get("puntos_fuertes", analisis_data.get("pros", [])),
            puntos_debiles=analisis_data.get("puntos_debiles", analisis_data.get("contras", [])),
            recomendacion_final=analisis_data.get("recomendacion_final", "Con reservas"),
            razon_recomendacion=analisis_data.get("razon_recomendacion", ""),
            explicaciones_dimensiones=analisis_data.get("explicaciones_dimensiones", {}),
            impacto_modelo_por_dimension=scores_data.get("impacto_modelo_por_dimension", {}),
        ),
    )

    dev_data = _build_dev_data(zona, scores_data, analisis_data, llm_grounding) if body.dev else None
    return LocalDetalleResponse(zona=zona_detalle, dev_data=dev_data)


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _build_dev_data(zona: dict, scores_data: dict, analisis_data: dict | None = None, llm_grounding: dict | None = None) -> dict:
    """Datos crudos para el DevPanel del frontend."""
    return {
        "zona_raw": {
            k: v for k, v in zona.items()
            if k not in ("competidores_cercanos",) and not callable(v)
        },
        "scores_raw": scores_data,
        "llm_grounding": llm_grounding or {},
        "analisis_raw": analisis_data or {},
    }



def _scores_fallback(zona: dict) -> dict:
    """
    Valores de fallback si la consulta a `scores_zona` falla.
    Usa el score global pre-calculado que viene en el dict de zona
    y rellena las dimensiones con 50 (neutro).
    """
    score = zona.get("score_global", 50.0)
    return {
        "score_global": score,
        "datos_reales": False,  # indica que son valores de fallback, no del modelo XGBoost
        "probabilidad_supervivencia_3a": 0.5,
        "scores_dimension": {
            "flujo_peatonal": 50.0,
            "demografia": 50.0,
            "competencia": 50.0,
            "precio_alquiler": 50.0,
            "transporte": 50.0,
            "seguridad": 50.0,
            "turismo": 50.0,
            "dinamismo": 50.0,
        },
        "explicaciones_shap": [],
        "impacto_modelo_por_dimension": {},
        "shap_values": {},
        "modelo_version": "fallback",
    }



