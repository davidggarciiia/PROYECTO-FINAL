"""
api/buscar.py — POST /api/buscar

Endpoint principal. El usuario manda su descripción en lenguaje natural
y recibe zonas rankeadas, o una pregunta si falta información.

Flujo interno:
  1. Crear / recuperar sesión (Redis → PostgreSQL tabla `sesiones`)
  2. Validar descripción con LLM (¿es retail? ¿hay suficiente info?)
  3. Según el resultado del LLM:
       a. No es retail             → estado "error_tipo_negocio"
       b. Es retail, bloqueado leg → estado "inviable_legal"
       c. Falta info               → estado "cuestionario" (inicia cuestionario)
       d. Info suficiente          → filtrar zonas + scoring → estado "ok"
  4. Guardar búsqueda en `busquedas` para analytics

Módulo de David — agente IA.
"""

from __future__ import annotations

import logging
from typing import Optional
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from schemas.models import ZonaResumen, ColorZona, EstadoBusqueda
from agente.validacion import validar_negocio
from agente.cuestionario import iniciar_cuestionario
from scoring.motor import calcular_scores_batch
from db.sesiones import crear_sesion, get_sesion, guardar_busqueda
from db.zonas import filtrar_zonas_candidatas

logger = logging.getLogger(__name__)
router = APIRouter(tags=["buscar"])


# ─── Request ──────────────────────────────────────────────────────────────────

class BuscarRequest(BaseModel):
    descripcion: str = Field(
        ...,
        min_length=10,
        max_length=1000,
        description="Descripción del negocio en lenguaje natural",
        examples=["Quiero abrir una cafetería de especialidad en un barrio de diseño"],
    )
    ciudad: str = Field(
        default="Barcelona",
        description="Ciudad donde buscar. Por ahora solo Barcelona.",
    )
    presupuesto_max: Optional[float] = Field(
        None,
        gt=0,
        description="Alquiler mensual máximo en €",
    )
    m2_min: Optional[float] = Field(None, gt=0)
    m2_max: Optional[float] = Field(None, gt=0)
    distritos: Optional[list[str]] = Field(
        None,
        description="Lista de distritos a incluir. None = buscar en toda la ciudad.",
    )
    session_id: Optional[str] = Field(
        None,
        description="Pasar si el usuario ya tiene sesión (ej: vuelve del cuestionario).",
    )


# ─── Response ─────────────────────────────────────────────────────────────────

class BuscarResponse(BaseModel):
    session_id: str
    estado: EstadoBusqueda
    zonas: Optional[list[ZonaResumen]] = None
    total_zonas_analizadas: Optional[int] = Field(
        None,
        description="Total de zonas evaluadas antes de filtrar (para mostrar en UI).",
    )
    pregunta: Optional[str] = Field(
        None,
        description="Pregunta del cuestionario si estado='cuestionario'.",
    )
    progreso_cuestionario: Optional[int] = Field(
        None,
        ge=0,
        le=100,
        description="% de avance del cuestionario (0-100).",
    )
    motivo: Optional[str] = Field(
        None,
        description=(
            "Mensaje explicativo para estados 'error_tipo_negocio' e 'inviable_legal'. "
            "El frontend debe mostrarlo tal cual, sin modificarlo."
        ),
    )


# ─── Helper ───────────────────────────────────────────────────────────────────

def _score_to_color(score: float) -> ColorZona:
    """Convierte un score numérico al color del marcador del mapa."""
    if score > 75:
        return ColorZona.VERDE
    if score >= 50:
        return ColorZona.AMARILLO
    return ColorZona.ROJO


def _build_zona_resumen(z: dict) -> ZonaResumen:
    """
    Convierte un dict de zona (resultado de scoring) a ZonaResumen.

    El dict z viene de `scoring.motor.calcular_scores_batch()` que combina:
      - Datos de `zonas` + `barrios` + `distritos` (PostgreSQL/PostGIS)
      - Scores calculados por XGBoost (`scores_zona`)
      - Precio estimado de `precios_alquiler_zona`
      - Resumen generado por Claude Sonnet (llm_router)
    """
    return ZonaResumen(
        zona_id=z["zona_id"],
        nombre=z["nombre"],
        barrio=z["barrio"],
        distrito=z["distrito"],
        score_global=round(z["score_global"], 1),
        probabilidad_supervivencia_3a=round(z["probabilidad_supervivencia_3a"], 2),
        alquiler_estimado=z["alquiler_estimado"],
        m2_disponibles=z.get("m2_disponibles"),
        color=_score_to_color(z["score_global"]),
        lat=z["lat"],
        lng=z["lng"],
        resumen_ia=z["resumen_ia"],
    )


# ─── Endpoint ────────────────────────────────────────────────────────────────

@router.post(
    "/buscar",
    response_model=BuscarResponse,
    summary="Búsqueda principal de zonas por descripción en lenguaje natural",
)
async def buscar(body: BuscarRequest, request: Request) -> BuscarResponse:
    """
    Endpoint principal. El frontend lo llama en dos momentos:
      1. Cuando el usuario envía la primera descripción.
      2. Cuando el cuestionario termina (trigger_busqueda=True en /api/cuestionario),
         pasando el mismo session_id y la descripción original.

    El estado de la respuesta determina qué hace el frontend:
      - "ok"                → renderizar mapa con zonas
      - "cuestionario"      → mostrar la pregunta al usuario
      - "error_tipo_negocio"→ mostrar mensaje de error
      - "inviable_legal"    → mostrar advertencia legal + botón "Saber más"
    """
    # ── 1. Sesión ─────────────────────────────────────────────────────────────
    # Las sesiones se almacenan en Redis (TTL 4h) y se persisten en PostgreSQL
    # tabla `sesiones`. La IP se guarda hasheada para analytics anónimos.
    session_id = body.session_id or str(uuid4())
    sesion = await get_sesion(session_id)

    if sesion is None:
        ip_hash = _hash_ip(request.client.host if request.client else "")
        sesion = await crear_sesion(
            session_id=session_id,
            datos={
                "descripcion_original": body.descripcion,
                "ciudad": body.ciudad,
                "filtros": {
                    "presupuesto_max": body.presupuesto_max,
                    "m2_min": body.m2_min,
                    "m2_max": body.m2_max,
                    "distritos": body.distritos,
                },
                "perfil": {},  # Se rellenará en el cuestionario
            },
            ip_hash=ip_hash,
        )

    # ── 2. Validación LLM ─────────────────────────────────────────────────────
    # `validar_negocio` usa Claude Sonnet (con fallback via llm_router) para:
    #   - Nivel 1: ¿Es un negocio retail que necesita local físico?
    #   - Nivel 2: ¿Hay información suficiente para buscar zonas?
    #   - Extra:   ¿Hay algún bloqueo legal conocido? (ej: cachimbería)
    #
    # Devuelve un dict con:
    #   {
    #     "es_retail": bool,
    #     "inviable_legal": bool,
    #     "motivo_legal": str | None,
    #     "informacion_suficiente": bool,
    #     "sector_detectado": str,  # "restauracion", "tatuajes", etc.
    #     "variables_conocidas": dict,
    #     "preguntas_necesarias": list[str],
    #   }
    try:
        validacion = await validar_negocio(body.descripcion, sesion)
    except Exception as exc:
        # Si el LLM falla completamente, no bloqueamos al usuario —
        # asumimos que el negocio es válido y pedimos info via cuestionario.
        logger.error("Error en validacion LLM: %s", exc, exc_info=True)
        validacion = {
            "es_retail": True,
            "inviable_legal": False,
            "informacion_suficiente": False,
            "sector_detectado": "desconocido",
            "variables_conocidas": {},
            "preguntas_necesarias": ["sector", "m2", "presupuesto", "cliente"],
        }

    # ── 3a. Negocio no apto para GeoRetail ───────────────────────────────────
    if not validacion["es_retail"]:
        return BuscarResponse(
            session_id=session_id,
            estado=EstadoBusqueda.ERROR_NEGOCIO,
            motivo=(
                validacion.get("motivo")
                or "GeoRetail está diseñado para negocios que necesitan un local físico "
                   "con atención presencial (tienda, restaurante, estudio...)."
            ),
        )

    # ── 3b. Negocio retail pero con bloqueo legal ─────────────────────────────
    # Ejemplo: cachimbería → illegal en interiores, hay que ir por modelo de
    # club privado de fumadores. El LLM detecta esto en el prompt de validacion.txt.
    if validacion.get("inviable_legal"):
        return BuscarResponse(
            session_id=session_id,
            estado=EstadoBusqueda.INVIABLE_LEGAL,
            motivo=validacion["motivo_legal"],
        )

    # ── 3c. Falta información → cuestionario ─────────────────────────────────
    # `iniciar_cuestionario` crea la primera pregunta basándose en qué variables
    # faltan. Guarda el estado en Redis + tabla `mensajes_cuestionario`.
    if not validacion["informacion_suficiente"]:
        primera_pregunta = await iniciar_cuestionario(session_id, validacion)
        return BuscarResponse(
            session_id=session_id,
            estado=EstadoBusqueda.CUESTIONARIO,
            pregunta=primera_pregunta["pregunta"],
            progreso_cuestionario=primera_pregunta["progreso"],
        )

    # ── 3d. Información suficiente → buscar y rankear zonas ──────────────────
    # Construimos el perfil del negocio combinando lo del cuestionario (sesion["perfil"])
    # con lo que el LLM ha extraído de la descripción inicial.
    perfil = {
        **sesion.get("perfil", {}),
        "sector": validacion["sector_detectado"],
        "variables": validacion["variables_conocidas"],
    }

    # `filtrar_zonas_candidatas` hace una query PostGIS:
    #   SELECT z.* FROM zonas z
    #   JOIN barrios b ON z.barrio_id = b.id
    #   JOIN distritos d ON b.distrito_id = d.id
    #   WHERE (d.nombre = ANY($distritos) OR $distritos IS NULL)
    #     AND z.id IN (SELECT zona_id FROM locales WHERE alquiler_mensual <= $presupuesto_max)
    #   ORDER BY z.id
    #
    # Devuelve dicts con datos básicos de la zona (sin score todavía).
    filtros = sesion["filtros"]
    zonas_candidatas = await filtrar_zonas_candidatas(filtros)

    if not zonas_candidatas:
        # No hay zonas que cumplan los filtros del usuario — ampliar búsqueda
        logger.warning(
            "Sin zonas candidatas para session_id=%s con filtros=%s",
            session_id, filtros,
        )
        # Intentar sin filtro de distritos
        filtros_amplios = {**filtros, "distritos": None}
        zonas_candidatas = await filtrar_zonas_candidatas(filtros_amplios)

    total_candidatas = len(zonas_candidatas)

    # `calcular_scores_batch` corre inferencia XGBoost para cada zona:
    #   - Carga features de `variables_zona` (datos más recientes por zona)
    #   - Infiere con el modelo `scoring/modelos/xgboost_v1.pkl` (cargado en startup)
    #   - Genera resumen IA con Claude para las top 10 zonas (las demás: resumen genérico)
    #   - Guarda / actualiza `scores_zona` con los nuevos valores
    #
    # Devuelve una lista de dicts ordenada por score_global desc.
    zonas_scored = await calcular_scores_batch(zonas_candidatas, perfil)

    zonas_response = [_build_zona_resumen(z) for z in zonas_scored]

    # ── 4. Guardar búsqueda para analytics ───────────────────────────────────
    # Tabla `busquedas` (PostgreSQL) — útil para entender qué buscan los usuarios
    # y para futuro fine-tuning del modelo.
    try:
        await guardar_busqueda(
            session_id=session_id,
            descripcion=body.descripcion,
            filtros=filtros,
            perfil=perfil,
            num_resultados=len(zonas_response),
        )
    except Exception as exc:
        # No bloqueamos la respuesta si falla el guardado de analytics
        logger.warning("No se pudo guardar busqueda en analytics: %s", exc)

    return BuscarResponse(
        session_id=session_id,
        estado=EstadoBusqueda.OK,
        zonas=zonas_response,
        total_zonas_analizadas=total_candidatas,
    )


# ─── Utilidades ───────────────────────────────────────────────────────────────

def _hash_ip(ip: str) -> str:
    """Hash SHA-256 de la IP para analytics anónimos. No almacenamos IPs en claro."""
    import hashlib
    return hashlib.sha256(ip.encode()).hexdigest()[:16]
