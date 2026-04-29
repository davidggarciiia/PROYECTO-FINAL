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
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
from typing import Literal, Optional
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from schemas.models import (
    ZonaResumen, ColorZona, EstadoBusqueda, PerfilRefinado,
    PerfilEstructurado, PublicoObjetivo, Operacion, UbicacionIdeal,
)
from api._utils import score_to_color
from agente.validador import validar_negocio
from scoring.taxonomia import subsector_valido
from agente.refinador import generar_pregunta_senal, refinar
from agente.traductor import traducir
from scoring.motor import calcular_scores_batch
from db.sesiones import crear_sesion, get_sesion, guardar_busqueda, actualizar_sesion
from db.zonas import filtrar_zonas_candidatas
from db.redis_client import get_redis

# Umbrales para el loop de preservación de señal (Fase 3).
_SIGNAL_THRESHOLD = 70
_SIGNAL_MAX_ROUNDS = 3

logger = logging.getLogger(__name__)
router = APIRouter(tags=["buscar"])


# ─── Request ──────────────────────────────────────────────────────────────────

class BuscarRequest(BaseModel):
    descripcion: str = Field(
        "",
        max_length=1000,
        description=(
            "Descripción del negocio en lenguaje natural. "
            "Obligatoria si `perfil_estructurado` no viene — en ese caso debe tener >=10 chars. "
            "Puede venir vacía cuando el frontend usa el formulario tipo test."
        ),
        examples=["Quiero abrir una cafetería de especialidad en un barrio de diseño"],
    )
    ciudad: Literal["Barcelona"] = Field(
        default="Barcelona",
        description="Ciudad donde buscar. Por ahora solo Barcelona.",
    )
    presupuesto_max: Optional[float] = Field(None, gt=0, description="Alquiler mensual máximo en €")
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
    # ── Camino rápido: cuestionario tipo test ───────────────────────────────
    # Si viene, el endpoint se salta `validar_negocio` y `refinar` (ahorra
    # 1-3s y coste LLM) y construye el perfil directamente desde el form.
    # Solo se llama a `refinar()` si `perfil_estructurado.matices` no está vacío.
    perfil_estructurado: Optional[PerfilEstructurado] = Field(
        None,
        description=(
            "Payload del cuestionario estructurado. Si viene, se usa como fuente "
            "de verdad y se omite el LLM (excepto para el campo opcional `matices`)."
        ),
    )


# ─── Response ─────────────────────────────────────────────────────────────────

class BuscarResponse(BaseModel):
    session_id: str
    estado: EstadoBusqueda
    zonas: Optional[list[ZonaResumen]] = None
    total_zonas_analizadas: Optional[int] = Field(
        None,
        description="Total de zonas evaluadas antes de filtrar.",
    )
    pregunta: Optional[str] = Field(None, description="Pregunta del cuestionario si estado='cuestionario'.")
    progreso_cuestionario: Optional[int] = Field(None, ge=0, le=100)
    motivo: Optional[str] = Field(
        None,
        description="Mensaje para estados 'error_tipo_negocio' e 'inviable_legal'.",
    )


# ─── Helper ───────────────────────────────────────────────────────────────────

def _validacion_desde_perfil_estructurado(pe: PerfilEstructurado) -> dict:
    """
    Construye el `dict validacion` que normalmente devuelve `validar_negocio` a
    partir del cuestionario estructurado. No llama al LLM: todos los campos se
    derivan del formulario.

    El subsector se valida contra taxonomia.py antes de usarlo. Si no pertenece
    al sector declarado se descarta silenciosamente para no contaminar el scoring.
    """
    sector = pe.sector or "desconocido"
    # Validar que el subsector pertenezca al sector declarado (taxonomia.py es la fuente de verdad)
    subsector_validado = (
        pe.subsector
        if (pe.subsector and subsector_valido(sector, pe.subsector))
        else None
    )
    idea_tags: list[str] = []
    if subsector_validado:
        idea_tags.append(subsector_validado)
    return {
        "es_retail":              True,
        "inviable_legal":         False,
        "motivo_legal":           None,
        "motivo":                 None,
        "informacion_suficiente": True,
        "sector_detectado":       sector,
        "subsector_detectado":    subsector_validado,
        "idea_tags":              idea_tags,
        "perfil_negocio":         {},
        "concepto_negocio":       {},
        "variables_conocidas":    {},
        "preguntas_necesarias":   [],
    }


def _perfil_refinado_desde_form(pe: PerfilEstructurado) -> dict:
    """
    Traduce el formulario estructurado a un `PerfilRefinado` serializable.
    Todo lo que el LLM normalmente extraería (público, operación, ubicación)
    viene ya tipado — se copia tal cual. `signal_preservation_score` = 100
    porque el usuario lo ha declarado explícitamente.
    """
    return PerfilRefinado(
        publico_objetivo=pe.publico_objetivo,
        operacion=pe.operacion,
        ubicacion_ideal=pe.ubicacion_ideal,
        nuances_detected=[],
        signal_preservation_score=100,
    ).model_dump()


def _build_zona_resumen(z: dict) -> ZonaResumen:
    """
    Convierte un dict fusionado (candidata + score) a ZonaResumen.
    Admite ambas variantes de nombre de campo para compatibilidad.
    """
    score = z.get("score_global")
    score = score if score is not None else 50.0
    _prob_3a = z.get("probabilidad_supervivencia_3a")
    prob = _prob_3a if _prob_3a is not None else z.get("probabilidad_supervivencia")
    return ZonaResumen(
        zona_id=z["zona_id"],
        nombre=z["nombre"],
        barrio=z["barrio"],
        distrito=z["distrito"],
        score_global=round(score, 1),
        probabilidad_supervivencia_3a=round(prob, 2) if prob is not None else None,
        alquiler_estimado=z.get("alquiler_estimado") or z.get("alquiler_mensual"),
        m2_disponibles=z.get("m2_disponibles") or z.get("m2"),
        color=score_to_color(score),
        lat=z["lat"],
        lng=z["lng"],
        resumen_ia=z.get("resumen_ia"),
    )


# ─── Endpoint ────────────────────────────────────────────────────────────────

@router.post(
    "/buscar",
    response_model=BuscarResponse,
    summary="Búsqueda principal de zonas por descripción en lenguaje natural",
)
async def buscar(body: BuscarRequest, request: Request) -> BuscarResponse:
    """
    El estado de la respuesta determina qué hace el frontend:
      - "ok"                → renderizar mapa con zonas
      - "cuestionario"      → mostrar la pregunta al usuario
      - "error_tipo_negocio"→ mostrar mensaje de error
      - "inviable_legal"    → mostrar advertencia legal + botón "Saber más"
    """
    # ── 0. Validar que hay cuestionario o descripción ────────────────────────
    if body.perfil_estructurado is None:
        if not body.descripcion or len(body.descripcion.strip()) < 10:
            raise HTTPException(
                status_code=422,
                detail=(
                    "Debes enviar `descripcion` (≥10 caracteres) o `perfil_estructurado`."
                ),
            )
    else:
        if not body.perfil_estructurado.sector:
            raise HTTPException(
                status_code=422,
                detail="`perfil_estructurado.sector` es obligatorio.",
            )

    # ── 1. Sesión ─────────────────────────────────────────────────────────────
    # Si viene el cuestionario estructurado, sus filtros prácticos tienen
    # precedencia sobre los `presupuesto_max/m2/distritos` del cuerpo raíz.
    pe = body.perfil_estructurado
    filtros_efectivos = {
        "presupuesto_max": pe.presupuesto_max if pe else body.presupuesto_max,
        "m2_min":          pe.m2_min          if pe else body.m2_min,
        "m2_max":          pe.m2_max          if pe else body.m2_max,
        "distritos":       pe.distritos       if pe else body.distritos,
    }

    session_id = body.session_id or str(uuid4())
    sesion = await get_sesion(session_id)

    if sesion is None:
        ip_hash = _hash_ip(request.client.host if request.client else "")
        sesion = await crear_sesion(
            session_id=session_id,
            datos={
                "descripcion_original": body.descripcion,
                "ciudad": body.ciudad,
                "filtros": filtros_efectivos,
                "perfil": {},
            },
            ip_hash=ip_hash,
        )
    elif pe is not None:
        # Sesión existente + nuevo cuestionario → actualizar filtros en la sesión.
        sesion["filtros"] = filtros_efectivos
        try:
            await actualizar_sesion(session_id, {"filtros": filtros_efectivos})
        except Exception as exc:
            logger.warning("No se pudieron actualizar filtros desde el test: %s", exc)

    # ── 2. Validación: LLM o desde cuestionario estructurado ─────────────────
    if pe is not None:
        # Fast path: el formulario ya contiene toda la información necesaria.
        # Ni `validar_negocio` ni `refinar` se llaman (salvo `matices`, más abajo).
        logger.info("buscar: fast path (cuestionario estructurado) sector=%s", pe.sector)
        validacion = _validacion_desde_perfil_estructurado(pe)
    else:
        try:
            validacion = await validar_negocio(body.descripcion, session_id)
        except Exception as exc:
            logger.error("Error en validacion LLM: %s", exc, exc_info=True)
            validacion = {
                "es_retail":             True,
                "inviable_legal":        False,
                "motivo_legal":          None,
                "motivo":                None,
                "informacion_suficiente": False,
                "sector_detectado":      "desconocido",
                "variables_conocidas":   {},
                "preguntas_necesarias":  ["sector", "m2", "presupuesto", "cliente"],
            }

    # ── 3a. Negocio no apto ───────────────────────────────────────────────────
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

    # ── 3b. Bloqueado legalmente ──────────────────────────────────────────────
    if validacion.get("inviable_legal"):
        return BuscarResponse(
            session_id=session_id,
            estado=EstadoBusqueda.INVIABLE_LEGAL,
            motivo=validacion.get("motivo_legal"),
        )

    # ── 3c/3d. Buscar y rankear zonas (sin cuestionario) ─────────────────────
    perfil = {
        **sesion.get("perfil", {}),
        "sector":         validacion["sector_detectado"],
        "idea_tags":      validacion.get("idea_tags") or [],
        "perfil_negocio": validacion.get("perfil_negocio") or {},
        "concepto_negocio": validacion.get("concepto_negocio") or {},
        "variables":      validacion["variables_conocidas"],
    }

    # Persistir flags legales + overrides financieros para que /api/legal y
    # /api/financiero puedan consumirlos sin volver a pedírselos al usuario.
    if pe is not None:
        if pe.flags_legales is not None:
            perfil["flags_legales"] = pe.flags_legales.model_dump(exclude_none=True)
        if pe.overrides_financieros is not None:
            perfil["overrides_financieros"] = pe.overrides_financieros.model_dump(exclude_none=True)
        # Usar el subsector ya validado contra taxonomia.py (puede ser None si era inválido)
        subsector_validado = validacion.get("subsector_detectado")
        if subsector_validado:
            perfil["subsector"] = subsector_validado

    # ── 3c-bis. Construir PerfilRefinado ────────────────────────────────────
    # Dos caminos posibles:
    #   (A) Fast path: el cuestionario ya contiene los campos estructurados.
    #       Se mapea directo y solo se invoca `refinar()` si hay `matices`.
    #   (B) Flujo clásico: el LLM refina la descripción y se cachea en Redis.

    if pe is not None:
        # ── (A) Fast path — sin LLM salvo `matices` ─────────────────────────
        perfil_refinado_dict = _perfil_refinado_desde_form(pe)
        if pe.matices and pe.matices.strip():
            try:
                perfil_matices = await refinar(
                    descripcion=pe.matices.strip(),
                    sector_detectado=pe.sector,
                    tags_previos=[validacion["subsector_detectado"]] if validacion.get("subsector_detectado") else [],
                    session_id=session_id,
                )
                # Fusionar nuances_detected y rellenar campos vacíos del form.
                matices_dict = perfil_matices.model_dump()
                perfil_refinado_dict["nuances_detected"] = (
                    matices_dict.get("nuances_detected") or []
                )
                # Si el form no especificó ciertos campos y el LLM los detecta
                # en los matices, los copiamos (no sobrescribimos lo ya dicho).
                for bloque in ("propuesta_valor",):
                    valor = matices_dict.get(bloque)
                    if valor:
                        perfil_refinado_dict[bloque] = valor
            except Exception as exc:
                logger.warning("refinar(matices) falló: %s — se ignora", exc)

        perfil["perfil_refinado"] = perfil_refinado_dict
        # No hay loop de señal en fast path (signal_preservation_score=100).

    else:
        # ── (B) Flujo clásico con LLM + cache Redis ─────────────────────────
        # Se cachea por hash(descripción) para evitar repetir la llamada LLM si el
        # usuario pulsa "buscar" otra vez con el mismo texto dentro de la sesión.
        #
        # FIX BUG-007: Race condition — dos requests concurrentes con la misma
        # descripción pueden llegar aquí con cache_prev vacío y ambas llaman al LLM.
        # Solución: distributed lock en Redis (SET NX EX 30s) antes de llamar al LLM.
        # Solo el primer waiter que obtenga el lock llama al LLM y escribe en Redis.
        # El segundo waiter espera brevemente y lee el resultado ya calculado.
        _TTL_REFINADO = 3600  # 1 hora en segundos
        descripcion_hash = hashlib.md5(body.descripcion.strip().encode("utf-8")).hexdigest()
        cache_prev = (sesion.get("perfil") or {}).get("perfil_refinado_cache") or {}
        perfil_refinado_dict: dict = cache_prev.get(descripcion_hash) or {}

        if not perfil_refinado_dict:
            redis = get_redis()
            redis_cache_key = f"perfil_refinado:{descripcion_hash}"
            redis_lock_key = f"{redis_cache_key}:lock"

            # Check Redis cache first (covers cross-session deduplication).
            cached_raw = await redis.get(redis_cache_key)
            if cached_raw:
                import json as _json
                try:
                    perfil_refinado_dict = _json.loads(cached_raw)
                except Exception:
                    perfil_refinado_dict = {}

            if not perfil_refinado_dict:
                # Try to acquire distributed lock (NX = only if not exists, EX = 30s TTL).
                got_lock = await redis.set(redis_lock_key, "1", nx=True, ex=30)
                if got_lock:
                    try:
                        perfil_refinado_model = await refinar(
                            descripcion=body.descripcion,
                            sector_detectado=perfil["sector"],
                            tags_previos=perfil["idea_tags"],
                            session_id=session_id,
                        )
                        perfil_refinado_dict = perfil_refinado_model.model_dump()
                    except Exception as exc:
                        logger.warning("Refinador falló: %s — continuando con perfil vacío", exc)
                        perfil_refinado_dict = PerfilRefinado().model_dump()
                    finally:
                        # Write result to Redis before releasing lock so waiters can read it.
                        try:
                            import json as _json
                            await redis.set(redis_cache_key, _json.dumps(perfil_refinado_dict), ex=_TTL_REFINADO)
                        except Exception as exc:
                            logger.warning("No se pudo escribir perfil_refinado en Redis: %s", exc)
                        await redis.delete(redis_lock_key)
                else:
                    # Another request is computing the same profile — poll cache until the
                    # holder publishes the result. The holder's lock TTL is 30s y el LLM
                    # tarda normalmente 2-5s; esperamos hasta 30s en pasos de 0.25s para no
                    # duplicar la llamada al LLM en requests concurrentes de la misma query.
                    import json as _json
                    _POLL_INTERVAL_S = 0.25
                    _POLL_MAX_S = 30.0
                    waited = 0.0
                    while waited < _POLL_MAX_S:
                        await asyncio.sleep(_POLL_INTERVAL_S)
                        waited += _POLL_INTERVAL_S
                        cached_raw = await redis.get(redis_cache_key)
                        if cached_raw:
                            try:
                                perfil_refinado_dict = _json.loads(cached_raw)
                            except Exception:
                                perfil_refinado_dict = {}
                            if perfil_refinado_dict:
                                break
                        # Si el lock desapareció SIN haber publicado cache, el holder falló
                        # → salimos del polling y caemos al fallback LLM propio.
                        lock_still_held = await redis.get(redis_lock_key)
                        if not lock_still_held and not cached_raw:
                            break
                    if not perfil_refinado_dict:
                        # Fallback: lock holder may have failed; call LLM ourselves.
                        try:
                            perfil_refinado_model = await refinar(
                                descripcion=body.descripcion,
                                sector_detectado=perfil["sector"],
                                tags_previos=perfil["idea_tags"],
                                session_id=session_id,
                            )
                            perfil_refinado_dict = perfil_refinado_model.model_dump()
                        except Exception as exc:
                            logger.warning("Refinador (fallback) falló: %s — continuando con perfil vacío", exc)
                            perfil_refinado_dict = PerfilRefinado().model_dump()

        perfil["perfil_refinado"] = perfil_refinado_dict
        perfil["perfil_refinado_cache"] = {**cache_prev, descripcion_hash: perfil_refinado_dict}

        # ── 3c-ter. Loop de preservación de señal ────────────────────────────
        # Si el LLM declara score<70 y aún no hemos gastado los 3 rounds, pedimos
        # al usuario una aclaración específica antes de rankear zonas.
        # Solo aplica al flujo clásico — el fast path tiene score=100.
        signal_score = int(perfil_refinado_dict.get("signal_preservation_score") or 0)
        rounds_usados = int((sesion.get("perfil") or {}).get("signal_rounds", 0))
        if signal_score < _SIGNAL_THRESHOLD and rounds_usados < _SIGNAL_MAX_ROUNDS:
            perfil_mod = PerfilRefinado(**perfil_refinado_dict)
            pregunta_en = await generar_pregunta_senal(
                perfil_refinado=perfil_mod,
                descripcion=body.descripcion,
                session_id=session_id,
            )
            if pregunta_en:
                try:
                    pregunta_es = await traducir(pregunta_en, session_id)
                except Exception:
                    pregunta_es = pregunta_en
                # Persistir: incrementamos rounds y guardamos perfil_refinado actualizado
                perfil["signal_rounds"] = rounds_usados + 1
                try:
                    await actualizar_sesion(session_id, {"perfil": perfil})
                except Exception as exc:
                    logger.warning("No se pudo actualizar perfil en loop señal: %s", exc)
                progreso = int(min(90, 30 + rounds_usados * 20))
                return BuscarResponse(
                    session_id=session_id,
                    estado=EstadoBusqueda.CUESTIONARIO,
                    pregunta=pregunta_es,
                    progreso_cuestionario=progreso,
                )

    filtros = sesion["filtros"]
    zonas_candidatas = await filtrar_zonas_candidatas(filtros)

    if not zonas_candidatas:
        logger.warning("Sin zonas candidatas para session_id=%s con filtros=%s", session_id, filtros)
        filtros_amplios = {**filtros, "distritos": None}
        zonas_candidatas = await filtrar_zonas_candidatas(filtros_amplios)

    total_candidatas = len(zonas_candidatas)

    # calcular_scores_batch espera lista de IDs y el código de sector
    zona_ids      = [z["zona_id"] for z in zonas_candidatas]
    sector_codigo = perfil.get("sector", "desconocido")
    # Los idea_tags vienen del LLM directamente — capturan el concepto específico
    # (ej: dog_friendly + clientela_local + specialty_coffee) con mucha más
    # precisión que el sector genérico. La descripción es fallback si no hay tags.
    scores_list = await calcular_scores_batch(
        zona_ids,
        sector_codigo,
        idea_tags=validacion.get("idea_tags") or [],
        descripcion_negocio=body.descripcion,
        perfil_negocio=validacion.get("perfil_negocio") or {},
        concepto_negocio=validacion.get("concepto_negocio") or {},
        perfil_refinado=perfil_refinado_dict or None,
    )

    # Construir lookup de scores por zona_id
    scores_by_id = {s["zona_id"]: s for s in scores_list}

    # Fusionar datos de la candidata con el score calculado
    zonas_merged = []
    for c in zonas_candidatas:
        score_data = scores_by_id.get(c["zona_id"], {"score_global": 50.0})
        zonas_merged.append({**c, **score_data})

    # Ordenar por score descendente y construir respuesta
    zonas_merged.sort(key=lambda z: z.get("score_global", 0), reverse=True)
    zonas_response = [_build_zona_resumen(z) for z in zonas_merged]

    # ── 4. Persistir zonas en sesión + guardar búsqueda para analytics ────────
    try:
        await actualizar_sesion(session_id, {
            "zonas_actuales": zonas_merged,
            "perfil": perfil,
        })
    except Exception as exc:
        logger.warning("No se pudo actualizar zonas_actuales en sesión: %s", exc)

    try:
        await guardar_busqueda(
            session_id=session_id,
            descripcion=body.descripcion,
            filtros=filtros,
            perfil=perfil,
            num_resultados=len(zonas_response),
        )
    except Exception as exc:
        logger.warning("No se pudo guardar busqueda en analytics: %s", exc)

    return BuscarResponse(
        session_id=session_id,
        estado=EstadoBusqueda.OK,
        zonas=zonas_response,
        total_zonas_analizadas=total_candidatas,
    )


# ─── Utilidades ───────────────────────────────────────────────────────────────

def _hash_ip(ip: str) -> str:
    """Hash SHA-256 de la IP para analytics anónimos."""
    return hashlib.sha256(ip.encode()).hexdigest()[:16]
