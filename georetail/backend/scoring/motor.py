"""
scoring/motor.py - Interfaz publica del modulo de scoring.

Los endpoints de la API importan desde aqui. Este modulo delega en scorer.py.

Funciones publicas:
  - calcular_scores_batch(zona_ids, sector_codigo) -> usado por POST /api/buscar
  - get_scores_zona(zona_id, sector_codigo)        -> usado por POST /api/local

Estrategia de cache:
  - Se incluyen scores de cualquier version (seed_v1 incluido).
  - El score_global se recalcula solo cuando hay 8 dimensiones manuales
    disponibles; los registros XGBoost o incompletos conservan su score base.
  - La afinidad de concepto no se persiste en BD: se calcula en runtime para el
    negocio concreto consultado y se aplica igual en batch y detalle.
"""
from __future__ import annotations

import json
import logging
from typing import Optional

from scoring.explainability import group_shap_by_dimension
from scoring.concepto.taxonomy import (
    aplicar_pesos_a_sector,
    compilar_concepto_negocio,
    idea_tags_visibles_desde_texto,
    zona_ideal_desde_perfil,
)
from scoring.scorer import calcular_scores_batch as _scorer_batch

logger = logging.getLogger(__name__)

# Peso de score_afinidad_concepto en score_global.
_PESO_AFINIDAD = 0.12


# ─── Reglas declarativas de modulación por PerfilRefinado (Fase 2) ──────────
# Cada regla se declara como ((dot_path, predicado), {peso: multiplicador, ...}).
# El `dot_path` navega el dict `perfil_refinado` (ej. "operacion.ticket_tier_p1_p5").
# Predicados soportados:
#   - string literal "X"       → equals, case-insensitive
#   - "contains:X"             → substring match (si valor es string) o
#                                 membresía case-insensitive (si valor es lista)
#   - ">=N" / ">N" / "<=N" / "<N" → comparación numérica
#
# Los multiplicadores se aplican ANTES de renormalizar a suma=1.0. No se mezclan
# con la lógica de concepto_negocio; actúan en cascada después.
REGLAS_MODULACION: list[tuple[tuple[str, str], dict[str, float]]] = [
    (("ubicacion_ideal.flujo_tipo", "premium residencial"),
        {"peso_demo": 1.15, "peso_transporte": 0.90}),
    (("operacion.horarios_apertura", "contains:noche"),
        {"peso_seguridad": 1.30, "peso_turismo": 1.20}),
    (("operacion.ticket_tier_p1_p5", ">=4"),
        {"peso_precio": 0.50}),
    (("publico_objetivo.nivel_socioeconomico", "alto"),
        {"peso_demo": 1.20}),
    (("publico_objetivo.estilo_vida", "contains:turista"),
        {"peso_turismo": 1.15, "peso_dinamismo": 1.10}),
]


def _resolver_path(obj: dict, dot_path: str):
    """Navega un dict con clave 'a.b.c'. Devuelve None si algo no existe."""
    current = obj
    for part in dot_path.split("."):
        if not isinstance(current, dict):
            return None
        current = current.get(part)
    return current


def _predicado_match(valor, predicado: str) -> bool:
    """Evalúa `predicado` contra `valor` extraído del perfil."""
    if valor is None:
        return False
    # Predicados numéricos
    for op in (">=", "<=", ">", "<"):
        if predicado.startswith(op):
            try:
                umbral = float(predicado[len(op):])
                num = float(valor)
            except (TypeError, ValueError):
                return False
            if op == ">=":
                return num >= umbral
            if op == "<=":
                return num <= umbral
            if op == ">":
                return num > umbral
            if op == "<":
                return num < umbral
    # contains:
    if predicado.startswith("contains:"):
        token = predicado[len("contains:"):].strip().lower()
        if not token:
            return False
        if isinstance(valor, list):
            return any(isinstance(x, str) and token in x.lower() for x in valor)
        if isinstance(valor, str):
            return token in valor.lower()
        return False
    # Equals (case-insensitive para strings)
    if isinstance(valor, str):
        return valor.strip().lower() == predicado.strip().lower()
    return valor == predicado


def _normalizar_pesos(pesos: dict[str, float]) -> dict[str, float]:
    """Renormaliza los pesos (claves peso_*) para que sumen 1.0."""
    campos = [k for k in pesos if k.startswith("peso_")]
    total = sum(float(pesos.get(k, 0.0) or 0.0) for k in campos)
    if total <= 0:
        return {k: float(pesos.get(k, 0.0) or 0.0) for k in campos}
    return {k: round(float(pesos.get(k, 0.0) or 0.0) / total, 4) for k in campos}


def aplicar_reglas_modulacion(pesos: dict, perfil_refinado: Optional[dict]) -> dict[str, float]:
    """Aplica las reglas declarativas sobre los pesos del sector.

    Si `perfil_refinado` es None/vacío, devuelve los pesos renormalizados sin
    cambios (idempotente). Cada regla que matchea multiplica los pesos
    indicados; al final se renormaliza a suma=1.0.
    """
    base = {k: float(v) for k, v in (pesos or {}).items() if isinstance(v, (int, float)) and k.startswith("peso_")}
    if not base:
        return {}
    if not perfil_refinado:
        return _normalizar_pesos(base)

    ajustado = dict(base)
    for (path, predicado), multiplicadores in REGLAS_MODULACION:
        valor = _resolver_path(perfil_refinado, path)
        if not _predicado_match(valor, predicado):
            continue
        for peso_key, mult in multiplicadores.items():
            if peso_key in ajustado:
                ajustado[peso_key] = ajustado[peso_key] * float(mult)

    return _normalizar_pesos(ajustado)

_DIMENSION_KEYS = (
    "score_flujo_peatonal",
    "score_demografia",
    "score_competencia",
    "score_transporte",
    "score_seguridad",
    "score_turismo",
    "score_dinamismo",
)


async def calcular_scores_batch(
    zona_ids: list[str],
    sector_codigo: str,
    m2: Optional[float] = None,
    idea_tags: Optional[list[str]] = None,
    descripcion_negocio: Optional[str] = None,
    perfil_negocio: Optional[dict] = None,
    concepto_negocio: Optional[dict] = None,
    perfil_refinado: Optional[dict] = None,
) -> list[dict]:
    """
    Calcula el score de viabilidad para una lista de zonas.

    Devuelve lista de dicts con zona_id + score_global + scores por dimension.
    Batch y detalle comparten la misma preparacion conceptual y la misma regla
    de score final.
    """
    if not zona_ids:
        return []

    try:
        from db.conexion import get_db

        pesos = await _get_pesos_sector(sector_codigo)
        contexto = _preparar_contexto_concepto(
            sector_codigo=sector_codigo,
            concepto_negocio=concepto_negocio,
            idea_tags_llm=idea_tags,
            descripcion_negocio=descripcion_negocio,
            perfil_negocio=perfil_negocio,
        )
        pesos_efectivos = _aplicar_pesos_concepto(pesos, contexto, perfil_refinado=perfil_refinado)

        async with get_db() as conn:
            rows = await conn.fetch(
                """
                SELECT DISTINCT ON (sz.zona_id)
                       sz.zona_id,
                       sz.score_global,
                       sz.score_flujo_peatonal,
                       sz.score_demografia,
                       sz.score_competencia,
                       sz.score_precio_alquiler,
                       sz.score_transporte,
                       sz.score_seguridad,
                       sz.score_turismo,
                       sz.probabilidad_supervivencia,
                       sz.shap_values,
                       sz.modelo_version,
                       dz.score_dinamismo
                FROM scores_zona sz
                JOIN sectores s ON s.id = sz.sector_id
                LEFT JOIN LATERAL (
                    SELECT score_dinamismo
                    FROM dinamismo_zonal
                    WHERE zona_id = sz.zona_id
                    ORDER BY updated_at DESC
                    LIMIT 1
                ) dz ON true
                WHERE sz.zona_id = ANY($1)
                  AND s.codigo   = $2
                ORDER BY sz.zona_id,
                         CASE sz.modelo_version
                             WHEN 'fallback'       THEN 0
                             WHEN 'fallback_error' THEN 0
                             WHEN 'seed_v1'        THEN 1
                             ELSE                       2
                         END DESC,
                         sz.fecha_calculo DESC
                """,
                zona_ids,
                sector_codigo,
            )

        resultados_por_zona = {dict(r)["zona_id"]: dict(r) for r in rows}
        zonas_sin_cache = [zona_id for zona_id in zona_ids if zona_id not in resultados_por_zona]

        if zonas_sin_cache:
            perfil = (concepto_negocio or {}).get("perfil_negocio") or perfil_negocio or {}
            frescos = await _scorer_batch(zonas_sin_cache, sector_codigo, idea_tags=contexto.get("tags_efectivos") or idea_tags, perfil_negocio=perfil)
            for zona_id in zonas_sin_cache:
                payload = frescos.get(zona_id)
                resultados_por_zona[zona_id] = (
                    {"zona_id": zona_id, **payload}
                    if payload is not None
                    else _build_fallback_row(zona_id)
                )

        datos_afinidad = {}
        if contexto["zona_ideal"]:
            datos_afinidad = await _cargar_datos_afinidad_zonas(zona_ids)

        resultados_finales: list[dict] = []
        for zona_id in zona_ids:
            raw = dict(resultados_por_zona.get(zona_id) or _build_fallback_row(zona_id))
            aplicado = _aplicar_contexto_score(
                raw,
                pesos_efectivos,
                contexto["zona_ideal"],
                datos_afinidad.get(zona_id),
            )
            # Adjuntamos los pesos modulados para que el frontend los explique.
            aplicado["pesos_modulados"] = pesos_efectivos
            resultados_finales.append(aplicado)

        return resultados_finales

    except Exception as exc:
        logger.error("calcular_scores_batch error: %s", exc, exc_info=True)
        return [_build_fallback_row(zona_id, modelo_version="fallback_error") for zona_id in zona_ids]


async def get_scores_zona(
    zona_id: str,
    sector_codigo: str,
    idea_tags: Optional[list[str]] = None,
    descripcion_negocio: Optional[str] = None,
    perfil_negocio: Optional[dict] = None,
    concepto_negocio: Optional[dict] = None,
    perfil_refinado: Optional[dict] = None,
) -> dict:
    """
    Devuelve el score detallado de una zona en el formato que espera api/local.py.

    El detalle reutiliza exactamente la misma preparacion conceptual y el mismo
    score final que el batch para evitar divergencias.
    """
    from db.conexion import get_db

    pesos = await _get_pesos_sector(sector_codigo)
    contexto = _preparar_contexto_concepto(
        sector_codigo=sector_codigo,
        concepto_negocio=concepto_negocio,
        idea_tags_llm=idea_tags,
        descripcion_negocio=descripcion_negocio,
        perfil_negocio=perfil_negocio,
    )
    pesos_efectivos = _aplicar_pesos_concepto(pesos, contexto, perfil_refinado=perfil_refinado)

    async with get_db() as conn:
        row = await conn.fetchrow(
            """
            SELECT sz.*, s.codigo AS sector_codigo,
                   dz.score_dinamismo, dz.tendencia,
                   dz.negocios_historico_count, dz.tasa_supervivencia_3a,
                   dz.ratio_apertura_cierre_1a, dz.renta_variacion_3a,
                   dz.hhi_sectorial,
                   vz.incidencias_por_1000hab, vz.hurtos_por_1000hab,
                   vz.robatoris_por_1000hab, vz.danys_por_1000hab,
                   vz.incidencias_noche_pct, vz.comisarias_1km,
                   vz.dist_comisaria_m, vz.seguridad_barri_score
            FROM scores_zona sz
            JOIN sectores s ON s.id = sz.sector_id
            LEFT JOIN LATERAL (
                SELECT score_dinamismo, tendencia, negocios_historico_count,
                       tasa_supervivencia_3a, ratio_apertura_cierre_1a,
                       renta_variacion_3a, hhi_sectorial
                FROM dinamismo_zonal
                WHERE zona_id = sz.zona_id
                ORDER BY updated_at DESC
                LIMIT 1
            ) dz ON true
            LEFT JOIN LATERAL (
                SELECT incidencias_por_1000hab, hurtos_por_1000hab,
                       robatoris_por_1000hab, danys_por_1000hab,
                       incidencias_noche_pct, comisarias_1km,
                       dist_comisaria_m, seguridad_barri_score
                FROM v_variables_zona
                WHERE zona_id = sz.zona_id
                ORDER BY (incidencias_por_1000hab IS NOT NULL) DESC,
                         fecha DESC
                LIMIT 1
            ) vz ON true
            WHERE sz.zona_id = $1
              AND s.codigo   = $2
            ORDER BY CASE sz.modelo_version
                         WHEN 'fallback'       THEN 0
                         WHEN 'fallback_error' THEN 0
                         WHEN 'seed_v1'        THEN 1
                         ELSE                       2
                     END DESC,
                     sz.fecha_calculo DESC
            LIMIT 1
            """,
            zona_id,
            sector_codigo,
        )

    if row is None:
        resultados = await calcular_scores_batch(
            [zona_id],
            sector_codigo,
            idea_tags=idea_tags,
            descripcion_negocio=descripcion_negocio,
            perfil_negocio=perfil_negocio,
            concepto_negocio=concepto_negocio,
            perfil_refinado=perfil_refinado,
        )
        raw = resultados[0] if resultados else _build_fallback_row(zona_id)
        return _format_scores_for_api(raw)

    raw = dict(row)

    # Escalar score_dinamismo de 0-10 (escala pipeline) a 0-100 (escala API)
    # usando el scorer de dimensión que también aplica ajustes por tendencia.
    # Si no hay row en dinamismo_zonal (pipeline aún no ejecutado), calcular_dinamismo
    # aplica su fallback interno (~50) contemplando tendencia cuando existe.
    try:
        from scoring.dimensiones.dinamismo import calcular_dinamismo
        din_result = calcular_dinamismo(raw)
        raw["score_dinamismo"] = din_result["score_dinamismo"]
    except Exception as exc:  # BUG-006 fix: log error instead of silently swallowing it
        logger.warning(
            "calcular_dinamismo failed for zona_id=%s, falling back to raw scale: %s",
            raw.get("zona_id"),
            exc,
        )
        if raw.get("score_dinamismo") is not None:
            raw["score_dinamismo"] = round(float(raw["score_dinamismo"]) * 10.0, 1)

    # Seguridad: si scores_zona cacheado no populó la columna, computar fresco
    # desde las variables crudas (v_variables_zona trae incidencias/hurtos/etc).
    if raw.get("score_seguridad") is None:
        try:
            from scoring.dimensiones.seguridad import calcular_score_seguridad
            seg_result = calcular_score_seguridad(raw, perfil_negocio=perfil_negocio)
            raw["score_seguridad"] = seg_result["score_seguridad"]
        except Exception as exc:
            logger.debug("recompute seguridad failed: %s", exc)

    # Solo recalcular si el registro no tiene NINGUNA dimensión manual (registro xgboost puro sin enriquecer)
    if all(raw.get(dim) is None for dim in _DIMENSION_KEYS):
        resultados = await calcular_scores_batch(
            [zona_id],
            sector_codigo,
            idea_tags=idea_tags,
            descripcion_negocio=descripcion_negocio,
            perfil_negocio=perfil_negocio,
            concepto_negocio=concepto_negocio,
            perfil_refinado=perfil_refinado,
        )
        raw = resultados[0] if resultados else _build_fallback_row(zona_id)

    datos_afinidad = {}
    if contexto["zona_ideal"]:
        datos_afinidad = await _cargar_datos_afinidad_zonas([zona_id])

    raw = _aplicar_contexto_score(
        raw,
        pesos_efectivos,
        contexto["zona_ideal"],
        datos_afinidad.get(zona_id),
    )
    raw["pesos_modulados"] = pesos_efectivos
    return _format_scores_for_api(raw)


def _combinar_tags(
    idea_tags_llm: Optional[list[str]],
    descripcion_negocio: Optional[str],
) -> list[str]:
    contexto = _preparar_contexto_concepto(
        sector_codigo="desconocido",
        concepto_negocio=None,
        idea_tags_llm=idea_tags_llm,
        descripcion_negocio=descripcion_negocio,
        perfil_negocio=None,
    )
    return contexto["tags_efectivos"]


async def _calcular_score_afinidad(
    zona_id: str,
    descripcion_negocio: Optional[str],
    tags_efectivos: list[str],
    perfil_negocio: Optional[dict] = None,
    concepto_negocio: Optional[dict] = None,
) -> Optional[float]:
    """
    Compatibilidad con tests y utilidades antiguas.

    Reutiliza la misma preparacion conceptual usada en batch y detalle.
    """
    contexto = _preparar_contexto_concepto(
        sector_codigo="desconocido",
        concepto_negocio=concepto_negocio,
        idea_tags_llm=tags_efectivos,
        descripcion_negocio=descripcion_negocio,
        perfil_negocio=perfil_negocio,
    )
    if not contexto["zona_ideal"]:
        return None

    datos = await _cargar_datos_afinidad_zonas([zona_id])
    return _score_zona_vs_ideal(datos.get(zona_id) or {}, contexto["zona_ideal"])


async def _get_pesos_sector(sector_codigo: str) -> dict:
    """Lee los pesos del sector desde la BD."""
    from db.conexion import get_db

    async with get_db() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM sectores WHERE codigo = $1",
            sector_codigo,
        )
    return dict(row) if row else {}


# ─── Invariante de pesos por sector ────────────────────────────────────────
# La suma de `peso_*` en la tabla `sectores` DEBE ser exactamente 1.0
# (tolerancia ±0.01). Si no, el score_base calculado en `_recalcular_global`
# está distorsionado sistemáticamente. Chequeo al arrancar para detectar
# regresiones introducidas por nuevas migraciones sin normalizar.
PESOS_TOLERANCIA = 0.01


async def verificar_pesos_sectores() -> list[dict]:
    """Chequea al arrancar que la suma de peso_* por sector esté en [0.99, 1.01].

    Devuelve una lista de dicts `{codigo, sum_pesos, delta}` para los sectores
    fuera de tolerancia. En producción, solo loguea WARNING (no aborta) para
    no romper despliegues en caliente; un operador debe revisar y aplicar la
    migración de normalización (`036_fix_pesos_sectores.sql`).
    """
    from db.conexion import get_db

    try:
        async with get_db() as conn:
            rows = await conn.fetch(
                """
                SELECT codigo,
                       COALESCE(peso_flujo,0)
                     + COALESCE(peso_demo,0)
                     + COALESCE(peso_competencia,0)
                     + COALESCE(peso_precio,0)
                     + COALESCE(peso_transporte,0)
                     + COALESCE(peso_seguridad,0)
                     + COALESCE(peso_turismo,0)
                     + COALESCE(peso_entorno,0) AS sum_pesos
                FROM sectores
                """
            )
    except Exception as exc:
        logger.warning("verificar_pesos_sectores: no se pudo consultar BD: %s", exc)
        return []

    desviados: list[dict] = []
    for row in rows:
        suma = float(row["sum_pesos"] or 0.0)
        delta = abs(suma - 1.0)
        if delta > PESOS_TOLERANCIA:
            desviados.append({"codigo": row["codigo"], "sum_pesos": round(suma, 4), "delta": round(delta, 4)})

    if desviados:
        logger.error(
            "Pesos por sector NO suman 1.0 (tolerancia ±%.2f): %s. "
            "Aplicar migración 036_fix_pesos_sectores.sql.",
            PESOS_TOLERANCIA,
            desviados,
        )
    else:
        logger.info("Pesos por sector OK (suma=1.0 ±%.2f en todos los sectores).", PESOS_TOLERANCIA)
    return desviados


async def _cargar_datos_afinidad_zonas(zona_ids: list[str]) -> dict[str, dict]:
    """Carga en batch las variables minimas necesarias para score_afinidad."""
    if not zona_ids:
        return {}

    from db.conexion import get_db

    async with get_db() as conn:
        rows = await conn.fetch(
            """
            SELECT DISTINCT ON (vz.zona_id)
                   vz.zona_id,
                   vz.renta_media_hogar,
                   vz.edad_media,
                   vz.flujo_peatonal_total,
                   vz.score_turismo,
                   vz.ratio_locales_comerciales,
                   vz.m2_zonas_verdes_cercanas,
                   vz.incidencias_por_1000hab
            FROM variables_zona vz
            WHERE vz.zona_id = ANY($1)
            ORDER BY vz.zona_id, vz.fecha DESC
            """,
            zona_ids,
        )
    return {dict(row)["zona_id"]: dict(row) for row in rows}


def _preparar_contexto_concepto(
    sector_codigo: Optional[str],
    concepto_negocio: Optional[dict],
    idea_tags_llm: Optional[list[str]],
    descripcion_negocio: Optional[str],
    perfil_negocio: Optional[dict],
) -> dict:
    matches, _matcher = _resolver_matcher(descripcion_negocio)

    if concepto_negocio and concepto_negocio.get("version") and concepto_negocio.get("perfil_negocio"):
        compiled = concepto_negocio
    else:
        compiled = compilar_concepto_negocio(
            sector=sector_codigo or (concepto_negocio or {}).get("sector"),
            base_concepts=(concepto_negocio or {}).get("base_concepts"),
            modifiers=(concepto_negocio or {}).get("modifiers"),
            idea_tags=(concepto_negocio or {}).get("idea_tags") or idea_tags_llm or [],
            perfil_hint=(concepto_negocio or {}).get("perfil_negocio") or perfil_negocio,
            descripcion=descripcion_negocio,
            matcher_matches=matches,
            confidence=(concepto_negocio or {}).get("confidence_global"),
            ambiguities=(concepto_negocio or {}).get("ambiguities"),
            justificacion_breve=(concepto_negocio or {}).get("justificacion_breve"),
        )

    _pn = compiled.get("perfil_negocio") or perfil_negocio
    zona_ideal = compiled.get("zona_ideal") or (_zona_ideal_desde_perfil(_pn) if _pn else {})
    return {
        "concepto_negocio": compiled,
        "tags_efectivos": _normalizar_tags(compiled.get("idea_tags") or idea_tags_llm or []),
        "matches": matches,
        "zona_ideal": zona_ideal,
    }


def _resolver_matcher(descripcion_negocio: Optional[str]):
    if not descripcion_negocio:
        return [], None

    try:
        from scoring.concepto_matcher import get_matcher

        matcher = get_matcher()
        matches = matcher.match(descripcion_negocio, top_k=4)
        return matches, matcher
    except Exception as exc:
        logger.debug("ConceptoMatcher no disponible: %s - usando taxonomia local", exc)
        return [], None


def _normalizar_tags(tags: Optional[list[str]]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []

    for tag in tags or []:
        if not isinstance(tag, str):
            continue
        clean = tag.strip()
        if not clean or clean in seen:
            continue
        seen.add(clean)
        result.append(clean)

    return result


def _zona_ideal_desde_perfil(perfil_negocio: Optional[dict]) -> dict:
    return zona_ideal_desde_perfil(perfil_negocio)


def _aplicar_pesos_concepto(
    pesos: dict,
    contexto: dict,
    perfil_refinado: Optional[dict] = None,
) -> dict:
    """Aplica la cascada completa de ajustes sobre los pesos del sector.

    Orden:
      1. concepto_negocio (taxonomía) → `aplicar_pesos_a_sector`.
      2. perfil_refinado (reglas declarativas) → `aplicar_reglas_modulacion`.
      3. Renormalización final a suma=1.0.

    Si `perfil_refinado` es None/vacío, no hay paso 2 (idempotente).
    """
    concepto = contexto.get("concepto_negocio")
    if concepto:
        pesos_ajustados = aplicar_pesos_a_sector(pesos, concepto)
    else:
        pesos_ajustados = {k: float(v) for k, v in (pesos or {}).items() if k.startswith("peso_")}

    logger.debug("idea_tags efectivos: %s", contexto.get("tags_efectivos"))

    if perfil_refinado:
        pesos_ajustados = aplicar_reglas_modulacion(pesos_ajustados, perfil_refinado)

    return pesos_ajustados


def _tiene_dimensiones_recalculables(scores: dict) -> bool:
    return all(scores.get(dim) is not None for dim in _DIMENSION_KEYS)


def _usa_score_base_original(scores: dict) -> bool:
    modelo_version = str(scores.get("modelo_version") or "")
    return modelo_version.startswith("xgboost_") or not _tiene_dimensiones_recalculables(scores)


def _valor_score(scores: dict, dim: str, default: float = 50.0) -> float:
    value = scores.get(dim)
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


# Pesos por defecto (suma = 1.00). Se usan para detectar si los pesos que
# llegan a `_calcular_score_base` fueron modulados por concepto/idea.
_PESOS_DEFAULT: dict[str, float] = {
    "peso_flujo":       0.25,
    "peso_demo":        0.25,
    "peso_competencia": 0.15,
    "peso_transporte":  0.15,
    "peso_dinamismo":   0.10,
    "peso_seguridad":   0.05,
    "peso_turismo":     0.05,
}

# Umbral (en puntos) a partir del cual consideramos incoherente la divergencia
# entre el score_global cacheado y el score recalculado desde dimensiones
# manuales. Si supera este umbral forzamos recálculo (evita "flujo 20 /
# global 86" en El Born).
_UMBRAL_DIVERGENCIA_PTS: float = 15.0


def _son_pesos_default(pesos: dict) -> bool:
    """
    True si `pesos` coincide con `_PESOS_DEFAULT` (delta ≤ 0.005 por clave).
    Se ignoran claves que no empiezan por `peso_`.
    """
    if not pesos:
        return True
    for k, v_default in _PESOS_DEFAULT.items():
        v = pesos.get(k)
        if v is None:
            continue
        try:
            if abs(float(v) - v_default) > 0.005:
                return False
        except (TypeError, ValueError):
            return False
    return True


def _calcular_score_base(scores: dict, pesos: dict) -> float:
    """
    Devuelve el score_global base, recalculando cuando es necesario.

    Reglas (en orden):
      1. Si los pesos fueron modulados por la idea del usuario (no default)
         y tenemos las 8 dimensiones manuales, recalculamos desde ellas:
         rankings distintos para ideas distintas.
      2. Si el score_global cacheado diverge > 15 pts del score derivado
         de las dimensiones manuales (inconsistencia tipo flujo=20 / global=86),
         recalculamos y anotamos modelo_version como "xgboost+manual_override".
      3. En otro caso, usamos el score_global cacheado (rápido).
    """
    tiene_dims = _tiene_dimensiones_recalculables(scores)
    pesos_modulados = not _son_pesos_default(pesos)

    # Regla 1: pesos modulados + dimensiones completas → recalcular siempre.
    if pesos_modulados and tiene_dims:
        nuevo = _recalcular_global(scores, pesos)
        # Marcamos el override para trazabilidad en logs/front.
        scores["modelo_version"] = _marcar_override(scores.get("modelo_version"))
        return nuevo

    # Regla 2: divergencia detectada entre cache XGBoost y dimensiones manuales.
    if _usa_score_base_original(scores) and tiene_dims:
        raw = scores.get("score_global")
        try:
            global_cache = float(raw) if raw is not None else None
        except (TypeError, ValueError):
            global_cache = None
        global_recalc = _recalcular_global(scores, pesos)
        if global_cache is not None and abs(global_cache - global_recalc) > _UMBRAL_DIVERGENCIA_PTS:
            logger.warning(
                "score_global divergente zona=%s cache=%.1f recalc=%.1f delta=%.1f → override",
                scores.get("zona_id"),
                global_cache,
                global_recalc,
                global_cache - global_recalc,
            )
            scores["modelo_version"] = _marcar_override(scores.get("modelo_version"))
            return global_recalc

    # Regla 3: cache coherente, lo usamos.
    if _usa_score_base_original(scores):
        raw = scores.get("score_global")
        try:
            return round(float(raw), 1)
        except (TypeError, ValueError):
            return 50.0
    return _recalcular_global(scores, pesos)


def _marcar_override(modelo_version) -> str:
    """Añade sufijo `+manual_override` a la versión si no lo tiene ya."""
    v = str(modelo_version or "")
    if not v:
        return "manual_override"
    if v.endswith("+manual_override"):
        return v
    return f"{v}+manual_override"


def _aplicar_contexto_score(
    scores: dict,
    pesos: dict,
    zona_ideal: Optional[dict],
    zona_data: Optional[dict],
) -> dict:
    base_score = _calcular_score_base(scores, pesos)
    scores["score_global"] = base_score

    if zona_ideal:
        score_afinidad = _score_zona_vs_ideal(zona_data or {}, zona_ideal)
        scores["score_afinidad_concepto"] = score_afinidad
        peso_afinidad = _calcular_peso_afinidad(zona_ideal)
        scores["score_global"] = round(
            base_score * (1.0 - peso_afinidad) + score_afinidad * peso_afinidad,
            1,
        )

    return scores


def _build_fallback_row(zona_id: str, modelo_version: str = "fallback_error") -> dict:
    return {
        "zona_id": zona_id,
        "score_global": 50.0,
        "score_flujo_peatonal": 50.0,
        "score_demografia": 50.0,
        "score_competencia": 50.0,
        "score_precio_alquiler": 50.0,
        "score_transporte": 50.0,
        "score_seguridad": 50.0,
        "score_turismo": 50.0,
        "score_dinamismo": 50.0,
        "probabilidad_supervivencia": 0.50,
        "shap_values": {},
        "modelo_version": modelo_version,
    }


def _score_zona_vs_ideal(zona_data: dict, zona_ideal: dict) -> float:
    """
    Intenta reutilizar la funcion pura compartida del matcher.

    Si el modulo semantico no puede cargarse, usa una implementacion local
    equivalente para no perder la afinidad derivada desde perfil_negocio.
    """
    try:
        from scoring.concepto.matcher import score_zona_vs_ideal as _shared_score

        return _shared_score(zona_data, zona_ideal)
    except Exception:
        return _score_zona_vs_ideal_local(zona_data, zona_ideal)


def _score_zona_vs_ideal_local(zona_data: dict, zona_ideal: dict) -> float:
    if not zona_ideal:
        return 50.0

    partial_scores: list[float] = []

    if "renta_ideal" in zona_ideal:
        renta_raw = zona_data.get("renta_media_hogar")
        renta = 32000.0 if renta_raw is None else float(renta_raw)
        renta_norm = max(0.0, min(1.0, (renta - 17000.0) / 43000.0))
        diff = abs(renta_norm - float(zona_ideal["renta_ideal"]))
        partial_scores.append(max(0.0, 100.0 - diff * 150.0))

    if "turismo_ideal" in zona_ideal:
        turismo_raw = zona_data.get("score_turismo")
        turismo = 45.0 if turismo_raw is None else float(turismo_raw)
        diff = abs(turismo - float(zona_ideal["turismo_ideal"]))
        partial_scores.append(max(0.0, 100.0 - diff * 1.5))

    if "flujo_min" in zona_ideal:
        flujo_raw = zona_data.get("flujo_peatonal_total")
        flujo = 0.0 if flujo_raw is None else float(flujo_raw)
        flujo_min = max(float(zona_ideal["flujo_min"]), 1.0)
        partial_scores.append(min(100.0, (flujo / flujo_min) * 100.0))

    if "edad_rango" in zona_ideal:
        edad_raw = zona_data.get("edad_media")
        edad = 42.5 if edad_raw is None else float(edad_raw)
        e_min, e_max = zona_ideal["edad_rango"]
        e_min = float(e_min)
        e_max = float(e_max)
        if e_min <= edad <= e_max:
            partial_scores.append(100.0)
        else:
            dist = min(abs(edad - e_min), abs(edad - e_max))
            partial_scores.append(max(0.0, 100.0 - dist * 5.0))

    if "ratio_comercial_min" in zona_ideal:
        ratio_raw = zona_data.get("ratio_locales_comerciales")
        ratio = 0.22 if ratio_raw is None else float(ratio_raw)
        min_req = max(float(zona_ideal["ratio_comercial_min"]), 1e-6)
        if ratio >= min_req:
            partial_scores.append(100.0)
        else:
            partial_scores.append(max(0.0, min(100.0, (ratio / min_req) * 100.0)))

    bonus_weight = float(zona_ideal.get("zonas_verdes_bonus", 0.0) or 0.0)
    if bonus_weight > 0.0:
        m2_verdes_raw = zona_data.get("m2_zonas_verdes_cercanas")
        m2_verdes = 1200.0 if m2_verdes_raw is None else float(m2_verdes_raw)
        score_v = min(100.0, max(0.0, (m2_verdes / 4000.0) * 100.0))
        partial_scores.append(score_v * bonus_weight + 50.0 * (1.0 - bonus_weight))

    if "seguridad_min" in zona_ideal:
        incidencias_raw = zona_data.get("incidencias_por_1000hab")
        incidencias = 35.0 if incidencias_raw is None else float(incidencias_raw)
        seg_score = min(100.0, max(0.0, (120.0 - incidencias) / 1.15))
        seg_min = max(float(zona_ideal["seguridad_min"]), 1e-6)
        if seg_score >= seg_min:
            partial_scores.append(100.0)
        else:
            partial_scores.append(max(0.0, min(100.0, (seg_score / seg_min) * 100.0)))

    if not partial_scores:
        return 50.0

    return round(sum(partial_scores) / len(partial_scores), 1)


def _recalcular_global(scores: dict, pesos: dict) -> float:
    """
    Recalcula score_global aplicando los pesos del sector sobre las 8 dimensiones.

    Solo los valores None usan fallback neutro. Un 0.0 real se preserva.
    """
    dims = {
        "score_flujo_peatonal":    pesos.get("peso_flujo",        0.25),
        "score_demografia":        pesos.get("peso_demo",         0.25),
        "score_competencia":       pesos.get("peso_competencia",  0.15),
        "score_transporte":        pesos.get("peso_transporte",   0.15),
        "score_dinamismo":         pesos.get("peso_dinamismo",    0.10),
        "score_seguridad":         pesos.get("peso_seguridad",    0.05),
        "score_turismo":           pesos.get("peso_turismo",      0.05),
    }
    total = sum(_valor_score(scores, dim) * peso for dim, peso in dims.items())
    return round(total, 1)


def _recalcular_global_con_afinidad(
    scores: dict,
    pesos: dict,
    score_afinidad: float,
    zona_ideal: Optional[dict] = None,
) -> float:
    """
    Recalcula score_global incorporando score_afinidad_concepto como 9a dimension.

    Si el registro no tiene dimensiones manuales completas, parte de su
    score_global original; si las tiene, recalcula desde las 8 dimensiones.
    """
    score_base = _calcular_score_base(scores, pesos)
    peso_afinidad = _calcular_peso_afinidad(zona_ideal)
    score_final = score_base * (1.0 - peso_afinidad) + score_afinidad * peso_afinidad
    return round(score_final, 1)


def _calcular_peso_afinidad(zona_ideal: Optional[dict]) -> float:
    """
    Ajusta cuánto pesa el encaje de concepto.

    Para negocios muy de barrio o poco dependientes del turismo elevamos el peso
    de afinidad para que el centro no gane por inercia estructural. Para
    conceptos muy turísticos o de alto flujo lo dejamos cerca del baseline.
    """
    if not zona_ideal:
        return _PESO_AFINIDAD

    peso = _PESO_AFINIDAD
    turismo_ideal = float(zona_ideal.get("turismo_ideal", 30.0) or 30.0)
    flujo_min = float(zona_ideal.get("flujo_min", 500.0) or 500.0)
    ratio_min = float(zona_ideal.get("ratio_comercial_min", 0.22) or 0.22)

    if turismo_ideal <= 20.0:
        peso += 0.08
    elif turismo_ideal <= 32.0:
        peso += 0.04

    if flujo_min <= 350.0:
        peso += 0.04
    elif flujo_min <= 550.0:
        peso += 0.02

    if ratio_min <= 0.18:
        peso += 0.02

    if turismo_ideal >= 45.0 or flujo_min >= 900.0:
        peso -= 0.03

    return max(0.10, min(0.26, round(peso, 3)))


def _format_scores_for_api(raw: dict) -> dict:
    """
    Transforma el dict plano (columnas de scores_zona o resultado del scorer)
    al formato anidado que espera api/local.py.
    """
    shap_raw = raw.get("shap_values") or {}
    if isinstance(shap_raw, str):
        try:
            shap_raw = json.loads(shap_raw)
        except Exception:
            shap_raw = {}

    explicaciones = [
        {"feature": k, "valor": round(float(v), 3)}
        for k, v in sorted(shap_raw.items(), key=lambda x: abs(x[1]), reverse=True)[:10]
    ] if shap_raw else []
    impacto_modelo = group_shap_by_dimension(shap_raw) if shap_raw else {}

    prob = raw.get("probabilidad_supervivencia_3a")
    if prob is None:
        prob = raw.get("probabilidad_supervivencia")

    scores_dim = {
        "flujo_peatonal": raw.get("score_flujo_peatonal"),
        "demografia": raw.get("score_demografia"),
        "competencia": raw.get("score_competencia"),
        "precio_alquiler": raw.get("score_precio_alquiler"),
        "transporte": raw.get("score_transporte"),
        "seguridad": raw.get("score_seguridad"),
        "turismo": raw.get("score_turismo"),
        "dinamismo": raw.get("score_dinamismo"),
    }

    # Si a estas alturas seguridad/dinamismo siguen a None, es porque ni el
    # cache ni la recomputación en get_scores_zona encontraron datos crudos.
    # Último fallback neutro 50.0 — expectativa: raro en producción porque
    # calcular_dinamismo y calcular_score_seguridad tienen sus propios defaults.
    for _dim_bootstrap in ("seguridad", "dinamismo"):
        if scores_dim.get(_dim_bootstrap) is None:
            scores_dim[_dim_bootstrap] = 50.0

    afinidad = raw.get("score_afinidad_concepto")
    if afinidad is not None:
        scores_dim["afinidad_concepto"] = round(float(afinidad), 1)

    score_global = raw.get("score_global")
    if score_global is None:
        score_global = 50.0

    pesos_modulados = raw.get("pesos_modulados") or None

    return {
        "score_global": round(float(score_global), 1),
        "probabilidad_supervivencia_3a": round(float(prob), 3) if prob is not None else None,
        "scores_dimension": scores_dim,
        "explicaciones_shap": explicaciones,
        "impacto_modelo_por_dimension": impacto_modelo,
        "shap_values": shap_raw or {},
        "modelo_version": raw.get("modelo_version"),
        "pesos_modulados": pesos_modulados,
    }
