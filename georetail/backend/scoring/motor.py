"""
scoring/motor.py — Interfaz pública del módulo de scoring.

Los endpoints de la API importan desde aquí. Este módulo delega en scorer.py.

Funciones públicas:
  - calcular_scores_batch(zona_ids, sector_codigo) → usado por POST /api/buscar
  - get_scores_zona(zona_id, sector_codigo)        → usado por POST /api/local

Estrategia de caché:
  - Se incluyen scores de cualquier versión (seed_v1 incluido).
  - El score_global SIEMPRE se recalcula aplicando los pesos del sector sobre
    las dimensiones almacenadas. Así el seed aporta diferenciación por zona
    y los pesos del sector producen rankings distintos por tipo de negocio.
  - Cuando llegan datos reales (model_version='manual_v1') el recálculo usa
    esos datos en lugar del seed, sin cambiar ninguna lógica.

Especificidad de concepto:
  - idea_tags (del LLM) + ConceptoMatcher (embeddings semánticos) se combinan
    para ajustar los pesos de scoring más allá del sector genérico.
  - En el detalle de zona (get_scores_zona) se añade score_afinidad_concepto
    como 9ª dimensión: mide cuánto encaja la zona con el concepto específico
    (ej: dog-friendly café necesita parques cercanos, no zona turística).
"""
from __future__ import annotations

import json
import logging
from typing import Optional

from scoring.scorer import calcular_scores_batch as _scorer_batch
from scoring.idea_tags import aplicar_idea_tags, extraer_tags_de_descripcion

logger = logging.getLogger(__name__)

# Peso de score_afinidad_concepto en score_global del detalle (api/local).
# El resto de dimensiones se reduce proporcionalmente.
_PESO_AFINIDAD = 0.12


async def calcular_scores_batch(
    zona_ids: list[str],
    sector_codigo: str,
    m2: Optional[float] = None,
    idea_tags: Optional[list[str]] = None,
    descripcion_negocio: Optional[str] = None,
) -> list[dict]:
    """
    Calcula el score de viabilidad para una lista de zonas.

    Devuelve lista de dicts con zona_id + score_global + scores por dimensión,
    ordenada por zona_id (el orden final lo hace api/buscar.py por score).

    Especificidad de concepto:
      1. idea_tags del LLM (señal primaria — más precisa)
      2. Si hay descripcion_negocio, ConceptoMatcher encuentra conceptos
         similares en la DB y enriquece los tags con señales adicionales
      3. La unión se aplica sobre los pesos del sector vía aplicar_idea_tags
    """
    if not zona_ids:
        return []

    try:
        from db.conexion import get_db

        # Pesos base del sector
        pesos = await _get_pesos_sector(sector_codigo)

        # ── Tags efectivos: LLM + matcher semántico ────────────────────────────
        tags_efectivos = _combinar_tags(idea_tags, descripcion_negocio)
        if tags_efectivos:
            pesos = aplicar_idea_tags(pesos, tags_efectivos)
            logger.debug("idea_tags efectivos (batch): %s", tags_efectivos)

        # ── 1. Caché (incluye seed_v1) ─────────────────────────────────────────
        # Tomamos el registro más reciente por zona, prefiriendo datos reales.
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
                       sz.score_entorno_comercial,
                       sz.probabilidad_supervivencia,
                       sz.shap_values,
                       sz.modelo_version
                FROM scores_zona sz
                JOIN sectores s ON s.id = sz.sector_id
                WHERE sz.zona_id = ANY($1)
                  AND s.codigo   = $2
                ORDER BY sz.zona_id,
                         (sz.modelo_version != 'seed_v1') DESC,
                         sz.fecha_calculo DESC
                """,
                zona_ids, sector_codigo,
            )

        cached = {}
        for r in rows:
            d = dict(r)
            d["score_global"] = _recalcular_global(d, pesos)
            cached[d["zona_id"]] = d

        if len(cached) == len(zona_ids):
            return list(cached.values())

        # ── 2. Zonas sin caché → calcular desde cero ──────────────────────────
        sin_cache = [z for z in zona_ids if z not in cached]
        resultados = await _scorer_batch(sin_cache, sector_codigo)
        calculados = [{"zona_id": k, **v} for k, v in resultados.items()]

        return list(cached.values()) + calculados

    except Exception as exc:
        logger.error("calcular_scores_batch error: %s", exc, exc_info=True)
        return [
            {
                "zona_id":                     z,
                "score_global":                50.0,
                "score_flujo_peatonal":        50.0,
                "score_demografia":            50.0,
                "score_competencia":           50.0,
                "score_precio_alquiler":       50.0,
                "score_transporte":            50.0,
                "score_seguridad":             50.0,
                "score_turismo":               50.0,
                "score_entorno_comercial":     50.0,
                "probabilidad_supervivencia":  0.50,
                "shap_values":                 {},
                "modelo_version":              "fallback_error",
            }
            for z in zona_ids
        ]


async def get_scores_zona(
    zona_id: str,
    sector_codigo: str,
    idea_tags: Optional[list[str]] = None,
    descripcion_negocio: Optional[str] = None,
) -> dict:
    """
    Devuelve el score detallado de una zona en el formato que espera api/local.py.

    Además de las 8 dimensiones estándar, calcula score_afinidad_concepto (9ª
    dimensión): cuánto encaja esta zona concreta con el concepto específico del
    negocio (no con el sector genérico). Esta señal es valiosa en el detalle
    pero demasiado costosa para el batch de búsqueda.
    """
    from db.conexion import get_db

    pesos = await _get_pesos_sector(sector_codigo)

    # Tags efectivos: LLM + matcher semántico
    tags_efectivos = _combinar_tags(idea_tags, descripcion_negocio)
    if tags_efectivos:
        pesos = aplicar_idea_tags(pesos, tags_efectivos)
        logger.debug("idea_tags efectivos (detalle zona=%s): %s", zona_id, tags_efectivos)

    async with get_db() as conn:
        row = await conn.fetchrow(
            """
            SELECT sz.*, s.codigo AS sector_codigo
            FROM scores_zona sz
            JOIN sectores s ON s.id = sz.sector_id
            WHERE sz.zona_id = $1
              AND s.codigo   = $2
            ORDER BY (sz.modelo_version != 'seed_v1') DESC,
                     sz.fecha_calculo DESC
            LIMIT 1
            """,
            zona_id, sector_codigo,
        )

    raw = dict(row) if row else None

    if raw is None:
        resultados = await calcular_scores_batch([zona_id], sector_codigo,
                                                 idea_tags=tags_efectivos)
        raw = resultados[0] if resultados else {"zona_id": zona_id, "score_global": 50.0}

    if raw:
        raw["score_global"] = _recalcular_global(raw, pesos)

    # ── Score de afinidad zona-concepto (9ª dimensión) ─────────────────────────
    # Se calcula con ConceptoMatcher usando los datos actuales de la zona.
    # Si el matcher no está disponible (sin sentence-transformers) se omite sin
    # romper el flujo.
    score_afinidad = await _calcular_score_afinidad(zona_id, descripcion_negocio,
                                                     tags_efectivos)
    if score_afinidad is not None:
        raw["score_afinidad_concepto"] = score_afinidad
        # Recalcular score_global incorporando la 9ª dimensión
        raw["score_global"] = _recalcular_global_con_afinidad(
            raw, pesos, score_afinidad
        )

    return _format_scores_for_api(raw)


# ─── Helpers privados ──────────────────────────────────────────────────────────

def _combinar_tags(
    idea_tags_llm: Optional[list[str]],
    descripcion_negocio: Optional[str],
) -> list[str]:
    """
    Combina los tags del LLM con los tags inferidos por el ConceptoMatcher.

    Estrategia:
    - LLM tags son la fuente primaria (alta precisión).
    - ConceptoMatcher enriquece con tags adicionales de los conceptos más
      similares, siempre que su peso ponderado supere el umbral (ver blend_tags).
    - La unión evita duplicados y respeta el máximo de tags razonables (~12).
    """
    base_tags = list(idea_tags_llm or [])

    if not descripcion_negocio:
        # Sin descripción, usar fallback de keywords si tampoco hay tags LLM
        if not base_tags:
            base_tags = extraer_tags_de_descripcion(None)
        return base_tags

    try:
        from scoring.concepto_matcher import get_matcher
        matcher = get_matcher()
        matches = matcher.match(descripcion_negocio, top_k=4)
        matcher_tags = matcher.blend_tags(matches)

        # Unión: LLM tags tienen prioridad; matcher añade los que falten
        combined = list(base_tags)
        for t in matcher_tags:
            if t not in combined:
                combined.append(t)

        return combined

    except Exception as e:
        logger.debug("ConceptoMatcher no disponible: %s — usando solo LLM tags", e)
        return base_tags or extraer_tags_de_descripcion(descripcion_negocio)


async def _calcular_score_afinidad(
    zona_id: str,
    descripcion_negocio: Optional[str],
    tags_efectivos: list[str],
) -> Optional[float]:
    """
    Computa score_afinidad_concepto (0-100) para una zona específica.

    Requiere:
    1. La descripción del negocio para el matching semántico.
    2. Los datos actuales de la zona (variables_zona).

    Devuelve None si no hay descripción o si el matcher falla.
    """
    if not descripcion_negocio:
        return None

    try:
        from scoring.concepto_matcher import get_matcher
        from db.conexion import get_db

        matcher = get_matcher()
        matches = matcher.match(descripcion_negocio, top_k=4)
        if not matches:
            return None

        # Datos de la zona necesarios para la afinidad
        async with get_db() as conn:
            row = await conn.fetchrow(
                """
                SELECT
                    vz.renta_media_hogar,
                    vz.edad_media,
                    vz.flujo_peatonal_total,
                    vz.score_turismo,
                    vz.ratio_locales_comerciales,
                    vz.m2_zonas_verdes_cercanas,
                    vz.incidencias_por_1000hab
                FROM variables_zona vz
                WHERE vz.zona_id = $1
                ORDER BY vz.fecha DESC
                LIMIT 1
                """,
                zona_id,
            )

        zona_data = dict(row) if row else {}
        return matcher.score_afinidad_zona(zona_data, matches)

    except Exception as e:
        logger.debug("score_afinidad_concepto no disponible para zona=%s: %s", zona_id, e)
        return None


async def _get_pesos_sector(sector_codigo: str) -> dict:
    """Lee los pesos del sector desde la BD."""
    from db.conexion import get_db
    async with get_db() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM sectores WHERE codigo = $1", sector_codigo
        )
    return dict(row) if row else {}


def _recalcular_global(scores: dict, pesos: dict) -> float:
    """
    Recalcula score_global aplicando los pesos del sector sobre las 8 dimensiones.

    - Zonas distintas puntúan distinto (las dimensiones varían por zona).
    - Sectores/conceptos distintos producen rankings distintos (los pesos varían).
    """
    dims = {
        "score_flujo_peatonal":    pesos.get("peso_flujo",        0.25),
        "score_demografia":        pesos.get("peso_demo",         0.20),
        "score_competencia":       pesos.get("peso_competencia",  0.15),
        "score_precio_alquiler":   pesos.get("peso_precio",       0.15),
        "score_transporte":        pesos.get("peso_transporte",   0.10),
        "score_seguridad":         pesos.get("peso_seguridad",    0.05),
        "score_turismo":           pesos.get("peso_turismo",      0.05),
        "score_entorno_comercial": pesos.get("peso_entorno",      0.05),
    }
    total = sum(
        (scores.get(dim) or 50.0) * peso
        for dim, peso in dims.items()
    )
    return round(total, 1)


def _recalcular_global_con_afinidad(
    scores: dict, pesos: dict, score_afinidad: float
) -> float:
    """
    Recalcula score_global incorporando score_afinidad_concepto como 9ª dimensión.

    Las 8 dimensiones originales contribuyen con (1 - _PESO_AFINIDAD) del total,
    y la afinidad aporta _PESO_AFINIDAD. Esto preserva la diferenciación interna
    de las 8 dimensiones mientras añade la señal de concepto.
    """
    score_8dims = _recalcular_global(scores, pesos)
    score_final = score_8dims * (1.0 - _PESO_AFINIDAD) + score_afinidad * _PESO_AFINIDAD
    return round(score_final, 1)


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

    prob = raw.get("probabilidad_supervivencia_3a") or raw.get("probabilidad_supervivencia")

    scores_dim = {
        "flujo_peatonal":    raw.get("score_flujo_peatonal"),
        "demografia":        raw.get("score_demografia"),
        "competencia":       raw.get("score_competencia"),
        "precio_alquiler":   raw.get("score_precio_alquiler"),
        "transporte":        raw.get("score_transporte"),
        "seguridad":         raw.get("score_seguridad"),
        "turismo":           raw.get("score_turismo"),
        "entorno_comercial": raw.get("score_entorno_comercial"),
    }

    # La 9ª dimensión solo aparece cuando se ha podido calcular
    afinidad = raw.get("score_afinidad_concepto")
    if afinidad is not None:
        scores_dim["afinidad_concepto"] = round(afinidad, 1)

    return {
        "score_global":                  raw.get("score_global", 50.0),
        "probabilidad_supervivencia_3a":  round(prob, 3) if prob is not None else None,
        "scores_dimension":               scores_dim,
        "explicaciones_shap":             explicaciones,
    }
