"""
scoring/scorer.py — Motor de scoring para zonas comerciales.

MVP: pesos manuales por sector (tabla `sectores`).
Post-MVP: XGBoost entrenado sobre `negocios_historico` + SHAP.

El scorer produce:
  - score_global (0-100)
  - scores por dimensión (flujo, demografía, competencia, etc.)
  - probabilidad_supervivencia (XGBoost) o None (pesos manuales)
  - shap_values (XGBoost) o None
"""
from __future__ import annotations
import json, logging, os
from typing import Optional
import numpy as np
from db.conexion import get_db

logger = logging.getLogger(__name__)

# XGBoost model — cargado en startup desde disco
_xgb_model = None
_xgb_version: Optional[str] = None


async def cargar_modelo() -> None:
    """Carga el modelo XGBoost activo desde disco. Llamar en startup."""
    global _xgb_model, _xgb_version
    try:
        async with get_db() as conn:
            row = await conn.fetchrow(
                "SELECT version FROM modelos_versiones WHERE activo=TRUE LIMIT 1")
        if not row:
            logger.info("Sin modelo XGBoost activo — usando pesos manuales")
            return

        version = row["version"]
        ruta = os.path.join(os.environ.get("MODELS_DIR","/data/models"), f"xgboost_{version}.json")
        if not os.path.exists(ruta):
            logger.warning("Modelo XGBoost %s no encontrado en disco: %s", version, ruta)
            return

        import xgboost as xgb
        _xgb_model = xgb.XGBClassifier()
        _xgb_model.load_model(ruta)
        _xgb_version = version
        logger.info("Modelo XGBoost %s cargado OK", version)
    except Exception as e:
        logger.error("Error cargando modelo XGBoost: %s", e)


async def calcular_score(zona_id: str, sector: str,
                          datos_zona: dict, datos_sector: dict) -> dict:
    """
    Calcula el score de una zona para un sector.

    Intenta usar XGBoost si hay modelo activo.
    Fallback: pesos manuales de la tabla `sectores`.

    Args:
        zona_id: ID de la zona
        sector: código de sector (ej: 'restauracion')
        datos_zona: dict con variables de la zona (de variables_zona + competencia + transporte)
        datos_sector: dict con pesos de la tabla `sectores`

    Returns:
        dict con score_global, scores_dimensiones, probabilidad_supervivencia, shap_values
    """
    if _xgb_model is not None:
        return await _score_xgboost(zona_id, sector, datos_zona)
    return _score_manual(datos_zona, datos_sector)


async def calcular_scores_batch(zona_ids: list[str], sector: str) -> dict[str, dict]:
    """
    Calcula scores para múltiples zonas en batch.
    Usa queries batch para eficiencia.
    """
    from scoring.features import construir_features_batch

    datos_sector = await _get_datos_sector(sector)

    if _xgb_model is not None:
        try:
            X, ids = await construir_features_batch(zona_ids, sector)
            return await _scores_xgboost_batch(X, ids, sector)
        except Exception as e:
            logger.error("Error XGBoost batch: %s — fallback a pesos manuales", e)

    # Fallback manual: queries individuales
    resultados = {}
    for zona_id in zona_ids:
        try:
            datos = await _get_datos_zona_completos(zona_id, sector)
            resultados[zona_id] = _score_manual(datos, datos_sector)
        except Exception as e:
            logger.warning("Score manual fallido zona=%s: %s", zona_id, e)
            resultados[zona_id] = _score_neutro()
    return resultados


def _score_manual(datos: dict, sector: dict) -> dict:
    """
    Scoring con pesos manuales. Fórmula:
        score_dim = normalizar_0_100(variable)
        score_global = Σ(score_dim × peso_dim)

    Los pesos se leen de la tabla `sectores`.
    """
    # ── Calcular scores por dimensión ─────────────────────────────────────────
    # Cada dimensión normaliza su(s) variable(s) a 0-100

    # FLUJO PEATONAL — normalizado sobre rango típico BCN (0–3000 pax/h)
    flujo = datos.get("flujo_peatonal_total") or 0
    s_flujo = min(100.0, flujo / 30.0)  # 3000 pax/h = score 100

    # DEMOGRAFÍA — renta media normalizada (17k-60k rango BCN)
    renta = datos.get("renta_media_hogar") or 30000
    s_demo = min(100.0, max(0.0, (renta - 17000) / 430.0))

    # COMPETENCIA — score_saturacion invertido (menos saturación = mejor)
    # Nota: se usa `is None` para respetar saturacion=0 (caso sin competencia)
    sat_raw = datos.get("score_saturacion")
    saturacion = 50 if sat_raw is None else sat_raw
    s_comp = max(0.0, min(100.0, 100.0 - saturacion))

    # PRECIO ALQUILER — inversamente proporcional al precio (más barato = mejor)
    # Rango BCN: 8-45 €/m². Score 100 = 8€/m², Score 0 = 45€/m²
    precio_m2 = datos.get("precio_m2") or 20
    s_precio = min(100.0, max(0.0, (45.0 - precio_m2) / 0.37))

    # TRANSPORTE — número de líneas a 500m (0-20 líneas rango BCN)
    lineas = datos.get("num_lineas_transporte") or 0
    s_trans = min(100.0, lineas * 5.0)

    # SEGURIDAD — incidencias invertidas (menos = mejor). Rango BCN: 5-120/1000hab
    incidencias = datos.get("incidencias_por_1000hab") or 35
    s_seg = min(100.0, max(0.0, (120.0 - incidencias) / 1.15))

    # TURISMO
    s_turismo = min(100.0, max(0.0, float(datos.get("score_turismo") or 40.0)))

    # ENTORNO COMERCIAL — % locales vacíos invertido + tasa de rotación
    vacios = datos.get("pct_locales_vacios") or 0.15
    rotacion = datos.get("tasa_rotacion_anual") or 0.18
    s_entorno = max(0.0, 100.0 - vacios * 200.0 - rotacion * 100.0)

    # ── Score global ponderado ────────────────────────────────────────────────
    dims = {
        "flujo":      (s_flujo,   sector.get("peso_flujo",0.25)),
        "demografia": (s_demo,    sector.get("peso_demo",0.20)),
        "competencia":(s_comp,    sector.get("peso_competencia",0.15)),
        "precio":     (s_precio,  sector.get("peso_precio",0.15)),
        "transporte": (s_trans,   sector.get("peso_transporte",0.10)),
        "seguridad":  (s_seg,     sector.get("peso_seguridad",0.05)),
        "turismo":    (s_turismo, sector.get("peso_turismo",0.05)),
        "entorno":    (s_entorno, sector.get("peso_entorno",0.05)),
    }

    score_global = sum(v * w for v, w in dims.values())

    return {
        "score_global":             round(score_global, 1),
        "score_flujo_peatonal":     round(s_flujo, 1),
        "score_demografia":         round(s_demo, 1),
        "score_competencia":        round(s_comp, 1),
        "score_precio_alquiler":    round(s_precio, 1),
        "score_transporte":         round(s_trans, 1),
        "score_seguridad":          round(s_seg, 1),
        "score_turismo":            round(s_turismo, 1),
        "score_entorno_comercial":  round(s_entorno, 1),
        "probabilidad_supervivencia": None,
        "shap_values":              None,
        "modelo_version":           "manual_v1",
    }


async def _score_xgboost(zona_id: str, sector: str, datos: dict) -> dict:
    from scoring.features import construir_features
    X = await construir_features(zona_id, sector)
    prob = float(_xgb_model.predict_proba(X)[0][1])

    # SHAP values
    shap = _calcular_shap(X)

    # Score global: prob de supervivencia → escala 0-100 con ajuste
    score_global = min(100.0, prob * 120.0)  # prob=1 → score≈100

    return {
        "score_global":             round(score_global, 1),
        "score_flujo_peatonal":     None,
        "score_demografia":         None,
        "score_competencia":        None,
        "score_precio_alquiler":    None,
        "score_transporte":         None,
        "score_seguridad":          None,
        "score_turismo":            None,
        "score_entorno_comercial":  None,
        "probabilidad_supervivencia": round(prob, 3),
        "shap_values":              shap,
        "modelo_version":           f"xgboost_{_xgb_version}",
    }


async def _scores_xgboost_batch(X: np.ndarray, ids: list[str], sector: str) -> dict[str, dict]:
    from scoring.features import FEATURE_NAMES
    probs = _xgb_model.predict_proba(X)[:,1]
    resultados = {}
    for i, zona_id in enumerate(ids):
        prob = float(probs[i])
        shap = _calcular_shap(X[i:i+1])
        resultados[zona_id] = {
            "score_global": round(min(100.0, prob*120.0), 1),
            "probabilidad_supervivencia": round(prob, 3),
            "shap_values": shap,
            "modelo_version": f"xgboost_{_xgb_version}",
        }
    return resultados


def _calcular_shap(X: np.ndarray) -> Optional[dict]:
    from scoring.features import FEATURE_NAMES
    try:
        import shap
        explainer = shap.TreeExplainer(_xgb_model)
        vals = explainer.shap_values(X)
        if isinstance(vals, list): vals = vals[1]
        return {FEATURE_NAMES[i]: round(float(vals[0][i]),3) for i in range(len(FEATURE_NAMES))}
    except Exception as e:
        logger.warning("SHAP error: %s", e)
        return None


def _score_neutro() -> dict:
    return {
        "score_global":               50.0,
        "score_flujo_peatonal":       50.0,
        "score_demografia":           50.0,
        "score_competencia":          50.0,
        "score_precio_alquiler":      50.0,
        "score_transporte":           50.0,
        "score_seguridad":            50.0,
        "score_turismo":              50.0,
        "score_entorno_comercial":    50.0,
        "probabilidad_supervivencia": None,
        "shap_values":                None,
        "modelo_version":             "fallback",
    }


async def _get_datos_sector(sector: str) -> dict:
    async with get_db() as conn:
        row = await conn.fetchrow("SELECT * FROM sectores WHERE codigo=$1", sector)
    return dict(row) if row else {}


async def _get_datos_zona_completos(zona_id: str, sector: str) -> dict:
    async with get_db() as conn:
        row = await conn.fetchrow("""
            SELECT
                vz.flujo_peatonal_total, vz.renta_media_hogar,
                vz.score_turismo, vz.pct_locales_vacios, vz.tasa_rotacion_anual,
                vz.incidencias_por_1000hab,
                cp.score_saturacion,
                paz.precio_m2,
                trans.cnt AS num_lineas_transporte
            FROM variables_zona vz
            LEFT JOIN competencia_por_local cp ON cp.zona_id=vz.zona_id
                AND cp.sector_codigo=$2 AND cp.radio_m=300
            LEFT JOIN LATERAL (
                SELECT precio_m2 FROM precios_alquiler_zona
                WHERE zona_id=vz.zona_id ORDER BY fecha DESC LIMIT 1
            ) paz ON TRUE
            LEFT JOIN LATERAL (
                SELECT COUNT(DISTINCT pl2.linea_id)::int AS cnt
                FROM paradas_transporte pt
                JOIN paradas_lineas pl2 ON pl2.parada_id=pt.id
                JOIN zonas z ON z.id=vz.zona_id
                WHERE ST_DWithin(pt.geometria::geography, z.geometria::geography,500)
            ) trans ON TRUE
            WHERE vz.zona_id=$1
            ORDER BY vz.fecha DESC LIMIT 1
        """, zona_id, sector)
    return dict(row) if row else {}


async def guardar_scores(zona_id: str, sector_id: int, scores: dict) -> None:
    """Persiste los scores calculados en `scores_zona`."""
    async with get_db() as conn:
        await conn.execute("""
            INSERT INTO scores_zona
                (zona_id, sector_id, modelo_version, score_global,
                 score_flujo_peatonal, score_demografia, score_competencia,
                 score_precio_alquiler, score_transporte, score_seguridad,
                 score_turismo, score_entorno_comercial,
                 probabilidad_supervivencia, shap_values)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
            ON CONFLICT (zona_id, sector_id, modelo_version)
            DO UPDATE SET
                score_global=EXCLUDED.score_global,
                score_flujo_peatonal=EXCLUDED.score_flujo_peatonal,
                score_demografia=EXCLUDED.score_demografia,
                score_competencia=EXCLUDED.score_competencia,
                score_precio_alquiler=EXCLUDED.score_precio_alquiler,
                score_transporte=EXCLUDED.score_transporte,
                score_seguridad=EXCLUDED.score_seguridad,
                score_turismo=EXCLUDED.score_turismo,
                score_entorno_comercial=EXCLUDED.score_entorno_comercial,
                probabilidad_supervivencia=EXCLUDED.probabilidad_supervivencia,
                shap_values=EXCLUDED.shap_values,
                fecha_calculo=NOW()
        """,
        zona_id, sector_id, scores.get("modelo_version","manual_v1"),
        scores.get("score_global"), scores.get("score_flujo_peatonal"),
        scores.get("score_demografia"), scores.get("score_competencia"),
        scores.get("score_precio_alquiler"), scores.get("score_transporte"),
        scores.get("score_seguridad"), scores.get("score_turismo"),
        scores.get("score_entorno_comercial"), scores.get("probabilidad_supervivencia"),
        json.dumps(scores.get("shap_values")) if scores.get("shap_values") else None)
