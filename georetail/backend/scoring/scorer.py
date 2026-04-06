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
_shap_explainer = None  # cacheado para no recrearlo en cada zona


async def cargar_modelo() -> None:
    """Carga el modelo XGBoost activo desde disco. Llamar en startup."""
    global _xgb_model, _xgb_version, _shap_explainer
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
        _shap_explainer = None  # resetear al cargar nuevo modelo
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


def _calcular_score_seguridad(datos: dict) -> float:
    """
    Fórmula compuesta de seguridad con 5 sub-scores (v7).

    | Sub-score          | Peso | Fuente                              |
    |--------------------|------|-------------------------------------|
    | Tasa criminal      | 35%  | incidencias_por_1000hab             |
    | Severidad delitos  | 20%  | hurtos, robatoris, danys ponderados |
    | Nocturnidad        | 15%  | incidencias_noche_pct               |
    | Proximidad policial| 15%  | comisarias_1km, dist_comisaria_m    |
    | Percepción IERMB   | 15%  | seguridad_barri_score (0-10)        |
    """
    # 1. Tasa criminal (35%) — inverso, rango BCN 5-120 por 1000hab
    incidencias = datos.get("incidencias_por_1000hab") or 35
    s_tasa = min(100.0, max(0.0, (120.0 - incidencias) / 1.15))

    # 2. Severidad (20%) — pondera tipos: robo×1.5 > daño×0.8 > hurto×0.5
    hurtos = datos.get("hurtos_por_1000hab") or 18.0
    robatoris = datos.get("robatoris_por_1000hab") or 8.0
    danys = datos.get("danys_por_1000hab") or 5.0
    severidad_raw = hurtos * 0.5 + robatoris * 1.5 + danys * 0.8
    # Rango esperado BCN: ~5 (Pedralbes) a ~90 (Raval). Normalizar inverso.
    s_severidad = min(100.0, max(0.0, (90.0 - severidad_raw) / 0.85))

    # 3. Nocturnidad (15%) — alto % nocturno = peor. Rango BCN: 0.15-0.55
    noche_pct = datos.get("incidencias_noche_pct") or 0.30
    s_noche = min(100.0, max(0.0, (0.55 - noche_pct) / 0.004))

    # 4. Proximidad policial (15%) — más comisarías + menor distancia = mejor
    comisarias = datos.get("comisarias_1km") or 2
    dist_com = datos.get("dist_comisaria_m") or 800
    # Sub-sub: comisarías (0-5 → 0-100) y distancia (0-3000m invertido)
    s_com_count = min(100.0, comisarias * 20.0)
    s_com_dist = min(100.0, max(0.0, (3000.0 - dist_com) / 30.0))
    s_policial = s_com_count * 0.5 + s_com_dist * 0.5

    # 5. Percepción IERMB (15%) — escala 0-10 → 0-100
    iermb = datos.get("seguridad_barri_score")
    if iermb is not None:
        s_iermb = min(100.0, max(0.0, float(iermb) * 10.0))
    else:
        s_iermb = 55.0  # fallback: valor medio-bajo BCN

    # Ponderación final
    score = (
        s_tasa      * 0.35 +
        s_severidad * 0.20 +
        s_noche     * 0.15 +
        s_policial  * 0.15 +
        s_iermb     * 0.15
    )
    return min(100.0, max(0.0, score))


def _calcular_score_entorno(datos: dict) -> float:
    """
    Fórmula compuesta de entorno comercial con 6 sub-scores (v8).

    | Sub-score          | Peso | Fuente                                    |
    |--------------------|------|-------------------------------------------|
    | Vitalidad comercial| 25%  | pct_locales_vacios, tasa_rotacion_anual   |
    | Dinamismo          | 20%  | licencias_nuevas_1a, ratio_locales_comerc.|
    | Confort acústico   | 15%  | nivel_ruido_db                            |
    | Equipamientos      | 15%  | score_equipamientos (0-100)               |
    | Zonas verdes       | 10%  | m2_zonas_verdes_cercanas                  |
    | Mercados y anclas  | 15%  | mercados_municipales_1km, eventos_cult.   |
    """
    # 1. Vitalidad comercial (25%) — menos vacíos + menos rotación = mejor
    _v = datos.get("pct_locales_vacios")
    vacios = _v if _v is not None else 0.15
    _r = datos.get("tasa_rotacion_anual")
    rotacion = _r if _r is not None else 0.18
    s_vitalidad = max(0.0, min(100.0, 100.0 - vacios * 200.0 - rotacion * 100.0))

    # 2. Dinamismo (20%) — licencias nuevas + densidad comercial
    licencias = datos.get("licencias_nuevas_1a") or 4.0
    ratio_com = datos.get("ratio_locales_comerciales") or 0.22
    s_licencias = min(100.0, licencias * 10.0)
    s_ratio = min(100.0, ratio_com * 250.0)
    s_dinamismo = s_licencias * 0.6 + s_ratio * 0.4

    # 3. Confort acústico (15%) — inverso. Rango BCN: 45-80 dB
    ruido = datos.get("nivel_ruido_db") or 63.0
    s_ruido = max(0.0, min(100.0, (80.0 - ruido) / 0.35))

    # 4. Equipamientos (15%) — ya normalizado 0-100
    equip = datos.get("score_equipamientos")
    s_equip = max(0.0, min(100.0, float(equip))) if equip is not None else 55.0

    # 5. Zonas verdes (10%) — Rango BCN: 0-8000 m²
    verdes = datos.get("m2_zonas_verdes_cercanas") or 1200.0
    s_verdes = min(100.0, max(0.0, verdes / 80.0))

    # 6. Mercados y anclas (15%) — mercados 1km + eventos culturales
    mercados = datos.get("mercados_municipales_1km") or 1
    eventos = datos.get("eventos_culturales_500m") or 3
    s_mercados = min(100.0, mercados * 30.0)
    s_eventos = min(100.0, eventos * 15.0)
    s_anclas = s_mercados * 0.6 + s_eventos * 0.4

    # Ponderación final
    score = (
        s_vitalidad * 0.25 +
        s_dinamismo * 0.20 +
        s_ruido     * 0.15 +
        s_equip     * 0.15 +
        s_verdes    * 0.10 +
        s_anclas    * 0.15
    )
    return min(100.0, max(0.0, score))


def _score_manual(datos: dict, sector: dict) -> dict:
    """
    Scoring con pesos manuales. Fórmula:
        score_dim = normalizar_0_100(variable)
        score_global = Σ(score_dim × peso_dim)

    Los pesos se leen de la tabla `sectores`.
    """
    # ── Calcular scores por dimensión ─────────────────────────────────────────
    # Cada dimensión normaliza su(s) variable(s) a 0-100

    # FLUJO PEATONAL — usa calcular_flujo_score (popular_times + vcity + ratio_locales)
    from scoring.flujo_peatonal import calcular_flujo_score
    s_flujo = calcular_flujo_score(
        popular_times_score=datos.get("flujo_popular_times_score"),
        vcity_flujo=datos.get("vcity_flujo_peatonal"),
        vianants_intensitat=datos.get("flujo_peatonal_total"),
        ratio_locales=datos.get("ratio_locales_comerciales"),
    )

    # DEMOGRAFÍA — renta media normalizada (17k-60k rango BCN)
    _renta_raw = datos.get("renta_media_hogar")
    renta = _renta_raw if _renta_raw is not None else 30000  # `or` sustituiría renta=0 legítima
    s_demo = min(100.0, max(0.0, (renta - 17000) / 430.0))

    # COMPETENCIA — usa score_competencia_v2 (ya 0-100, mayor=mejor)
    # Fallback a v1 (invertir score_saturacion) si no hay datos v2
    comp_v2 = datos.get("score_competencia_v2")
    if comp_v2 is not None:
        s_comp = max(0.0, min(100.0, float(comp_v2)))
    else:
        sat_raw = datos.get("score_saturacion")
        saturacion = 50 if sat_raw is None else sat_raw
        s_comp = max(0.0, min(100.0, 100.0 - saturacion))

    # PRECIO ALQUILER — inversamente proporcional al precio (más barato = mejor)
    # Rango BCN: 8-45 €/m². Score 100 = 8€/m², Score 0 = 45€/m²
    _precio_raw = datos.get("precio_m2")
    precio_m2 = _precio_raw if _precio_raw is not None else 20  # `or` trataría precio=0 como None
    s_precio = min(100.0, max(0.0, (45.0 - precio_m2) / 0.37))

    # TRANSPORTE — número de líneas a 500m (0-20 líneas rango BCN)
    lineas = datos.get("num_lineas_transporte") or 0
    s_trans = min(100.0, lineas * 5.0)

    # SEGURIDAD — fórmula compuesta multivariable (v7)
    s_seg = _calcular_score_seguridad(datos)

    # TURISMO — corregido por proximidad real al litoral BCN.
    # El campo score_turismo de variables_zona se agrega a nivel de barrio/distrito,
    # lo que hace que zonas junto al mar reciban valores bajos (25) cuando deberían
    # ser altos. dist_playa_m da granularidad real a nivel zona.
    turismo_base = float(datos.get("score_turismo") or 40.0)
    dist_playa = datos.get("dist_playa_m")
    if dist_playa is not None:
        if dist_playa < 300:
            turismo_base = max(turismo_base, 85.0)   # frente al mar
        elif dist_playa < 700:
            turismo_base = max(turismo_base, 70.0)   # muy cerca del litoral
        elif dist_playa < 1500:
            turismo_base = max(turismo_base, 55.0)   # zona marítima amplia
    s_turismo = min(100.0, max(0.0, turismo_base))

    # ENTORNO COMERCIAL — fórmula compuesta multivariable (v8)
    s_entorno = _calcular_score_entorno(datos)

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

    # Score global: prob de supervivencia → escala 0-100
    score_global = min(100.0, prob * 100.0)

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
            "score_global": round(min(100.0, prob*100.0), 1),
            "probabilidad_supervivencia": round(prob, 3),
            "shap_values": shap,
            "modelo_version": f"xgboost_{_xgb_version}",
        }
    return resultados


def _calcular_shap(X: np.ndarray) -> Optional[dict]:
    global _shap_explainer
    from scoring.features import FEATURE_NAMES
    try:
        import shap
        if _shap_explainer is None:
            _shap_explainer = shap.TreeExplainer(_xgb_model)
        vals = _shap_explainer.shap_values(X)
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
                vz.flujo_peatonal_total, vz.flujo_popular_times_score,
                vz.vcity_flujo_peatonal, vz.renta_media_hogar,
                vz.score_turismo, vz.pct_locales_vacios, vz.tasa_rotacion_anual,
                vz.incidencias_por_1000hab, vz.ratio_locales_comerciales,
                vz.hurtos_por_1000hab, vz.robatoris_por_1000hab,
                vz.danys_por_1000hab, vz.incidencias_noche_pct,
                vz.comisarias_1km, vz.dist_comisaria_m,
                vz.seguridad_barri_score,
                vz.nivel_ruido_db, vz.score_equipamientos,
                vz.m2_zonas_verdes_cercanas, vz.licencias_nuevas_1a,
                vz.mercados_municipales_1km, vz.eventos_culturales_500m,
                cp.score_saturacion,
                cdz.score_competencia_v2,
                paz.precio_m2,
                trans.cnt AS num_lineas_transporte,
                -- distancia al litoral BCN: granularidad real de nivel zona
                ST_Distance(
                    ST_Centroid(z.geometria)::geography,
                    ST_GeomFromText(
                        'LINESTRING(2.1850 41.3740,2.1940 41.3792,2.2030 41.3840,'
                        '2.2130 41.3900,2.2250 41.3970,2.2380 41.4020)', 4326
                    )::geography
                )::int AS dist_playa_m
            FROM v_variables_zona vz
            JOIN zonas z ON z.id = vz.zona_id
            LEFT JOIN competencia_por_local cp ON cp.zona_id=vz.zona_id
                AND cp.sector_codigo=$2 AND cp.radio_m=300
            LEFT JOIN LATERAL (
                SELECT cdz.score_competencia_v2
                FROM competencia_detalle_zona cdz
                WHERE cdz.zona_id = vz.zona_id
                  AND cdz.sector_codigo = $2
                  AND cdz.radio_m = 500
                ORDER BY cdz.fecha DESC LIMIT 1
            ) cdz ON TRUE
            LEFT JOIN LATERAL (
                SELECT precio_m2 FROM precios_alquiler_zona
                WHERE zona_id=vz.zona_id ORDER BY fecha DESC LIMIT 1
            ) paz ON TRUE
            LEFT JOIN LATERAL (
                SELECT COUNT(DISTINCT pl2.linea_id)::int AS cnt
                FROM paradas_transporte pt
                JOIN paradas_lineas pl2 ON pl2.parada_id=pt.id
                WHERE ST_DWithin(pt.geometria::geography, z.geometria::geography, 500)
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
