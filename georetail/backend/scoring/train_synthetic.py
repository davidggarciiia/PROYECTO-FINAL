"""
scoring/train_synthetic.py — Entrenamiento XGBoost con datos sintéticos (v3, 29 features).

Genera un dataset sintético de 2000 muestras con distribuciones realistas de Barcelona
para entrenar y validar el modelo XGBoost sin necesidad de base de datos.

Uso:
    cd georetail/backend && python -m scoring.train_synthetic

Salida:
    - Modelo guardado en: georetail/backend/models/xgboost_synthetic_v3.json
    - Métricas: AUC-ROC, PR-AUC, Brier score, confusion matrix
    - SHAP feature importance top 10
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from pathlib import Path

import numpy as np
import xgboost as xgb
from sklearn.metrics import (
    average_precision_score,
    brier_score_loss,
    confusion_matrix,
    roc_auc_score,
)
from sklearn.model_selection import StratifiedKFold, train_test_split

# Añadir el directorio padre al path para poder importar scoring.features
_BACKEND_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_BACKEND_DIR))

# Importar FEATURE_NAMES y _MEDIAS directamente sin depender de db.conexion
# (importación selectiva para evitar el import transitivo de asyncpg)
from scoring.features import FEATURE_NAMES, _MEDIAS  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ── Directorio de salida ────────────────────────────────────────────────────────
_MODELS_DIR = Path(__file__).resolve().parent.parent / "models"
_MODEL_PATH = _MODELS_DIR / "xgboost_synthetic_v3.json"

# ── Hiperparámetros ─────────────────────────────────────────────────────────────
_PARAMS = {
    "objective":        "binary:logistic",
    "eval_metric":      ["logloss", "auc"],
    "n_estimators":     400,
    "max_depth":        5,
    "learning_rate":    0.05,
    "subsample":        0.80,
    "colsample_bytree": 0.75,
    "min_child_weight": 8,
    "gamma":            0.15,
    "reg_alpha":        0.10,
    "reg_lambda":       1.50,
    "tree_method":      "hist",
    "random_state":     42,
    "n_jobs":           -1,
}

N_SAMPLES = 2000
POSITIVE_RATIO = 0.35   # ~35% de negocios sobreviven ≥ 3 años (típico retail BCN)
RANDOM_SEED = 42


# ── Generación de dataset sintético ────────────────────────────────────────────

def generar_dataset_sintetico(
    n_samples: int = N_SAMPLES,
    seed: int = RANDOM_SEED,
) -> tuple[np.ndarray, np.ndarray]:
    """
    Genera un dataset sintético de n_samples filas con 29 features y labels binarios.

    Distribuciones basadas en estadísticas reales de Barcelona (Open Data BCN,
    Inside Airbnb 2024, padró municipal). Las correlaciones son realistas:
    - Más flujo peatonal → más supervivencia
    - Más saturación     → menos supervivencia
    - Renta alta         → más supervivencia
    - Precio alto        → menos supervivencia
    - Turismo + Airbnb   → más supervivencia (en general)

    Returns:
        X: (n_samples, 29) float32
        y: (n_samples,)   int32 (0/1)
    """
    rng = np.random.default_rng(seed)
    n = n_samples

    # ── 1. Features base (ruidosas, distribución BCN) ────────────────────────

    # Flujo peatonal total (pax/h) — LogNormal típico BCN
    flujo_total = rng.lognormal(mean=6.5, sigma=0.7, size=n).clip(10, 5000)

    # Fracciones de flujo por franja (simplex Dirichlet)
    fracs = rng.dirichlet(alpha=[3.5, 4.5, 2.0], size=n)   # mañana, tarde, noche
    flujo_manana_pct = fracs[:, 0]
    flujo_tarde_pct  = fracs[:, 1]
    flujo_noche_pct  = fracs[:, 2]

    # Demografía
    renta_media_hogar = rng.normal(32000, 8000, n).clip(17000, 70000)
    edad_media        = rng.normal(42, 6, n).clip(25, 65)
    pct_extranjeros   = rng.beta(2.5, 8, n).clip(0, 0.6)
    densidad_hab_km2  = rng.lognormal(10.0, 0.4, n).clip(5000, 50000)

    # Competencia
    num_competidores_300m      = rng.poisson(8, n).clip(0, 40).astype(float)
    rating_medio_competidores  = rng.normal(3.9, 0.4, n).clip(1.0, 5.0)
    score_saturacion           = rng.beta(3, 4, n) * 100

    # Precio alquiler (€/m²)
    precio_m2_alquiler = rng.normal(18, 5, n).clip(8, 45)

    # Entorno comercial
    pct_locales_vacios   = rng.beta(2, 10, n).clip(0, 0.5)
    tasa_rotacion_anual  = rng.beta(2, 8,  n).clip(0, 0.6)

    # Score turismo (0-100) — correlaciona con dist_playa
    score_turismo = rng.normal(45, 20, n).clip(0, 100)

    # Incidencias, ruido, equipamientos
    incidencias_por_1000hab = rng.lognormal(3.5, 0.5, n).clip(5, 150)
    nivel_ruido_db          = rng.normal(63, 6, n).clip(45, 85)
    score_equipamientos     = rng.normal(55, 15, n).clip(0, 100)

    # Transporte
    num_lineas_transporte = rng.poisson(6, n).clip(0, 20).astype(float)
    num_paradas_500m      = rng.poisson(4, n).clip(0, 15).astype(float)

    # Zonas verdes
    m2_zonas_verdes_cercanas = rng.lognormal(6.8, 0.8, n).clip(100, 20000)

    # v2: geografía
    dist_playa_m              = rng.lognormal(8.0, 0.6, n).clip(50, 15000)
    ratio_locales_comerciales = rng.beta(2.5, 9, n).clip(0, 0.7)

    # v3: turismo y dinamismo
    airbnb_density_500m       = rng.lognormal(3.2, 0.8, n).clip(0, 200)
    airbnb_occupancy_est      = rng.beta(5, 3, n).clip(0.2, 0.95)
    google_review_count_medio = rng.lognormal(4.8, 0.8, n).clip(5, 1500)
    licencias_nuevas_1a       = rng.poisson(4, n).clip(0, 20).astype(float)
    eventos_culturales_500m   = rng.poisson(3, n).clip(0, 20).astype(float)
    booking_hoteles_500m      = rng.poisson(2, n).clip(0, 15).astype(float)

    # ── 2. Función de supervivencia (logit lineal + ruido) ───────────────────
    # Combina varios factores con correlaciones realistas

    logit = (
        # Flujo peatonal: positivo moderado
        + 0.0008 * (flujo_total - 850)
        # Renta: positivo (más poder adquisitivo → más ventas)
        + 0.00003 * (renta_media_hogar - 32000)
        # Saturación: negativo (más competencia = más difícil)
        - 0.020 * (score_saturacion - 50)
        # Precio alquiler: negativo (coste fijo alto)
        - 0.060 * (precio_m2_alquiler - 18)
        # Locales vacíos: negativo (señal de mala zona)
        - 2.0 * (pct_locales_vacios - 0.15)
        # Tasa rotación: negativo (zona con mucho cierre)
        - 2.5 * (tasa_rotacion_anual - 0.18)
        # Transporte: positivo
        + 0.05 * (num_lineas_transporte - 6)
        # Turismo: ligeramente positivo (tráfico extra)
        + 0.008 * (score_turismo - 45)
        # Airbnb density: ligeramente positivo (turismo informal)
        + 0.004 * (airbnb_density_500m - 28)
        # Reviews: ligeramente positivo (zona más activa = más negocio)
        + 0.0008 * (google_review_count_medio - 145)
        # Licencias nuevas: positivo (dinamismo comercial)
        + 0.04 * (licencias_nuevas_1a - 4)
        # Eventos culturales: positivo (atrae público)
        + 0.06 * (eventos_culturales_500m - 3)
        # Hoteles booking: ligeramente positivo
        + 0.03 * (booking_hoteles_500m - 2)
        # Ruido: ligeramente negativo (mala imagen)
        - 0.008 * (nivel_ruido_db - 63)
        # Incidencias: negativo
        - 0.004 * (incidencias_por_1000hab - 35)
    )

    # Ajustar sesgo para que ~35% sean positivos
    # Calibración: prob = sigmoid(logit + bias)
    # Con seed=42 este bias da ~35%
    bias = -0.30
    prob_surviv = 1.0 / (1.0 + np.exp(-(logit + bias)))

    # Añadir ruido aleatorio para dificultar el aprendizaje (más realista)
    noise = rng.normal(0, 0.12, n)
    prob_surviv_noisy = np.clip(prob_surviv + noise, 0.02, 0.98)

    y = (rng.uniform(0, 1, n) < prob_surviv_noisy).astype(np.int32)

    logger.info(
        "Dataset sintético: n=%d | positivos=%d (%.1f%%) | negativos=%d (%.1f%%)",
        n, y.sum(), y.mean() * 100, (y == 0).sum(), (1 - y.mean()) * 100,
    )

    # ── 3. Ensamblar matrix de features ─────────────────────────────────────
    X_dict = {
        "flujo_peatonal_total":       flujo_total,
        "flujo_manana_pct":           flujo_manana_pct,
        "flujo_tarde_pct":            flujo_tarde_pct,
        "flujo_noche_pct":            flujo_noche_pct,
        "renta_media_hogar":          renta_media_hogar,
        "edad_media":                 edad_media,
        "pct_extranjeros":            pct_extranjeros,
        "densidad_hab_km2":           densidad_hab_km2,
        "num_competidores_300m":      num_competidores_300m,
        "rating_medio_competidores":  rating_medio_competidores,
        "score_saturacion":           score_saturacion,
        "precio_m2_alquiler":         precio_m2_alquiler,
        "pct_locales_vacios":         pct_locales_vacios,
        "tasa_rotacion_anual":        tasa_rotacion_anual,
        "score_turismo":              score_turismo,
        "incidencias_por_1000hab":    incidencias_por_1000hab,
        "nivel_ruido_db":             nivel_ruido_db,
        "score_equipamientos":        score_equipamientos,
        "num_lineas_transporte":      num_lineas_transporte,
        "num_paradas_500m":           num_paradas_500m,
        "m2_zonas_verdes_cercanas":   m2_zonas_verdes_cercanas,
        "dist_playa_m":               dist_playa_m,
        "ratio_locales_comerciales":  ratio_locales_comerciales,
        "airbnb_density_500m":        airbnb_density_500m,
        "airbnb_occupancy_est":       airbnb_occupancy_est,
        "google_review_count_medio":  google_review_count_medio,
        "licencias_nuevas_1a":        licencias_nuevas_1a,
        "eventos_culturales_500m":    eventos_culturales_500m,
        "booking_hoteles_500m":       booking_hoteles_500m,
    }

    # Ordenar exactamente como FEATURE_NAMES (garantía de orden correcto)
    X = np.column_stack([X_dict[f] for f in FEATURE_NAMES]).astype(np.float32)
    assert X.shape == (n, len(FEATURE_NAMES)), (
        f"Shape incorrecto: {X.shape} != ({n}, {len(FEATURE_NAMES)})"
    )

    return X, y


# ── Entrenamiento ───────────────────────────────────────────────────────────────

def entrenar(X: np.ndarray, y: np.ndarray) -> dict:
    """
    Entrena XGBoost con CV 5-fold y devuelve métricas + modelo final.

    Returns:
        dict con modelo, métricas CV y métricas test.
    """
    t0 = time.perf_counter()

    # Balancear clases
    n_neg = int((y == 0).sum())
    n_pos = int((y == 1).sum())
    scale_pos_weight = round(n_neg / n_pos, 3) if n_pos > 0 else 1.0
    params = {**_PARAMS, "scale_pos_weight": scale_pos_weight}

    logger.info(
        "scale_pos_weight=%.3f (n_neg=%d, n_pos=%d)",
        scale_pos_weight, n_neg, n_pos,
    )

    # Split train/test (80/20) estratificado
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.20, stratify=y, random_state=42,
    )

    # CV 5-fold sobre train
    skf = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    aucs_cv: list[float] = []
    pr_aucs_cv: list[float] = []

    logger.info("Validación cruzada 5-fold...")
    for fold, (tr_idx, val_idx) in enumerate(skf.split(X_train, y_train), 1):
        m = xgb.XGBClassifier(**params)
        m.fit(
            X_train[tr_idx], y_train[tr_idx],
            eval_set=[(X_train[val_idx], y_train[val_idx])],
            verbose=False,
        )
        probs = m.predict_proba(X_train[val_idx])[:, 1]
        auc    = roc_auc_score(y_train[val_idx], probs)
        pr_auc = average_precision_score(y_train[val_idx], probs)
        aucs_cv.append(auc)
        pr_aucs_cv.append(pr_auc)
        logger.info("  Fold %d — AUC=%.4f | PR-AUC=%.4f", fold, auc, pr_auc)

    auc_cv    = float(np.mean(aucs_cv))
    pr_auc_cv = float(np.mean(pr_aucs_cv))
    auc_std   = float(np.std(aucs_cv))
    logger.info("CV — AUC=%.4f ± %.4f | PR-AUC=%.4f", auc_cv, auc_std, pr_auc_cv)

    # Modelo final sobre todo el train
    logger.info("Entrenando modelo final sobre %d muestras...", len(X_train))
    modelo_final = xgb.XGBClassifier(**params)
    modelo_final.fit(X_train, y_train, verbose=False)

    # Métricas sobre test hold-out
    probs_test = modelo_final.predict_proba(X_test)[:, 1]
    preds_test = (probs_test >= 0.5).astype(int)

    auc_test    = roc_auc_score(y_test, probs_test)
    pr_auc_test = average_precision_score(y_test, probs_test)
    brier_test  = brier_score_loss(y_test, probs_test)
    cm          = confusion_matrix(y_test, preds_test)

    elapsed = round(time.perf_counter() - t0, 1)

    return {
        "modelo":       modelo_final,
        "params":       params,
        "auc_cv":       auc_cv,
        "auc_std":      auc_std,
        "pr_auc_cv":    pr_auc_cv,
        "auc_test":     auc_test,
        "pr_auc_test":  pr_auc_test,
        "brier_test":   brier_test,
        "cm":           cm,
        "n_train":      len(X_train),
        "n_test":       len(X_test),
        "elapsed_s":    elapsed,
    }


# ── SHAP feature importance ─────────────────────────────────────────────────────

def calcular_shap_top10(modelo: xgb.XGBClassifier, X_sample: np.ndarray) -> list[tuple[str, float]]:
    """Calcula SHAP mean(|shap|) top-10 features."""
    try:
        import shap
        explainer = shap.TreeExplainer(modelo)
        shap_values = explainer.shap_values(X_sample)
        if isinstance(shap_values, list):
            shap_values = shap_values[1]
        importancia = np.mean(np.abs(shap_values), axis=0)
        total = importancia.sum() or 1.0
        importancia_norm = importancia / total
        pares = sorted(
            zip(FEATURE_NAMES, importancia_norm),
            key=lambda x: x[1],
            reverse=True,
        )
        return pares[:10]
    except ImportError:
        logger.warning("shap no disponible — usando feature_importance por ganancia")
        importances = modelo.get_booster().get_score(importance_type="gain")
        total = sum(importances.values()) or 1.0
        pares = [
            (feat, round(importances.get(f"f{i}", 0.0) / total, 4))
            for i, feat in enumerate(FEATURE_NAMES)
        ]
        return sorted(pares, key=lambda x: x[1], reverse=True)[:10]


# ── Informe ─────────────────────────────────────────────────────────────────────

def imprimir_informe(resultado: dict, top10_shap: list[tuple[str, float]]) -> None:
    cm = resultado["cm"]
    tn, fp, fn, tp = cm.ravel()
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    recall    = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    f1        = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0

    print()
    print("=" * 65)
    print("  ENTRENAMIENTO SINTÉTICO — XGBOOST v3 (29 features)")
    print("=" * 65)
    print(f"  Dataset:      {resultado['n_train']} train + {resultado['n_test']} test")
    print(f"  Features:     {len(FEATURE_NAMES)} (v3)")
    print()
    print("  Métricas CV (5-fold sobre train):")
    print(f"    AUC-ROC:    {resultado['auc_cv']:.4f} ± {resultado['auc_std']:.4f}")
    print(f"    PR-AUC:     {resultado['pr_auc_cv']:.4f}")
    print()
    print("  Métricas test hold-out (20%):")
    print(f"    AUC-ROC:    {resultado['auc_test']:.4f}")
    print(f"    PR-AUC:     {resultado['pr_auc_test']:.4f}")
    print(f"    Brier:      {resultado['brier_test']:.4f}  (↓ mejor)")
    print(f"    Precision:  {precision:.4f}")
    print(f"    Recall:     {recall:.4f}")
    print(f"    F1:         {f1:.4f}")
    print()
    print("  Confusion matrix (umbral=0.5):")
    print(f"                  Pred 0    Pred 1")
    print(f"    Real 0   TN={tn:5d}   FP={fp:5d}")
    print(f"    Real 1   FN={fn:5d}   TP={tp:5d}")
    print()
    print("  SHAP feature importance — top 10:")
    for feat, imp in top10_shap:
        bar = "█" * max(1, int(imp * 40))
        print(f"    {feat:<35} {bar} {imp:.4f}")
    print()
    print(f"  Tiempo:       {resultado['elapsed_s']:.1f}s")
    print(f"  Modelo:       {_MODEL_PATH}")
    print("=" * 65)
    print()


# ── Main ────────────────────────────────────────────────────────────────────────

def main() -> None:
    logger.info("=== train_synthetic.py — XGBoost v3 (29 features) ===")
    logger.info("Generando dataset sintético (n=%d)...", N_SAMPLES)

    X, y = generar_dataset_sintetico(n_samples=N_SAMPLES, seed=RANDOM_SEED)
    logger.info("Dataset generado: X.shape=%s | y.dtype=%s", X.shape, y.dtype)

    resultado = entrenar(X, y)

    # Guardar modelo
    _MODELS_DIR.mkdir(parents=True, exist_ok=True)
    resultado["modelo"].save_model(str(_MODEL_PATH))
    logger.info("Modelo guardado en %s", _MODEL_PATH)

    # SHAP top-10 (sobre muestra de 500 para velocidad)
    rng = np.random.default_rng(42)
    idx_sample = rng.choice(len(X), min(500, len(X)), replace=False)
    top10_shap = calcular_shap_top10(resultado["modelo"], X[idx_sample])

    imprimir_informe(resultado, top10_shap)

    # Verificación mínima
    auc = resultado["auc_test"]
    if auc < 0.70:
        logger.warning(
            "AUC=%.4f < 0.70 con datos sintéticos. "
            "Revisar distribuciones o aumentar n_samples.",
            auc,
        )
    else:
        logger.info("AUC=%.4f >= 0.70. Modelo sintético aceptable.", auc)


if __name__ == "__main__":
    main()
