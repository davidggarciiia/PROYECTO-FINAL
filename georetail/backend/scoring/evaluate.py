"""
scoring/evaluate.py — Evaluación y métricas del modelo XGBoost.

Funciones:
  - evaluar_modelo()    → métricas completas sobre un conjunto de test
  - evaluar_por_sector() → métricas desagregadas por sector
  - comparar_versiones() → compara dos versiones del modelo
  - imprimir_reporte()  → imprime un resumen legible en consola
  - calcular_shap_global() → importancia global por SHAP (no por ganancia)

Uso desde CLI:
    python -m scoring.evaluate --version restauracion_20260310_1430
    python -m scoring.evaluate --comparar restauracion_20260301 restauracion_20260310
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
from pathlib import Path
from typing import Optional
import os

import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.metrics import (
    roc_auc_score,
    average_precision_score,
    classification_report,
    confusion_matrix,
    brier_score_loss,
    RocCurveDisplay,
)
from sklearn.model_selection import StratifiedKFold

from db.conexion import get_db
from scoring.dataset import construir_dataset
from scoring.features import FEATURE_NAMES

logger = logging.getLogger(__name__)

MODELS_DIR = Path(os.environ.get("MODELS_DIR", "/data/models"))


def evaluar_modelo(
    modelo: xgb.XGBClassifier,
    X_test: np.ndarray,
    y_test: np.ndarray,
    umbral: float = 0.5,
) -> dict:
    """
    Calcula métricas de clasificación completas para un modelo entrenado.

    Args:
        modelo:  Modelo XGBoost ya entrenado.
        X_test:  Features de test (n_samples, 21).
        y_test:  Labels de test (n_samples,).
        umbral:  Umbral de decisión para convertir probabilidades → clase (default 0.5).
                 Para maximizar recall en fracasos, bajar a 0.4.

    Returns:
        dict con todas las métricas.
    """
    probs = modelo.predict_proba(X_test)[:, 1]
    preds = (probs >= umbral).astype(int)

    auc      = roc_auc_score(y_test, probs)
    pr_auc   = average_precision_score(y_test, probs)
    brier    = brier_score_loss(y_test, probs)

    cm = confusion_matrix(y_test, preds)
    tn, fp, fn, tp = cm.ravel()

    # Métricas derivadas de la matriz de confusión
    precision  = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    recall     = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    f1         = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0
    specificity = tn / (tn + fp) if (tn + fp) > 0 else 0.0

    return {
        # Métricas principales (threshold-free)
        "roc_auc":   round(auc, 4),
        "pr_auc":    round(pr_auc, 4),
        "brier":     round(brier, 4),   # Más bajo = mejor calibrado (0 = perfecto)

        # Métricas con umbral
        "umbral":    umbral,
        "precision": round(precision, 4),
        "recall":    round(recall, 4),
        "f1":        round(f1, 4),
        "specificity": round(specificity, 4),

        # Matriz de confusión
        "tp": int(tp), "fp": int(fp),
        "tn": int(tn), "fn": int(fn),

        # Distribución del dataset de test
        "n_test":     int(len(y_test)),
        "n_positivos": int(y_test.sum()),
        "n_negativos": int((y_test == 0).sum()),
        "ratio_positivos": round(float(y_test.mean()), 3),
    }


async def evaluar_por_sector(
    version: str,
) -> dict[str, dict]:
    """
    Evalúa el modelo desagregado por sector.
    Útil para detectar sectores donde el modelo rinde mal.

    Carga el modelo de disco y lo evalúa sobre el dataset de cada sector.
    """
    ruta = MODELS_DIR / f"xgboost_{version}.json"
    if not ruta.exists():
        raise FileNotFoundError(f"Modelo {version} no encontrado en {MODELS_DIR}")

    modelo = xgb.XGBClassifier()
    modelo.load_model(str(ruta))

    resultados: dict[str, dict] = {}

    # Sectores conocidos — en producción leer de BD
    sectores = ["restauracion", "tatuajes", "moda", "estetica", "shisha_lounge"]

    for sector in sectores:
        try:
            X, y, _ = await construir_dataset(sector=sector)
            if len(X) < 50:
                logger.warning("Sector %s: solo %d muestras — métricas no fiables", sector, len(X))
                continue
            metricas = evaluar_modelo(modelo, X, y)
            metricas["n_samples"] = len(X)
            resultados[sector] = metricas
            logger.info("Sector %s — AUC=%.4f | PR-AUC=%.4f", sector, metricas["roc_auc"], metricas["pr_auc"])
        except Exception as exc:
            logger.warning("No se pudo evaluar sector %s: %s", sector, exc)

    return resultados


async def comparar_versiones(
    version_a: str,
    version_b: str,
    sector: Optional[str] = None,
) -> dict:
    """
    Compara dos versiones del modelo en el mismo dataset.

    Útil para decidir si el modelo nuevo debe reemplazar al actual.
    Devuelve las métricas de cada uno y la diferencia.
    """
    X, y, _ = await construir_dataset(sector=sector)

    metricas: dict[str, dict] = {}
    for version in [version_a, version_b]:
        ruta = MODELS_DIR / f"xgboost_{version}.json"
        if not ruta.exists():
            raise FileNotFoundError(f"Modelo {version} no encontrado")
        modelo = xgb.XGBClassifier()
        modelo.load_model(str(ruta))
        metricas[version] = evaluar_modelo(modelo, X, y)

    diferencia = {
        "roc_auc_delta": round(metricas[version_b]["roc_auc"] - metricas[version_a]["roc_auc"], 4),
        "pr_auc_delta":  round(metricas[version_b]["pr_auc"]  - metricas[version_a]["pr_auc"],  4),
        "brier_delta":   round(metricas[version_b]["brier"]    - metricas[version_a]["brier"],   4),
        "ganador":       version_b if metricas[version_b]["roc_auc"] > metricas[version_a]["roc_auc"] else version_a,
    }

    return {
        "version_a": {**metricas[version_a], "version": version_a},
        "version_b": {**metricas[version_b], "version": version_b},
        "diferencia": diferencia,
        "dataset_n": len(X),
    }


async def calcular_shap_global(
    version: str,
    sector: Optional[str] = None,
    n_samples: int = 500,
) -> dict[str, float]:
    """
    Calcula la importancia global de features usando SHAP mean(|shap_value|).

    Más preciso que feature_importance por ganancia porque considera interacciones.
    Usa una muestra aleatoria de n_samples para eficiencia.

    Returns:
        dict feature_name → importancia_media (normalizada a suma=1)
    """
    try:
        import shap
    except ImportError:
        logger.error("shap no instalado. Instalar con: pip install shap")
        return {}

    ruta = MODELS_DIR / f"xgboost_{version}.json"
    modelo = xgb.XGBClassifier()
    modelo.load_model(str(ruta))

    X, y, _ = await construir_dataset(sector=sector)

    # Muestra aleatoria para eficiencia (seed fijo para reproducibilidad)
    if len(X) > n_samples:
        rng = np.random.default_rng(42)
        idx = rng.choice(len(X), n_samples, replace=False)
        X_sample = X[idx]
    else:
        X_sample = X

    explainer = shap.TreeExplainer(modelo)
    shap_values = explainer.shap_values(X_sample)

    # Si es clasificador binario, shap_values puede ser lista de 2 arrays
    if isinstance(shap_values, list):
        shap_values = shap_values[1]

    # Importancia = media del valor absoluto por feature
    importancia_absoluta = np.mean(np.abs(shap_values), axis=0)
    total = importancia_absoluta.sum() or 1.0
    importancia_norm = importancia_absoluta / total

    resultado = {
        feat: round(float(importancia_norm[i]), 4)
        for i, feat in enumerate(FEATURE_NAMES)
    }

    # Ordenar de mayor a menor
    return dict(sorted(resultado.items(), key=lambda x: x[1], reverse=True))


def umbral_optimo(
    modelo: xgb.XGBClassifier,
    X_val: np.ndarray,
    y_val: np.ndarray,
    metrica: str = "f1",
) -> float:
    """
    Encuentra el umbral de clasificación que maximiza una métrica en validación.

    Args:
        metrica: 'f1' | 'precision' | 'recall' | 'balanced_accuracy'

    Returns:
        Umbral óptimo en el rango [0.1, 0.9]
    """
    from sklearn.metrics import f1_score, precision_score, recall_score, balanced_accuracy_score

    _metricas = {
        "f1":                lambda y, p: f1_score(y, p, zero_division=0),
        "precision":         lambda y, p: precision_score(y, p, zero_division=0),
        "recall":            lambda y, p: recall_score(y, p, zero_division=0),
        "balanced_accuracy": lambda y, p: balanced_accuracy_score(y, p),
    }

    fn_metrica = _metricas.get(metrica, _metricas["f1"])
    probs = modelo.predict_proba(X_val)[:, 1]

    mejor_umbral = 0.5
    mejor_score  = 0.0

    for umbral in np.arange(0.10, 0.91, 0.05):
        preds = (probs >= umbral).astype(int)
        score = fn_metrica(y_val, preds)
        if score > mejor_score:
            mejor_score  = score
            mejor_umbral = umbral

    logger.info("Umbral óptimo (%s): %.2f → %.4f", metrica, mejor_umbral, mejor_score)
    return round(float(mejor_umbral), 2)


def imprimir_reporte(resultado: dict) -> None:
    """Imprime un resumen legible del resultado de entrenamiento o evaluación."""
    print("\n" + "=" * 60)
    print("  REPORTE DE ENTRENAMIENTO XGBOOST — GEORETAIL")
    print("=" * 60)

    if not resultado.get("guardado", True):
        print(f"  ❌ Modelo NO guardado: {resultado.get('motivo', '—')}")
        print(f"     AUC CV:   {resultado.get('auc_cv', 0):.4f}")
        print("=" * 60)
        return

    print(f"  Versión:   {resultado.get('version', '—')}")
    print(f"  Samples:   {resultado.get('n_train', '—')}")
    print(f"  AUC CV:    {resultado.get('auc_cv', 0):.4f} ± {resultado.get('auc_std', 0):.4f}")
    print(f"  PR-AUC CV: {resultado.get('pr_auc_cv', 0):.4f}")
    print(f"  Promovido: {'✅ Sí' if resultado.get('promovido') else '⏸  No'}")
    print(f"  Tiempo:    {resultado.get('elapsed_s', 0):.1f}s")

    if resultado.get("importancia"):
        print("\n  Top 5 features por ganancia:")
        top5 = sorted(resultado["importancia"].items(), key=lambda x: x[1], reverse=True)[:5]
        for feat, imp in top5:
            bar = "█" * int(imp * 30)
            print(f"    {feat:<35} {bar} {imp:.4f}")

    print("=" * 60 + "\n")


# ─── CLI ─────────────────────────────────────────────────────────────────────

async def _main() -> None:
    parser = argparse.ArgumentParser(description="Evaluar modelo XGBoost GeoRetail")
    parser.add_argument("--version", help="Versión del modelo a evaluar")
    parser.add_argument("--sector",  default=None, help="Filtrar por sector")
    parser.add_argument("--comparar", nargs=2, metavar=("VERSION_A", "VERSION_B"),
                        help="Comparar dos versiones del modelo")
    parser.add_argument("--shap",    action="store_true",
                        help="Calcular importancia SHAP global (más lento)")
    parser.add_argument("--por-sector", action="store_true",
                        help="Desglosar métricas por sector")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    if args.comparar:
        resultado = await comparar_versiones(*args.comparar, sector=args.sector)
        print(json.dumps(resultado, indent=2))
        return

    if not args.version:
        print("ERROR: --version es obligatorio (o usa --comparar VERSION_A VERSION_B)")
        return

    if args.por_sector:
        resultado = await evaluar_por_sector(args.version)
        print(json.dumps(resultado, indent=2))
        return

    if args.shap:
        importancia = await calcular_shap_global(args.version, sector=args.sector)
        print("\nImportancia SHAP global:")
        for feat, imp in importancia.items():
            bar = "█" * int(imp * 40)
            print(f"  {feat:<35} {bar} {imp:.4f}")
        return

    # Evaluación estándar
    ruta = MODELS_DIR / f"xgboost_{args.version}.json"
    modelo = xgb.XGBClassifier()
    modelo.load_model(str(ruta))
    X, y, _ = await construir_dataset(sector=args.sector)
    metricas = evaluar_modelo(modelo, X, y)
    print(json.dumps(metricas, indent=2))


if __name__ == "__main__":
    asyncio.run(_main())
