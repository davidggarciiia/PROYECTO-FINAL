"""
scoring/hyperparams.py — Búsqueda de hiperparámetros XGBoost con Optuna.

Se ejecuta manualmente cuando el modelo empieza a degradarse o cuando
hay suficientes datos nuevos para justificar una búsqueda completa.

NO forma parte del scheduler semanal — es un proceso manual que
puede tardar varias horas dependiendo de n_trials.

Uso:
    python -m scoring.hyperparams --sector restauracion --trials 100
    python -m scoring.hyperparams --sector todos --trials 200 --timeout 3600

Tras la búsqueda, imprime los mejores parámetros listos para copiar
en `_DEFAULT_PARAMS` de scoring/train.py.

Optuna guarda el historial de trials en un SQLite local (/data/optuna/)
para poder reanudar una búsqueda interrumpida.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
from pathlib import Path
from typing import Optional

import numpy as np
import optuna
import xgboost as xgb
from sklearn.model_selection import StratifiedKFold
from sklearn.metrics import roc_auc_score

from scoring.ml.dataset import construir_dataset
from scoring.features import FEATURE_NAMES

logger = logging.getLogger(__name__)

# Directorio donde Optuna guarda el historial de trials
OPTUNA_DIR = Path(os.environ.get("OPTUNA_DIR", "/data/optuna"))

# AUC mínimo que debe superar el mejor trial para considerar los params válidos
_MIN_AUC_VALIDO = 0.68


def _crear_estudio(sector: Optional[str], storage: Optional[str] = None) -> optuna.Study:
    """
    Crea o reanuda un estudio Optuna.

    Si el estudio ya existe en el storage, reanuda los trials anteriores.
    Esto es útil si el proceso se interrumpe.
    """
    nombre = f"georetail_xgb_{sector or 'global'}"

    if storage is None:
        OPTUNA_DIR.mkdir(parents=True, exist_ok=True)
        storage = f"sqlite:///{OPTUNA_DIR}/{nombre}.db"

    # Silenciar logs de Optuna — solo queremos el resultado final
    optuna.logging.set_verbosity(optuna.logging.WARNING)

    study = optuna.create_study(
        study_name=nombre,
        direction="maximize",   # maximizar ROC-AUC
        storage=storage,
        load_if_exists=True,    # reanudar si ya existe
        sampler=optuna.samplers.TPESampler(seed=42),
        pruner=optuna.pruners.MedianPruner(n_startup_trials=10, n_warmup_steps=2),
    )

    n_previos = len(study.trials)
    if n_previos > 0:
        logger.info("Reanudando estudio '%s' con %d trials previos", nombre, n_previos)
    else:
        logger.info("Nuevo estudio '%s'", nombre)

    return study


def _objective_factory(X: np.ndarray, y: np.ndarray):
    """
    Crea la función objetivo para Optuna.
    Usa validación cruzada 3-fold para estabilidad con menos tiempo de cómputo.
    """
    skf = StratifiedKFold(n_splits=3, shuffle=True, random_state=42)
    n_neg = int((y == 0).sum())
    n_pos = int((y == 1).sum())
    scale_pos = n_neg / n_pos if n_pos > 0 else 1.0

    def objective(trial: optuna.Trial) -> float:
        params = {
            "objective":        "binary:logistic",
            "eval_metric":      "auc",
            "tree_method":      "hist",
            "random_state":     42,
            "n_jobs":           -1,
            "scale_pos_weight": scale_pos,

            # Hiperparámetros a optimizar
            "n_estimators": trial.suggest_int("n_estimators", 100, 800),
            "max_depth":    trial.suggest_int("max_depth", 3, 8),
            "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.20, log=True),
            "subsample":    trial.suggest_float("subsample", 0.50, 1.00),
            "colsample_bytree": trial.suggest_float("colsample_bytree", 0.50, 1.00),
            "min_child_weight": trial.suggest_int("min_child_weight", 1, 20),
            "gamma":        trial.suggest_float("gamma", 0.0, 1.0),
            "reg_alpha":    trial.suggest_float("reg_alpha", 1e-4, 10.0, log=True),
            "reg_lambda":   trial.suggest_float("reg_lambda", 1e-4, 10.0, log=True),
        }

        aucs: list[float] = []
        for step, (train_idx, val_idx) in enumerate(skf.split(X, y)):
            X_tr, X_val = X[train_idx], X[val_idx]
            y_tr, y_val = y[train_idx], y[val_idx]

            model = xgb.XGBClassifier(**params)
            model.fit(
                X_tr, y_tr,
                eval_set=[(X_val, y_val)],
                verbose=False,
            )

            probs = model.predict_proba(X_val)[:, 1]
            auc = roc_auc_score(y_val, probs)
            aucs.append(auc)

            # Pruning — si el fold intermedio ya es malo, cancelar trial
            # (MedianPruner necesita al menos 2 pasos para hacer comparaciones)
            trial.report(np.mean(aucs), step)
            if step >= 1 and trial.should_prune():
                raise optuna.exceptions.TrialPruned()

        return float(np.mean(aucs))

    return objective


async def buscar_hiperparametros(
    sector: Optional[str] = None,
    n_trials: int = 100,
    timeout_s: Optional[int] = None,
) -> dict:
    """
    Lanza la búsqueda de hiperparámetros con Optuna.

    Args:
        sector:    Sector para el que optimizar. None = dataset global.
        n_trials:  Número de combinaciones a probar.
        timeout_s: Timeout en segundos. None = sin límite.

    Returns:
        dict con los mejores parámetros y la métrica obtenida.
    """
    logger.info(
        "Iniciando búsqueda de hiperparámetros — sector=%s | trials=%d",
        sector or "global", n_trials,
    )

    # Cargar dataset
    X, y, _ = await construir_dataset(sector=sector)
    logger.info("Dataset: %d samples | %d features", len(X), X.shape[1])

    study = _crear_estudio(sector)
    objective = _objective_factory(X, y)

    # Callbacks para logging de progreso
    def callback(study: optuna.Study, trial: optuna.Trial) -> None:
        if trial.number % 10 == 0 or trial.value == study.best_value:
            logger.info(
                "Trial %d/%d — AUC=%.4f | Mejor=%.4f",
                trial.number + 1, n_trials,
                trial.value or 0.0,
                study.best_value,
            )

    study.optimize(
        objective,
        n_trials=n_trials,
        timeout=timeout_s,
        callbacks=[callback],
        gc_after_trial=True,
        show_progress_bar=False,
    )

    mejor_trial = study.best_trial
    mejor_auc   = mejor_trial.value

    logger.info(
        "Búsqueda completada — %d trials | Mejor AUC=%.4f",
        len(study.trials), mejor_auc,
    )

    if mejor_auc < _MIN_AUC_VALIDO:
        logger.warning(
            "Mejor AUC=%.4f por debajo del mínimo válido=%.4f. "
            "Revisa los datos o amplía n_trials.",
            mejor_auc, _MIN_AUC_VALIDO,
        )

    # Construir los params completos listos para copiar en train.py
    mejores_params = {
        "objective":        "binary:logistic",
        "eval_metric":      ["logloss", "auc"],
        "tree_method":      "hist",
        "random_state":     42,
        "n_jobs":           -1,
        **mejor_trial.params,
    }

    # Estadísticas de la búsqueda
    aucs_todos = [t.value for t in study.trials if t.value is not None]
    stats = {
        "n_trials_completados": len(aucs_todos),
        "mejor_auc":   round(mejor_auc, 4),
        "media_auc":   round(float(np.mean(aucs_todos)), 4),
        "std_auc":     round(float(np.std(aucs_todos)), 4),
        "percentil_90": round(float(np.percentile(aucs_todos, 90)), 4),
    }

    return {
        "sector":        sector or "global",
        "mejores_params": mejores_params,
        "stats":          stats,
        "top5_trials": [
            {
                "trial": t.number,
                "auc":   round(t.value, 4),
                "params": t.params,
            }
            for t in sorted(study.trials, key=lambda t: t.value or 0, reverse=True)[:5]
            if t.value is not None
        ],
    }


def imprimir_mejores_params(resultado: dict) -> None:
    """Imprime los mejores parámetros listos para copiar en train.py."""
    print("\n" + "=" * 60)
    print("  MEJORES HIPERPARÁMETROS — XGBOOST GEORETAIL")
    print("=" * 60)
    print(f"  Sector:     {resultado['sector']}")
    print(f"  Mejor AUC:  {resultado['stats']['mejor_auc']:.4f}")
    print(f"  Trials OK:  {resultado['stats']['n_trials_completados']}")
    print()
    print("  Copiar en scoring/train.py → _DEFAULT_PARAMS:")
    print()
    print("  _DEFAULT_PARAMS = {")
    for k, v in resultado["mejores_params"].items():
        if isinstance(v, str):
            print(f'      "{k}": "{v}",')
        elif isinstance(v, list):
            print(f'      "{k}": {v},')
        else:
            print(f'      "{k}": {v},')
    print("  }")
    print()
    print("  Top 5 trials:")
    for t in resultado["top5_trials"]:
        print(f"    Trial {t['trial']:3d} — AUC={t['auc']:.4f} | n_estimators={t['params'].get('n_estimators')} | max_depth={t['params'].get('max_depth')} | lr={t['params'].get('learning_rate', 0):.4f}")
    print("=" * 60 + "\n")


# ─── CLI ─────────────────────────────────────────────────────────────────────

async def _main() -> None:
    parser = argparse.ArgumentParser(description="Búsqueda de hiperparámetros XGBoost GeoRetail")
    parser.add_argument("--sector",   default=None, help="Sector a optimizar (None = global)")
    parser.add_argument("--trials",   type=int, default=100, help="Número de trials Optuna")
    parser.add_argument("--timeout",  type=int, default=None, help="Timeout en segundos")
    parser.add_argument("--json",     action="store_true", help="Salida en JSON en lugar de texto")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    resultado = await buscar_hiperparametros(
        sector=args.sector,
        n_trials=args.trials,
        timeout_s=args.timeout,
    )

    if args.json:
        print(json.dumps(resultado, indent=2))
    else:
        imprimir_mejores_params(resultado)


if __name__ == "__main__":
    asyncio.run(_main())
