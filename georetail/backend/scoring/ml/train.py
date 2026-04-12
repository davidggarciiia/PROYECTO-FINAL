"""
scoring/train.py — Entrenamiento del modelo XGBoost de supervivencia comercial.

Flujo:
  1. Construir dataset desde PostgreSQL (scoring/dataset.py)
  2. Balancear clases con scale_pos_weight
  3. Entrenar XGBoost con validación cruzada estratificada (5 folds)
  4. Evaluar con métricas de clasificación + SHAP feature importance
  5. Guardar modelo en disco + registrar en `modelos_versiones` (PostgreSQL)
  6. Si el nuevo modelo supera al activo en ROC-AUC → promoverlo a activo

Uso desde CLI:
    python -m scoring.train --sector restauracion
    python -m scoring.train --sector todos --min-auc 0.72

Uso programático (desde pipelines/scores.py en el scheduler semanal):
    from scoring.ml.train import entrenar_modelo
    resultado = await entrenar_modelo(sector="restauracion")
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Sequence

import numpy as np
import xgboost as xgb
from sklearn.model_selection import StratifiedKFold
from sklearn.metrics import roc_auc_score, average_precision_score

from db.conexion import get_db
from scoring.ml.dataset import construir_dataset, resumen_dataset
from scoring.infra.governance import (
    build_model_feature_names,
    default_active_demography_features,
    load_active_demography_features_from_artifact,
    load_active_temporal_features_from_artifact,
    load_temporal_feature_gates,
    slice_feature_matrix,
)
from scoring.ml.evaluate import evaluar_modelo, imprimir_reporte
from scoring.features import FEATURE_NAMES

logger = logging.getLogger(__name__)

# Directorio donde se guardan los modelos entrenados
MODELS_DIR = Path(os.environ.get("MODELS_DIR", "/data/models"))

# Umbral mínimo de ROC-AUC para promover el modelo a activo
_MIN_AUC_PARA_PROMOVER = float(os.environ.get("MIN_AUC_MODELO", "0.70"))

# Hiperparámetros por defecto — resultado del último run de Optuna
# Ver scoring/hyperparams.py para lanzar una nueva búsqueda
_DEFAULT_PARAMS = {
    "objective":        "binary:logistic",
    "eval_metric":      ["logloss", "auc"],
    "n_estimators":     450,
    "max_depth":        5,
    "learning_rate":    0.05,
    "subsample":        0.80,
    "colsample_bytree": 0.75,
    "min_child_weight": 8,
    "gamma":            0.15,
    "reg_alpha":        0.10,   # L1
    "reg_lambda":       1.50,   # L2
    "tree_method":      "hist", # Rápido y compatible con CPU
    "random_state":     42,
    "n_jobs":           -1,
}


async def entrenar_modelo(
    sector: Optional[str] = None,
    params: Optional[dict] = None,
    min_auc: float = _MIN_AUC_PARA_PROMOVER,
    promover_si_supera: bool = True,
    demografia_feature_names: Optional[Sequence[str]] = None,
    demografia_features_path: Optional[str] = None,
    temporal_feature_names: Optional[Sequence[str]] = None,
    temporal_features_path: Optional[str] = None,
) -> dict:
    """
    Entrena un modelo XGBoost y opcionalmente lo promueve a activo.

    Args:
        sector: Sector a entrenar. None = modelo global (todos los sectores).
        params: Hiperparámetros XGBoost. None = _DEFAULT_PARAMS.
        min_auc: ROC-AUC mínimo para guardar el modelo. Si no se supera, no se guarda.
        promover_si_supera: Si True, promueve el nuevo modelo a activo si supera al actual.

    Returns:
        dict con: version, auc_cv, pr_auc_cv, n_train, n_test, promovido, ruta_modelo
    """
    t_inicio = time.perf_counter()
    params = {**_DEFAULT_PARAMS, **(params or {})}

    # ── Resumen previo ────────────────────────────────────────────────────────
    resumen = await resumen_dataset(sector)
    logger.info(
        "Dataset: total=%d | supervivientes=%d (%.1f%%) | fracasos=%d",
        resumen["total_negocios"],
        resumen["supervivientes"],
        resumen["ratio_supervivencia"] * 100,
        resumen["fracasos"],
    )

    # ── Construir dataset ─────────────────────────────────────────────────────
    X, y, meta = await construir_dataset(sector=sector)

    if demografia_features_path:
        active_demography_features = load_active_demography_features_from_artifact(
            demografia_features_path
        )
    elif demografia_feature_names is not None:
        active_demography_features = list(demografia_feature_names)
    else:
        active_demography_features = default_active_demography_features()

    temporal_gate: dict = {}
    if temporal_features_path:
        temporal_gate = load_temporal_feature_gates(temporal_features_path)
        active_temporal_features = load_active_temporal_features_from_artifact(
            temporal_features_path
        )
        overall_gate = temporal_gate.get("overall_gate") or {}
        if (
            overall_gate.get("status") != "pass"
            or not bool(overall_gate.get("accepted"))
        ):
            motivo = (
                "Gate temporal estricto no superado; el modelo se mantiene sin cambios. "
                f"Detalles: {overall_gate or 'sin artifact válido'}"
            )
            logger.warning(motivo)
            return {
                "guardado": False,
                "auc_cv": None,
                "pr_auc_cv": None,
                "motivo": motivo,
                "temporal_gate": overall_gate,
            }
    elif temporal_feature_names is not None:
        active_temporal_features = list(temporal_feature_names)
    else:
        active_temporal_features = []

    model_feature_names = build_model_feature_names(
        active_demography_features,
        active_temporal_features,
    )
    X = slice_feature_matrix(X, FEATURE_NAMES, model_feature_names)

    # ── Balancear clases ──────────────────────────────────────────────────────
    # XGBoost usa scale_pos_weight para compensar el desbalanceo.
    # Si hay 70% supervivientes y 30% fracasos → scale_pos_weight ≈ 2.3
    n_neg = int((y == 0).sum())
    n_pos = int((y == 1).sum())
    scale_pos_weight = n_neg / n_pos if n_pos > 0 else 1.0
    params["scale_pos_weight"] = round(scale_pos_weight, 3)

    logger.info(
        "Balance de clases — positivos=%d | negativos=%d | scale_pos_weight=%.2f",
        n_pos, n_neg, scale_pos_weight,
    )

    # ── Validación cruzada 5 folds ────────────────────────────────────────────
    skf = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    aucs: list[float] = []
    pr_aucs: list[float] = []

    logger.info("Iniciando validación cruzada 5-fold...")
    for fold, (train_idx, val_idx) in enumerate(skf.split(X, y), 1):
        X_train, X_val = X[train_idx], X[val_idx]
        y_train, y_val = y[train_idx], y[val_idx]

        model_fold = xgb.XGBClassifier(**params)
        model_fold.fit(
            X_train, y_train,
            eval_set=[(X_val, y_val)],
            verbose=False,
        )

        probs = model_fold.predict_proba(X_val)[:, 1]
        auc    = roc_auc_score(y_val, probs)
        pr_auc = average_precision_score(y_val, probs)

        aucs.append(auc)
        pr_aucs.append(pr_auc)
        logger.info("  Fold %d — AUC=%.4f | PR-AUC=%.4f", fold, auc, pr_auc)

    auc_cv    = float(np.mean(aucs))
    pr_auc_cv = float(np.mean(pr_aucs))
    auc_std   = float(np.std(aucs))

    logger.info(
        "CV completada — AUC=%.4f ± %.4f | PR-AUC=%.4f",
        auc_cv, auc_std, pr_auc_cv,
    )

    # ── Umbral mínimo ─────────────────────────────────────────────────────────
    if auc_cv < min_auc:
        logger.warning(
            "AUC=%.4f por debajo del umbral mínimo=%.4f. Modelo NO guardado.",
            auc_cv, min_auc,
        )
        return {
            "guardado": False,
            "auc_cv": auc_cv,
            "pr_auc_cv": pr_auc_cv,
            "motivo": f"AUC {auc_cv:.4f} < umbral {min_auc:.4f}",
        }

    # ── Entrenar modelo final sobre TODO el dataset ───────────────────────────
    logger.info("Entrenando modelo final sobre dataset completo (%d samples)...", len(X))
    modelo_final = xgb.XGBClassifier(**params)
    modelo_final.fit(X, y, verbose=False)

    # ── Guardar modelo en disco ───────────────────────────────────────────────
    version   = _generar_version(sector)
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    ruta      = MODELS_DIR / f"xgboost_{version}.json"
    modelo_final.save_model(str(ruta))
    logger.info("Modelo guardado en %s", ruta)

    # ── Feature importance (ganancia) ─────────────────────────────────────────
    importancia = _calcular_importancia(modelo_final, model_feature_names)

    # ── Registrar en BD ───────────────────────────────────────────────────────
    await _registrar_version(
        version=version,
        sector=sector,
        params=params,
        metricas={
            "auc_cv": auc_cv,
            "pr_auc_cv": pr_auc_cv,
            "auc_std": auc_std,
            "n_samples": len(X),
            "n_positivos": n_pos,
            "n_negativos": n_neg,
            "temporal_feature_names": list(active_temporal_features),
        },
        importancia=importancia,
        ruta=str(ruta),
        demografia_feature_names=active_demography_features,
    )

    # ── Promover a activo si supera al modelo actual ──────────────────────────
    promovido = False
    if promover_si_supera:
        promovido = await _promover_si_mejor(version=version, auc_nuevo=auc_cv, sector=sector)

    elapsed = round(time.perf_counter() - t_inicio, 1)
    logger.info(
        "Entrenamiento completo en %.1fs — version=%s | AUC=%.4f | promovido=%s",
        elapsed, version, auc_cv, promovido,
    )

    return {
        "guardado":    True,
        "version":     version,
        "auc_cv":      auc_cv,
        "pr_auc_cv":   pr_auc_cv,
        "auc_std":     auc_std,
        "n_train":     len(X),
        "promovido":   promovido,
        "ruta_modelo": str(ruta),
        "importancia": importancia,
        "model_feature_names": model_feature_names,
        "demografia_feature_names": active_demography_features,
        "temporal_feature_names": active_temporal_features,
        "temporal_gate": temporal_gate.get("overall_gate") if temporal_gate else None,
        "elapsed_s":   elapsed,
    }


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _generar_version(sector: Optional[str]) -> str:
    """Genera un identificador único para la versión del modelo (v3 = 29 features)."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
    sufijo = sector or "global"
    return f"{sufijo}_v3_{ts}"


def _calcular_importancia(
    modelo: xgb.XGBClassifier,
    feature_names: Sequence[str],
) -> dict[str, float]:
    """Feature importance por ganancia (más interpretable que frecuencia)."""
    importances = modelo.get_booster().get_score(importance_type="gain")
    total = sum(importances.values()) or 1.0
    return {
        feat: round(importances.get(f"f{i}", 0.0) / total, 4)
        for i, feat in enumerate(feature_names)
    }


async def _registrar_version(
    version: str,
    sector: Optional[str],
    params: dict,
    metricas: dict,
    importancia: dict,
    ruta: str,
    demografia_feature_names: Sequence[str],
) -> None:
    """Inserta la nueva versión en `modelos_versiones`."""
    async with get_db() as conn:
        await conn.execute(
            """
            INSERT INTO modelos_versiones
                (
                    version,
                    sector,
                    params,
                    metricas,
                    importancia_features,
                    ruta_disco,
                    demografia_feature_names,
                    activo
                )
            VALUES ($1, $2, $3, $4, $5, $6, $7, FALSE)
            ON CONFLICT (version) DO NOTHING
            """,
            version,
            sector,
            json.dumps(params),
            json.dumps(metricas),
            json.dumps(importancia),
            ruta,
            json.dumps(list(demografia_feature_names)),
        )
    logger.info("Versión %s registrada en modelos_versiones", version)


async def _promover_si_mejor(
    version: str,
    auc_nuevo: float,
    sector: Optional[str],
) -> bool:
    """
    Promueve el modelo nuevo a activo si su AUC supera al modelo activo actual.
    Desactiva el anterior.
    """
    async with get_db() as conn:
        async with conn.transaction():
            # Buscar AUC del modelo activo actual (del mismo sector) — con FOR UPDATE
            # para evitar promociones simultáneas en ejecuciones paralelas.
            row = await conn.fetchrow(
                """
                SELECT version, metricas->>'auc_cv' AS auc_cv
                FROM modelos_versiones
                WHERE activo = TRUE AND ($1::text IS NULL OR sector = $1)
                ORDER BY created_at DESC LIMIT 1
                FOR UPDATE
                """,
                sector,
            )

            auc_actual = float(row["auc_cv"]) if row and row["auc_cv"] else 0.0

            if auc_nuevo <= auc_actual:
                logger.info(
                    "Modelo nuevo (AUC=%.4f) no supera al activo (AUC=%.4f). No se promueve.",
                    auc_nuevo, auc_actual,
                )
                return False

            # Desactivar el modelo anterior
            await conn.execute(
                "UPDATE modelos_versiones SET activo=FALSE WHERE activo=TRUE AND ($1::text IS NULL OR sector=$1)",
                sector,
            )
            # Promover el nuevo
            await conn.execute(
                "UPDATE modelos_versiones SET activo=TRUE WHERE version=$1",
                version,
            )

    logger.info(
        "Modelo %s promovido a activo (AUC=%.4f > anterior=%.4f)",
        version, auc_nuevo, auc_actual,
    )
    return True


# ─── CLI ────────────────────────────────────────────────────────────────────

async def _main() -> None:
    parser = argparse.ArgumentParser(description="Entrenar modelo XGBoost GeoRetail")
    parser.add_argument("--sector",  default=None,
                        help="Sector a entrenar (restauracion, tatuajes...). Vacío = global.")
    parser.add_argument("--min-auc", type=float, default=_MIN_AUC_PARA_PROMOVER,
                        help=f"AUC mínimo para guardar el modelo (default: {_MIN_AUC_PARA_PROMOVER})")
    parser.add_argument("--no-promover", action="store_true",
                        help="No promover a activo aunque supere al actual")
    parser.add_argument("--resumen", action="store_true",
                        help="Solo mostrar resumen del dataset, sin entrenar")
    parser.add_argument(
        "--demografia-features-path",
        default=None,
        help="Ruta al artifact demography_feature_decisions.json del readiness demografico.",
    )
    parser.add_argument(
        "--temporal-features-path",
        default=None,
        help="Ruta al artifact temporal_feature_gates.json del readiness temporal estricto.",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    if args.resumen:
        resumen = await resumen_dataset(args.sector)
        print(json.dumps(resumen, indent=2))
        return

    resultado = await entrenar_modelo(
        sector=args.sector,
        min_auc=args.min_auc,
        promover_si_supera=not args.no_promover,
        demografia_features_path=args.demografia_features_path,
        temporal_features_path=args.temporal_features_path,
    )
    imprimir_reporte(resultado)


if __name__ == "__main__":
    asyncio.run(_main())
