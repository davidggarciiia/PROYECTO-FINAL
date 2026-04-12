"""
scoring/demografia_readiness.py - readiness demografico con DuckDB, cleanlab y scikit-learn.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import re
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.base import BaseEstimator, ClassifierMixin
from sklearn.feature_selection import RFECV
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.model_selection import StratifiedKFold

from scoring.ml.dataset import construir_dataset_auditoria
from scoring.infra.governance import (
    CSV_ROOT,
    LATEST_ROOT,
    READINESS_ROOT,
    REPO_ROOT,
    DEMOGRAFIA_BATCH_FINAL_FEATURES,
    DEMOGRAFIA_CANDIDATE_FEATURES,
    DEMOGRAFIA_DEFAULT_ACTIVE_FEATURES,
    DEMOGRAFIA_MODEL_CORE_FEATURES,
    DEMOGRAFIA_SELECTABLE_EXISTING_FEATURES,
    SOURCE_APPROVED_STATUSES,
    build_model_feature_names,
    dataset_intended_demography_features,
    dataset_is_experimental_only,
    dataset_is_support_only,
    fixed_model_feature_names,
    granularity_priority,
    is_demography_dataset,
    provider_priority,
    slice_feature_matrix,
)
from scoring.features import FEATURE_NAMES
from scoring.ml.train import _DEFAULT_PARAMS

logger = logging.getLogger(__name__)

_ROW_DROP_LABEL_THRESHOLD = 0.80
_ROW_DROP_OUTLIER_THRESHOLD = 0.90
_ROW_REVIEW_THRESHOLD = 0.60
_MAX_PROFILE_FILES = 8
_SUPPORTED_TABULAR_SUFFIXES = {".csv", ".tsv", ".txt", ".parquet"}
_APPROVED_SOURCE_DECISIONS = set(SOURCE_APPROVED_STATUSES) | {"experimental"}
_FIXED_MODEL_FEATURES = fixed_model_feature_names()
_LOW_SPATIAL_GRANULARITIES = {"municipi", "ciutat", "unknown"}


class _FrozenFixedFeatureEstimator(BaseEstimator, ClassifierMixin):
    """Wrapper para RFECV que protege las features fijas del modelo."""

    def __init__(self, fixed_feature_count: int, xgb_params: Optional[dict] = None):
        self.fixed_feature_count = fixed_feature_count
        self.xgb_params = xgb_params or {}

    def fit(self, X: np.ndarray, y: np.ndarray) -> "_FrozenFixedFeatureEstimator":
        self.model_ = xgb.XGBClassifier(**self.xgb_params)
        self.model_.fit(X, y, verbose=False)
        return self

    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        return self.model_.predict_proba(X)

    def predict(self, X: np.ndarray) -> np.ndarray:
        return self.model_.predict(X)

    @property
    def classes_(self) -> np.ndarray:
        return self.model_.classes_

    @property
    def feature_importances_(self) -> np.ndarray:
        values = np.asarray(self.model_.feature_importances_, dtype=float)
        if not values.size:
            return values
        protected = values.copy()
        if self.fixed_feature_count > 0:
            max_value = float(values.max()) if values.size else 1.0
            protected[: self.fixed_feature_count] = max(max_value, 1.0) + 1.0
        return protected


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso_now() -> str:
    return _utc_now().isoformat().replace("+00:00", "Z")


def _slugify(value: str) -> str:
    return re.sub(r"[^a-z0-9_-]+", "-", value.lower()).strip("-") or "run"


def _load_manifest(path: Optional[str]) -> dict[str, Any]:
    manifest_path = Path(path) if path else CSV_ROOT / "_meta" / "public_data_manifest_latest.json"
    return json.loads(manifest_path.read_text(encoding="utf-8"))


def _resolve_local_path(raw_path: str | None) -> Optional[Path]:
    if not raw_path:
        return None

    normalized = raw_path.replace("\\", "/")
    candidate = Path(normalized)
    if candidate.is_absolute():
        return candidate

    if candidate.parts and candidate.parts[0].lower() == "csv":
        return REPO_ROOT / candidate
    return REPO_ROOT / candidate


def _expanded_tabular_files(path: Path) -> list[Path]:
    if not path.exists():
        return []

    if path.is_dir():
        return sorted(
            subpath
            for subpath in path.rglob("*")
            if subpath.is_file() and subpath.suffix.lower() in _SUPPORTED_TABULAR_SUFFIXES
        )

    if path.suffix.lower() == ".zip":
        sibling_dir = path.with_suffix("")
        if sibling_dir.exists():
            return _expanded_tabular_files(sibling_dir)
        return []

    if path.suffix.lower() in _SUPPORTED_TABULAR_SUFFIXES:
        return [path]

    return []


def _collect_dataset_files(entry: Mapping[str, Any]) -> list[Path]:
    files: list[Path] = []
    seen: set[Path] = set()
    for file_entry in entry.get("files") or []:
        path = _resolve_local_path(file_entry.get("local_path"))
        if not path:
            continue
        for expanded in _expanded_tabular_files(path):
            if expanded not in seen:
                files.append(expanded)
                seen.add(expanded)
    return files


def _normalize_column_name(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


def _detect_delimiter(path: Path) -> str:
    with path.open("r", encoding="utf-8", errors="ignore", newline="") as handle:
        sample = handle.read(4096)
    try:
        return csv.Sniffer().sniff(sample, delimiters=",;\t|").delimiter
    except csv.Error:
        if ";" in sample and "," not in sample:
            return ";"
        if "\t" in sample:
            return "\t"
        return ","


def _profile_with_duckdb(path: Path) -> Optional[dict[str, Any]]:
    try:
        import duckdb
    except ImportError:
        return None

    file_path = str(path).replace("'", "''")
    con = duckdb.connect(database=":memory:")
    try:
        if path.suffix.lower() == ".parquet":
            relation = f"read_parquet('{file_path}')"
        else:
            delimiter = _detect_delimiter(path)
            relation = (
                "read_csv_auto("
                f"'{file_path}', delim='{delimiter}', header=true, "
                "sample_size=20000, ignore_errors=true)"
            )

        preview = con.execute(f"SELECT * FROM {relation} LIMIT 200").df()
        row_count = int(con.execute(f"SELECT COUNT(*) FROM {relation}").fetchone()[0] or 0)
        return {"row_count": row_count, "preview": preview}
    except Exception as exc:
        logger.debug("DuckDB no pudo perfilar %s: %s", path, exc)
        return None
    finally:
        con.close()


def _profile_with_pandas(path: Path) -> dict[str, Any]:
    if path.suffix.lower() == ".parquet":
        preview = pd.read_parquet(path).head(200)
        row_count = int(len(pd.read_parquet(path)))
        return {"row_count": row_count, "preview": preview}

    delimiter = _detect_delimiter(path)
    preview = pd.read_csv(
        path,
        sep=delimiter,
        nrows=200,
        encoding="utf-8",
        encoding_errors="ignore",
        on_bad_lines="skip",
    )
    with path.open("r", encoding="utf-8", errors="ignore", newline="") as handle:
        row_count = max(sum(1 for _ in handle) - 1, 0)
    return {"row_count": row_count, "preview": preview}


def _profile_single_file(path: Path) -> dict[str, Any]:
    result = _profile_with_duckdb(path)
    if result is None:
        result = _profile_with_pandas(path)

    preview = result["preview"]
    preview = preview if isinstance(preview, pd.DataFrame) else pd.DataFrame()
    preview = preview.head(200)
    row_count = int(result["row_count"])
    columns = [_normalize_column_name(str(column)) for column in preview.columns]
    non_null_ratio = 0.0 if preview.empty else float(1.0 - preview.isna().mean().mean())
    return {
        "path": str(path),
        "row_count": row_count,
        "columns": columns,
        "non_null_ratio": round(non_null_ratio, 4),
    }


def _infer_granularity(entry: Mapping[str, Any], columns: Sequence[str]) -> str:
    manifest_value = str(entry.get("granularity") or "unknown").strip().lower()
    if manifest_value and manifest_value != "unknown":
        return manifest_value

    joined = " ".join(
        [
            str(entry.get("dataset_id") or "").lower(),
            str(entry.get("title") or "").lower(),
            " ".join(columns),
        ]
    )
    patterns = [
        ("seccio_censal", ("seccio", "seccion", "censal", "_sec", "sec_")),
        ("barri", ("barri", "barrio")),
        ("districte", ("districte", "distrito")),
        ("municipi", ("municipi", "municipio", "_mun", " codi_muni")),
        ("ciutat", ("ciutat", "ciudad")),
    ]
    for granularity, tokens in patterns:
        if any(token in joined for token in tokens):
            return granularity
    return "unknown"


def _detect_geo_keys(columns: Sequence[str]) -> list[str]:
    patterns = {
        "zona_id": ("zona_id",),
        "seccio_censal": ("seccio", "seccion", "censal"),
        "barri": ("barri", "barrio"),
        "districte": ("districte", "distrito"),
        "municipi": ("municipi", "municipio", "muni"),
        "codi_postal": ("postal", "cp"),
    }
    found: list[str] = []
    joined = " ".join(columns)
    for key, tokens in patterns.items():
        if any(token in joined for token in tokens):
            found.append(key)
    return found


def _profile_dataset(entry: Mapping[str, Any]) -> dict[str, Any]:
    files = _collect_dataset_files(entry)
    profiled_files: list[dict[str, Any]] = []
    errors: list[str] = []

    for path in files[:_MAX_PROFILE_FILES]:
        try:
            profiled_files.append(_profile_single_file(path))
        except Exception as exc:
            errors.append(f"{path.name}: {exc}")

    row_count_total = int(sum(item["row_count"] for item in profiled_files))
    columns = sorted({column for item in profiled_files for column in item["columns"]})
    non_null_ratio = round(
        float(np.mean([item["non_null_ratio"] for item in profiled_files])) if profiled_files else 0.0,
        4,
    )
    geo_keys = _detect_geo_keys(columns)
    granularity = _infer_granularity(entry, columns)
    years = [str(value) for value in (entry.get("years") or [])]
    return {
        "files_found": len(files),
        "files_profiled": len(profiled_files),
        "sample_files": [item["path"] for item in profiled_files],
        "row_count_estimate": row_count_total,
        "columns": columns,
        "non_null_ratio": non_null_ratio,
        "geo_keys": geo_keys,
        "granularity_detected": granularity,
        "years": years,
        "errors": errors,
    }


def _score_dataset(entry: Mapping[str, Any], profile: Mapping[str, Any]) -> tuple[float, dict[str, float]]:
    intended = dataset_intended_demography_features(entry)
    granularity = profile.get("granularity_detected")
    geo_keys = profile.get("geo_keys") or []
    non_null_ratio = float(profile.get("non_null_ratio") or 0.0)
    years = profile.get("years") or []

    relevance = 30.0 if any(feature in DEMOGRAFIA_MODEL_CORE_FEATURES for feature in intended) else 18.0
    if len(intended) > 1:
        relevance = min(30.0, relevance + 4.0)

    joinability = 25.0 if geo_keys else 15.0 if granularity_priority(str(granularity)) >= 2 else 0.0
    granularity_score = 20.0 * (granularity_priority(str(granularity)) / 5.0)
    completeness = 15.0 * non_null_ratio
    temporal = 10.0 * min(1.0, len(years) / 5.0)

    if int(profile.get("row_count_estimate") or 0) <= 0:
        joinability = 0.0
        completeness = 0.0
        temporal = 0.0

    total = round(relevance + joinability + granularity_score + completeness + temporal, 2)
    return total, {
        "relevance": round(relevance, 2),
        "joinability": round(joinability, 2),
        "granularity": round(granularity_score, 2),
        "completeness": round(completeness, 2),
        "temporal": round(temporal, 2),
    }


def _provider_sort_key(entry: Mapping[str, Any], profile: Mapping[str, Any], score: float) -> tuple[int, int, float, int]:
    return (
        granularity_priority(str(profile.get("granularity_detected"))),
        provider_priority(str(entry.get("provider"))),
        score,
        int(profile.get("row_count_estimate") or 0),
    )


def _decide_source_catalog(entries: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    profiled: list[dict[str, Any]] = []
    by_feature: dict[str, list[dict[str, Any]]] = defaultdict(list)

    for entry in entries:
        profile = _profile_dataset(entry)
        score, breakdown = _score_dataset(entry, profile)
        item = {
            "dataset_id": entry.get("dataset_id"),
            "provider": entry.get("provider"),
            "title": entry.get("title"),
            "priority": entry.get("priority"),
            "intended_features": dataset_intended_demography_features(entry),
            "profile": profile,
            "score": score,
            "score_breakdown": breakdown,
        }
        profiled.append(item)
        for feature in item["intended_features"]:
            by_feature[feature].append(item)

    champions: dict[str, str] = {}
    backups: dict[str, set[str]] = defaultdict(set)
    for feature, candidates in by_feature.items():
        viable = [
            candidate
            for candidate in candidates
            if (
                candidate["score"] >= 40.0
                and candidate["profile"]["files_found"] > 0
                and not dataset_is_support_only(str(candidate["dataset_id"]))
                and not dataset_is_experimental_only(str(candidate["dataset_id"]))
                and (
                    candidate["profile"]["geo_keys"]
                    or str(candidate["profile"]["granularity_detected"]) not in _LOW_SPATIAL_GRANULARITIES
                )
            )
        ]
        if not viable:
            continue
        ranked = sorted(
            viable,
            key=lambda candidate: _provider_sort_key(candidate, candidate["profile"], candidate["score"]),
            reverse=True,
        )
        champions[feature] = ranked[0]["dataset_id"]
        for candidate in ranked[1:3]:
            backups[candidate["dataset_id"]].add(feature)

    decided: list[dict[str, Any]] = []
    for item in profiled:
        dataset_id = item["dataset_id"]
        intended = item["intended_features"]
        primary_for = [feature for feature in intended if champions.get(feature) == dataset_id]
        backup_for = sorted(backups.get(dataset_id, set()))
        reasons: list[str] = []
        granularity = str(item["profile"]["granularity_detected"])
        support_only_dataset = (
            dataset_is_support_only(str(dataset_id))
            or (
                not primary_for
                and not backup_for
                and granularity in _LOW_SPATIAL_GRANULARITIES
                and not item["profile"]["geo_keys"]
            )
        )

        if item["profile"]["files_found"] == 0:
            decision = "discard"
            reasons.append("sin archivos locales legibles")
        elif item["profile"]["errors"] and not item["profile"]["files_profiled"]:
            decision = "discard"
            reasons.append("errores de lectura en todos los ficheros")
        elif support_only_dataset and item["score"] >= 35.0:
            decision = "support_only"
            reasons.append("solo aporta contexto barcelona-año; no diferencia zonas")
        elif dataset_is_experimental_only(str(dataset_id)) and item["score"] >= 35.0:
            decision = "experimental"
            reasons.append("dataset util pero aun sin codificacion estable para el modelo")
        elif primary_for and item["score"] >= 55.0:
            decision = "primary"
            reasons.append("mejor fuente para al menos una feature demografica")
        elif backup_for and item["score"] >= 45.0:
            decision = "backup"
            reasons.append("fuente de respaldo valida para features demograficas")
        elif any(feature in DEMOGRAFIA_CANDIDATE_FEATURES for feature in intended) and item["score"] >= 45.0:
            decision = "experimental"
            reasons.append("util para candidatas nuevas pero aun fuera del core del modelo")
        elif item["score"] >= 50.0:
            decision = "experimental"
            reasons.append("fuente util pero no prioritaria frente a alternativas mejores")
        else:
            decision = "discard"
            reasons.append("granularidad/cobertura/utilidad insuficiente para el modelo actual")

        decided.append(
            {
                "dataset_id": dataset_id,
                "provider": item["provider"],
                "title": item["title"],
                "priority": item["priority"],
                "decision": decision,
                "score": item["score"],
                "score_breakdown": item["score_breakdown"],
                "intended_features": intended,
                "primary_for_features": primary_for,
                "backup_for_features": backup_for,
                "granularity": item["profile"]["granularity_detected"],
                "geo_keys": item["profile"]["geo_keys"],
                "row_count_estimate": item["profile"]["row_count_estimate"],
                "files_found": item["profile"]["files_found"],
                "files_profiled": item["profile"]["files_profiled"],
                "sample_files": item["profile"]["sample_files"],
                "non_null_ratio": item["profile"]["non_null_ratio"],
                "years": item["profile"]["years"],
                "errors": item["profile"]["errors"],
                "reasons": reasons,
            }
        )

    summary = {
        "total": len(decided),
        "primary": sum(1 for item in decided if item["decision"] == "primary"),
        "backup": sum(1 for item in decided if item["decision"] == "backup"),
        "support_only": sum(1 for item in decided if item["decision"] == "support_only"),
        "experimental": sum(1 for item in decided if item["decision"] == "experimental"),
        "discard": sum(1 for item in decided if item["decision"] == "discard"),
    }
    return {"datasets": sorted(decided, key=lambda item: (item["decision"], -item["score"], item["dataset_id"])), "summary": summary}


def _base_xgb_params(y: np.ndarray) -> dict[str, Any]:
    params = dict(_DEFAULT_PARAMS)
    n_neg = int((y == 0).sum())
    n_pos = int((y == 1).sum())
    params["scale_pos_weight"] = round(n_neg / n_pos, 3) if n_pos > 0 else 1.0
    return params


def _classification_metrics(y: np.ndarray, probs: np.ndarray) -> dict[str, float]:
    if len(np.unique(y)) < 2:
        return {"roc_auc": 0.5, "pr_auc": float(y.mean()) if len(y) else 0.0}
    return {
        "roc_auc": round(float(roc_auc_score(y, probs)), 4),
        "pr_auc": round(float(average_precision_score(y, probs)), 4),
    }


def _oof_predictions(X: np.ndarray, y: np.ndarray) -> tuple[np.ndarray, dict[str, float]]:
    probs = np.zeros(len(y), dtype=float)
    if len(np.unique(y)) < 2:
        probs.fill(float(y.mean()) if len(y) else 0.5)
        return probs, _classification_metrics(y, probs)

    params = _base_xgb_params(y)
    splitter = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    for train_idx, val_idx in splitter.split(X, y):
        model = xgb.XGBClassifier(**params)
        model.fit(X[train_idx], y[train_idx], verbose=False)
        probs[val_idx] = model.predict_proba(X[val_idx])[:, 1]
    return probs, _classification_metrics(y, probs)


def _label_issue_scores(y: np.ndarray, probs: np.ndarray) -> tuple[np.ndarray, list[str]]:
    warnings: list[str] = []
    pred_probs = np.column_stack([1.0 - probs, probs])
    try:
        from cleanlab.rank import get_label_quality_scores

        quality = get_label_quality_scores(labels=y, pred_probs=pred_probs)
        scores = np.clip(1.0 - np.asarray(quality, dtype=float), 0.0, 1.0)
        return scores, warnings
    except Exception as exc:
        warnings.append(f"cleanlab no disponible o fallo al ejecutarse: {exc}")
        return np.abs(y.astype(float) - probs), warnings


def _robust_outlier_scores(X: np.ndarray, demography_features: Sequence[str]) -> np.ndarray:
    if not demography_features:
        return np.zeros(X.shape[0], dtype=float)

    X_demo = slice_feature_matrix(X, FEATURE_NAMES, list(demography_features))
    X_demo = np.asarray(X_demo, dtype=float)
    medians = np.nanmedian(X_demo, axis=0)
    mad = np.nanmedian(np.abs(X_demo - medians), axis=0)
    mad = np.where(mad == 0, 1e-6, mad)
    robust_z = np.abs((X_demo - medians) / (1.4826 * mad))
    max_z = np.nanmax(robust_z, axis=1)
    extreme_ratio = np.nanmean(robust_z > 4.0, axis=1)
    return np.clip((max_z / 8.0) * 0.7 + extreme_ratio * 0.3, 0.0, 1.0)


def _duplicate_scores(meta: pd.DataFrame, y: np.ndarray) -> np.ndarray:
    if meta.empty:
        return np.zeros(len(y), dtype=float)

    keys = meta[["zona_id", "sector", "fecha_apertura"]].astype(str).agg("|".join, axis=1)
    counts = keys.value_counts()
    duplicate_groups = {key for key, count in counts.items() if count > 1}
    scores = np.zeros(len(y), dtype=float)

    for key in duplicate_groups:
        idx = np.where(keys.to_numpy() == key)[0]
        labels = set(int(y[position]) for position in idx)
        scores[idx] = 1.0 if len(labels) > 1 else 0.75

    return scores


def _build_row_issues(
    meta: pd.DataFrame,
    audit: pd.DataFrame,
    y: np.ndarray,
    probs: np.ndarray,
    X: np.ndarray,
) -> tuple[pd.DataFrame, list[str]]:
    label_scores, warnings = _label_issue_scores(y, probs)
    outlier_scores = _robust_outlier_scores(X, DEMOGRAFIA_SELECTABLE_EXISTING_FEATURES)
    duplicate_scores = _duplicate_scores(meta, y)

    row_issues = pd.concat([meta.reset_index(drop=True), audit.reset_index(drop=True)], axis=1)
    row_issues["pred_prob_oof"] = np.round(probs, 6)
    row_issues["label_issue_score"] = np.round(label_scores, 6)
    row_issues["outlier_score"] = np.round(outlier_scores, 6)
    row_issues["duplicate_score"] = np.round(duplicate_scores, 6)

    actions: list[str] = []
    reasons: list[str] = []
    for _, row in row_issues.iterrows():
        row_reasons: list[str] = []
        if float(row["label_issue_score"]) >= _ROW_DROP_LABEL_THRESHOLD:
            row_reasons.append("label_issue_severo")
        if float(row["outlier_score"]) >= _ROW_DROP_OUTLIER_THRESHOLD:
            row_reasons.append("outlier_demografico")
        if float(row["duplicate_score"]) >= 0.95:
            row_reasons.append("duplicado_conflictivo")

        if row_reasons:
            action = "drop"
        elif (
            max(
                float(row["label_issue_score"]),
                float(row["outlier_score"]),
                float(row["duplicate_score"]),
            ) >= _ROW_REVIEW_THRESHOLD
            or float(row.get("demografia_missing_ratio", 0.0)) >= 0.50
        ):
            action = "review"
            if not row_reasons:
                row_reasons.append("senal_media_de_riesgo")
        else:
            action = "keep"

        actions.append(action)
        reasons.append("|".join(row_reasons) if row_reasons else "")

    row_issues["action"] = actions
    row_issues["reasons"] = reasons
    return row_issues, warnings


def _feature_missing_ratios(audit: pd.DataFrame) -> dict[str, float]:
    ratios: dict[str, float] = {}
    for feature in DEMOGRAFIA_SELECTABLE_EXISTING_FEATURES:
        column = f"{feature}_present"
        if column not in audit.columns or audit.empty:
            ratios[feature] = 0.0
            continue
        present = audit[column].astype(bool)
        ratios[feature] = round(float(1.0 - present.mean()), 4)
    return ratios


def _feature_source_support(source_decisions: Sequence[Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
    support: dict[str, dict[str, Any]] = defaultdict(lambda: {"approved": False, "governed": False, "datasets": []})
    for entry in source_decisions:
        for feature in entry.get("intended_features") or []:
            bucket = support[feature]
            bucket["governed"] = True
            bucket["datasets"].append({"dataset_id": entry["dataset_id"], "decision": entry["decision"]})
            if entry["decision"] in _APPROVED_SOURCE_DECISIONS:
                bucket["approved"] = True
    return support


def _select_demography_features(
    X: np.ndarray,
    y: np.ndarray,
    audit: pd.DataFrame,
    source_decisions: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    feature_missing = _feature_missing_ratios(audit)
    source_support = _feature_source_support(source_decisions)
    pre_drops: dict[str, list[str]] = {}

    for feature in DEMOGRAFIA_SELECTABLE_EXISTING_FEATURES:
        reasons: list[str] = []
        feature_idx = FEATURE_NAMES.index(feature)
        if feature_missing.get(feature, 0.0) > 0.70:
            reasons.append("cobertura_util_muy_baja")
        if float(np.var(X[:, feature_idx])) <= 1e-12:
            reasons.append("varianza_cero")
        support = source_support.get(feature)
        if support and support["governed"] and not support["approved"]:
            reasons.append("solo_depende_de_fuentes_descartadas")
        if reasons:
            pre_drops[feature] = reasons

    candidate_features = [
        feature
        for feature in DEMOGRAFIA_BATCH_FINAL_FEATURES
        if feature not in pre_drops
    ]
    selected_by_rfecv: list[str] = []
    rfecv_best_score: Optional[float] = None
    rfecv_curve: list[float] = []

    if candidate_features:
        baseline_features = [
            feature
            for feature in DEMOGRAFIA_DEFAULT_ACTIVE_FEATURES
            if feature not in pre_drops
        ]
        candidate_feature_names = list(_FIXED_MODEL_FEATURES) + baseline_features + candidate_features
        X_candidate = slice_feature_matrix(X, FEATURE_NAMES, candidate_feature_names)
        fixed_count = len(_FIXED_MODEL_FEATURES) + len(baseline_features)
        selector = RFECV(
            estimator=_FrozenFixedFeatureEstimator(
                fixed_feature_count=fixed_count,
                xgb_params=_base_xgb_params(y),
            ),
            step=1,
            cv=StratifiedKFold(n_splits=5, shuffle=True, random_state=42),
            scoring="roc_auc",
            min_features_to_select=max(fixed_count, 1),
        )
        selector.fit(X_candidate, y)
        support_mask = selector.support_[fixed_count:]
        selected_by_rfecv = [
            feature
            for feature, keep in zip(candidate_features, support_mask)
            if keep
        ]
        scores = selector.cv_results_.get("mean_test_score")
        if scores is not None:
            rfecv_curve = [round(float(value), 4) for value in scores]
            rfecv_best_score = round(float(np.max(scores)), 4)

    decisions: list[dict[str, Any]] = []
    proposed_active_demography_features: list[str] = []

    for feature in DEMOGRAFIA_SELECTABLE_EXISTING_FEATURES:
        if feature in pre_drops:
            decision = "discard"
            reasons = list(pre_drops.get(feature, []))
            basis = "hard_drop"
        elif feature in DEMOGRAFIA_DEFAULT_ACTIVE_FEATURES:
            decision = "use"
            reasons = []
            basis = "default_active_preserved"
            if feature in DEMOGRAFIA_MODEL_CORE_FEATURES:
                reasons.append("core_actual_mantenido_por_estabilidad")
            elif feature == "indice_potencial_consumo":
                reasons.append("feature_actual_mantenida_como_parte_del_baseline")
        elif feature in selected_by_rfecv:
            decision = "use"
            reasons = ["rfecv_selecciona_feature_nueva_sobre_el_baseline"]
            basis = "rfecv_selected_batch_bcn"
        else:
            decision = "review"
            reasons = ["feature_nueva_no_seleccionada_sobre_el_baseline_actual"]
            basis = "candidate_not_selected"

        if decision == "use":
            proposed_active_demography_features.append(feature)

        decisions.append(
            {
                "feature": feature,
                "decision": decision,
                "basis": basis,
                "missing_ratio": feature_missing.get(feature, 0.0),
                "source_support": source_support.get(feature, {}),
                "reasons": reasons,
            }
        )

    for feature in DEMOGRAFIA_CANDIDATE_FEATURES:
        support = source_support.get(feature, {})
        if support.get("approved"):
            decision = "review"
            reasons = ["fuente_aprobada_pero_feature_aun_no_materializada_en_feature_vector"]
        elif support.get("governed"):
            decision = "discard"
            reasons = ["solo_disponible_en_fuentes_descartadas"]
        else:
            decision = "review"
            reasons = ["sin_fuente_gobernada_explicita_en_el_manifest"]
        decisions.append(
            {
                "feature": feature,
                "decision": decision,
                "basis": "not_materialized",
                "missing_ratio": None,
                "source_support": support,
                "reasons": reasons,
            }
        )

    proposed_active_demography_features = [
        feature
        for feature in DEMOGRAFIA_SELECTABLE_EXISTING_FEATURES
        if feature in proposed_active_demography_features
    ]
    return {
        "active_demography_features": list(DEMOGRAFIA_DEFAULT_ACTIVE_FEATURES),
        "proposed_active_demography_features": proposed_active_demography_features,
        "model_feature_names": build_model_feature_names(proposed_active_demography_features),
        "decisions": decisions,
        "rfecv_selected_demography_features": selected_by_rfecv,
        "rfecv_best_score": rfecv_best_score,
        "rfecv_curve": rfecv_curve,
        "pre_drops": pre_drops,
    }


def _evaluate_feature_set(X: np.ndarray, y: np.ndarray, active_demography_features: Sequence[str]) -> dict[str, Any]:
    model_feature_names = build_model_feature_names(active_demography_features)
    X_model = slice_feature_matrix(X, FEATURE_NAMES, model_feature_names)
    probs, metrics = _oof_predictions(np.asarray(X_model, dtype=np.float32), y)
    return {
        "model_feature_names": model_feature_names,
        "demografia_feature_names": list(active_demography_features),
        "n_features_modelo": len(model_feature_names),
        **metrics,
    }


def _apply_baseline_guardrail(
    feature_selection: dict[str, Any],
    baseline_metrics: Mapping[str, Any],
    candidate_metrics: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], list[str]]:
    warnings: list[str] = []
    active = list(feature_selection.get("proposed_active_demography_features") or [])
    selected_new_features = [
        feature
        for feature in active
        if feature in DEMOGRAFIA_BATCH_FINAL_FEATURES
    ]
    baseline_auc = float(baseline_metrics.get("roc_auc") or 0.0)
    candidate_auc = float(candidate_metrics.get("roc_auc") or 0.0)
    auc_delta = round(candidate_auc - baseline_auc, 4)

    allow_candidate = bool(selected_new_features) and auc_delta >= -0.002
    final_active = (
        active
        if allow_candidate
        else list(DEMOGRAFIA_DEFAULT_ACTIVE_FEATURES)
    )

    if not selected_new_features:
        warnings.append("ninguna_feature_nueva_supero_rfecv; se mantiene el baseline demografico actual")
    elif auc_delta < -0.002:
        warnings.append(
            f"candidate_demografico_revertido_por_guardrail_auc_delta={auc_delta:.4f}"
        )

    for item in feature_selection["decisions"]:
        feature = item["feature"]
        if feature not in DEMOGRAFIA_BATCH_FINAL_FEATURES:
            continue
        if feature in selected_new_features and not allow_candidate:
            item["decision"] = "review"
            item["basis"] = "guardrail_reverted"
            item["reasons"] = list(item.get("reasons") or []) + [
                "mejora_insuficiente_frente_al_baseline_actual"
            ]

    feature_selection["selected_new_demography_features"] = selected_new_features
    feature_selection["active_demography_features"] = final_active
    feature_selection["model_feature_names"] = build_model_feature_names(final_active)
    feature_selection["guardrail"] = {
        "baseline_roc_auc": baseline_auc,
        "candidate_roc_auc": candidate_auc,
        "roc_auc_delta": auc_delta,
        "selected_new_features": selected_new_features,
        "accepted": allow_candidate,
    }
    return feature_selection, {
        **candidate_metrics,
        "demografia_feature_names": list(active),
        "selected_new_demography_features": selected_new_features,
    }, warnings


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def _write_csv(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)


def _publish_artifact(
    run_dir: Path,
    latest_dir: Path,
    name: str,
    *,
    frame: Optional[pd.DataFrame] = None,
    payload: Optional[Mapping[str, Any]] = None,
) -> None:
    if frame is not None:
        _write_csv(run_dir / name, frame)
        _write_csv(latest_dir / name, frame)
        return
    if payload is not None:
        _write_json(run_dir / name, payload)
        _write_json(latest_dir / name, payload)


async def ejecutar_readiness(
    sector: Optional[str] = None,
    mode: str = "strict",
    manifest_path: Optional[str] = None,
) -> dict[str, Any]:
    manifest = _load_manifest(manifest_path)
    manifest_entries = [
        entry
        for entry in manifest.get("datasets") or []
        if is_demography_dataset(entry)
    ]
    source_catalog = _decide_source_catalog(manifest_entries)

    X, y, meta, audit = await construir_dataset_auditoria(sector=sector)
    X = np.asarray(X, dtype=np.float32)
    y = np.asarray(y, dtype=np.int32)

    baseline = _evaluate_feature_set(X, y, DEMOGRAFIA_DEFAULT_ACTIVE_FEATURES)
    probs, _ = _oof_predictions(X, y)
    row_issues, warnings = _build_row_issues(meta, audit, y, probs, X)

    strict_mask = row_issues["action"].ne("drop").to_numpy() if mode == "strict" else np.ones(len(row_issues), dtype=bool)
    X_clean = X[strict_mask]
    y_clean = y[strict_mask]
    audit_clean = audit.loc[strict_mask].reset_index(drop=True)

    after_clean = _evaluate_feature_set(X_clean, y_clean, DEMOGRAFIA_DEFAULT_ACTIVE_FEATURES)
    feature_selection = _select_demography_features(X_clean, y_clean, audit_clean, source_catalog["datasets"])
    candidate_metrics = _evaluate_feature_set(
        X_clean,
        y_clean,
        feature_selection["proposed_active_demography_features"],
    )
    feature_selection, guarded_candidate_metrics, guardrail_warnings = _apply_baseline_guardrail(
        feature_selection,
        after_clean,
        candidate_metrics,
    )
    warnings.extend(guardrail_warnings)
    final_metrics = _evaluate_feature_set(
        X_clean,
        y_clean,
        feature_selection["active_demography_features"],
    )

    timestamp = _utc_now().strftime("%Y%m%dT%H%M%SZ")
    sector_slug = _slugify(sector or "todos")
    run_dir = READINESS_ROOT / f"{timestamp}_{sector_slug}_{mode}"
    latest_dir = LATEST_ROOT
    run_dir.mkdir(parents=True, exist_ok=True)
    latest_dir.mkdir(parents=True, exist_ok=True)

    source_decisions_payload = {
        "generated_at": _iso_now(),
        "sector": sector or "todos",
        "mode": mode,
        "summary": source_catalog["summary"],
        "datasets": source_catalog["datasets"],
    }
    feature_decisions_payload = {
        "generated_at": _iso_now(),
        "sector": sector or "todos",
        "mode": mode,
        "active_demography_features": feature_selection["active_demography_features"],
        "model_feature_names": feature_selection["model_feature_names"],
        "rfecv_selected_demography_features": feature_selection["rfecv_selected_demography_features"],
        "rfecv_best_score": feature_selection["rfecv_best_score"],
        "rfecv_curve": feature_selection["rfecv_curve"],
        "selected_new_demography_features": feature_selection.get("selected_new_demography_features", []),
        "guardrail": feature_selection.get("guardrail", {}),
        "decisions": feature_selection["decisions"],
    }
    model_report_payload = {
        "generated_at": _iso_now(),
        "sector": sector or "todos",
        "mode": mode,
        "dataset": {
            "rows_total": int(len(y)),
            "rows_after_strict_drop": int(len(y_clean)),
            "rows_dropped": int(len(y) - len(y_clean)),
        },
        "source_summary": source_catalog["summary"],
        "metrics": {
            "baseline": baseline,
            "after_row_cleaning": after_clean,
            "candidate_selected": guarded_candidate_metrics,
            "final_selected": final_metrics,
        },
        "warnings": warnings,
    }

    _publish_artifact(run_dir, latest_dir, "source_decisions.json", payload=source_decisions_payload)
    _publish_artifact(run_dir, latest_dir, "row_issues.csv", frame=row_issues)
    _publish_artifact(run_dir, latest_dir, "demography_feature_decisions.json", payload=feature_decisions_payload)
    _publish_artifact(run_dir, latest_dir, "demography_model_report.json", payload=model_report_payload)

    return {
        "run_dir": str(run_dir),
        "latest_dir": str(latest_dir),
        "sector": sector or "todos",
        "mode": mode,
        "source_summary": source_catalog["summary"],
        "row_actions": row_issues["action"].value_counts().to_dict(),
        "active_demography_features": feature_selection["active_demography_features"],
        "final_metrics": final_metrics,
        "warnings": warnings,
    }


async def _main() -> None:
    parser = argparse.ArgumentParser(description="Readiness demografico para GeoRetail")
    parser.add_argument("--sector", default=None, help="Sector a auditar. Vacio = todos.")
    parser.add_argument("--mode", default="strict", choices=["strict", "report"], help="Modo strict elimina filas drop antes de seleccionar features.")
    parser.add_argument("--manifest", default=None, help="Ruta opcional al manifest public_data_manifest_latest.json.")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    result = await ejecutar_readiness(sector=args.sector, mode=args.mode, manifest_path=args.manifest)
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    asyncio.run(_main())
