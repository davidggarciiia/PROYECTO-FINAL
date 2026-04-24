"""
scoring/temporal_readiness.py - gate estricto para integrar temporalidad al modelo.

Objetivo:
  - auditar si las señales temporales tienen histórico real y cobertura suficiente;
  - bloquear cualquier promoción si el bloque semanal/live o el estacional falla;
  - dejar artifacts trazables con fallback de fuentes oficiales/institucionales.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.model_selection import StratifiedKFold

from db.conexion import get_db
from scoring.features import FEATURE_NAMES
from scoring.infra.governance import (
    CSV_ROOT,
    TEMPORAL_LATEST_ROOT,
    TEMPORAL_MODELABLE_FEATURES,
    TEMPORAL_READINESS_ROOT,
    TEMPORAL_SEASONAL_FEATURES,
    TEMPORAL_WEEKLY_FEATURES,
    build_model_feature_names,
    default_active_demography_features,
    slice_feature_matrix,
)
from scoring.ml.dataset import construir_dataset
from scoring.ml.train import _DEFAULT_PARAMS

logger = logging.getLogger(__name__)

_AFORAMENTS_ROOT = CSV_ROOT / "aforaments"
_AFORAMENTS_DETAIL_GLOB = "*aforament_detall_valor*.csv"
_TEMPORAL_GUARDRAIL_DELTA = -0.002

_WEEKLY_SHARE_FEATURES = {
    "weekday_midday_share",
    "weekend_evening_share",
    "late_night_share",
}
_WEEKLY_LIFT_FEATURES = {"weekend_lift", "sunday_lift"}
_SEASONAL_LIFT_FEATURES = {
    "seasonality_summer_lift",
    "seasonality_christmas_lift",
    "seasonality_rebajas_lift",
}

_FALLBACK_CANDIDATES: tuple[dict[str, Any], ...] = (
    {
        "source_id": "bcn_trams_peatonales",
        "provider": "barcelona_open_data",
        "scope": "seasonal",
        "fit_score": 0.94,
        "granularidad_espacial": "tramo -> zona",
        "granularidad_temporal": "mensual",
        "profundidad_historica": "alta",
        "cobertura_bcn": "media",
        "licencia_estabilidad": "alta",
        "mapeable_a_zonas": True,
        "anti_leakage_seguro": True,
        "recomendacion": "Fuente prioritaria para seasonality_* con backfill oficial.",
    },
    {
        "source_id": "bsc_vcity_historico",
        "provider": "bsc",
        "scope": "weekly_and_seasonal",
        "fit_score": 0.82,
        "granularidad_espacial": "zona/celda",
        "granularidad_temporal": "diaria o agregable",
        "profundidad_historica": "media",
        "cobertura_bcn": "alta",
        "licencia_estabilidad": "media",
        "mapeable_a_zonas": True,
        "anti_leakage_seguro": True,
        "recomendacion": "Buen complemento académico si se habilita histórico trazable.",
    },
    {
        "source_id": "atm_gtfs_horarios",
        "provider": "atm",
        "scope": "holiday_proxy_transport",
        "fit_score": 0.71,
        "granularidad_espacial": "parada/linea",
        "granularidad_temporal": "servicio por dia_tipo",
        "profundidad_historica": "media",
        "cobertura_bcn": "alta",
        "licencia_estabilidad": "alta",
        "mapeable_a_zonas": True,
        "anti_leakage_seguro": True,
        "recomendacion": "Refuerza festivos/transporte, no resuelve por sí solo popular_times histórico.",
    },
)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso_now() -> str:
    return _utc_now().isoformat().replace("+00:00", "Z")


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def _publish_json(run_dir: Path, latest_dir: Path, name: str, payload: Mapping[str, Any]) -> None:
    _write_json(run_dir / name, payload)
    _write_json(latest_dir / name, payload)


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _non_null_ratio(frame: pd.DataFrame, column: str) -> float:
    if frame.empty or column not in frame.columns:
        return 0.0
    return float(frame[column].notna().mean())


def _plausibility_summary(frame: pd.DataFrame, features: Sequence[str]) -> dict[str, Any]:
    violations: dict[str, int] = {}
    checks = 0
    for feature in features:
        if feature not in frame.columns or frame.empty:
            continue
        series = pd.to_numeric(frame[feature], errors="coerce").dropna()
        if series.empty:
            continue
        if feature in _WEEKLY_SHARE_FEATURES:
            invalid = int(((series < 0.0) | (series > 1.0)).sum())
        elif feature in _WEEKLY_LIFT_FEATURES:
            invalid = int(((series < 0.25) | (series > 4.0)).sum())
        elif feature == "holiday_proxy_score":
            invalid = int(((series < 0.0) | (series > 100.0)).sum())
        elif feature == "temporal_confianza":
            invalid = int(((series < 0.0) | (series > 1.0)).sum())
        elif feature in _SEASONAL_LIFT_FEATURES:
            invalid = int(((series < 0.25) | (series > 4.0)).sum())
        elif feature == "seasonality_volatility":
            invalid = int(((series < 0.0) | (series > 3.0)).sum())
        elif feature == "seasonality_peak_concentration":
            invalid = int(((series < 0.5) | (series > 6.0)).sum())
        else:
            invalid = 0
        violations[feature] = invalid
        checks += len(series)
    invalid_total = sum(violations.values())
    return {
        "checks": int(checks),
        "invalid_total": int(invalid_total),
        "ok": invalid_total == 0,
        "violations_by_feature": violations,
    }


def _base_xgb_params(y: np.ndarray) -> dict[str, Any]:
    params = dict(_DEFAULT_PARAMS)
    n_neg = int((y == 0).sum())
    n_pos = int((y == 1).sum())
    params["scale_pos_weight"] = round(n_neg / n_pos, 3) if n_pos > 0 else 1.0
    return params


def _classification_metrics(y: np.ndarray, probs: np.ndarray) -> dict[str, float]:
    if len(np.unique(y)) < 2:
        mean = float(y.mean()) if len(y) else 0.0
        return {"roc_auc": 0.5, "pr_auc": mean}
    return {
        "roc_auc": round(float(roc_auc_score(y, probs)), 4),
        "pr_auc": round(float(average_precision_score(y, probs)), 4),
    }


def _oof_predictions(X: np.ndarray, y: np.ndarray) -> tuple[np.ndarray, dict[str, float]]:
    probs = np.zeros(len(y), dtype=float)
    if len(np.unique(y)) < 2:
        probs.fill(float(y.mean()) if len(y) else 0.5)
        return probs, _classification_metrics(y, probs)

    splitter = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    params = _base_xgb_params(y)
    for train_idx, val_idx in splitter.split(X, y):
        model = xgb.XGBClassifier(**params)
        model.fit(X[train_idx], y[train_idx], verbose=False)
        probs[val_idx] = model.predict_proba(X[val_idx])[:, 1]
    return probs, _classification_metrics(y, probs)


def _evaluate_feature_set(
    X: np.ndarray,
    y: np.ndarray,
    *,
    temporal_features: Sequence[str],
) -> dict[str, Any]:
    model_feature_names = build_model_feature_names(
        default_active_demography_features(),
        temporal_features,
    )
    X_model = slice_feature_matrix(X, FEATURE_NAMES, model_feature_names)
    probs, metrics = _oof_predictions(np.asarray(X_model, dtype=np.float32), y)
    return {
        "model_feature_names": model_feature_names,
        "temporal_feature_names": list(temporal_features),
        "n_features_modelo": len(model_feature_names),
        **metrics,
    }


async def _load_temporal_snapshot_frame(
    meta: pd.DataFrame,
    features: Sequence[str],
) -> pd.DataFrame:
    if meta.empty:
        return pd.DataFrame(columns=["idx", "snapshot_fecha", *features])

    columns_sql = ",\n                ".join(f"vz.{feature}" for feature in features)
    zone_ids = meta["zona_id"].astype(str).tolist()
    fechas = pd.to_datetime(meta["fecha_apertura"]).dt.date.tolist()

    query = f"""
        WITH input AS (
            SELECT
                (ordinality - 1)::int AS idx,
                zone_id,
                fecha_ref
            FROM unnest($1::text[], $2::date[]) WITH ORDINALITY AS t(zone_id, fecha_ref, ordinality)
        )
        SELECT
            i.idx,
            snap.snapshot_fecha,
            {", ".join(f"snap.{feature}" for feature in features)}
        FROM input i
        LEFT JOIN LATERAL (
            SELECT
                vz.fecha AS snapshot_fecha,
                {columns_sql}
            FROM v_variables_zona vz
            WHERE vz.zona_id = i.zone_id
              AND vz.fecha <= i.fecha_ref
            ORDER BY vz.fecha DESC
            LIMIT 1
        ) snap ON TRUE
        ORDER BY i.idx
    """

    async with get_db() as conn:
        rows = await conn.fetch(query, zone_ids, fechas)
    frame = pd.DataFrame([dict(row) for row in rows])
    if frame.empty:
        frame = pd.DataFrame(columns=["idx", "snapshot_fecha", *features])
    return frame


async def _load_vianants_monthly_frame(zona_ids: Sequence[str]) -> pd.DataFrame:
    zones = sorted({str(zona_id) for zona_id in zona_ids if zona_id})
    if not zones:
        return pd.DataFrame(columns=["zona_id", "anyo", "mes", "bucket_date", "intensidad"])

    query = """
        SELECT
            zona_id,
            anyo,
            mes,
            make_date(anyo, mes, 1) AS bucket_date,
            AVG(intensitat)::float AS intensidad
        FROM trams_peatonales
        WHERE zona_id = ANY($1)
          AND anyo IS NOT NULL
          AND mes IS NOT NULL
          AND mes BETWEEN 1 AND 12
          AND intensitat IS NOT NULL
          AND intensitat > 0
        GROUP BY zona_id, anyo, mes
        ORDER BY zona_id, anyo, mes
    """
    async with get_db() as conn:
        rows = await conn.fetch(query, zones)
    return pd.DataFrame([dict(row) for row in rows])


async def _load_weekly_source_stats(zona_ids: Sequence[str]) -> dict[str, Any]:
    detail_years: list[int] = []
    for path in sorted(_AFORAMENTS_ROOT.glob(_AFORAMENTS_DETAIL_GLOB)):
        try:
            detail_years.append(int(path.name[:4]))
        except ValueError:
            continue

    current_zone_coverage = 0.0
    try:
        zones = sorted({str(zona_id) for zona_id in zona_ids if zona_id})
        if zones:
            async with get_db() as conn:
                covered = await conn.fetchval(
                    """
                    SELECT COUNT(DISTINCT zona_id)
                    FROM negocios_activos
                    WHERE es_activo = TRUE
                      AND zona_id = ANY($1)
                      AND popular_times IS NOT NULL
                    """,
                    zones,
                )
            current_zone_coverage = round(float((covered or 0) / len(zones)), 4)
    except Exception as exc:
        logger.info("No se pudo calcular cobertura actual de popular_times: %s", exc)

    return {
        "popular_times_historical": False,
        "popular_times_current_zone_coverage": current_zone_coverage,
        "aforaments_detail_years": detail_years,
        "aforaments_historical_detail": len(set(detail_years)) >= 2,
    }


def _evaluate_weekly_gate(
    snapshot_frame: pd.DataFrame,
    source_stats: Mapping[str, Any],
    *,
    min_coverage: float = 0.60,
) -> dict[str, Any]:
    coverage_by_feature = {
        feature: round(_non_null_ratio(snapshot_frame, feature), 4)
        for feature in TEMPORAL_WEEKLY_FEATURES
    }
    plausibility = _plausibility_summary(snapshot_frame, TEMPORAL_WEEKLY_FEATURES)
    snapshot_coverage = round(
        float(np.mean(list(coverage_by_feature.values()))) if coverage_by_feature else 0.0,
        4,
    )

    reasons: list[str] = []
    if not source_stats.get("popular_times_historical", False):
        reasons.append("popular_times no tiene snapshot histórico utilizable para train")
    if not source_stats.get("aforaments_historical_detail", False):
        reasons.append("aforaments detallado no cubre suficientes años para reconstrucción histórica")
    if snapshot_coverage < min_coverage:
        reasons.append(f"cobertura histórica semanal insuficiente ({snapshot_coverage:.2%} < {min_coverage:.0%})")
    if not plausibility["ok"]:
        reasons.append("hay valores semanales fuera de rangos plausibles")

    status = "pass" if not reasons else "fail"
    return {
        "status": status,
        "historical_support": bool(source_stats.get("popular_times_historical")) and bool(source_stats.get("aforaments_historical_detail")),
        "coverage_threshold": min_coverage,
        "snapshot_coverage": snapshot_coverage,
        "coverage_by_feature": coverage_by_feature,
        "plausibility": plausibility,
        "reasons": reasons,
        "source_stats": dict(source_stats),
    }


def _evaluate_seasonal_gate(
    snapshot_frame: pd.DataFrame,
    monthly_frame: pd.DataFrame,
    *,
    dataset_zone_ids: Sequence[str],
    min_snapshot_coverage: float = 0.30,
) -> dict[str, Any]:
    coverage_by_feature = {
        feature: round(_non_null_ratio(snapshot_frame, feature), 4)
        for feature in TEMPORAL_SEASONAL_FEATURES
    }
    snapshot_coverage = round(
        float(np.mean(list(coverage_by_feature.values()))) if coverage_by_feature else 0.0,
        4,
    )

    zones = sorted({str(zona_id) for zona_id in dataset_zone_ids if zona_id})
    if monthly_frame.empty:
        zone_bucket_ratio_12 = 0.0
        zone_bucket_ratio_6 = 0.0
        zones_with_any_monthly = 0
    else:
        counts = (
            monthly_frame.groupby("zona_id")["bucket_date"]
            .nunique()
            .to_dict()
        )
        zones_with_any_monthly = int(sum(1 for value in counts.values() if value > 0))
        denom = max(len(zones), 1)
        zone_bucket_ratio_12 = round(
            sum(1 for zona_id in zones if counts.get(zona_id, 0) >= 12) / denom,
            4,
        )
        zone_bucket_ratio_6 = round(
            sum(1 for zona_id in zones if counts.get(zona_id, 0) >= 6) / denom,
            4,
        )

    plausibility = _plausibility_summary(snapshot_frame, TEMPORAL_SEASONAL_FEATURES)
    reasons: list[str] = []
    if snapshot_coverage < min_snapshot_coverage:
        reasons.append(
            f"cobertura estacional histórica insuficiente ({snapshot_coverage:.2%} < {min_snapshot_coverage:.0%})"
        )
    if zone_bucket_ratio_12 < min_snapshot_coverage and zone_bucket_ratio_6 < min_snapshot_coverage:
        reasons.append("trams_peatonales no aporta suficiente profundidad mensual por zona")
    if not plausibility["ok"]:
        reasons.append("hay valores estacionales fuera de rangos plausibles")

    status = "pass" if not reasons else "fail"
    return {
        "status": status,
        "historical_support": snapshot_coverage >= min_snapshot_coverage,
        "coverage_threshold": min_snapshot_coverage,
        "snapshot_coverage": snapshot_coverage,
        "coverage_by_feature": coverage_by_feature,
        "zones_with_any_monthly": zones_with_any_monthly,
        "zone_bucket_ratio_12": zone_bucket_ratio_12,
        "zone_bucket_ratio_6": zone_bucket_ratio_6,
        "plausibility": plausibility,
        "reasons": reasons,
    }


def _build_fallback_shortlist(
    weekly_gate: Mapping[str, Any],
    seasonal_gate: Mapping[str, Any],
) -> dict[str, Any]:
    shortlist = [dict(candidate) for candidate in _FALLBACK_CANDIDATES]
    recommendation = shortlist[0]["source_id"]
    if weekly_gate.get("status") != "pass" and seasonal_gate.get("status") == "pass":
        recommendation = "bsc_vcity_historico"
    return {
        "shortlist": shortlist,
        "recommended_source_id": recommendation,
        "notes": [
            "popular_times actual sigue siendo útil para score/explicación, no para train histórico",
            "el backfill oficial de vianants es la opción más sólida para seasonality_*",
        ],
    }


async def ejecutar_readiness(
    sector: Optional[str] = None,
    mode: str = "strict",
) -> dict[str, Any]:
    if mode != "strict":
        raise ValueError("Temporal readiness solo soporta mode='strict'")

    X, y, meta = await construir_dataset(sector=sector)
    X = np.asarray(X, dtype=np.float32)
    y = np.asarray(y, dtype=np.int32)

    warnings: list[str] = []
    try:
        snapshot_frame = await _load_temporal_snapshot_frame(meta, TEMPORAL_MODELABLE_FEATURES)
    except Exception as exc:
        logger.warning("No se pudo auditar snapshots temporales: %s", exc)
        snapshot_frame = pd.DataFrame(columns=["idx", "snapshot_fecha", *TEMPORAL_MODELABLE_FEATURES])
        warnings.append(f"snapshot_temporal_audit_failed: {exc}")

    try:
        monthly_frame = await _load_vianants_monthly_frame(meta["zona_id"].astype(str).tolist())
    except Exception as exc:
        logger.warning("No se pudo auditar trams_peatonales: %s", exc)
        monthly_frame = pd.DataFrame(columns=["zona_id", "anyo", "mes", "bucket_date", "intensidad"])
        warnings.append(f"vianants_monthly_audit_failed: {exc}")

    try:
        weekly_source_stats = await _load_weekly_source_stats(meta["zona_id"].astype(str).tolist())
    except Exception as exc:
        weekly_source_stats = {
            "popular_times_historical": False,
            "popular_times_current_zone_coverage": 0.0,
            "aforaments_detail_years": [],
            "aforaments_historical_detail": False,
        }
        warnings.append(f"weekly_source_stats_failed: {exc}")

    weekly_gate = _evaluate_weekly_gate(snapshot_frame, weekly_source_stats)
    seasonal_gate = _evaluate_seasonal_gate(
        snapshot_frame,
        monthly_frame,
        dataset_zone_ids=meta["zona_id"].astype(str).tolist(),
    )

    gate_pass = weekly_gate["status"] == "pass" and seasonal_gate["status"] == "pass"
    baseline_metrics = _evaluate_feature_set(X, y, temporal_features=[])

    if gate_pass:
        candidate_temporal_features = list(TEMPORAL_MODELABLE_FEATURES)
        candidate_metrics = _evaluate_feature_set(X, y, temporal_features=candidate_temporal_features)
        auc_delta = round(
            float(candidate_metrics["roc_auc"]) - float(baseline_metrics["roc_auc"]),
            4,
        )
        accepted = auc_delta >= _TEMPORAL_GUARDRAIL_DELTA
        if not accepted:
            warnings.append(
                f"candidate_temporal_revertido_por_guardrail_auc_delta={auc_delta:.4f}"
            )
        active_temporal_features = candidate_temporal_features if accepted else []
    else:
        candidate_metrics = None
        auc_delta = None
        accepted = False
        active_temporal_features = []

    timestamp = _utc_now().strftime("%Y%m%dT%H%M%SZ")
    run_dir = TEMPORAL_READINESS_ROOT / timestamp
    latest_dir = TEMPORAL_LATEST_ROOT

    gates_payload = {
        "generated_at": _iso_now(),
        "mode": mode,
        "sector": sector or "todos",
        "dataset_rows": int(len(meta)),
        "dataset_zones": int(meta["zona_id"].astype(str).nunique()) if not meta.empty else 0,
        "weekly_live_gate": weekly_gate,
        "seasonal_gate": seasonal_gate,
        "overall_gate": {
            "status": "pass" if gate_pass else "fail",
            "policy": "all_or_nothing",
            "accepted": accepted,
            "active_temporal_features": active_temporal_features,
        },
        "active_temporal_features": active_temporal_features,
        "warnings": warnings,
    }

    source_audit = {
        "generated_at": _iso_now(),
        "current_sources": {
            "weekly_live": {
                "aforaments": weekly_source_stats,
                "popular_times": {
                    "historical_supported": False,
                    "current_zone_coverage": weekly_source_stats.get("popular_times_current_zone_coverage", 0.0),
                },
            },
            "seasonal": {
                "trams_peatonales_monthly_rows": int(len(monthly_frame)),
                "dataset_zone_ratio_12_months": seasonal_gate["zone_bucket_ratio_12"],
                "dataset_zone_ratio_6_months": seasonal_gate["zone_bucket_ratio_6"],
            },
        },
        "fallback": _build_fallback_shortlist(weekly_gate, seasonal_gate),
    }

    model_report = {
        "generated_at": _iso_now(),
        "sector": sector or "todos",
        "baseline": baseline_metrics,
        "candidate": candidate_metrics,
        "guardrail": {
            "roc_auc_delta": auc_delta,
            "threshold": _TEMPORAL_GUARDRAIL_DELTA,
            "accepted": accepted,
            "promotion_allowed": gate_pass and accepted,
        },
        "final_model_feature_names": build_model_feature_names(
            default_active_demography_features(),
            active_temporal_features,
        ),
        "active_temporal_features": active_temporal_features,
    }

    _publish_json(run_dir, latest_dir, "temporal_feature_gates.json", gates_payload)
    _publish_json(run_dir, latest_dir, "temporal_source_audit.json", source_audit)
    _publish_json(run_dir, latest_dir, "temporal_model_report.json", model_report)

    return {
        "run_dir": str(run_dir),
        "overall_gate": gates_payload["overall_gate"],
        "warnings": warnings,
    }


async def _main() -> None:
    parser = argparse.ArgumentParser(description="Gate estricto de temporalidad para GeoRetail")
    parser.add_argument("--sector", default=None, help="Sector a auditar. Vacío = todos.")
    parser.add_argument("--mode", default="strict", help="Modo de calidad. Solo 'strict'.")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    result = await ejecutar_readiness(sector=args.sector, mode=args.mode)
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    asyncio.run(_main())
