# ------------------------------------
# models.py
#
# Dieses Modul enthaelt den sklearn-Teil der hierarchischen ML-Pipeline.
# Es trainiert Zielmodelle fuer Starten, Einwechslung, Minuten,
# Rohpunkte und On-Top-Punkte auf derselben Feature-Matrix.
#
# Outputs
# ------------------------------------
# 1) SklearnHierarchicalBundle mit trainierten Estimatoren
# 2) CV-Metriken pro Zielvariable und Modellkandidat
# 3) Hierarchische Vorhersagen fuer Power-BI/Gold-Tabellen
#
# Usage
# ------------------------------------
# - bundle = fit_sklearn_hierarchical_models(prepared, frame, time_splits=splits)
# - pred = predict_with_sklearn_bundle(bundle, prepared)
# - report = evaluate_hierarchical_predictions(pred, frame)
# ------------------------------------

from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Any


@dataclass(frozen=True)
class SklearnTargetResult:
    best_model_name: str
    objective: str
    score: float


@dataclass(frozen=True)
class SklearnHierarchicalBundle:
    feature_names: list[str]
    estimators: dict[str, Any]
    target_results: dict[str, SklearnTargetResult]
    metadata: dict[str, Any]


def _require_numpy() -> Any:
    try:
        import numpy as np
    except ModuleNotFoundError as exc:  # pragma: no cover - runtime guard
        raise RuntimeError("numpy ist nicht installiert. Bitte ML-Abhaengigkeiten installieren.") from exc
    return np


def _require_pandas() -> Any:
    try:
        import pandas as pd
    except ModuleNotFoundError as exc:  # pragma: no cover - runtime guard
        raise RuntimeError(
            "pandas ist nicht installiert. Installiere ML-Abhaengigkeiten (pandas, numpy, scikit-learn)."
        ) from exc
    return pd


def _require_sklearn() -> dict[str, Any]:
    try:
        from sklearn.base import clone
        from sklearn.dummy import DummyClassifier, DummyRegressor
        from sklearn.ensemble import HistGradientBoostingClassifier, HistGradientBoostingRegressor, RandomForestClassifier, RandomForestRegressor
        from sklearn.linear_model import LogisticRegression, Ridge
    except ModuleNotFoundError as exc:  # pragma: no cover - runtime guard
        raise RuntimeError(
            "scikit-learn ist nicht installiert. Bitte ML-Abhaengigkeiten installieren."
        ) from exc

    return {
        "clone": clone,
        "DummyClassifier": DummyClassifier,
        "DummyRegressor": DummyRegressor,
        "HistGradientBoostingClassifier": HistGradientBoostingClassifier,
        "HistGradientBoostingRegressor": HistGradientBoostingRegressor,
        "RandomForestClassifier": RandomForestClassifier,
        "RandomForestRegressor": RandomForestRegressor,
        "LogisticRegression": LogisticRegression,
        "Ridge": Ridge,
    }


def _dense_matrix(X: Any) -> Any:
    np = _require_numpy()
    if hasattr(X, "toarray"):
        return np.asarray(X.toarray(), dtype=float)
    return np.asarray(X, dtype=float)


def _safe_binary_target(values: Any) -> Any:
    np = _require_numpy()
    arr = np.asarray(values, dtype=float)
    return (arr >= 0.5).astype(int)


def _regression_metrics(y_true: Any, y_pred: Any) -> dict[str, float]:
    np = _require_numpy()
    residual = np.asarray(y_pred, dtype=float) - np.asarray(y_true, dtype=float)
    abs_err = np.abs(residual)
    sq_err = residual * residual
    return {
        "mae": float(np.mean(abs_err)) if abs_err.size else 0.0,
        "rmse": float(np.sqrt(np.mean(sq_err))) if sq_err.size else 0.0,
        "bias": float(np.mean(residual)) if residual.size else 0.0,
    }


def _brier_score(y_true: Any, y_prob: Any) -> float:
    np = _require_numpy()
    diff = np.asarray(y_prob, dtype=float) - np.asarray(y_true, dtype=float)
    return float(np.mean(diff * diff)) if diff.size else 0.0


def _candidate_regressors(random_state: int) -> dict[str, Any]:
    sk = _require_sklearn()
    return {
        "ridge": sk["Ridge"](alpha=1.0),
        "random_forest": sk["RandomForestRegressor"](
            n_estimators=250,
            max_depth=10,
            min_samples_leaf=3,
            random_state=random_state,
            n_jobs=-1,
        ),
        "hist_gbr": sk["HistGradientBoostingRegressor"](
            learning_rate=0.05,
            max_depth=6,
            max_iter=350,
            random_state=random_state,
        ),
    }


def _candidate_classifiers(random_state: int) -> dict[str, Any]:
    sk = _require_sklearn()
    return {
        "logistic": sk["LogisticRegression"](max_iter=1000, C=1.0),
        "random_forest": sk["RandomForestClassifier"](
            n_estimators=300,
            max_depth=10,
            min_samples_leaf=3,
            random_state=random_state,
            n_jobs=-1,
        ),
        "hist_gbc": sk["HistGradientBoostingClassifier"](
            learning_rate=0.05,
            max_depth=6,
            max_iter=350,
            random_state=random_state,
        ),
    }


def _evaluate_candidate_regressor(
    estimator: Any,
    X: Any,
    y: Any,
    *,
    splits: list[Any],
) -> float:
    sk = _require_sklearn()
    np = _require_numpy()

    if not splits:
        model = sk["clone"](estimator)
        model.fit(X, y)
        pred = model.predict(X)
        metrics = _regression_metrics(y, pred)
        return float(metrics["rmse"])

    fold_scores: list[float] = []
    for split in splits:
        model = sk["clone"](estimator)
        train_idx = np.asarray(split.train_index, dtype=int)
        valid_idx = np.asarray(split.validation_index, dtype=int)
        model.fit(X[train_idx], y[train_idx])
        pred = model.predict(X[valid_idx])
        metrics = _regression_metrics(y[valid_idx], pred)
        fold_scores.append(metrics["rmse"])

    if not fold_scores:
        return math.nan
    return float(sum(fold_scores) / len(fold_scores))


def _evaluate_candidate_classifier(
    estimator: Any,
    X: Any,
    y: Any,
    *,
    splits: list[Any],
) -> float:
    sk = _require_sklearn()
    np = _require_numpy()

    if not splits:
        model = sk["clone"](estimator)
        model.fit(X, y)
        if hasattr(model, "predict_proba"):
            prob = model.predict_proba(X)[:, 1]
        else:
            prob = model.predict(X)
        return _brier_score(y, prob)

    fold_scores: list[float] = []
    for split in splits:
        model = sk["clone"](estimator)
        train_idx = np.asarray(split.train_index, dtype=int)
        valid_idx = np.asarray(split.validation_index, dtype=int)
        model.fit(X[train_idx], y[train_idx])

        if hasattr(model, "predict_proba"):
            prob = model.predict_proba(X[valid_idx])[:, 1]
        else:
            prob = model.predict(X[valid_idx])

        score = _brier_score(y[valid_idx], prob)
        fold_scores.append(score)

    if not fold_scores:
        return math.nan
    return float(sum(fold_scores) / len(fold_scores))


def _fit_best_regressor(
    X: Any,
    y: Any,
    *,
    splits: list[Any],
    random_state: int,
) -> tuple[Any, SklearnTargetResult]:
    sk = _require_sklearn()
    np = _require_numpy()

    if y.size == 0:
        dummy = sk["DummyRegressor"](strategy="constant", constant=0.0)
        dummy.fit(X, np.zeros(X.shape[0], dtype=float))
        return dummy, SklearnTargetResult(best_model_name="dummy", objective="rmse", score=math.nan)

    best_name = ""
    best_score = float("inf")
    best_estimator: Any | None = None

    for name, estimator in _candidate_regressors(random_state=random_state).items():
        score = _evaluate_candidate_regressor(estimator, X, y, splits=splits)
        if math.isnan(score):
            score = float("inf")
        if score < best_score:
            best_name = name
            best_score = score
            best_estimator = estimator

    if best_estimator is None:
        best_name = "dummy"
        best_estimator = sk["DummyRegressor"](strategy="median")

    model = sk["clone"](best_estimator)
    model.fit(X, y)
    return model, SklearnTargetResult(best_model_name=best_name, objective="rmse", score=best_score)


def _fit_best_classifier(
    X: Any,
    y: Any,
    *,
    splits: list[Any],
    random_state: int,
) -> tuple[Any, SklearnTargetResult]:
    sk = _require_sklearn()
    np = _require_numpy()

    if y.size == 0:
        dummy = sk["DummyClassifier"](strategy="constant", constant=0)
        dummy.fit(X, np.zeros(X.shape[0], dtype=int))
        return dummy, SklearnTargetResult(best_model_name="dummy", objective="brier", score=math.nan)

    unique = np.unique(y)
    if unique.size < 2:
        constant = int(unique[0]) if unique.size == 1 else 0
        dummy = sk["DummyClassifier"](strategy="constant", constant=constant)
        dummy.fit(X, y)
        return dummy, SklearnTargetResult(best_model_name="dummy", objective="brier", score=0.0)

    best_name = ""
    best_score = float("inf")
    best_estimator: Any | None = None

    for name, estimator in _candidate_classifiers(random_state=random_state).items():
        score = _evaluate_candidate_classifier(estimator, X, y, splits=splits)
        if math.isnan(score):
            score = float("inf")
        if score < best_score:
            best_name = name
            best_score = score
            best_estimator = estimator

    if best_estimator is None:
        best_name = "dummy"
        best_estimator = sk["DummyClassifier"](strategy="prior")

    model = sk["clone"](best_estimator)
    model.fit(X, y)
    return model, SklearnTargetResult(best_model_name=best_name, objective="brier", score=best_score)


def fit_sklearn_hierarchical_models(
    prepared_matrix: Any,
    frame_with_targets: Any,
    *,
    time_splits: list[Any] | None = None,
    random_state: int = 42,
) -> SklearnHierarchicalBundle:
    np = _require_numpy()

    X = _dense_matrix(prepared_matrix.X)
    frame = frame_with_targets.copy()
    splits = time_splits or []

    target_started = _safe_binary_target(frame.get("target_started", np.zeros(shape=(len(frame),))))
    target_subbed = _safe_binary_target(frame.get("target_subbed_in", np.zeros(shape=(len(frame),))))
    target_minutes = np.asarray(frame.get("target_minutes", np.zeros(shape=(len(frame),))), dtype=float)
    target_raw = np.asarray(frame.get("target_raw_points", np.zeros(shape=(len(frame),))), dtype=float)
    target_on_top = np.asarray(frame.get("target_on_top_points", np.zeros(shape=(len(frame),))), dtype=float)

    estimators: dict[str, Any] = {}
    results: dict[str, SklearnTargetResult] = {}

    estimators["start_probability"], results["start_probability"] = _fit_best_classifier(
        X, target_started, splits=splits, random_state=random_state
    )
    estimators["sub_probability"], results["sub_probability"] = _fit_best_classifier(
        X, target_subbed, splits=splits, random_state=random_state
    )
    estimators["expected_minutes"], results["expected_minutes"] = _fit_best_regressor(
        X, target_minutes, splits=splits, random_state=random_state
    )
    estimators["raw_points_if_full"], results["raw_points_if_full"] = _fit_best_regressor(
        X, target_raw, splits=splits, random_state=random_state
    )
    estimators["on_top_points_if_full"], results["on_top_points_if_full"] = _fit_best_regressor(
        X, target_on_top, splits=splits, random_state=random_state
    )

    metadata = {
        "rows": int(len(frame)),
        "n_features": int(X.shape[1]) if len(X.shape) > 1 else 0,
        "n_splits": int(len(splits)),
        "random_state": int(random_state),
    }

    return SklearnHierarchicalBundle(
        feature_names=[str(name) for name in prepared_matrix.feature_names],
        estimators=estimators,
        target_results=results,
        metadata=metadata,
    )


def _predict_probability(estimator: Any, X: Any) -> Any:
    np = _require_numpy()

    if hasattr(estimator, "predict_proba"):
        proba = estimator.predict_proba(X)
        if proba.ndim == 2 and proba.shape[1] >= 2:
            return np.asarray(proba[:, 1], dtype=float)
        return np.asarray(proba[:, 0], dtype=float)

    pred = estimator.predict(X)
    return np.asarray(pred, dtype=float)


def predict_with_sklearn_bundle(bundle: SklearnHierarchicalBundle, prepared_matrix: Any) -> Any:
    pd = _require_pandas()
    np = _require_numpy()

    X = _dense_matrix(prepared_matrix.X)
    frame = prepared_matrix.frame.copy()

    start_probability = _predict_probability(bundle.estimators["start_probability"], X)
    sub_probability = _predict_probability(bundle.estimators["sub_probability"], X)

    expected_minutes = np.asarray(bundle.estimators["expected_minutes"].predict(X), dtype=float)
    raw_points_if_full = np.asarray(bundle.estimators["raw_points_if_full"].predict(X), dtype=float)
    on_top_points_if_full = np.asarray(bundle.estimators["on_top_points_if_full"].predict(X), dtype=float)

    start_probability = np.clip(start_probability, 0.0, 0.99)
    sub_probability = np.clip(sub_probability, 0.0, 0.99)
    expected_minutes = np.clip(expected_minutes, 0.0, 110.0)
    raw_points_if_full = np.clip(raw_points_if_full, 0.0, None)
    on_top_points_if_full = np.clip(on_top_points_if_full, 0.0, None)

    full_points = raw_points_if_full + on_top_points_if_full
    minute_scale = np.clip(expected_minutes / 90.0, 0.0, 1.2)

    expected_raw_next = start_probability * raw_points_if_full * minute_scale + sub_probability * raw_points_if_full * 0.30
    expected_on_top_next = (
        start_probability * on_top_points_if_full * minute_scale
        + sub_probability * on_top_points_if_full * 0.30
    )
    expected_points_next = expected_raw_next + expected_on_top_next

    out = pd.DataFrame(
        {
            "start_probability": start_probability,
            "sub_probability": sub_probability,
            "expected_minutes": expected_minutes,
            "raw_points_if_full": raw_points_if_full,
            "on_top_points_if_full": on_top_points_if_full,
            "expected_points_if_full": full_points,
            "expected_raw_points_next_match": expected_raw_next,
            "expected_on_top_points_next_match": expected_on_top_next,
            "expected_points_next_match": expected_points_next,
        }
    )

    for col in ["player_uid", "player_name", "team_uid", "match_uid", "snapshot_date"]:
        if col in frame.columns:
            out[col] = frame[col]

    ordered_cols = [
        col
        for col in [
            "player_uid",
            "player_name",
            "team_uid",
            "match_uid",
            "snapshot_date",
            "start_probability",
            "sub_probability",
            "expected_minutes",
            "expected_points_next_match",
            "expected_points_if_full",
            "expected_raw_points_next_match",
            "expected_on_top_points_next_match",
            "raw_points_if_full",
            "on_top_points_if_full",
        ]
        if col in out.columns
    ]

    return out[ordered_cols].copy()


def evaluate_hierarchical_predictions(predictions: Any, truth_frame: Any) -> dict[str, dict[str, float]]:
    np = _require_numpy()

    merged = predictions.copy()
    if "target_points_total" in truth_frame.columns:
        merged["target_points_total"] = truth_frame["target_points_total"].to_numpy()
    if "target_raw_points" in truth_frame.columns:
        merged["target_raw_points"] = truth_frame["target_raw_points"].to_numpy()
    if "target_on_top_points" in truth_frame.columns:
        merged["target_on_top_points"] = truth_frame["target_on_top_points"].to_numpy()
    if "target_started" in truth_frame.columns:
        merged["target_started"] = truth_frame["target_started"].to_numpy()
    if "target_subbed_in" in truth_frame.columns:
        merged["target_subbed_in"] = truth_frame["target_subbed_in"].to_numpy()

    metrics: dict[str, dict[str, float]] = {}

    if {"expected_points_next_match", "target_points_total"}.issubset(merged.columns):
        metrics["points_total"] = _regression_metrics(
            merged["target_points_total"].to_numpy(dtype=float),
            merged["expected_points_next_match"].to_numpy(dtype=float),
        )

    if {"expected_raw_points_next_match", "target_raw_points"}.issubset(merged.columns):
        metrics["raw_points"] = _regression_metrics(
            merged["target_raw_points"].to_numpy(dtype=float),
            merged["expected_raw_points_next_match"].to_numpy(dtype=float),
        )

    if {"expected_on_top_points_next_match", "target_on_top_points"}.issubset(merged.columns):
        metrics["on_top_points"] = _regression_metrics(
            merged["target_on_top_points"].to_numpy(dtype=float),
            merged["expected_on_top_points_next_match"].to_numpy(dtype=float),
        )

    if {"start_probability", "target_started"}.issubset(merged.columns):
        metrics["start_probability"] = {
            "brier": _brier_score(
                merged["target_started"].to_numpy(dtype=float),
                merged["start_probability"].to_numpy(dtype=float),
            )
        }

    if {"sub_probability", "target_subbed_in"}.issubset(merged.columns):
        metrics["sub_probability"] = {
            "brier": _brier_score(
                merged["target_subbed_in"].to_numpy(dtype=float),
                merged["sub_probability"].to_numpy(dtype=float),
            )
        }

    return metrics
