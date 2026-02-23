# ------------------------------------
# train_hierarchical_models.py
#
# Dieses Skript trainiert die hierarchischen ML-Modelle fuer Kickbase:
# - sklearn (Modellvergleich je Ziel)
# - optional PyTorch Multitask-Netz
#
# Trainingsdaten kommen aus der lokalen Postgres-RAW-History.
# Inferenzdaten kommen aus den aktuellen Silver-Snapshots.
#
# Outputs
# ------------------------------------
# 1) data/ml_runs/<run_ts>/cv_sklearn_folds.csv
# 2) data/ml_runs/<run_ts>/cv_sklearn_summary.json
# 3) data/ml_runs/<run_ts>/live_predictions_sklearn.csv
# 4) data/ml_runs/<run_ts>/live_predictions_champion.csv
# 5) data/ml_runs/<run_ts>/champion_selection.json
# 6) optional: Torch-CV + Torch-Live-Predictions
# 7) data/ml_runs/<run_ts>/run_summary.json
#
# Usage
# ------------------------------------
# - python -m scripts.ml.train_hierarchical_models --env-file .env
# - python -m scripts.ml.train_hierarchical_models --env-file .env --cv-splits 4 --validation-days 21
# - python -m scripts.ml.train_hierarchical_models --env-file .env --skip-torch
# ------------------------------------

from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
from pathlib import Path
import sys
from typing import Any

try:
    from dotenv import load_dotenv
except ModuleNotFoundError:
    def load_dotenv(*_args: object, **_kwargs: object) -> bool:  # type: ignore[override]
        return False

from databricks.jobs.common_io import write_csv
from src.features_engineering import (
    add_domain_features,
    add_history_sequence_features,
    attach_history_targets,
    attach_recent_history_features_to_live,
    build_history_supervision_frame,
    default_feature_columns,
    enforce_team_probability_constraints,
)
from src.models import (
    evaluate_hierarchical_predictions,
    fit_sklearn_hierarchical_models,
    predict_with_sklearn_bundle,
)
from src.nn_models import (
    TorchTrainingConfig,
    predict_with_torch_bundle,
    train_torch_multitask_model,
)
from src.preprocessing import (
    build_base_training_frame,
    build_shared_preprocessor,
    fit_transform_frame,
    load_latest_silver_frames,
    make_rolling_time_splits,
    to_torch_ready_matrix,
    transform_frame,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train sklearn + torch hierarchical models with historical CV.")
    parser.add_argument("--env-file", default=".env")
    parser.add_argument("--lakehouse-silver-dir", type=Path, default=Path("data/lakehouse/silver"))
    parser.add_argument("--silver-timestamp", default=None)
    parser.add_argument("--out-dir", type=Path, default=Path("data/ml_runs"))
    parser.add_argument("--season-window", type=int, default=3, help="Anzahl Saisons fuer Training (inkl. aktuell)")
    parser.add_argument("--history-max-players", type=int, default=0)
    parser.add_argument("--cv-splits", type=int, default=4)
    parser.add_argument("--min-train-days", type=int, default=60)
    parser.add_argument("--validation-days", type=int, default=21)
    parser.add_argument("--random-state", type=int, default=42)
    parser.add_argument("--skip-torch", action="store_true")
    parser.add_argument("--torch-epochs", type=int, default=120)
    parser.add_argument("--torch-batch-size", type=int, default=256)
    parser.add_argument("--torch-device", default="cpu")
    parser.add_argument("--live-limit", type=int, default=0)
    return parser.parse_args(argv)


def _require_pandas() -> Any:
    try:
        import pandas as pd
    except ModuleNotFoundError as exc:  # pragma: no cover - runtime guard
        raise RuntimeError(
            "pandas fehlt fuer ML-Training. Bitte `python -m pip install -r requirements-ml.txt` ausfuehren."
        ) from exc
    return pd


def _flatten_metrics(metrics: dict[str, dict[str, float]], *, prefix: str) -> dict[str, float]:
    out: dict[str, float] = {}
    for scope, values in metrics.items():
        for metric_name, metric_value in values.items():
            key = f"{prefix}_{scope}_{metric_name}"
            out[key] = float(metric_value)
    return out


def _mean_metric_rows(rows: list[dict[str, Any]], *, metric_prefix: str) -> dict[str, float]:
    if not rows:
        return {}

    numeric_keys = sorted(
        {
            key
            for row in rows
            for key, value in row.items()
            if key.startswith(metric_prefix) and isinstance(value, (int, float))
        }
    )

    summary: dict[str, float] = {}
    for key in numeric_keys:
        values = [float(row[key]) for row in rows if key in row]
        summary[key] = float(sum(values) / max(len(values), 1))
    return summary


def _select_champion_model(
    sklearn_summary: dict[str, Any],
    torch_summary: dict[str, Any] | None,
) -> dict[str, Any]:
    metric_key = "metric_points_total_rmse"
    sklearn_metrics = sklearn_summary.get("metrics_mean", {})
    sklearn_score = sklearn_metrics.get(metric_key)

    torch_score: float | None = None
    if torch_summary and isinstance(torch_summary, dict):
        torch_metrics = torch_summary.get("metrics_mean", {})
        maybe_score = torch_metrics.get(metric_key)
        if isinstance(maybe_score, (int, float)):
            torch_score = float(maybe_score)

    champion_model = "sklearn"
    champion_reason = "torch metrics unavailable"
    if isinstance(sklearn_score, (int, float)) and torch_score is not None:
        if torch_score < float(sklearn_score):
            champion_model = "torch"
            champion_reason = "lower points_total_rmse on historical CV"
        else:
            champion_reason = "lower or equal points_total_rmse on historical CV"
    elif isinstance(sklearn_score, (int, float)):
        champion_reason = "only sklearn score available"

    return {
        "metric_key": metric_key,
        "sklearn_score": float(sklearn_score) if isinstance(sklearn_score, (int, float)) else None,
        "torch_score": torch_score,
        "champion_model": champion_model,
        "champion_reason": champion_reason,
    }


def _read_frame(conn: Any, sql: str, params: tuple[Any, ...] = ()) -> Any:
    pd = _require_pandas()

    with conn.cursor() as cur:
        cur.execute(sql, params)
        columns = [desc.name for desc in cur.description]
        rows = [dict(zip(columns, raw, strict=False)) for raw in cur.fetchall()]

    return pd.DataFrame(rows)


def _season_floor(conn: Any, season_window: int) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT COALESCE(MAX(season_uid), 0) FROM dim_match")
        raw = cur.fetchone()
        max_uid = int(raw[0] or 0)

    safe_window = max(1, int(season_window))
    return max_uid - (safe_window - 1) * 101


def _load_history_frames(
    *,
    db_config: Any,
    season_window: int,
    max_players: int,
) -> tuple[Any, Any, Any]:
    pd = _require_pandas()
    try:
        from src.db import get_connection
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "psycopg2 fehlt fuer Postgres-Zugriff. Bitte zuerst `python -m pip install -r requirements.txt` ausfuehren."
        ) from exc

    with get_connection(db_config) as conn:
        min_season_uid = _season_floor(conn, season_window)

        players_filter_sql = ""
        params: list[Any] = [min_season_uid]
        if max_players > 0:
            players_filter_sql = """
                AND pm.player_uid IN (
                    SELECT player_uid
                    FROM fact_player_match pm2
                    JOIN dim_match dm2 ON dm2.match_uid = pm2.match_uid
                    WHERE dm2.season_uid >= %s
                    GROUP BY player_uid
                    ORDER BY player_uid
                    LIMIT %s
                )
            """
            params.extend([min_season_uid, int(max_players)])

        match_sql = f"""
            SELECT
                pm.player_uid,
                pm.match_uid,
                pm.points_total,
                pm.is_home,
                pm.match_result,
                pm.raw_json,
                dp.player_name,
                dp.player_position,
                dp.team_uid,
                dm.season_uid,
                dm.season_label,
                dm.matchday,
                COALESCE(dm.kickoff_ts::date, CURRENT_DATE) AS match_date,
                dm.home_team_uid,
                dm.away_team_uid,
                CASE
                    WHEN pm.is_home IS TRUE THEN dm.away_team_uid
                    WHEN pm.is_home IS FALSE THEN dm.home_team_uid
                    ELSE NULL
                END AS opponent_team_uid,
                mv.market_value
            FROM fact_player_match AS pm
            JOIN dim_player AS dp
              ON dp.player_uid = pm.player_uid
            JOIN dim_match AS dm
              ON dm.match_uid = pm.match_uid
            LEFT JOIN LATERAL (
                SELECT f.market_value
                FROM fact_market_value_daily AS f
                WHERE f.player_uid = pm.player_uid
                  AND f.mv_date <= COALESCE(dm.kickoff_ts::date, CURRENT_DATE)
                ORDER BY f.mv_date DESC
                LIMIT 1
            ) AS mv ON TRUE
            WHERE dm.season_uid >= %s
            {players_filter_sql}
            ORDER BY pm.player_uid, dm.season_uid, dm.matchday, dm.kickoff_ts NULLS LAST
        """

        event_sql = f"""
            SELECT
                e.event_hash,
                e.player_uid,
                e.match_uid,
                e.event_type_id,
                e.points,
                e.mt,
                e.att,
                e.raw_event
            FROM fact_player_event AS e
            JOIN dim_match AS dm
              ON dm.match_uid = e.match_uid
            WHERE dm.season_uid >= %s
            {players_filter_sql.replace('pm.player_uid', 'e.player_uid')}
            ORDER BY e.player_uid, e.match_uid
        """

        event_type_sql = """
            SELECT event_type_id, event_name
            FROM dim_event_type
            ORDER BY event_type_id
        """

        match_df = _read_frame(conn, match_sql, tuple(params))
        event_df = _read_frame(conn, event_sql, tuple(params))
        event_type_df = _read_frame(conn, event_type_sql)

    if match_df.empty:
        raise RuntimeError("Keine historischen Match-Daten in Postgres gefunden.")

    if "match_date" in match_df.columns:
        match_df["match_date"] = pd.to_datetime(match_df["match_date"], errors="coerce")

    return match_df, event_df, event_type_df


def _prepare_history_training_frame(
    match_df: Any,
    event_df: Any,
    event_type_df: Any,
) -> Any:
    pd = _require_pandas()

    targets = build_history_supervision_frame(match_df, event_df, event_type_df)
    base = attach_history_targets(match_df, targets, key_columns=("player_uid", "match_uid"))

    if "match_date" in base.columns:
        base["snapshot_date"] = pd.to_datetime(base["match_date"], errors="coerce")
    else:
        base["snapshot_date"] = pd.NaT

    base["last_matchday"] = pd.to_numeric(base.get("matchday"), errors="coerce").fillna(0.0)
    base["player_position"] = base.get("player_position", "UNKNOWN").fillna("UNKNOWN")
    base["team_uid"] = base.get("team_uid", "UNK").fillna("UNK")
    base["opponent_team_uid"] = base.get("opponent_team_uid", "UNK").fillna("UNK")
    base["season_uid"] = base.get("season_uid", "0").fillna("0").astype(str)

    base["win_probability"] = base.get("match_result").map(lambda value: 1.0 if str(value) == "W" else 0.0)
    base["draw_probability"] = base.get("match_result").map(lambda value: 1.0 if str(value) == "D" else 0.0)
    base["loss_probability"] = base.get("match_result").map(lambda value: 1.0 if str(value) == "L" else 0.0)
    base["likely_result"] = base.get("match_result").map(
        lambda value: "win" if str(value) == "W" else ("draw" if str(value) == "D" else ("loss" if str(value) == "L" else "unknown"))
    )

    base["is_home_numeric"] = base.get("is_home", False).map(lambda value: 1.0 if bool(value) else 0.0)
    base["start_probability_label"] = base.get("target_started", 0.0).map(
        lambda value: "sicher" if float(value) >= 0.5 else "unwahrscheinlich"
    )
    base["li_predicted_lineup"] = base.get("target_started", 0.0).map(
        lambda value: "safe starter" if float(value) >= 0.5 else "bench"
    )
    base["injury_status"] = "fit"
    base["li_status"] = "fit"

    history_features = add_history_sequence_features(base, player_column="player_uid", date_column="snapshot_date")
    history_features = add_domain_features(history_features)

    required_targets = [
        "target_started",
        "target_subbed_in",
        "target_minutes",
        "target_raw_points",
        "target_on_top_points",
        "target_points_total",
    ]
    for col in required_targets:
        history_features[col] = pd.to_numeric(history_features.get(col), errors="coerce")

    history_features = history_features.dropna(subset=["snapshot_date", "target_points_total"]).copy()
    history_features = history_features.reset_index(drop=True)
    return history_features


def _run_sklearn_cv(
    *,
    frame: Any,
    numeric_columns: list[str],
    categorical_columns: list[str],
    cv_splits: int,
    min_train_days: int,
    validation_days: int,
    random_state: int,
) -> tuple[list[dict[str, Any]], Any, Any, Any]:
    pd = _require_pandas()

    splits = make_rolling_time_splits(
        frame,
        date_column="snapshot_date",
        n_splits=cv_splits,
        min_train_days=min_train_days,
        validation_days=validation_days,
    )
    if not splits:
        raise RuntimeError("Keine gueltigen Time-CV-Splits erzeugt. Bitte mehr Historie oder kleinere Fenster verwenden.")

    fold_rows: list[dict[str, Any]] = []
    last_preprocessor = None
    last_bundle = None
    last_features = None

    for fold_idx, split in enumerate(splits, start=1):
        train_fold = frame.iloc[split.train_index].reset_index(drop=True)
        valid_fold = frame.iloc[split.validation_index].reset_index(drop=True)

        preprocessor = build_shared_preprocessor(numeric_columns, categorical_columns)
        prepared_train = fit_transform_frame(
            train_fold,
            preprocessor,
            numeric_columns=numeric_columns,
            categorical_columns=categorical_columns,
            target_column="target_points_total",
        )

        bundle = fit_sklearn_hierarchical_models(
            prepared_train,
            train_fold,
            time_splits=[],
            random_state=random_state + fold_idx,
        )

        prepared_valid = transform_frame(
            valid_fold,
            preprocessor,
            numeric_columns=numeric_columns,
            categorical_columns=categorical_columns,
            target_column="target_points_total",
        )
        fold_pred = predict_with_sklearn_bundle(bundle, prepared_valid)
        fold_pred = enforce_team_probability_constraints(fold_pred)
        fold_metrics = evaluate_hierarchical_predictions(fold_pred, valid_fold)

        row: dict[str, Any] = {
            "fold": fold_idx,
            "train_rows": int(len(train_fold)),
            "valid_rows": int(len(valid_fold)),
        }
        row.update(_flatten_metrics(fold_metrics, prefix="metric"))

        for target_name, target_result in bundle.target_results.items():
            row[f"selected_model_{target_name}"] = target_result.best_model_name
            row[f"selected_score_{target_name}"] = float(target_result.score)

        fold_rows.append(row)
        last_preprocessor = preprocessor
        last_bundle = bundle
        last_features = prepared_valid

    return fold_rows, last_preprocessor, last_bundle, last_features


def _run_torch_cv(
    *,
    frame: Any,
    numeric_columns: list[str],
    categorical_columns: list[str],
    cv_splits: int,
    min_train_days: int,
    validation_days: int,
    random_state: int,
    torch_epochs: int,
    torch_batch_size: int,
    torch_device: str,
) -> tuple[list[dict[str, Any]], Any]:
    splits = make_rolling_time_splits(
        frame,
        date_column="snapshot_date",
        n_splits=cv_splits,
        min_train_days=min_train_days,
        validation_days=validation_days,
    )
    if not splits:
        raise RuntimeError("Keine gueltigen Time-CV-Splits fuer Torch erzeugt.")

    fold_rows: list[dict[str, Any]] = []
    last_bundle = None

    for fold_idx, split in enumerate(splits, start=1):
        train_fold = frame.iloc[split.train_index].reset_index(drop=True)
        valid_fold = frame.iloc[split.validation_index].reset_index(drop=True)

        preprocessor = build_shared_preprocessor(numeric_columns, categorical_columns)
        prepared_train = fit_transform_frame(
            train_fold,
            preprocessor,
            numeric_columns=numeric_columns,
            categorical_columns=categorical_columns,
        )
        prepared_valid = transform_frame(
            valid_fold,
            preprocessor,
            numeric_columns=numeric_columns,
            categorical_columns=categorical_columns,
        )

        X_train, _ = to_torch_ready_matrix(prepared_train)
        X_valid, _ = to_torch_ready_matrix(prepared_valid)

        config = TorchTrainingConfig(
            epochs=max(10, int(torch_epochs)),
            batch_size=max(16, int(torch_batch_size)),
            seed=random_state + fold_idx,
            device=torch_device,
        )

        bundle = train_torch_multitask_model(
            X_train,
            train_fold,
            X_valid,
            valid_fold,
            config=config,
        )
        fold_pred = predict_with_torch_bundle(bundle, X_valid, meta_frame=prepared_valid.frame)
        fold_pred = enforce_team_probability_constraints(fold_pred)
        fold_metrics = evaluate_hierarchical_predictions(fold_pred, valid_fold)

        row: dict[str, Any] = {
            "fold": fold_idx,
            "train_rows": int(len(train_fold)),
            "valid_rows": int(len(valid_fold)),
            "torch_best_valid_loss": float(bundle.metadata.get("best_valid_loss", 0.0)),
            "torch_epochs_trained": int(bundle.metadata.get("epochs_trained", 0)),
        }
        row.update(_flatten_metrics(fold_metrics, prefix="metric"))
        fold_rows.append(row)
        last_bundle = bundle

    return fold_rows, last_bundle


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True), encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    load_dotenv(args.env_file)

    run_ts = datetime.now(UTC).strftime("%Y-%m-%dT%H%M%SZ")
    run_dir = args.out_dir / run_ts
    run_dir.mkdir(parents=True, exist_ok=True)

    try:
        from src.db import DbConfig
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "psycopg2 fehlt fuer Postgres-Zugriff. Bitte zuerst `python -m pip install -r requirements.txt` ausfuehren."
        ) from exc

    match_df, event_df, event_type_df = _load_history_frames(
        db_config=DbConfig.from_env(),
        season_window=args.season_window,
        max_players=args.history_max_players,
    )

    history_frame = _prepare_history_training_frame(match_df, event_df, event_type_df)
    contract = default_feature_columns(history_frame)
    if not contract.numeric_columns and not contract.categorical_columns:
        raise RuntimeError("Keine Feature-Spalten fuer Training verfuegbar.")

    sklearn_folds, sklearn_preprocessor, sklearn_bundle_last, _ = _run_sklearn_cv(
        frame=history_frame,
        numeric_columns=contract.numeric_columns,
        categorical_columns=contract.categorical_columns,
        cv_splits=args.cv_splits,
        min_train_days=args.min_train_days,
        validation_days=args.validation_days,
        random_state=args.random_state,
    )

    sklearn_summary = {
        "rows": int(len(history_frame)),
        "folds": len(sklearn_folds),
        "metrics_mean": _mean_metric_rows(sklearn_folds, metric_prefix="metric_"),
        "feature_count": len(contract.numeric_columns) + len(contract.categorical_columns),
        "numeric_columns": contract.numeric_columns,
        "categorical_columns": contract.categorical_columns,
        "target_means": {
            "target_started_mean": float(history_frame["target_started"].mean()) if "target_started" in history_frame.columns else 0.0,
            "target_subbed_in_mean": float(history_frame["target_subbed_in"].mean()) if "target_subbed_in" in history_frame.columns else 0.0,
            "target_minutes_mean": float(history_frame["target_minutes"].mean()) if "target_minutes" in history_frame.columns else 0.0,
            "target_raw_points_mean": float(history_frame["target_raw_points"].mean()) if "target_raw_points" in history_frame.columns else 0.0,
            "target_on_top_points_mean": float(history_frame["target_on_top_points"].mean()) if "target_on_top_points" in history_frame.columns else 0.0,
            "target_points_total_mean": float(history_frame["target_points_total"].mean()) if "target_points_total" in history_frame.columns else 0.0,
        },
    }

    write_csv(
        run_dir / "cv_sklearn_folds.csv",
        sklearn_folds,
        fieldnames=sorted({key for row in sklearn_folds for key in row.keys()}),
    )
    _write_json(run_dir / "cv_sklearn_summary.json", sklearn_summary)

    # Finales sklearn-Modell auf vollem historischen Frame.
    final_preprocessor = build_shared_preprocessor(contract.numeric_columns, contract.categorical_columns)
    prepared_full = fit_transform_frame(
        history_frame,
        final_preprocessor,
        numeric_columns=contract.numeric_columns,
        categorical_columns=contract.categorical_columns,
    )
    full_time_splits = make_rolling_time_splits(
        history_frame,
        date_column="snapshot_date",
        n_splits=args.cv_splits,
        min_train_days=args.min_train_days,
        validation_days=args.validation_days,
    )
    final_sklearn_bundle = fit_sklearn_hierarchical_models(
        prepared_full,
        history_frame,
        time_splits=full_time_splits,
        random_state=args.random_state,
    )

    silver_players, silver_teams = load_latest_silver_frames(
        lakehouse_silver_dir=args.lakehouse_silver_dir,
        timestamp=args.silver_timestamp,
    )
    live_frame = build_base_training_frame(
        silver_players,
        silver_teams,
        mode="live",
    )
    live_frame = attach_recent_history_features_to_live(live_frame, history_frame)
    live_frame = add_domain_features(live_frame)
    if args.live_limit > 0:
        live_frame = live_frame.head(int(args.live_limit)).copy()

    prepared_live = transform_frame(
        live_frame,
        final_preprocessor,
        numeric_columns=contract.numeric_columns,
        categorical_columns=contract.categorical_columns,
    )

    live_pred_sklearn = predict_with_sklearn_bundle(final_sklearn_bundle, prepared_live)
    live_pred_sklearn = enforce_team_probability_constraints(live_pred_sklearn)

    write_csv(
        run_dir / "live_predictions_sklearn.csv",
        live_pred_sklearn.to_dict(orient="records"),
        fieldnames=list(live_pred_sklearn.columns),
    )

    torch_summary: dict[str, Any] | None = None
    live_pred_torch: Any | None = None
    torch_folds: list[dict[str, Any]] = []

    if not args.skip_torch:
        try:
            torch_folds, _ = _run_torch_cv(
                frame=history_frame,
                numeric_columns=contract.numeric_columns,
                categorical_columns=contract.categorical_columns,
                cv_splits=args.cv_splits,
                min_train_days=args.min_train_days,
                validation_days=args.validation_days,
                random_state=args.random_state,
                torch_epochs=args.torch_epochs,
                torch_batch_size=args.torch_batch_size,
                torch_device=args.torch_device,
            )

            write_csv(
                run_dir / "cv_torch_folds.csv",
                torch_folds,
                fieldnames=sorted({key for row in torch_folds for key in row.keys()}),
            )

            # Finale Torch-Inferenz (gleiche Features wie sklearn).
            X_full, _ = to_torch_ready_matrix(prepared_full)
            X_live, _ = to_torch_ready_matrix(prepared_live)
            torch_bundle_final = train_torch_multitask_model(
                X_full,
                history_frame,
                X_full,
                history_frame,
                config=TorchTrainingConfig(
                    epochs=max(10, int(args.torch_epochs)),
                    batch_size=max(16, int(args.torch_batch_size)),
                    seed=args.random_state,
                    device=args.torch_device,
                ),
            )
            live_pred_torch = predict_with_torch_bundle(torch_bundle_final, X_live, meta_frame=prepared_live.frame)
            live_pred_torch = enforce_team_probability_constraints(live_pred_torch)

            write_csv(
                run_dir / "live_predictions_torch.csv",
                live_pred_torch.to_dict(orient="records"),
                fieldnames=list(live_pred_torch.columns),
            )

            torch_summary = {
                "rows": int(len(history_frame)),
                "folds": len(torch_folds),
                "metrics_mean": _mean_metric_rows(torch_folds, metric_prefix="metric_"),
            }
            _write_json(run_dir / "cv_torch_summary.json", torch_summary)
        except RuntimeError as exc:
            torch_summary = {
                "status": "skipped",
                "reason": str(exc),
            }

    champion = _select_champion_model(sklearn_summary, torch_summary)
    champion_model = champion["champion_model"]
    champion_frame = live_pred_torch if champion_model == "torch" and live_pred_torch is not None else live_pred_sklearn

    write_csv(
        run_dir / "live_predictions_champion.csv",
        champion_frame.to_dict(orient="records"),
        fieldnames=list(champion_frame.columns),
    )
    _write_json(run_dir / "champion_selection.json", champion)

    run_summary = {
        "status": "success",
        "run_ts": run_ts,
        "output_dir": str(run_dir),
        "history_rows": int(len(history_frame)),
        "history_players": int(history_frame["player_uid"].nunique()) if "player_uid" in history_frame.columns else 0,
        "live_rows": int(len(live_frame)),
        "sklearn_summary": sklearn_summary,
        "torch_summary": torch_summary,
        "champion": champion,
        "files_written": sorted(str(path) for path in run_dir.glob("*")),
    }
    _write_json(run_dir / "run_summary.json", run_summary)

    print(json.dumps(run_summary, ensure_ascii=True, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except RuntimeError as exc:
        print(
            json.dumps(
                {
                    "status": "error",
                    "reason": "runtime_error",
                    "message": str(exc),
                },
                ensure_ascii=True,
                indent=2,
            ),
            file=sys.stderr,
        )
        raise SystemExit(2) from exc
