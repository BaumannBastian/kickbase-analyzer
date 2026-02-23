# ------------------------------------
# preprocessing.py
#
# Dieses Modul kapselt das gemeinsame Preprocessing fuer die ML-Pipeline.
# Es stellt einen stabilen Einstieg bereit, um dieselben Eingaben fuer
# sklearn-Modelle und PyTorch-Modelle zu erzeugen.
#
# Outputs
# ------------------------------------
# 1) Konsolidierter Basis-Frame aus Silver player/team Snapshots
# 2) Gemeinsamer sklearn-Preprocessor (ColumnTransformer)
# 3) Transformierte Matrizen fuer sklearn und PyTorch
#
# Usage
# ------------------------------------
# - players = load_latest_snapshot_frame(Path("data/lakehouse/silver/player_snapshot"))
# - teams = load_latest_snapshot_frame(Path("data/lakehouse/silver/team_matchup_snapshot"))
# - frame = build_base_training_frame(players, teams, mode="live")
# - pre = build_shared_preprocessor(num_cols, cat_cols)
# - prepared = fit_transform_frame(frame, pre, numeric_columns=num_cols, categorical_columns=cat_cols)
# ------------------------------------

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any, Literal

ModeLiteral = Literal["live", "historical_cv"]


@dataclass(frozen=True)
class PreparedMatrix:
    frame: Any
    X: Any
    y: Any
    feature_names: list[str]
    preprocessor: Any


@dataclass(frozen=True)
class TimeSplit:
    train_index: list[int]
    validation_index: list[int]


def _require_pandas() -> Any:
    try:
        import pandas as pd
    except ModuleNotFoundError as exc:  # pragma: no cover - runtime guard
        raise RuntimeError(
            "pandas ist nicht installiert. Installiere ML-Abhaengigkeiten (pandas, numpy, scikit-learn)."
        ) from exc
    return pd


def _require_numpy() -> Any:
    try:
        import numpy as np
    except ModuleNotFoundError as exc:  # pragma: no cover - runtime guard
        raise RuntimeError("numpy ist nicht installiert. Bitte ML-Abhaengigkeiten installieren.") from exc
    return np


def _require_sklearn_components() -> tuple[Any, Any, Any, Any]:
    try:
        from sklearn.compose import ColumnTransformer
        from sklearn.impute import SimpleImputer
        from sklearn.pipeline import Pipeline
        from sklearn.preprocessing import OneHotEncoder, StandardScaler
    except ModuleNotFoundError as exc:  # pragma: no cover - runtime guard
        raise RuntimeError(
            "scikit-learn ist nicht installiert. Bitte ML-Abhaengigkeiten installieren."
        ) from exc
    return ColumnTransformer, SimpleImputer, Pipeline, (OneHotEncoder, StandardScaler)


def load_latest_snapshot_frame(dataset_dir: Path, *, timestamp: str | None = None) -> Any:
    pd = _require_pandas()

    if not dataset_dir.exists():
        raise FileNotFoundError(f"Dataset-Verzeichnis existiert nicht: {dataset_dir}")

    if timestamp:
        snapshot_path = dataset_dir / f"snapshot_{timestamp}.ndjson"
        if not snapshot_path.exists():
            raise FileNotFoundError(f"Snapshot nicht gefunden: {snapshot_path}")
    else:
        candidates = sorted(dataset_dir.glob("snapshot_*.ndjson"))
        if not candidates:
            raise FileNotFoundError(f"Keine Snapshot-Datei in {dataset_dir} gefunden")
        snapshot_path = candidates[-1]

    rows: list[dict[str, Any]] = []
    with snapshot_path.open("r", encoding="utf-8") as handle:
        for raw in handle:
            line = raw.strip()
            if not line:
                continue
            payload = json.loads(line)
            if isinstance(payload, dict):
                rows.append(payload)

    return pd.DataFrame(rows)


def load_latest_silver_frames(
    *,
    lakehouse_silver_dir: Path,
    timestamp: str | None = None,
) -> tuple[Any, Any]:
    player_frame = load_latest_snapshot_frame(
        lakehouse_silver_dir / "player_snapshot",
        timestamp=timestamp,
    )
    team_frame = load_latest_snapshot_frame(
        lakehouse_silver_dir / "team_matchup_snapshot",
        timestamp=timestamp,
    )
    return player_frame, team_frame


def _kb_start_label_to_probability(label: str) -> float:
    mapping = {
        "sicher": 0.92,
        "starter": 0.80,
        "wahrscheinlich": 0.65,
        "unwahrscheinlich": 0.25,
        "ausgeschlossen": 0.02,
    }
    cleaned = (label or "").strip().lower()
    return mapping.get(cleaned, 0.55)


def _li_lineup_to_probability(label: str) -> float:
    mapping = {
        "safe starter": 0.90,
        "potential starter": 0.62,
        "bench": 0.15,
        "unknown": 0.50,
    }
    cleaned = (label or "").strip().lower()
    return mapping.get(cleaned, 0.50)


def _li_lineup_to_sub_probability(label: str) -> float:
    mapping = {
        "safe starter": 0.04,
        "potential starter": 0.18,
        "bench": 0.45,
        "unknown": 0.20,
    }
    cleaned = (label or "").strip().lower()
    return mapping.get(cleaned, 0.20)


def _ensure_float_column(frame: Any, column: str, default: float = 0.0) -> Any:
    pd = _require_pandas()
    if column not in frame.columns:
        frame[column] = default
    frame[column] = pd.to_numeric(frame[column], errors="coerce").fillna(default)
    return frame


def _prepare_team_snapshot(team_matchup_snapshot: Any) -> Any:
    pd = _require_pandas()
    teams = team_matchup_snapshot.copy()
    if teams.empty:
        return teams

    if "snapshot_date" in teams.columns:
        teams["snapshot_date"] = pd.to_datetime(teams["snapshot_date"], errors="coerce").dt.date

    keep_cols = [
        "team_uid",
        "snapshot_date",
        "match_uid",
        "is_home",
        "opponent_team_uid",
        "opponent_team_name",
        "win_probability",
        "draw_probability",
        "loss_probability",
        "likely_result",
        "totals_line",
        "totals_over_probability",
        "totals_under_probability",
        "odds_event_id",
        "commence_time",
        "safe_starter_count",
        "potential_starter_count",
        "bench_count",
        "unknown_count",
    ]
    available_cols = [col for col in keep_cols if col in teams.columns]
    teams = teams[available_cols].copy()

    teams = teams.sort_values(by=[col for col in ["snapshot_date", "commence_time"] if col in teams.columns])
    teams = teams.drop_duplicates(subset=["team_uid", "snapshot_date"], keep="last")

    for col in ["win_probability", "draw_probability", "loss_probability"]:
        if col in teams.columns:
            teams = _ensure_float_column(teams, col, default=0.0)

    return teams


def build_base_training_frame(
    player_snapshot: Any,
    team_matchup_snapshot: Any,
    *,
    mode: ModeLiteral = "live",
    historical_overrides: Any | None = None,
) -> Any:
    pd = _require_pandas()

    players = player_snapshot.copy()
    if players.empty:
        return players

    if "snapshot_date" in players.columns:
        players["snapshot_date"] = pd.to_datetime(players["snapshot_date"], errors="coerce").dt.date

    teams = _prepare_team_snapshot(team_matchup_snapshot)

    if not teams.empty and "team_uid" in players.columns and "snapshot_date" in players.columns:
        frame = players.merge(teams, on=["team_uid", "snapshot_date"], how="left", suffixes=("", "_team"))
    else:
        frame = players

    if "start_probability_label" in frame.columns:
        frame["kb_start_probability_prior"] = frame["start_probability_label"].map(_kb_start_label_to_probability)
    else:
        frame["kb_start_probability_prior"] = 0.55

    if "li_predicted_lineup" in frame.columns:
        frame["li_start_probability_prior"] = frame["li_predicted_lineup"].map(_li_lineup_to_probability)
        frame["li_sub_probability_prior"] = frame["li_predicted_lineup"].map(_li_lineup_to_sub_probability)
    else:
        frame["li_start_probability_prior"] = 0.50
        frame["li_sub_probability_prior"] = 0.20

    frame["kb_start_probability_prior"] = frame["kb_start_probability_prior"].fillna(0.55)
    frame["li_start_probability_prior"] = frame["li_start_probability_prior"].fillna(0.50)
    frame["li_sub_probability_prior"] = frame["li_sub_probability_prior"].fillna(0.20)

    frame["start_probability_prior"] = (
        0.60 * frame["kb_start_probability_prior"] + 0.40 * frame["li_start_probability_prior"]
    ).clip(lower=0.0, upper=0.99)
    frame["sub_probability_prior"] = frame["li_sub_probability_prior"].clip(lower=0.0, upper=0.99)

    frame = _ensure_float_column(frame, "average_minutes", default=0.0)
    frame = _ensure_float_column(frame, "last_matchday", default=0.0)

    frame["expected_minutes_prior"] = (
        frame["start_probability_prior"] * frame["average_minutes"].clip(lower=0.0, upper=110.0)
        + frame["sub_probability_prior"] * 24.0
    ).clip(lower=0.0, upper=110.0)

    if mode == "historical_cv" and historical_overrides is not None and not historical_overrides.empty:
        overrides = historical_overrides.copy()
        join_cols = [
            col
            for col in ["player_uid", "match_uid", "snapshot_date"]
            if col in overrides.columns and col in frame.columns
        ]
        if join_cols:
            frame = frame.merge(overrides, on=join_cols, how="left", suffixes=("", "_actual"))

            if "actual_started" in frame.columns:
                frame["start_probability_prior"] = frame["actual_started"].fillna(frame["start_probability_prior"])  # type: ignore[assignment]
            if "actual_subbed_in" in frame.columns:
                frame["sub_probability_prior"] = frame["actual_subbed_in"].fillna(frame["sub_probability_prior"])  # type: ignore[assignment]
            if "actual_minutes" in frame.columns:
                frame["expected_minutes_prior"] = frame["actual_minutes"].fillna(frame["expected_minutes_prior"])  # type: ignore[assignment]
            if "actual_match_result" in frame.columns and "likely_result" in frame.columns:
                frame["likely_result"] = frame["actual_match_result"].fillna(frame["likely_result"])

    return frame


def time_ordered_split(
    frame: Any,
    *,
    date_column: str = "snapshot_date",
    validation_days: int = 14,
) -> tuple[Any, Any]:
    pd = _require_pandas()

    if frame.empty:
        return frame.copy(), frame.copy()

    out = frame.copy()
    out[date_column] = pd.to_datetime(out[date_column], errors="coerce").dt.date
    out = out[out[date_column].notna()].copy()
    if out.empty:
        return frame.copy(), frame.iloc[0:0].copy()

    max_date = out[date_column].max()
    if max_date is None:
        return out.copy(), out.iloc[0:0].copy()

    cutoff = max_date - pd.Timedelta(days=max(validation_days, 1)).to_pytimedelta()
    train = out[out[date_column] <= cutoff].copy()
    valid = out[out[date_column] > cutoff].copy()
    return train, valid


def make_rolling_time_splits(
    frame: Any,
    *,
    date_column: str = "snapshot_date",
    n_splits: int = 4,
    min_train_days: int = 90,
    validation_days: int = 14,
) -> list[TimeSplit]:
    pd = _require_pandas()

    if frame.empty:
        return []

    ordered = frame.copy()
    ordered[date_column] = pd.to_datetime(ordered[date_column], errors="coerce").dt.date
    ordered = ordered[ordered[date_column].notna()].copy()
    if ordered.empty:
        return []

    ordered = ordered.sort_values(date_column).reset_index(drop=True)
    unique_days = sorted(day for day in ordered[date_column].dropna().unique())

    if len(unique_days) < max(min_train_days + validation_days, 2):
        return []

    splits: list[TimeSplit] = []
    anchor = len(unique_days) - (n_splits * validation_days)
    anchor = max(anchor, min_train_days)

    for split_idx in range(n_splits):
        train_end_pos = anchor + split_idx * validation_days
        val_start_pos = train_end_pos
        val_end_pos = min(val_start_pos + validation_days, len(unique_days))
        if val_end_pos <= val_start_pos:
            continue

        train_days = set(unique_days[:train_end_pos])
        valid_days = set(unique_days[val_start_pos:val_end_pos])
        if len(train_days) < min_train_days:
            continue

        train_index = ordered.index[ordered[date_column].isin(train_days)].tolist()
        valid_index = ordered.index[ordered[date_column].isin(valid_days)].tolist()
        if not train_index or not valid_index:
            continue

        splits.append(TimeSplit(train_index=train_index, validation_index=valid_index))

    return splits


def build_shared_preprocessor(numeric_columns: list[str], categorical_columns: list[str]) -> Any:
    ColumnTransformer, SimpleImputer, Pipeline, encoder_scaler = _require_sklearn_components()
    OneHotEncoder, StandardScaler = encoder_scaler

    try:
        encoder = OneHotEncoder(handle_unknown="ignore", sparse_output=False)
    except TypeError:  # pragma: no cover - sklearn < 1.2 fallback
        encoder = OneHotEncoder(handle_unknown="ignore", sparse=False)

    numeric_pipeline = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler()),
        ]
    )
    categorical_pipeline = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="most_frequent")),
            ("onehot", encoder),
        ]
    )

    return ColumnTransformer(
        transformers=[
            ("num", numeric_pipeline, numeric_columns),
            ("cat", categorical_pipeline, categorical_columns),
        ],
        remainder="drop",
        verbose_feature_names_out=False,
    )


def _ensure_required_columns(frame: Any, required_columns: list[str]) -> Any:
    np = _require_numpy()
    working = frame.copy()
    for col in required_columns:
        if col not in working.columns:
            working[col] = np.nan
    return working


def fit_transform_frame(
    frame: Any,
    preprocessor: Any,
    *,
    numeric_columns: list[str],
    categorical_columns: list[str],
    target_column: str | None = None,
) -> PreparedMatrix:
    np = _require_numpy()

    required_columns = list(numeric_columns) + list(categorical_columns)
    working = _ensure_required_columns(frame, required_columns)

    X = preprocessor.fit_transform(working[required_columns])

    if hasattr(preprocessor, "get_feature_names_out"):
        feature_names = [str(name) for name in preprocessor.get_feature_names_out()]
    else:  # pragma: no cover - sklearn legacy fallback
        feature_names = [f"feature_{idx}" for idx in range(X.shape[1])]

    y = None
    if target_column is not None:
        if target_column in working.columns:
            y = working[target_column].to_numpy()
        else:
            y = np.zeros(shape=(len(working),), dtype=float)

    return PreparedMatrix(
        frame=working,
        X=X,
        y=y,
        feature_names=feature_names,
        preprocessor=preprocessor,
    )


def transform_frame(
    frame: Any,
    preprocessor: Any,
    *,
    numeric_columns: list[str],
    categorical_columns: list[str],
    target_column: str | None = None,
) -> PreparedMatrix:
    np = _require_numpy()

    required_columns = list(numeric_columns) + list(categorical_columns)
    working = _ensure_required_columns(frame, required_columns)

    X = preprocessor.transform(working[required_columns])

    if hasattr(preprocessor, "get_feature_names_out"):
        feature_names = [str(name) for name in preprocessor.get_feature_names_out()]
    else:  # pragma: no cover - sklearn legacy fallback
        feature_names = [f"feature_{idx}" for idx in range(X.shape[1])]

    y = None
    if target_column is not None:
        if target_column in working.columns:
            y = working[target_column].to_numpy()
        else:
            y = np.zeros(shape=(len(working),), dtype=float)

    return PreparedMatrix(
        frame=working,
        X=X,
        y=y,
        feature_names=feature_names,
        preprocessor=preprocessor,
    )


def to_torch_ready_matrix(prepared: PreparedMatrix) -> tuple[Any, Any | None]:
    np = _require_numpy()

    X = prepared.X
    if hasattr(X, "toarray"):
        X = X.toarray()
    X_np = np.asarray(X, dtype=np.float32)

    y_np = None
    if prepared.y is not None:
        y_np = np.asarray(prepared.y, dtype=np.float32)

    return X_np, y_np
