# ------------------------------------
# features_engineering.py
#
# Dieses Modul erzeugt Features und Zielvariablen fuer die hierarchische
# Spielerpraediktions-Pipeline (Starten, Minuten, Rohpunkte, On-Top-Punkte).
# Es verbindet Silver-Live-Snapshots mit historischer RAW-History aus Postgres.
#
# Outputs
# ------------------------------------
# 1) Modell-Feature-Frame (numerisch + kategorisch)
# 2) Historische Zielvariablen fuer Training/CV
# 3) Post-Processing-Hilfen fuer Team- und Kader-Konsistenz
#
# Usage
# ------------------------------------
# - base = build_modeling_frame(player_snapshot, team_snapshot)
# - targets = build_history_supervision_frame(match_df, event_df, event_type_df)
# - train = attach_history_targets(base, targets)
# - train = add_domain_features(train)
# - num_cols, cat_cols = default_feature_columns(train)
# ------------------------------------

from __future__ import annotations

from dataclasses import dataclass
import math
import re
from typing import Any
import unicodedata


@dataclass(frozen=True)
class FeatureContract:
    numeric_columns: list[str]
    categorical_columns: list[str]
    target_columns: list[str]


_RAW_EVENT_KEYWORDS = {
    "startelf",
    "starting 11",
    "von anfang an gespielt",
    "minutenbonus",
    "played minutes bonus",
    "pass vorderes drittel",
    "pass gegn. halfte",
    "pass gegn. haelfte",
    "pass final third",
    "forward zone pass",
    "praziser langer pass",
    "praeziser langer pass",
    "accurate long ball",
    "zweikampf gewonnen",
    "tackle won",
    "contest won",
    "luftzweikampf gewonnen",
    "luftzweikampf verloren",
    "aerial won",
    "aerial lost",
    "dribbling gewonnen",
    "dribbling verloren",
    "ballverlust",
    "possession lost",
    "fehlpass",
    "cross",
    "cross blocked",
    "cross blocked and possession",
    "shot assist",
    "shot blocked",
    "shot on goal",
    "shot on target",
    "shot on target (blocked)",
    "shot on target (narrow miss)",
    "offside",
    "foul",
    "fouled last third",
    "interception",
    "ball intercepted",
    "corner won",
    "cleared (in the box)",
    "cleared (outside the box)",
    "cleared on the line",
    "gehalten",
    "abgewehrt",
    "geblockt",
    "abgefangen",
}

_ON_TOP_EVENT_KEYWORDS = {
    "tor",
    "goal",
    "assist",
    "deadly pass",
    "big chance created",
    "big chance missed",
    "mistake before shot",
    "mistake before goal",
    "elfmeter",
    "penalty",
    "penalty scored",
    "penalty conceded",
    "spiel gewonnen",
    "game won",
    "game lost",
    "zu null",
    "clean sheet",
    "gelbe karte",
    "yellow card",
    "rote karte",
    "red card",
    "gelb-rote karte",
    "yellow-red card",
    "eigentor",
    "own goal",
    "own goal forced",
    "fehler vor gegentor",
    "tor kassiert",
    "goal conceded",
    "team goal",
    "verschossen",
}

_STARTER_KEYWORDS = {
    "startelf",
    "durchgespielt",
    "starting 11",
    "von anfang an gespielt",
}
_SUB_IN_KEYWORDS = {"eingewechselt", "einwechslung", "subbed on", "substituted in"}
_SUB_OUT_KEYWORDS = {"ausgewechselt", "auswechslung", "substituted out"}


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


def _safe_text(value: Any) -> str:
    return str(value or "").strip()


def _normalize_text(value: Any) -> str:
    text = _safe_text(value).lower()
    text = unicodedata.normalize("NFKD", text)
    text = text.encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"\s+", " ", text)
    return text


def _extract_event_name(frame: Any) -> Any:
    pd = _require_pandas()
    if "event_name" in frame.columns:
        return frame["event_name"].fillna("").astype(str)
    if "name" in frame.columns:
        return frame["name"].fillna("").astype(str)
    if "event_type_name" in frame.columns:
        return frame["event_type_name"].fillna("").astype(str)
    return pd.Series([""] * len(frame), index=frame.index, dtype=str)


def _extract_event_points(frame: Any) -> Any:
    pd = _require_pandas()
    if "points" in frame.columns:
        return pd.to_numeric(frame["points"], errors="coerce").fillna(0.0)
    if "point_value" in frame.columns:
        return pd.to_numeric(frame["point_value"], errors="coerce").fillna(0.0)
    return pd.Series([0.0] * len(frame), index=frame.index, dtype=float)


def _coalesce_match_uid(frame: Any) -> Any:
    pd = _require_pandas()
    if "match_uid" in frame.columns:
        return frame["match_uid"].fillna("").astype(str)
    if "match_id" in frame.columns:
        return frame["match_id"].fillna("").astype(str)
    return pd.Series([""] * len(frame), index=frame.index, dtype=str)


def split_event_points(
    event_rows: Any,
    *,
    event_type_rows: Any | None = None,
) -> Any:
    pd = _require_pandas()
    np = _require_numpy()

    events = event_rows.copy()
    if events.empty:
        return pd.DataFrame(
            columns=[
                "player_uid",
                "match_uid",
                "raw_points_actual",
                "on_top_points_actual",
                "uncategorized_points_actual",
                "event_count",
                "started_flag",
                "subbed_in_flag",
                "subbed_out_flag",
            ]
        )

    if event_type_rows is not None and not event_type_rows.empty and "event_name" not in events.columns:
        if "event_type_id" in events.columns and "event_type_id" in event_type_rows.columns:
            lookup = event_type_rows[["event_type_id", "event_name"]].drop_duplicates("event_type_id")
            events = events.merge(lookup, on="event_type_id", how="left")

    events["player_uid"] = events.get("player_uid", "").astype(str)
    events["match_uid"] = _coalesce_match_uid(events)
    events["event_name"] = _extract_event_name(events)
    events["points"] = _extract_event_points(events)

    normalized_name = events["event_name"].map(_normalize_text)

    is_raw = normalized_name.map(
        lambda text: any(keyword in text for keyword in _RAW_EVENT_KEYWORDS)
    )
    is_on_top = normalized_name.map(
        lambda text: any(keyword in text for keyword in _ON_TOP_EVENT_KEYWORDS)
    )

    events["raw_points_component"] = np.where(is_raw, events["points"], 0.0)
    events["on_top_points_component"] = np.where(is_on_top, events["points"], 0.0)
    events["uncategorized_points_component"] = np.where(~(is_raw | is_on_top), events["points"], 0.0)

    events["started_flag"] = normalized_name.map(
        lambda text: 1.0 if any(keyword in text for keyword in _STARTER_KEYWORDS) else 0.0
    )
    events["subbed_in_flag"] = normalized_name.map(
        lambda text: 1.0 if any(keyword in text for keyword in _SUB_IN_KEYWORDS) else 0.0
    )
    events["subbed_out_flag"] = normalized_name.map(
        lambda text: 1.0 if any(keyword in text for keyword in _SUB_OUT_KEYWORDS) else 0.0
    )

    grouped = (
        events.groupby(["player_uid", "match_uid"], as_index=False)
        .agg(
            raw_points_actual=("raw_points_component", "sum"),
            on_top_points_actual=("on_top_points_component", "sum"),
            uncategorized_points_actual=("uncategorized_points_component", "sum"),
            event_count=("points", "size"),
            started_flag=("started_flag", "max"),
            subbed_in_flag=("subbed_in_flag", "max"),
            subbed_out_flag=("subbed_out_flag", "max"),
        )
        .reset_index(drop=True)
    )

    return grouped


def _extract_minutes_from_raw_json(value: Any) -> float | None:
    if not isinstance(value, dict):
        return None

    for key in (
        "minutes",
        "minutesPlayed",
        "minutes_played",
        "m",
        "playedMinutes",
        "mp",
    ):
        raw = value.get(key)
        if raw is None:
            continue
        if isinstance(raw, str):
            stripped = raw.replace("'", "").strip()
            if stripped:
                try:
                    return float(stripped)
                except (TypeError, ValueError):
                    pass
        try:
            return float(raw)
        except (TypeError, ValueError):
            continue
    return None


def build_history_supervision_frame(
    player_match_rows: Any,
    player_event_rows: Any,
    event_type_rows: Any | None = None,
) -> Any:
    pd = _require_pandas()

    matches = player_match_rows.copy()
    if matches.empty:
        return pd.DataFrame(
            columns=[
                "player_uid",
                "match_uid",
                "target_started",
                "target_subbed_in",
                "target_minutes",
                "target_raw_points",
                "target_on_top_points",
                "target_points_total",
                "actual_match_result",
            ]
        )

    if "player_uid" not in matches.columns:
        matches["player_uid"] = ""
    matches["player_uid"] = matches["player_uid"].astype(str)
    matches["match_uid"] = _coalesce_match_uid(matches)

    if "points_total" in matches.columns:
        matches["points_total"] = pd.to_numeric(matches["points_total"], errors="coerce").fillna(0.0)
    else:
        matches["points_total"] = 0.0

    split = split_event_points(player_event_rows, event_type_rows=event_type_rows)
    frame = matches.merge(split, on=["player_uid", "match_uid"], how="left")

    for col in [
        "raw_points_actual",
        "on_top_points_actual",
        "uncategorized_points_actual",
        "event_count",
        "started_flag",
        "subbed_in_flag",
        "subbed_out_flag",
    ]:
        if col not in frame.columns:
            frame[col] = 0.0
        frame[col] = pd.to_numeric(frame[col], errors="coerce").fillna(0.0)

    if "raw_json" in frame.columns:
        frame["minutes_from_raw_json"] = frame["raw_json"].map(_extract_minutes_from_raw_json)
    else:
        frame["minutes_from_raw_json"] = None

    default_minutes = frame["started_flag"].map(lambda value: 90.0 if value >= 1.0 else 22.0)
    frame["target_minutes"] = pd.to_numeric(frame["minutes_from_raw_json"], errors="coerce").fillna(default_minutes)
    inferred_starter = (frame["target_minutes"] >= 45.0).astype(float)
    frame["target_started"] = ((frame["started_flag"] >= 1.0) | (inferred_starter >= 1.0)).astype(float)
    frame["target_subbed_in"] = (frame["subbed_in_flag"] >= 1.0).astype(float)

    frame["target_points_total"] = frame["points_total"]
    frame["target_on_top_points"] = frame["on_top_points_actual"]
    frame["target_raw_points"] = frame["target_points_total"] - frame["target_on_top_points"]

    if "match_result" in frame.columns:
        frame["actual_match_result"] = frame["match_result"].fillna("unknown").astype(str)
    else:
        frame["actual_match_result"] = "unknown"

    return frame[
        [
            "player_uid",
            "match_uid",
            "target_started",
            "target_subbed_in",
            "target_minutes",
            "target_raw_points",
            "target_on_top_points",
            "target_points_total",
            "actual_match_result",
            "event_count",
            "uncategorized_points_actual",
        ]
    ].copy()


def attach_history_targets(
    modeling_frame: Any,
    history_targets: Any,
    *,
    key_columns: tuple[str, ...] = ("player_uid", "match_uid"),
) -> Any:
    if modeling_frame is None or history_targets is None:
        return modeling_frame

    frame = modeling_frame.copy()
    targets = history_targets.copy()
    join_cols = [col for col in key_columns if col in frame.columns and col in targets.columns]
    if not join_cols:
        return frame

    return frame.merge(targets, on=join_cols, how="left", suffixes=("", "_target"))


def build_modeling_frame(player_snapshot: Any, team_matchup_snapshot: Any) -> Any:
    from src.preprocessing import build_base_training_frame

    return build_base_training_frame(
        player_snapshot=player_snapshot,
        team_matchup_snapshot=team_matchup_snapshot,
        mode="live",
    )


def _safe_numeric(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def add_domain_features(frame: Any) -> Any:
    pd = _require_pandas()

    out = frame.copy()
    if out.empty:
        return out

    numeric_defaults = {
        "market_value": 0.0,
        "average_points": 0.0,
        "last_match_points": 0.0,
        "average_minutes": 0.0,
        "last_matchday": 0.0,
        "li_competition_player_count": 0.0,
        "start_probability_prior": 0.0,
        "sub_probability_prior": 0.0,
        "expected_minutes_prior": 0.0,
        "win_probability": 0.0,
        "draw_probability": 0.0,
        "loss_probability": 0.0,
        "totals_line": 0.0,
        "totals_over_probability": 0.0,
        "totals_under_probability": 0.0,
    }
    for column, default in numeric_defaults.items():
        if column not in out.columns:
            out[column] = default
        out[column] = pd.to_numeric(out[column], errors="coerce").fillna(default)

    out["market_value_log"] = out["market_value"].map(lambda value: math.log1p(max(0.0, value)))
    out["value_per_point"] = out.apply(
        lambda row: row["market_value"] / max(row["average_points"], 1.0),
        axis=1,
    )
    out["availability_index"] = (
        0.65 * out["start_probability_prior"] + 0.35 * (1.0 - out["sub_probability_prior"])
    ).clip(lower=0.0, upper=1.0)
    out["recency_points_delta"] = out["last_match_points"] - out["average_points"]
    out["odds_goal_environment"] = out["totals_line"] * out["totals_over_probability"]
    out["season_progress"] = (out["last_matchday"] / 34.0).clip(lower=0.0, upper=1.25)
    out["is_home_numeric"] = out.get("is_home", False).map(lambda value: 1.0 if bool(value) else 0.0)

    return out


def add_history_sequence_features(
    frame: Any,
    *,
    player_column: str = "player_uid",
    date_column: str = "snapshot_date",
) -> Any:
    pd = _require_pandas()

    out = frame.copy()
    if out.empty:
        return out

    if player_column not in out.columns:
        return out

    if date_column not in out.columns:
        out[date_column] = pd.NaT

    out[date_column] = pd.to_datetime(out[date_column], errors="coerce")

    numeric_target_defaults = {
        "target_points_total": 0.0,
        "target_raw_points": 0.0,
        "target_on_top_points": 0.0,
        "target_started": 0.0,
        "target_subbed_in": 0.0,
        "target_minutes": 0.0,
    }
    for column, default in numeric_target_defaults.items():
        if column not in out.columns:
            out[column] = default
        out[column] = pd.to_numeric(out[column], errors="coerce").fillna(default)

    sort_cols = [player_column]
    if date_column in out.columns:
        sort_cols.append(date_column)
    if "match_uid" in out.columns:
        sort_cols.append("match_uid")

    out = out.sort_values(sort_cols).reset_index(drop=True)
    grouped = out.groupby(player_column, sort=False)

    out["hist_points_lag_1"] = grouped["target_points_total"].transform(lambda s: s.shift(1))
    out["hist_points_avg_3"] = grouped["target_points_total"].transform(
        lambda s: s.shift(1).rolling(window=3, min_periods=1).mean()
    )
    out["hist_points_avg_5"] = grouped["target_points_total"].transform(
        lambda s: s.shift(1).rolling(window=5, min_periods=1).mean()
    )
    out["hist_points_avg_10"] = grouped["target_points_total"].transform(
        lambda s: s.shift(1).rolling(window=10, min_periods=1).mean()
    )
    out["hist_points_std_5"] = grouped["target_points_total"].transform(
        lambda s: s.shift(1).rolling(window=5, min_periods=2).std(ddof=0)
    )

    out["hist_raw_points_avg_5"] = grouped["target_raw_points"].transform(
        lambda s: s.shift(1).rolling(window=5, min_periods=1).mean()
    )
    out["hist_on_top_points_avg_5"] = grouped["target_on_top_points"].transform(
        lambda s: s.shift(1).rolling(window=5, min_periods=1).mean()
    )
    out["hist_started_rate_5"] = grouped["target_started"].transform(
        lambda s: s.shift(1).rolling(window=5, min_periods=1).mean()
    )
    out["hist_subbed_in_rate_5"] = grouped["target_subbed_in"].transform(
        lambda s: s.shift(1).rolling(window=5, min_periods=1).mean()
    )
    out["hist_minutes_avg_5"] = grouped["target_minutes"].transform(
        lambda s: s.shift(1).rolling(window=5, min_periods=1).mean()
    )

    fill_defaults = {
        "hist_points_lag_1": 0.0,
        "hist_points_avg_3": 0.0,
        "hist_points_avg_5": 0.0,
        "hist_points_avg_10": 0.0,
        "hist_points_std_5": 0.0,
        "hist_raw_points_avg_5": 0.0,
        "hist_on_top_points_avg_5": 0.0,
        "hist_started_rate_5": 0.50,
        "hist_subbed_in_rate_5": 0.20,
        "hist_minutes_avg_5": 60.0,
    }
    for column, default in fill_defaults.items():
        out[column] = pd.to_numeric(out[column], errors="coerce").fillna(default)

    if "start_probability_prior" not in out.columns:
        out["start_probability_prior"] = out["hist_started_rate_5"]
    if "sub_probability_prior" not in out.columns:
        out["sub_probability_prior"] = out["hist_subbed_in_rate_5"]
    if "expected_minutes_prior" not in out.columns:
        out["expected_minutes_prior"] = out["hist_minutes_avg_5"]
    if "average_points" not in out.columns:
        out["average_points"] = out["hist_points_avg_5"]
    if "last_match_points" not in out.columns:
        out["last_match_points"] = out["hist_points_lag_1"]
    if "average_minutes" not in out.columns:
        out["average_minutes"] = out["hist_minutes_avg_5"]

    return out


def extract_latest_history_player_features(
    history_feature_frame: Any,
    *,
    player_column: str = "player_uid",
    date_column: str = "snapshot_date",
) -> Any:
    pd = _require_pandas()

    out = history_feature_frame.copy()
    if out.empty:
        return pd.DataFrame(columns=[player_column])
    if player_column not in out.columns:
        return pd.DataFrame(columns=[player_column])

    if date_column in out.columns:
        out[date_column] = pd.to_datetime(out[date_column], errors="coerce")
        out = out.sort_values([player_column, date_column, "match_uid" if "match_uid" in out.columns else player_column])
    else:
        out = out.sort_values([player_column])

    feature_cols = [
        "hist_points_lag_1",
        "hist_points_avg_3",
        "hist_points_avg_5",
        "hist_points_avg_10",
        "hist_points_std_5",
        "hist_raw_points_avg_5",
        "hist_on_top_points_avg_5",
        "hist_started_rate_5",
        "hist_subbed_in_rate_5",
        "hist_minutes_avg_5",
    ]
    available_cols = [col for col in feature_cols if col in out.columns]
    selected_cols = [player_column] + available_cols

    latest = out.groupby(player_column, as_index=False).tail(1)[selected_cols].copy()
    latest = latest.drop_duplicates(subset=[player_column], keep="last")
    return latest


def attach_recent_history_features_to_live(
    live_frame: Any,
    history_feature_frame: Any,
    *,
    player_column: str = "player_uid",
) -> Any:
    pd = _require_pandas()

    if live_frame is None:
        return live_frame

    live = live_frame.copy()
    if live.empty:
        return live

    latest = extract_latest_history_player_features(
        history_feature_frame,
        player_column=player_column,
    )
    if latest.empty or player_column not in live.columns:
        return live

    out = live.merge(latest, on=player_column, how="left")
    for col, default in {
        "hist_points_lag_1": 0.0,
        "hist_points_avg_3": 0.0,
        "hist_points_avg_5": 0.0,
        "hist_points_avg_10": 0.0,
        "hist_points_std_5": 0.0,
        "hist_raw_points_avg_5": 0.0,
        "hist_on_top_points_avg_5": 0.0,
        "hist_started_rate_5": 0.50,
        "hist_subbed_in_rate_5": 0.20,
        "hist_minutes_avg_5": 60.0,
    }.items():
        if col not in out.columns:
            out[col] = default
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(default)

    out["start_probability_prior"] = (
        0.70 * pd.to_numeric(out.get("start_probability_prior"), errors="coerce").fillna(0.50)
        + 0.30 * out["hist_started_rate_5"]
    ).clip(lower=0.0, upper=0.99)
    out["sub_probability_prior"] = (
        0.70 * pd.to_numeric(out.get("sub_probability_prior"), errors="coerce").fillna(0.20)
        + 0.30 * out["hist_subbed_in_rate_5"]
    ).clip(lower=0.0, upper=0.99)
    out["expected_minutes_prior"] = (
        0.70 * pd.to_numeric(out.get("expected_minutes_prior"), errors="coerce").fillna(60.0)
        + 0.30 * out["hist_minutes_avg_5"]
    ).clip(lower=0.0, upper=110.0)

    return out


def default_feature_columns(frame: Any) -> FeatureContract:
    numeric_candidates = [
        "market_value",
        "market_value_day_change",
        "market_value_high_365d",
        "market_value_low_365d",
        "average_paid_price_10d",
        "average_points",
        "last_match_points",
        "total_points",
        "average_minutes",
        "appearances_total",
        "starts_total",
        "goals_total",
        "assists_total",
        "yellow_cards_total",
        "red_cards_total",
        "last_matchday",
        "li_competition_player_count",
        "win_probability",
        "draw_probability",
        "loss_probability",
        "totals_line",
        "totals_over_probability",
        "totals_under_probability",
        "start_probability_prior",
        "sub_probability_prior",
        "expected_minutes_prior",
        "market_value_log",
        "value_per_point",
        "availability_index",
        "recency_points_delta",
        "odds_goal_environment",
        "season_progress",
        "is_home_numeric",
        "hist_points_lag_1",
        "hist_points_avg_3",
        "hist_points_avg_5",
        "hist_points_avg_10",
        "hist_points_std_5",
        "hist_raw_points_avg_5",
        "hist_on_top_points_avg_5",
        "hist_started_rate_5",
        "hist_subbed_in_rate_5",
        "hist_minutes_avg_5",
    ]
    categorical_candidates = [
        "player_position",
        "team_uid",
        "opponent_team_uid",
        "likely_result",
        "start_probability_label",
        "li_predicted_lineup",
        "injury_status",
        "li_status",
        "season_uid",
    ]
    target_candidates = [
        "target_started",
        "target_subbed_in",
        "target_minutes",
        "target_raw_points",
        "target_on_top_points",
        "target_points_total",
    ]

    available_numeric = [col for col in numeric_candidates if col in frame.columns]
    available_categorical = [col for col in categorical_candidates if col in frame.columns]
    available_targets = [col for col in target_candidates if col in frame.columns]

    return FeatureContract(
        numeric_columns=available_numeric,
        categorical_columns=available_categorical,
        target_columns=available_targets,
    )


def enforce_team_probability_constraints(
    prediction_frame: Any,
    *,
    team_column: str = "team_uid",
    match_column: str = "match_uid",
    start_column: str = "start_probability",
    sub_column: str = "sub_probability",
    max_starters: int = 11,
    max_subs: int = 5,
) -> Any:
    pd = _require_pandas()

    out = prediction_frame.copy()
    if out.empty:
        return out

    for col in [start_column, sub_column]:
        if col not in out.columns:
            out[col] = 0.0
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0.0).clip(lower=0.0, upper=1.0)

    if team_column not in out.columns:
        return out
    if match_column not in out.columns:
        out[match_column] = "unknown-match"

    adjusted_rows: list[Any] = []
    for (_, _), group in out.groupby([match_column, team_column], dropna=False):
        grp = group.copy()

        starter_sum = float(grp[start_column].sum())
        if starter_sum > float(max_starters):
            scale = float(max_starters) / starter_sum
            grp[start_column] = grp[start_column] * scale

        sub_sum = float(grp[sub_column].sum())
        if sub_sum > float(max_subs):
            scale = float(max_subs) / sub_sum
            grp[sub_column] = grp[sub_column] * scale

        combined = grp[start_column] + grp[sub_column]
        overflow_mask = combined > 1.0
        if overflow_mask.any():
            ratio = 1.0 / combined[overflow_mask]
            grp.loc[overflow_mask, start_column] = grp.loc[overflow_mask, start_column] * ratio
            grp.loc[overflow_mask, sub_column] = grp.loc[overflow_mask, sub_column] * ratio

        adjusted_rows.append(grp)

    return pd.concat(adjusted_rows, ignore_index=True)


def estimate_team_expected_goals(
    team_matchup_frame: Any,
    *,
    totals_line_column: str = "totals_line",
    win_prob_column: str = "win_probability",
    draw_prob_column: str = "draw_probability",
    loss_prob_column: str = "loss_probability",
) -> Any:
    pd = _require_pandas()

    teams = team_matchup_frame.copy()
    if teams.empty:
        return teams

    for col in [totals_line_column, win_prob_column, draw_prob_column, loss_prob_column]:
        if col not in teams.columns:
            teams[col] = 0.0
        teams[col] = pd.to_numeric(teams[col], errors="coerce").fillna(0.0)

    # Einfache, robuste Approximation fuer Team-Expected-Goals aus Odds.
    balance = teams[win_prob_column] - teams[loss_prob_column]
    teams["team_expected_goals"] = (
        0.5 * teams[totals_line_column] + 0.9 * balance + 0.2 * teams[draw_prob_column]
    ).clip(lower=0.0, upper=5.5)
    return teams
