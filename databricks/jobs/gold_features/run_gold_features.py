# ------------------------------------
# run_gold_features.py
#
# Dieses Skript erzeugt Gold-Feature-Tabellen aus den Silver-Daten.
# Es berechnet Baseline-Praediktionen und Explainability-Komponenten
# fuer die naechste Spieltagsbewertung.
#
# Outputs
# ------------------------------------
# 1) data/lakehouse/gold/feat_player_daily/snapshot_<ts>.ndjson
# 2) data/lakehouse/gold/feat_player_matchday/snapshot_<ts>.ndjson
# 3) data/lakehouse/gold/points_components_matchday/snapshot_<ts>.ndjson
# 4) data/lakehouse/gold/quality_metrics/snapshot_<ts>.ndjson
#
# Usage
# ------------------------------------
# - python -m databricks.jobs.gold_features.run_gold_features
# - python -m databricks.jobs.gold_features.run_gold_features --timestamp 2026-02-21T131500Z
# - python -m databricks.jobs.gold_features.run_gold_features --mc-samples 1000
# ------------------------------------

from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path
import random
import statistics
from typing import Any, Sequence

from databricks.jobs.common_io import (
    find_partitioned_files_for_timestamp,
    latest_timestamp_common_partitioned,
    read_ndjson,
    write_ndjson,
)


INPUT_DATASETS = [
    "player_snapshot",
    "team_matchup_snapshot",
]


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build gold feature tables from silver snapshots.")
    parser.add_argument("--lakehouse-silver-dir", type=Path, default=Path("data/lakehouse/silver"))
    parser.add_argument("--lakehouse-gold-dir", type=Path, default=Path("data/lakehouse/gold"))
    parser.add_argument("--timestamp", type=str, default=None)
    parser.add_argument("--mc-samples", type=int, default=400)
    return parser.parse_args(argv)


def _to_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _clamp(value: float, min_value: float, max_value: float) -> float:
    return max(min_value, min(max_value, value))


def _start_probability(status: str, lineup: str, competition_risk: str) -> float:
    status_scores = {
        "fit": 0.84,
        "questionable": 0.58,
        "injured": 0.08,
        "suspended": 0.05,
    }
    lineup_shift = {
        "starter": 0.10,
        "safe starter": 0.12,
        "potential starter": 0.04,
        "bench": -0.18,
    }
    risk_shift = {
        "low": 0.03,
        "medium": -0.06,
        "high": -0.12,
    }

    base = status_scores.get(status.lower().strip(), 0.72)
    base += lineup_shift.get(lineup.lower().strip(), 0.0)
    base += risk_shift.get(competition_risk.lower().strip(), 0.0)
    return _clamp(base, 0.01, 0.98)


def _scorer_probability(position: str) -> float:
    probs = {
        "FWD": 0.24,
        "MID": 0.15,
        "DEF": 0.07,
        "GK": 0.01,
    }
    return probs.get(position.strip().upper(), 0.10)


def _card_risk(status: str, competition_risk: str) -> float:
    risk = 0.12
    if status.lower().strip() == "questionable":
        risk += 0.05
    if competition_risk.lower().strip() == "high":
        risk += 0.04
    return _clamp(risk, 0.05, 0.30)


def _team_context_by_uid(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}
    for row in rows:
        team_uid = str(row.get("team_uid", "")).strip()
        if not team_uid:
            continue
        current = latest.get(team_uid)
        if current is None:
            latest[team_uid] = row
            continue

        current_has_odds = current.get("win_probability") is not None
        candidate_has_odds = row.get("win_probability") is not None
        if not current_has_odds and candidate_has_odds:
            latest[team_uid] = row
            continue

        current_commence = str(current.get("commence_time", "")).strip()
        candidate_commence = str(row.get("commence_time", "")).strip()
        if candidate_commence and (not current_commence or candidate_commence < current_commence):
            latest[team_uid] = row
    return latest


def _stable_seed(*parts: str) -> int:
    joined = "|".join(parts)
    digest = hashlib.sha256(joined.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], byteorder="big", signed=False)


def _quantile(sorted_values: list[float], q: float) -> float:
    if not sorted_values:
        return 0.0
    if q <= 0.0:
        return sorted_values[0]
    if q >= 1.0:
        return sorted_values[-1]

    idx = (len(sorted_values) - 1) * q
    lo = int(math.floor(idx))
    hi = int(math.ceil(idx))
    if lo == hi:
        return sorted_values[lo]

    weight = idx - lo
    return sorted_values[lo] * (1.0 - weight) + sorted_values[hi] * weight


def _simulate_points_distribution(
    *,
    start_prob: float,
    raw_points_recent: float,
    scorer_prob: float,
    team_win_prob: float,
    card_risk: float,
    samples: int,
    seed: int,
) -> dict[str, float]:
    safe_samples = max(100, samples)
    rng = random.Random(seed)

    base_mean = raw_points_recent * 0.78
    base_std = max(4.0, math.sqrt(max(raw_points_recent, 1.0)) * 0.9)

    draws: list[float] = []
    for _ in range(safe_samples):
        starts = rng.random() < start_prob
        if not starts:
            draws.append(0.0)
            continue

        base_component = max(0.0, rng.gauss(base_mean, base_std))
        scorer_component = 14.0 if rng.random() < scorer_prob else 0.0
        win_component = 6.0 if rng.random() < team_win_prob else 0.0
        minutes_component = 4.0
        cards_component = -4.0 if rng.random() < card_risk else 0.0

        total_points = max(
            0.0,
            base_component + scorer_component + win_component + minutes_component + cards_component,
        )
        draws.append(total_points)

    draws.sort()
    mean_points = statistics.fmean(draws)
    stddev_points = statistics.pstdev(draws) if len(draws) > 1 else 0.0

    return {
        "mean_points": mean_points,
        "stddev_points": stddev_points,
        "p10_points": _quantile(draws, 0.10),
        "p50_points": _quantile(draws, 0.50),
        "p90_points": _quantile(draws, 0.90),
    }


def run_gold_features(
    lakehouse_silver_dir: Path,
    lakehouse_gold_dir: Path,
    *,
    timestamp: str | None = None,
    mc_samples: int = 400,
) -> dict[str, object]:
    selected_timestamp = timestamp or latest_timestamp_common_partitioned(
        lakehouse_silver_dir,
        INPUT_DATASETS,
    )

    input_files = find_partitioned_files_for_timestamp(
        lakehouse_silver_dir,
        INPUT_DATASETS,
        selected_timestamp,
    )

    player_rows = read_ndjson(input_files["player_snapshot"])
    team_rows = read_ndjson(input_files["team_matchup_snapshot"])
    team_context_by_uid = _team_context_by_uid(team_rows)

    feat_player_daily: list[dict[str, Any]] = []
    feat_player_matchday: list[dict[str, Any]] = []
    points_components_matchday: list[dict[str, Any]] = []

    players_with_li_fields = 0

    for player in player_rows:
        player_uid = str(player.get("player_uid", "")).strip()
        if not player_uid:
            continue

        position = str(player.get("player_position", ""))
        status = str(player.get("injury_status", "")).strip() or str(player.get("li_status", "")).strip()
        lineup = str(player.get("li_predicted_lineup", "")).strip()
        competition_risk = str(player.get("li_competition_risk", "")).strip()
        if not competition_risk:
            competition_count = int(_to_float(player.get("li_competition_player_count"), 0.0))
            if competition_count <= 0:
                competition_risk = "low"
            elif competition_count == 1:
                competition_risk = "medium"
            else:
                competition_risk = "high"

        if lineup or competition_risk:
            players_with_li_fields += 1

        start_prob = _start_probability(status, lineup, competition_risk)
        p_dnp = 1.0 - start_prob

        raw_points_recent = _to_float(player.get("last_match_points"), _to_float(player.get("average_points"), 45.0))
        matchday = int(_to_float(player.get("last_matchday"), 0.0))

        scorer_prob = _scorer_probability(position)
        team_uid = str(player.get("team_uid", "")).strip()
        team_context = team_context_by_uid.get(team_uid, {})
        team_win_prob = _to_float(team_context.get("win_probability"), 0.5)
        card_risk = _card_risk(status, competition_risk)

        base_raw_ev = start_prob * (raw_points_recent * 0.78)
        scorer_ev = start_prob * scorer_prob * 14.0
        win_ev = start_prob * team_win_prob * 6.0
        minutes_bonus_ev = start_prob * 4.0
        cards_negative_ev = -start_prob * card_risk * 4.0

        mc_summary = _simulate_points_distribution(
            start_prob=start_prob,
            raw_points_recent=raw_points_recent,
            scorer_prob=scorer_prob,
            team_win_prob=team_win_prob,
            card_risk=card_risk,
            samples=mc_samples,
            seed=_stable_seed(player_uid, selected_timestamp),
        )

        pred_total = mc_summary["mean_points"]
        stddev_points = mc_summary["stddev_points"]
        p10_points = mc_summary["p10_points"]
        p50_points = mc_summary["p50_points"]
        p90_points = mc_summary["p90_points"]

        market_value = _to_float(player.get("market_value"), 0.0)
        expected_mv_change_1d = market_value * ((pred_total - 50.0) / 1000.0)
        expected_mv_next = market_value + expected_mv_change_1d
        expected_mv_change_7d = expected_mv_change_1d * 7.0

        value_score = 0.0
        if market_value > 0:
            value_score = (pred_total / (market_value / 1_000_000.0)) * (1.0 - 0.5 * p_dnp)

        base_row = {
            "player_uid": player_uid,
            "player_name": player.get("player_name"),
            "team": player.get("team_uid") or player.get("team_name"),
            "position": position,
            "snapshot_date": player.get("snapshot_date"),
            "start_probability": round(start_prob, 4),
            "expected_points_next_matchday": round(pred_total, 3),
            "p_dnp": round(p_dnp, 4),
            "stddev_points": round(stddev_points, 3),
            "p10_points": round(p10_points, 3),
            "p50_points": round(p50_points, 3),
            "p90_points": round(p90_points, 3),
            "risk_method": "monte_carlo_v1",
            "monte_carlo_samples": max(100, int(mc_samples)),
            "market_value": round(market_value, 2),
            "expected_marketvalue_next_matchday": round(expected_mv_next, 2),
            "expected_marketvalue_change_7d": round(expected_mv_change_7d, 2),
            "value_score": round(value_score, 6),
        }

        feat_player_daily.append(base_row)

        feat_player_matchday.append(
            {
                **base_row,
                "matchday": matchday,
            }
        )

        points_components_matchday.append(
            {
                "player_uid": player_uid,
                "player_name": player.get("player_name"),
                "matchday": matchday,
                "base_raw_ev": round(base_raw_ev, 3),
                "scorer_ev": round(scorer_ev, 3),
                "win_ev": round(win_ev, 3),
                "minutes_bonus_ev": round(minutes_bonus_ev, 3),
                "cards_negative_ev": round(cards_negative_ev, 3),
                "pred_total": round(pred_total, 3),
            }
        )

    quality_metrics = [
        {
            "metric": "player_count",
            "value": len(feat_player_daily),
            "timestamp": selected_timestamp,
        },
        {
            "metric": "li_coverage_ratio",
            "value": round(players_with_li_fields / max(len(feat_player_daily), 1), 4),
            "timestamp": selected_timestamp,
        },
        {
            "metric": "risk_mc_samples",
            "value": max(100, int(mc_samples)),
            "timestamp": selected_timestamp,
        },
    ]

    output_tables = {
        "feat_player_daily": feat_player_daily,
        "feat_player_matchday": feat_player_matchday,
        "points_components_matchday": points_components_matchday,
        "quality_metrics": quality_metrics,
    }

    files_written: list[str] = []
    rows_written = 0

    for table_name, rows in output_tables.items():
        output_path = lakehouse_gold_dir / table_name / f"snapshot_{selected_timestamp}.ndjson"
        write_ndjson(output_path, rows)
        files_written.append(str(output_path))
        rows_written += len(rows)

    return {
        "status": "success",
        "timestamp": selected_timestamp,
        "mc_samples": max(100, int(mc_samples)),
        "rows_written": rows_written,
        "tables_written": sorted(output_tables.keys()),
        "files_written": files_written,
    }


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    summary = run_gold_features(
        args.lakehouse_silver_dir,
        args.lakehouse_gold_dir,
        timestamp=args.timestamp,
        mc_samples=args.mc_samples,
    )
    print(json.dumps(summary, ensure_ascii=True, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
