# ------------------------------------
# prepare_history_exports.py
#
# Dieses Skript exportiert selektive History-Daten aus lokaler
# PostgreSQL-RAW-History sowie aktuelle Team-Snapshots aus Bronze
# in BigQuery-RAW JSONL-Dateien.
#
# Outputs
# ------------------------------------
# 1) data/warehouse/raw_history/hist_player_profile_snapshot.jsonl
# 2) data/warehouse/raw_history/hist_team_snapshot.jsonl
# 3) data/warehouse/raw_history/hist_player_marketvalue_daily.jsonl
# 4) data/warehouse/raw_history/hist_player_match_summary.jsonl
# 5) data/warehouse/raw_history/hist_player_match_components.jsonl
# 6) data/warehouse/raw_history/hist_team_lineup_players.jsonl
# 7) data/warehouse/raw_history/hist_team_odds_snapshot.jsonl
# 8) data/warehouse/raw_history/manifest.json
#
# Usage
# ------------------------------------
# - python -m bigquery.raw_load.prepare_history_exports --env-file .env
# - python -m bigquery.raw_load.prepare_history_exports --output-dir data/warehouse/raw_history
# - python -m bigquery.raw_load.prepare_history_exports --skip-live-snapshots
# ------------------------------------

from __future__ import annotations

import argparse
from datetime import UTC, date, datetime
from decimal import Decimal
import json
from pathlib import Path
import re
from typing import Any, Sequence
import unicodedata

from databricks.jobs.common_io import read_ndjson

try:
    from dotenv import load_dotenv
except ModuleNotFoundError:
    def load_dotenv(*_args: object, **_kwargs: object) -> bool:  # type: ignore[override]
        return False


TEAM_ALIAS_TO_CODE = {
    # Bayern
    "bayern": "FCB",
    "fc bayern": "FCB",
    "bayern munich": "FCB",
    "bayern muenchen": "FCB",
    "fc bayern munich": "FCB",
    "fc bayern munchen": "FCB",
    # Dortmund
    "borussia dortmund": "BVB",
    "dortmund": "BVB",
    # Leverkusen
    "bayer leverkusen": "B04",
    "bayer 04 leverkusen": "B04",
    "leverkusen": "B04",
    # Leipzig
    "rb leipzig": "RBL",
    "leipzig": "RBL",
    # Mainz
    "mainz": "M05",
    "mainz 05": "M05",
    "1 fsv mainz 05": "M05",
    # Bremen
    "werder bremen": "SVW",
    "sv werder bremen": "SVW",
    "bremen": "SVW",
    # Freiburg
    "sc freiburg": "SCF",
    "freiburg": "SCF",
    # Augsburg
    "fc augsburg": "FCA",
    "augsburg": "FCA",
    # Union
    "union berlin": "FCU",
    "1 fc union berlin": "FCU",
    # Gladbach
    "borussia monchengladbach": "BMG",
    "borussia moenchengladbach": "BMG",
    "monchengladbach": "BMG",
    "moenchengladbach": "BMG",
    "gladbach": "BMG",
    # Wolfsburg
    "vfl wolfsburg": "WOB",
    "wolfsburg": "WOB",
    # Hoffenheim
    "tsg hoffenheim": "TSG",
    "hoffenheim": "TSG",
    # Frankfurt
    "eintracht frankfurt": "SGE",
    "frankfurt": "SGE",
    # Stuttgart
    "vfb stuttgart": "VFB",
    "stuttgart": "VFB",
    # Koeln
    "1 fc koln": "KOE",
    "1 fc koeln": "KOE",
    "fc koln": "KOE",
    "fc koeln": "KOE",
    "koln": "KOE",
    "koeln": "KOE",
    # St. Pauli
    "fc st pauli": "STP",
    "st pauli": "STP",
    # Heidenheim
    "1 fc heidenheim": "FCH",
    "fc heidenheim": "FCH",
    "heidenheim": "FCH",
    # Hamburg
    "hamburger sv": "HSV",
    "hamburg": "HSV",
}


POSTGRES_EXPORTS: dict[str, str] = {
    "hist_player_profile_snapshot": """
        SELECT
            p.player_uid,
            p.player_name,
            p.player_birthdate,
            p.player_position,
            p.team_uid,
            t.team_name,
            t.kickbase_team_id,
            p.kb_player_id,
            p.ligainsider_player_id,
            p.ligainsider_player_slug,
            p.ligainsider_name,
            p.ligainsider_profile_url,
            p.image_mime,
            p.image_sha256,
            p.image_local_path,
            p.updated_at AS player_updated_at
        FROM {schema}.dim_player AS p
        LEFT JOIN {schema}.dim_team AS t
            ON t.team_uid = p.team_uid
        ORDER BY p.player_uid
    """,
    "hist_team_snapshot": """
        SELECT
            team_uid,
            team_name,
            kickbase_team_id,
            ligainsider_team_url,
            updated_at
        FROM {schema}.dim_team
        ORDER BY team_uid
    """,
    "hist_player_marketvalue_daily": """
        SELECT
            player_uid,
            mv_date,
            market_value,
            source_dt_days,
            ingested_at
        FROM {schema}.fact_market_value_daily
        ORDER BY player_uid, mv_date
    """,
    "hist_player_match_summary": """
        SELECT
            f.player_uid,
            f.match_uid,
            f.points_total,
            f.is_home,
            f.match_result,
            m.league_key,
            m.season_uid,
            m.season_label,
            m.matchday,
            m.kickoff_ts,
            m.home_team_uid,
            home.team_name AS home_team_name,
            m.away_team_uid,
            away.team_name AS away_team_name,
            m.score_home,
            m.score_away,
            CASE
                WHEN f.is_home IS TRUE THEN m.home_team_uid
                WHEN f.is_home IS FALSE THEN m.away_team_uid
                ELSE NULL
            END AS team_uid,
            CASE
                WHEN f.is_home IS TRUE THEN m.away_team_uid
                WHEN f.is_home IS FALSE THEN m.home_team_uid
                ELSE NULL
            END AS opponent_team_uid,
            f.ingested_at
        FROM {schema}.fact_player_match AS f
        JOIN {schema}.dim_match AS m
            ON m.match_uid = f.match_uid
        LEFT JOIN {schema}.dim_team AS home
            ON home.team_uid = m.home_team_uid
        LEFT JOIN {schema}.dim_team AS away
            ON away.team_uid = m.away_team_uid
        ORDER BY f.player_uid, m.season_uid, m.matchday, f.match_uid
    """,
    "hist_player_match_components": """
        SELECT
            e.player_uid,
            e.match_uid,
            e.event_type_id,
            et.event_name,
            COUNT(*) AS event_count,
            SUM(e.points) AS points_sum,
            MIN(e.ingested_at) AS first_ingested_at,
            MAX(e.ingested_at) AS last_ingested_at
        FROM {schema}.fact_player_event AS e
        LEFT JOIN {schema}.dim_event_type AS et
            ON et.event_type_id = e.event_type_id
        GROUP BY
            e.player_uid,
            e.match_uid,
            e.event_type_id,
            et.event_name
        ORDER BY e.player_uid, e.match_uid, e.event_type_id
    """,
}


def now_utc_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Prepare selective PostgreSQL history + live bronze snapshots for BigQuery RAW."
    )
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--output-dir", type=Path, default=Path("data/warehouse/raw_history"))
    parser.add_argument("--bronze-dir", type=Path, default=Path("data/bronze"))
    parser.add_argument("--skip-live-snapshots", action="store_true")
    return parser.parse_args(argv)


def _qualified_schema(schema: str) -> str:
    cleaned = schema.strip()
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", cleaned):
        raise ValueError(f"Invalid PostgreSQL schema name: {schema!r}")
    return cleaned


def _normalize_text(value: str | None) -> str:
    if not value:
        return ""
    normalized = unicodedata.normalize("NFKD", value)
    ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
    ascii_text = ascii_text.lower().strip()
    return re.sub(r"[^a-z0-9]+", " ", ascii_text).strip()


def _normalize_li_url(value: str | None) -> str | None:
    if value is None:
        return None
    text = value.strip()
    if not text:
        return None
    text = text.replace("http://", "https://")
    text = text.split("?", 1)[0].split("#", 1)[0]
    if not text.endswith("/"):
        text += "/"
    return text


def _extract_latest_snapshot_file(bronze_dir: Path, dataset_prefix: str) -> tuple[Path | None, str | None]:
    paths = sorted(bronze_dir.glob(f"{dataset_prefix}_*.ndjson"))
    if not paths:
        return None, None

    latest_path = paths[-1]
    match = re.search(r"(\d{4}-\d{2}-\d{2}T\d{6}Z)", latest_path.name)
    return latest_path, (match.group(1) if match else None)


def _json_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, bytes):
        return None
    if isinstance(value, list):
        return [_json_value(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _json_value(item) for key, item in value.items()}
    return str(value)


def _serialize_rows(rows: list[dict[str, Any]], *, exported_at: str, source_snapshot_ts: str | None) -> list[dict[str, Any]]:
    serialized: list[dict[str, Any]] = []
    for row in rows:
        payload = {key: _json_value(value) for key, value in row.items()}
        payload["raw_exported_at"] = exported_at
        payload["source_snapshot_ts"] = source_snapshot_ts
        serialized.append(payload)
    return serialized


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=True, sort_keys=True))
            handle.write("\n")


def _fetch_rows(conn: Any, query: str) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(query)
        columns = [str(desc[0]) for desc in cur.description]
        rows = cur.fetchall()
    return [dict(zip(columns, row, strict=False)) for row in rows]


def _build_team_maps(team_rows: list[dict[str, Any]]) -> tuple[dict[str, str], dict[str, str], dict[str, str]]:
    team_uid_by_li_url: dict[str, str] = {}
    team_uid_by_norm_name: dict[str, str] = {}
    team_uid_by_code: dict[str, str] = {}

    for row in team_rows:
        team_uid = str(row.get("team_uid") or "").strip()
        if not team_uid:
            continue

        team_uid_by_code[team_uid.upper()] = team_uid

        li_url = _normalize_li_url(_to_text_or_none(row.get("ligainsider_team_url")))
        if li_url:
            team_uid_by_li_url[li_url] = team_uid

        team_name = _to_text_or_none(row.get("team_name"))
        if team_name:
            team_uid_by_norm_name[_normalize_text(team_name)] = team_uid
            inner = _extract_team_name_inner(team_name)
            if inner:
                team_uid_by_norm_name[_normalize_text(inner)] = team_uid

        code_norm = _normalize_text(team_uid)
        if code_norm:
            team_uid_by_norm_name[code_norm] = team_uid

    for alias, code in TEAM_ALIAS_TO_CODE.items():
        team_uid = team_uid_by_code.get(code)
        if team_uid:
            team_uid_by_norm_name[_normalize_text(alias)] = team_uid

    return team_uid_by_li_url, team_uid_by_norm_name, team_uid_by_code


def _extract_team_name_inner(team_name: str) -> str | None:
    match = re.match(r"^[A-Z0-9]{2,5}\s*\((.+)\)$", team_name.strip())
    if match is None:
        return None
    inner = match.group(1).strip()
    return inner or None


def _to_text_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _resolve_team_uid_for_odds(team_name: str | None, team_uid_by_norm_name: dict[str, str]) -> str | None:
    normalized = _normalize_text(team_name)
    if not normalized:
        return None
    return team_uid_by_norm_name.get(normalized)


def _build_lineup_rows(
    *,
    bronze_dir: Path,
    exported_at: str,
    team_uid_by_li_url: dict[str, str],
) -> tuple[list[dict[str, Any]], str | None]:
    path, snapshot_ts = _extract_latest_snapshot_file(bronze_dir, "ligainsider_status_snapshot")
    if path is None:
        return [], None

    rows = read_ndjson(path)
    out_rows: list[dict[str, Any]] = []
    for row in rows:
        source_url = _normalize_li_url(_to_text_or_none(row.get("source_url")))
        out_rows.append(
            {
                "team_uid": team_uid_by_li_url.get(source_url or ""),
                "team_source_url": source_url,
                "ligainsider_player_id": _to_text_or_none(row.get("ligainsider_player_id")),
                "ligainsider_player_slug": _to_text_or_none(row.get("ligainsider_player_slug")),
                "ligainsider_profile_url": _to_text_or_none(row.get("ligainsider_profile_url")),
                "player_name": _to_text_or_none(row.get("player_name")),
                "predicted_lineup": _to_text_or_none(row.get("predicted_lineup")),
                "status": _to_text_or_none(row.get("status")),
                "competition_player_count": row.get("competition_player_count"),
                "competition_player_names": row.get("competition_player_names") or [],
                "scraped_at": _json_value(row.get("scraped_at")),
                "ingested_at": _json_value(row.get("ingested_at")),
                "last_changed_at": _json_value(row.get("last_changed_at")),
                "raw_exported_at": exported_at,
                "source_snapshot_ts": snapshot_ts,
            }
        )

    return out_rows, snapshot_ts


def _build_odds_rows(
    *,
    bronze_dir: Path,
    exported_at: str,
    team_uid_by_norm_name: dict[str, str],
) -> tuple[list[dict[str, Any]], str | None]:
    path, snapshot_ts = _extract_latest_snapshot_file(bronze_dir, "odds_match_snapshot")
    if path is None:
        return [], None

    rows = read_ndjson(path)
    out_rows: list[dict[str, Any]] = []
    for row in rows:
        home_team_name = _to_text_or_none(row.get("home_team"))
        away_team_name = _to_text_or_none(row.get("away_team"))
        out_rows.append(
            {
                "odds_event_id": _to_text_or_none(row.get("odds_event_id")),
                "sport_key": _to_text_or_none(row.get("sport_key")),
                "sport_title": _to_text_or_none(row.get("sport_title")),
                "commence_time": _json_value(row.get("commence_time")),
                "odds_last_changed_at": _json_value(row.get("odds_last_changed_at")),
                "odds_collected_at": _json_value(row.get("odds_collected_at")),
                "home_team_name": home_team_name,
                "away_team_name": away_team_name,
                "home_team_uid": _resolve_team_uid_for_odds(home_team_name, team_uid_by_norm_name),
                "away_team_uid": _resolve_team_uid_for_odds(away_team_name, team_uid_by_norm_name),
                "h2h_home_odds": _json_value(row.get("h2h_home_odds")),
                "h2h_draw_odds": _json_value(row.get("h2h_draw_odds")),
                "h2h_away_odds": _json_value(row.get("h2h_away_odds")),
                "h2h_home_implied_prob": _json_value(row.get("h2h_home_implied_prob")),
                "h2h_draw_implied_prob": _json_value(row.get("h2h_draw_implied_prob")),
                "h2h_away_implied_prob": _json_value(row.get("h2h_away_implied_prob")),
                "totals_line": _json_value(row.get("totals_line")),
                "totals_over_odds": _json_value(row.get("totals_over_odds")),
                "totals_under_odds": _json_value(row.get("totals_under_odds")),
                "totals_over_implied_prob": _json_value(row.get("totals_over_implied_prob")),
                "totals_under_implied_prob": _json_value(row.get("totals_under_implied_prob")),
                "bookmaker_count": _json_value(row.get("bookmaker_count")),
                "ingested_at": _json_value(row.get("ingested_at")),
                "raw_exported_at": exported_at,
                "source_snapshot_ts": snapshot_ts,
            }
        )

    return out_rows, snapshot_ts


def run_prepare_history_exports(
    *,
    output_dir: Path,
    bronze_dir: Path,
    include_live_snapshots: bool,
) -> dict[str, Any]:
    from src.db import DbConfig, get_connection

    db_cfg = DbConfig.from_env()
    schema = _qualified_schema(db_cfg.schema)
    exported_at = now_utc_iso()

    output_dir.mkdir(parents=True, exist_ok=True)

    files_written: list[str] = []
    row_count_by_table: dict[str, int] = {}
    bronze_snapshot_by_table: dict[str, str | None] = {}

    with get_connection(db_cfg) as conn:
        conn.autocommit = True

        # Team rows zuerst laden fuer Mappings.
        team_query = POSTGRES_EXPORTS["hist_team_snapshot"].format(schema=schema)
        team_rows_raw = _fetch_rows(conn, team_query)
        team_rows = _serialize_rows(team_rows_raw, exported_at=exported_at, source_snapshot_ts=exported_at)

        team_uid_by_li_url, team_uid_by_norm_name, _team_uid_by_code = _build_team_maps(team_rows_raw)

        out_team_path = output_dir / "hist_team_snapshot.jsonl"
        _write_jsonl(out_team_path, team_rows)
        files_written.append(str(out_team_path))
        row_count_by_table["hist_team_snapshot"] = len(team_rows)

        for table_name, query_template in POSTGRES_EXPORTS.items():
            if table_name == "hist_team_snapshot":
                continue

            query = query_template.format(schema=schema)
            rows = _fetch_rows(conn, query)
            serialized = _serialize_rows(rows, exported_at=exported_at, source_snapshot_ts=exported_at)

            out_path = output_dir / f"{table_name}.jsonl"
            _write_jsonl(out_path, serialized)
            files_written.append(str(out_path))
            row_count_by_table[table_name] = len(serialized)

        if include_live_snapshots:
            lineup_rows, lineup_ts = _build_lineup_rows(
                bronze_dir=bronze_dir,
                exported_at=exported_at,
                team_uid_by_li_url=team_uid_by_li_url,
            )
            odds_rows, odds_ts = _build_odds_rows(
                bronze_dir=bronze_dir,
                exported_at=exported_at,
                team_uid_by_norm_name=team_uid_by_norm_name,
            )

            lineup_out = output_dir / "hist_team_lineup_players.jsonl"
            odds_out = output_dir / "hist_team_odds_snapshot.jsonl"
            _write_jsonl(lineup_out, lineup_rows)
            _write_jsonl(odds_out, odds_rows)

            files_written.extend([str(lineup_out), str(odds_out)])
            row_count_by_table["hist_team_lineup_players"] = len(lineup_rows)
            row_count_by_table["hist_team_odds_snapshot"] = len(odds_rows)
            bronze_snapshot_by_table["hist_team_lineup_players"] = lineup_ts
            bronze_snapshot_by_table["hist_team_odds_snapshot"] = odds_ts

    manifest = {
        "generated_at": exported_at,
        "postgres_schema": db_cfg.schema,
        "files": files_written,
        "tables": row_count_by_table,
        "bronze_source_snapshot_ts": bronze_snapshot_by_table,
    }
    manifest_path = output_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=True, indent=2, sort_keys=True), encoding="utf-8")

    return {
        "status": "success",
        "files_written": files_written + [str(manifest_path)],
        "row_count_by_table": row_count_by_table,
        "generated_at": exported_at,
        "postgres_schema": db_cfg.schema,
        "bronze_source_snapshot_ts": bronze_snapshot_by_table,
    }


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    load_dotenv(args.env_file)

    summary = run_prepare_history_exports(
        output_dir=args.output_dir,
        bronze_dir=args.bronze_dir,
        include_live_snapshots=not args.skip_live_snapshots,
    )
    print(json.dumps(summary, ensure_ascii=True, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
