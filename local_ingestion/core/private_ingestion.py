# ------------------------------------
# private_ingestion.py
#
# Dieses Modul orchestriert den private Ingestion-Flow:
# Kickbase Auth, Snapshot-Abruf, optionales LigaInsider-File
# und Bronze-Schreiben mit Telemetrie.
#
# Outputs
# ------------------------------------
# 1) data/bronze/<dataset>_<timestamp>.ndjson
# 2) data/bronze/ingestion_runs.ndjson
#
# Usage
# ------------------------------------
# - run_private_ingestion(config, Path("data/bronze"))
# ------------------------------------

from __future__ import annotations

from datetime import UTC, datetime
import json
from pathlib import Path
from typing import Any

from local_ingestion.core.bronze_writer import write_bronze_outputs
from local_ingestion.core.cache import JsonFileCache
from local_ingestion.core.config import PrivateIngestionConfig
from local_ingestion.core.kickbase_bronze_builder import build_kickbase_player_row
from local_ingestion.core.ligainsider_bronze_builder import build_ligainsider_rows
from local_ingestion.kickbase_client.client import HttpTransport, KickbaseClient
from local_ingestion.ligainsider_scraper.scraper import HtmlTransport, LigaInsiderScraper


def _kickbase_player_id(row: dict[str, Any]) -> str:
    for key in ("kickbase_player_id", "player_id", "id", "i", "pi"):
        value = row.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def _merge_unique_players(
    current_rows: list[dict[str, Any]],
    new_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    seen: set[str] = set()
    merged: list[dict[str, Any]] = []

    for row in [*current_rows, *new_rows]:
        player_id = _kickbase_player_id(row)
        if player_id and player_id in seen:
            continue
        if player_id:
            seen.add(player_id)
        merged.append(row)

    return merged


def load_optional_snapshot(path: Path | None) -> list[dict[str, Any]]:
    if path is None:
        return []
    if not path.exists():
        return []

    with path.open("r", encoding="utf-8") as f:
        payload = json.load(f)

    if not isinstance(payload, list):
        raise ValueError(f"Expected list payload in {path}, got {type(payload)!r}")

    rows: list[dict[str, Any]] = []
    for item in payload:
        if not isinstance(item, dict):
            raise ValueError(f"Expected object rows in {path}, got {type(item)!r}")
        rows.append(item)
    return rows


def load_latest_dataset_snapshot(out_dir: Path, dataset_name: str) -> list[dict[str, Any]]:
    pattern = f"{dataset_name}_*.ndjson"
    candidates = sorted(out_dir.glob(pattern))
    if not candidates:
        return []

    latest_path = candidates[-1]
    rows: list[dict[str, Any]] = []
    with latest_path.open("r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line:
                continue
            payload = json.loads(line)
            if isinstance(payload, dict):
                rows.append(payload)
    return rows


def run_private_ingestion(
    config: PrivateIngestionConfig,
    out_dir: Path,
    *,
    now: datetime | None = None,
    transport: HttpTransport | None = None,
    ligainsider_transport: HtmlTransport | None = None,
) -> dict[str, Any]:
    cache = JsonFileCache(config.cache_dir)
    client = KickbaseClient(
        base_url=config.base_url,
        auth_path=config.auth_path,
        auth_email_field=config.auth_email_field,
        auth_password_field=config.auth_password_field,
        player_snapshot_path=config.player_snapshot_path,
        competition_players_search_path=config.competition_players_search_path,
        match_stats_path=config.match_stats_path,
        player_details_path=config.player_details_path,
        player_market_value_history_path=config.player_market_value_history_path,
        player_performance_path=config.player_performance_path,
        player_transfers_path=config.player_transfers_path,
        email=config.email,
        password=config.password,
        user_agent=config.kickbase_user_agent,
        retry_config=config.retry,
        cache=cache,
        cache_ttl_seconds=config.cache_ttl_seconds,
        transport=transport,
    )

    token = client.authenticate()
    competition_id = config.competition_id or client.discover_competition_id(league_id=config.league_id)
    kickbase_market_rows: list[dict[str, Any]] = []
    if competition_id:
        kickbase_market_rows = client.fetch_all_competition_players(
            token=token,
            competition_id=competition_id,
            league_id=config.league_id,
            query=config.competition_players_query,
            page_size=config.competition_players_page_size,
        )

        # Manche API-Staende liefern bei leerem Query nur eine kleine Teilmenge.
        # Dann aggregieren wir zusaetzlich ueber Prefix-Queries und deduplizieren per Player-ID.
        if (
            not config.competition_players_query.strip()
            and len(kickbase_market_rows) <= config.competition_players_page_size
        ):
            for query in list("abcdefghijklmnopqrstuvwxyz") + ["ä", "ö", "ü"]:
                extra_rows = client.fetch_all_competition_players(
                    token=token,
                    competition_id=competition_id,
                    league_id=config.league_id,
                    query=query,
                    page_size=config.competition_players_page_size,
                )
                kickbase_market_rows = _merge_unique_players(kickbase_market_rows, extra_rows)

    if not kickbase_market_rows:
        kickbase_market_rows = client.fetch_player_snapshot(token=token, league_id=config.league_id)
    kickbase_players: list[dict[str, Any]] = []

    for market_row in kickbase_market_rows:
        player_id = str(
            market_row.get("kickbase_player_id")
            or market_row.get("player_id")
            or market_row.get("id")
            or market_row.get("i")
            or market_row.get("pi")
            or ""
        ).strip()
        if not player_id:
            continue

        details = client.fetch_player_details(
            token=token,
            league_id=config.league_id,
            player_id=player_id,
        )
        market_values = client.fetch_player_market_value_history(token=token, player_id=player_id)
        performance = client.fetch_player_performance(token=token, player_id=player_id)
        transfers = client.fetch_player_transfers(
            token=token,
            league_id=config.league_id,
            player_id=player_id,
        )

        snapshot_ts = now.astimezone(UTC) if now is not None and now.tzinfo else (
            now.replace(tzinfo=UTC) if now is not None else datetime.now(UTC)
        )

        kickbase_players.append(
            build_kickbase_player_row(
                market_row=market_row,
                details_payload=details,
                market_value_history_payload=market_values,
                performance_payload=performance,
                transfers_payload=transfers,
                snapshot_ts=snapshot_ts,
            )
        )

    ligainsider_rows: list[dict[str, Any]]
    if config.ligainsider_status_file is not None:
        ligainsider_rows = load_optional_snapshot(config.ligainsider_status_file)
    elif config.ligainsider_status_url:
        ligainsider_scraper = LigaInsiderScraper(
            user_agent=config.ligainsider_user_agent,
            retry_config=config.ligainsider_retry,
            cache=cache,
            cache_ttl_seconds=config.cache_ttl_seconds,
            transport=ligainsider_transport,
        )
        ligainsider_rows = []
        seen_keys: set[tuple[str, str]] = set()
        for raw_url in str(config.ligainsider_status_url).split(","):
            url = raw_url.strip()
            if not url:
                continue
            rows_for_url = ligainsider_scraper.fetch_status_snapshot(url)
            for row in rows_for_url:
                key = (
                    str(row.get("ligainsider_player_slug", "")).strip().lower(),
                    str(row.get("player_name", "")).strip().lower(),
                )
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                ligainsider_rows.append(row)
    else:
        ligainsider_rows = []

    previous_ligainsider_rows = load_latest_dataset_snapshot(
        out_dir,
        "ligainsider_status_snapshot",
    )
    ligainsider_rows = build_ligainsider_rows(
        raw_rows=ligainsider_rows,
        kickbase_rows=kickbase_players,
        previous_rows=previous_ligainsider_rows,
    )

    rows_by_dataset = {
        "kickbase_player_snapshot": kickbase_players,
        "ligainsider_status_snapshot": ligainsider_rows,
    }

    return write_bronze_outputs(
        rows_by_dataset,
        out_dir,
        mode="private",
        now=now,
        source_version=config.source_version,
    )
