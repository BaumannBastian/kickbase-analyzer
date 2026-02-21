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

from datetime import datetime
import json
from pathlib import Path
from typing import Any

from local_ingestion.core.bronze_writer import write_bronze_outputs
from local_ingestion.core.cache import JsonFileCache
from local_ingestion.core.config import PrivateIngestionConfig
from local_ingestion.kickbase_client.client import HttpTransport, KickbaseClient
from local_ingestion.ligainsider_scraper.scraper import HtmlTransport, LigaInsiderScraper


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
        match_stats_path=config.match_stats_path,
        email=config.email,
        password=config.password,
        retry_config=config.retry,
        cache=cache,
        cache_ttl_seconds=config.cache_ttl_seconds,
        transport=transport,
    )

    token = client.authenticate()
    kickbase_players = client.fetch_player_snapshot(token=token, league_id=config.league_id)
    kickbase_match_stats = client.fetch_match_stats(token=token, league_id=config.league_id)

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

    rows_by_dataset = {
        "kickbase_player_snapshot": kickbase_players,
        "kickbase_match_stats": kickbase_match_stats,
        "ligainsider_status_snapshot": ligainsider_rows,
    }

    return write_bronze_outputs(
        rows_by_dataset,
        out_dir,
        mode="private",
        now=now,
        source_version=config.source_version,
    )
