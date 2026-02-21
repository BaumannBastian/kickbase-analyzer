# ------------------------------------
# check_kickbase_auth.py
#
# Dieses Skript validiert die Kickbase-Authentifizierung mit den
# konfigurierten Credentials aus `.env` oder Umgebungsvariablen.
# Optional werden danach Snapshot-Endpunkte testweise abgefragt.
#
# Outputs
# ------------------------------------
# 1) Keine Dateiausgabe; schreibt ein JSON-Resultat auf stdout.
#
# Usage
# ------------------------------------
# - python -m scripts.auth.check_kickbase_auth --env-file .env
# - python -m scripts.auth.check_kickbase_auth --env-file .env --verify-snapshots
# ------------------------------------

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Sequence

from local_ingestion.core.config import load_private_ingestion_config
from local_ingestion.kickbase_client.client import KickbaseClient, KickbaseClientError


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate Kickbase API auth and optional snapshot access.")
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--verify-snapshots", action="store_true")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        config = load_private_ingestion_config(args.env_file)
    except ValueError as exc:
        print(
            json.dumps(
                {
                    "status": "error",
                    "stage": "config",
                    "message": str(exc),
                },
                ensure_ascii=True,
                indent=2,
                sort_keys=True,
            )
        )
        return 1

    client = KickbaseClient(
        base_url=config.base_url,
        auth_path=config.auth_path,
        auth_email_field=config.auth_email_field,
        auth_password_field=config.auth_password_field,
        player_snapshot_path=config.player_snapshot_path,
        match_stats_path=config.match_stats_path,
        email=config.email,
        password=config.password,
        user_agent=config.kickbase_user_agent,
        retry_config=config.retry,
        cache=None,
    )

    try:
        token = client.authenticate()
    except KickbaseClientError as exc:
        print(
            json.dumps(
                {
                    "status": "error",
                    "stage": "authenticate",
                    "message": str(exc),
                },
                ensure_ascii=True,
                indent=2,
                sort_keys=True,
            )
        )
        return 1

    summary: dict[str, object] = {
        "status": "success",
        "stage": "authenticate",
        "token_prefix": token[:8],
    }

    if args.verify_snapshots:
        try:
            players = client.fetch_player_snapshot(token=token, league_id=config.league_id)
            match_stats = client.fetch_match_stats(token=token, league_id=config.league_id)
        except KickbaseClientError as exc:
            print(
                json.dumps(
                    {
                        "status": "error",
                        "stage": "snapshot_fetch",
                        "message": str(exc),
                    },
                    ensure_ascii=True,
                    indent=2,
                    sort_keys=True,
                )
            )
            return 1

        summary["stage"] = "snapshot_fetch"
        summary["player_rows"] = len(players)
        summary["match_stats_rows"] = len(match_stats)

    print(json.dumps(summary, ensure_ascii=True, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
