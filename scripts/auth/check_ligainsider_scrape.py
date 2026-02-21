# ------------------------------------
# check_ligainsider_scrape.py
#
# Dieses Skript validiert den LigaInsider-Scrape-Flow mit den
# konfigurierten Variablen aus `.env`.
#
# Outputs
# ------------------------------------
# 1) Keine Dateiausgabe; schreibt ein JSON-Resultat auf stdout.
#
# Usage
# ------------------------------------
# - python -m scripts.auth.check_ligainsider_scrape --env-file .env
# ------------------------------------

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Sequence

from local_ingestion.core.cache import JsonFileCache
from local_ingestion.core.config import RetryConfig, load_dotenv_file
from local_ingestion.ligainsider_scraper.scraper import LigaInsiderScraper, LigaInsiderScraperError


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate LigaInsider scrape with current config.")
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--preview", type=int, default=5)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    load_dotenv_file(args.env_file)
    status_url = os.environ.get("LIGAINSIDER_STATUS_URL", "").strip()
    if not status_url:
        raise SystemExit("Missing LIGAINSIDER_STATUS_URL in .env.")

    user_agent = (
        os.environ.get("LIGAINSIDER_USER_AGENT", "kickbase-analyzer/0.1 (+private-use)").strip()
        or "kickbase-analyzer/0.1 (+private-use)"
    )
    timeout_seconds = float(os.environ.get("LIGAINSIDER_TIMEOUT_SECONDS", "20"))
    max_retries = int(os.environ.get("LIGAINSIDER_MAX_RETRIES", "2"))
    backoff_seconds = float(os.environ.get("LIGAINSIDER_BACKOFF_SECONDS", "1"))
    rate_limit_seconds = float(os.environ.get("LIGAINSIDER_RATE_LIMIT_SECONDS", "1.0"))
    cache_ttl_seconds = int(os.environ.get("KICKBASE_CACHE_TTL_SECONDS", "300"))
    cache_dir = Path(os.environ.get("KICKBASE_CACHE_DIR", "data/cache/kickbase"))

    scraper = LigaInsiderScraper(
        user_agent=user_agent,
        retry_config=RetryConfig(
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
            backoff_seconds=backoff_seconds,
            rate_limit_seconds=rate_limit_seconds,
        ),
        cache=JsonFileCache(cache_dir),
        cache_ttl_seconds=cache_ttl_seconds,
    )

    try:
        rows = scraper.fetch_status_snapshot(status_url)
    except LigaInsiderScraperError as exc:
        print(
            json.dumps(
                {
                    "status": "error",
                    "message": str(exc),
                },
                ensure_ascii=True,
                indent=2,
                sort_keys=True,
            )
        )
        return 1

    summary = {
        "status": "success",
        "row_count": len(rows),
        "preview": rows[: max(0, args.preview)],
    }
    print(json.dumps(summary, ensure_ascii=True, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
