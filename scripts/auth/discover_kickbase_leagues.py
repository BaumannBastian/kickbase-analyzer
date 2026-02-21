# ------------------------------------
# discover_kickbase_leagues.py
#
# Dieses Skript authentifiziert sich bei Kickbase und extrahiert die
# verfuegbaren Ligen aus der Login-Antwort (`srvl`), damit die
# `KICKBASE_LEAGUE_ID` fuer `.env` bestimmt werden kann.
#
# Outputs
# ------------------------------------
# 1) Keine Dateiausgabe; schreibt League-Liste als JSON auf stdout.
#
# Usage
# ------------------------------------
# - python -m scripts.auth.discover_kickbase_leagues --env-file .env
# ------------------------------------

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any, Sequence

from local_ingestion.core.config import load_dotenv_file
from local_ingestion.kickbase_client.client import KickbaseClientError, UrllibTransport


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Discover Kickbase league IDs from login response.")
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    return parser.parse_args(argv)


def _required_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def _join_url(base_url: str, path: str) -> str:
    if path.startswith("http://") or path.startswith("https://"):
        return path
    if not path.startswith("/"):
        path = f"/{path}"
    return f"{base_url.rstrip('/')}{path}"


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    load_dotenv_file(args.env_file)

    try:
        base_url = _required_env("KICKBASE_BASE_URL")
        email = _required_env("KICKBASE_EMAIL")
        password = _required_env("KICKBASE_PASSWORD")
    except ValueError as exc:
        print(
            json.dumps(
                {"status": "error", "stage": "config", "message": str(exc)},
                ensure_ascii=True,
                indent=2,
                sort_keys=True,
            )
        )
        return 1

    auth_path = os.environ.get("KICKBASE_AUTH_PATH", "/v4/user/login").strip() or "/v4/user/login"
    email_field = os.environ.get("KICKBASE_AUTH_EMAIL_FIELD", "em").strip() or "em"
    password_field = os.environ.get("KICKBASE_AUTH_PASSWORD_FIELD", "pass").strip() or "pass"
    user_agent = os.environ.get("KICKBASE_USER_AGENT", "okhttp/4.11.0").strip() or "okhttp/4.11.0"

    payload: dict[str, Any] = {
        email_field: email,
        password_field: password,
    }
    if email_field == "em" and password_field == "pass":
        payload["loy"] = False
        payload["rep"] = {}

    transport = UrllibTransport()
    url = _join_url(base_url, auth_path)
    try:
        response = transport.request(
            method="POST",
            url=url,
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
                "User-Agent": user_agent,
            },
            payload=payload,
            timeout_seconds=20.0,
        )
    except ConnectionError as exc:
        print(
            json.dumps(
                {"status": "error", "stage": "request", "message": str(exc)},
                ensure_ascii=True,
                indent=2,
                sort_keys=True,
            )
        )
        return 1

    if not (200 <= response.status_code < 300):
        print(
            json.dumps(
                {
                    "status": "error",
                    "stage": "auth",
                    "http_status": response.status_code,
                    "body_preview": response.body[:400],
                },
                ensure_ascii=True,
                indent=2,
                sort_keys=True,
            )
        )
        return 1

    try:
        data = json.loads(response.body)
    except json.JSONDecodeError as exc:
        raise KickbaseClientError(f"Invalid login JSON response: {response.body[:400]}") from exc

    if not isinstance(data, dict):
        print(
            json.dumps(
                {"status": "error", "stage": "parse", "message": "Unexpected login payload type."},
                ensure_ascii=True,
                indent=2,
                sort_keys=True,
            )
        )
        return 1

    leagues_raw = data.get("srvl")
    leagues: list[dict[str, Any]] = []
    if isinstance(leagues_raw, list):
        for item in leagues_raw:
            if not isinstance(item, dict):
                continue
            leagues.append(
                {
                    "league_id": item.get("id"),
                    "league_name": item.get("name"),
                    "manager_count": item.get("mu"),
                    "is_public": item.get("pub"),
                }
            )

    print(
        json.dumps(
            {
                "status": "success",
                "league_count": len(leagues),
                "leagues": leagues,
            },
            ensure_ascii=True,
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
