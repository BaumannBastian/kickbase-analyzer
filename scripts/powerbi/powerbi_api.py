# ------------------------------------
# powerbi_api.py
#
# Dieses Skript steuert die Power BI REST API via Service Principal
# (Client Credentials Flow) und ermoeglicht Basisoperationen fuer
# Workspaces, Datasets und Refresh-Jobs.
#
# Outputs
# ------------------------------------
# 1) Keine Dateiausgabe; schreibt API-Antworten als JSON auf stdout.
#
# Usage
# ------------------------------------
# - python -m scripts.powerbi.powerbi_api list-workspaces
# - python -m scripts.powerbi.powerbi_api list-datasets --workspace-id <workspace_id>
# - python -m scripts.powerbi.powerbi_api trigger-refresh --workspace-id <workspace_id> --dataset-id <dataset_id>
# - python -m scripts.powerbi.powerbi_api list-refreshes --workspace-id <workspace_id> --dataset-id <dataset_id>
# ------------------------------------

from __future__ import annotations

import argparse
import json
import os
from typing import Any, Sequence
from urllib import error, parse, request


POWERBI_API_ROOT = "https://api.powerbi.com/v1.0/myorg"
DEFAULT_SCOPE = "https://analysis.windows.net/powerbi/api/.default"


class PowerBIClientError(RuntimeError):
    pass


def _required_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise PowerBIClientError(f"Missing required environment variable: {name}")
    return value


def _request_json(
    *,
    method: str,
    url: str,
    headers: dict[str, str],
    body: bytes | None = None,
) -> dict[str, Any] | list[Any]:
    req = request.Request(url=url, method=method, headers=headers, data=body)
    try:
        with request.urlopen(req, timeout=30) as resp:
            raw = resp.read().decode("utf-8", errors="replace").strip()
            if not raw:
                return {}
            return json.loads(raw)
    except error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace").strip()
        raise PowerBIClientError(f"HTTP {exc.code} for {method} {url}: {raw[:400]}") from exc
    except error.URLError as exc:
        raise PowerBIClientError(f"Network error for {method} {url}: {exc}") from exc


def acquire_access_token() -> str:
    tenant_id = _required_env("POWERBI_TENANT_ID")
    client_id = _required_env("POWERBI_CLIENT_ID")
    client_secret = _required_env("POWERBI_CLIENT_SECRET")
    scope = os.environ.get("POWERBI_SCOPE", DEFAULT_SCOPE).strip() or DEFAULT_SCOPE

    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    form = parse.urlencode(
        {
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": scope,
            "grant_type": "client_credentials",
        }
    ).encode("utf-8")
    payload = _request_json(
        method="POST",
        url=token_url,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        body=form,
    )
    if not isinstance(payload, dict):
        raise PowerBIClientError("Token endpoint returned unexpected payload type.")

    token = payload.get("access_token")
    if not isinstance(token, str) or not token.strip():
        raise PowerBIClientError("Token endpoint did not return access_token.")
    return token


def call_powerbi_api(
    *,
    token: str,
    method: str,
    path: str,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any] | list[Any]:
    url = f"{POWERBI_API_ROOT}{path}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }
    body: bytes | None = None
    if payload is not None:
        headers["Content-Type"] = "application/json"
        body = json.dumps(payload, ensure_ascii=True, sort_keys=True).encode("utf-8")
    return _request_json(method=method, url=url, headers=headers, body=body)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Power BI REST API helper.")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("list-workspaces")

    ds = sub.add_parser("list-datasets")
    ds.add_argument("--workspace-id", required=False, default=os.environ.get("POWERBI_WORKSPACE_ID"))

    rf = sub.add_parser("trigger-refresh")
    rf.add_argument("--workspace-id", required=False, default=os.environ.get("POWERBI_WORKSPACE_ID"))
    rf.add_argument("--dataset-id", required=True)

    rr = sub.add_parser("list-refreshes")
    rr.add_argument("--workspace-id", required=False, default=os.environ.get("POWERBI_WORKSPACE_ID"))
    rr.add_argument("--dataset-id", required=True)

    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args(argv)


def _workspace_id_or_error(raw: str | None) -> str:
    workspace_id = (raw or "").strip()
    if not workspace_id:
        raise PowerBIClientError("Missing workspace id. Use --workspace-id or POWERBI_WORKSPACE_ID.")
    return workspace_id


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    command = args.command

    if command in {"list-datasets", "trigger-refresh", "list-refreshes"}:
        _workspace_id_or_error(args.workspace_id)

    if args.dry_run:
        print(
            json.dumps(
                {
                    "status": "dry_run",
                    "command": command,
                },
                ensure_ascii=True,
                indent=2,
                sort_keys=True,
            )
        )
        return 0

    token = acquire_access_token()

    if command == "list-workspaces":
        payload = call_powerbi_api(token=token, method="GET", path="/groups")
    elif command == "list-datasets":
        workspace_id = _workspace_id_or_error(args.workspace_id)
        payload = call_powerbi_api(
            token=token,
            method="GET",
            path=f"/groups/{workspace_id}/datasets",
        )
    elif command == "trigger-refresh":
        workspace_id = _workspace_id_or_error(args.workspace_id)
        payload = call_powerbi_api(
            token=token,
            method="POST",
            path=f"/groups/{workspace_id}/datasets/{args.dataset_id}/refreshes",
            payload={},
        )
    elif command == "list-refreshes":
        workspace_id = _workspace_id_or_error(args.workspace_id)
        payload = call_powerbi_api(
            token=token,
            method="GET",
            path=f"/groups/{workspace_id}/datasets/{args.dataset_id}/refreshes",
        )
    else:
        raise PowerBIClientError(f"Unsupported command: {command}")

    print(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
