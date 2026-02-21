# ------------------------------------
# client.py
#
# Dieses Modul implementiert einen Kickbase HTTP-Client mit
# Authentifizierung, Retry, Rate-Limit und optionalem Dateicache.
#
# Outputs
# ------------------------------------
# 1) Keine direkten Files; liefert normalisierte JSON-Objekte.
#
# Usage
# ------------------------------------
# - client = KickbaseClient(...)
# - token = client.authenticate()
# - rows = client.fetch_player_snapshot(token, league_id)
# ------------------------------------

from __future__ import annotations

from dataclasses import dataclass
import json
import time
from typing import Any, Protocol
from urllib import error, request

from local_ingestion.core.cache import JsonFileCache
from local_ingestion.core.config import RetryConfig


@dataclass(frozen=True)
class HttpResponse:
    status_code: int
    body: str


class HttpTransport(Protocol):
    def request(
        self,
        *,
        method: str,
        url: str,
        headers: dict[str, str],
        payload: dict[str, Any] | None,
        timeout_seconds: float,
    ) -> HttpResponse:
        ...


class UrllibTransport:
    def request(
        self,
        *,
        method: str,
        url: str,
        headers: dict[str, str],
        payload: dict[str, Any] | None,
        timeout_seconds: float,
    ) -> HttpResponse:
        data: bytes | None = None
        if payload is not None:
            data = json.dumps(payload, ensure_ascii=True, sort_keys=True).encode("utf-8")

        req = request.Request(url=url, method=method, headers=headers, data=data)

        try:
            with request.urlopen(req, timeout=timeout_seconds) as resp:
                body = resp.read().decode("utf-8")
                return HttpResponse(status_code=resp.status, body=body)
        except error.HTTPError as exc:
            body = exc.read().decode("utf-8")
            return HttpResponse(status_code=exc.code, body=body)
        except error.URLError as exc:
            raise ConnectionError(str(exc)) from exc


class KickbaseClientError(RuntimeError):
    pass


class KickbaseAuthError(KickbaseClientError):
    pass


class KickbaseRequestError(KickbaseClientError):
    pass


class KickbaseClient:
    def __init__(
        self,
        *,
        base_url: str,
        auth_path: str,
        auth_email_field: str = "email",
        auth_password_field: str = "password",
        player_snapshot_path: str,
        match_stats_path: str,
        email: str,
        password: str,
        retry_config: RetryConfig,
        cache: JsonFileCache | None = None,
        cache_ttl_seconds: int = 300,
        transport: HttpTransport | None = None,
        sleep_fn: Any | None = None,
        now_fn: Any | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.auth_path = auth_path
        self.auth_email_field = auth_email_field
        self.auth_password_field = auth_password_field
        self.player_snapshot_path = player_snapshot_path
        self.match_stats_path = match_stats_path
        self.email = email
        self.password = password
        self.retry_config = retry_config
        self.cache = cache
        self.cache_ttl_seconds = cache_ttl_seconds
        self.transport = transport or UrllibTransport()
        self._sleep_fn = sleep_fn or time.sleep
        self._now_fn = now_fn or time.monotonic
        self._next_request_ts = 0.0

    def authenticate(self) -> str:
        payload = self._request_json(
            method="POST",
            path=self.auth_path,
            payload={
                self.auth_email_field: self.email,
                self.auth_password_field: self.password,
            },
            token=None,
        )

        token_candidates = [
            payload.get("token") if isinstance(payload, dict) else None,
            payload.get("access_token") if isinstance(payload, dict) else None,
            payload.get("jwt") if isinstance(payload, dict) else None,
        ]

        if isinstance(payload, dict):
            data = payload.get("data")
            if isinstance(data, dict):
                token_candidates.extend(
                    [
                        data.get("token"),
                        data.get("access_token"),
                        data.get("jwt"),
                    ]
                )

        for token in token_candidates:
            if isinstance(token, str) and token.strip():
                return token

        raise KickbaseAuthError("Authentication response did not contain a token")

    def fetch_player_snapshot(self, *, token: str, league_id: str) -> list[dict[str, Any]]:
        path = self.player_snapshot_path.format(league_id=league_id)
        return self._fetch_list(path=path, token=token, cache_key=f"player_snapshot:{league_id}")

    def fetch_match_stats(self, *, token: str, league_id: str) -> list[dict[str, Any]]:
        path = self.match_stats_path.format(league_id=league_id)
        return self._fetch_list(path=path, token=token, cache_key=f"match_stats:{league_id}")

    def _fetch_list(self, *, path: str, token: str, cache_key: str) -> list[dict[str, Any]]:
        if self.cache is not None:
            cached = self.cache.get(cache_key, ttl_seconds=self.cache_ttl_seconds)
            if cached is not None:
                return self._extract_rows(cached)

        payload = self._request_json(method="GET", path=path, payload=None, token=token)
        rows = self._extract_rows(payload)

        if self.cache is not None:
            self.cache.set(cache_key, rows)
        return rows

    def _apply_rate_limit(self) -> None:
        now = float(self._now_fn())
        if now < self._next_request_ts:
            self._sleep_fn(self._next_request_ts - now)
        self._next_request_ts = float(self._now_fn()) + self.retry_config.rate_limit_seconds

    def _request_json(
        self,
        *,
        method: str,
        path: str,
        payload: dict[str, Any] | None,
        token: str | None,
    ) -> dict[str, Any] | list[Any]:
        url = self._join_url(path)
        headers = {"Accept": "application/json"}
        if payload is not None:
            headers["Content-Type"] = "application/json"
        if token is not None:
            headers["Authorization"] = f"Bearer {token}"

        last_error: Exception | None = None
        max_attempts = self.retry_config.max_retries + 1

        for attempt in range(max_attempts):
            self._apply_rate_limit()
            try:
                response = self.transport.request(
                    method=method,
                    url=url,
                    headers=headers,
                    payload=payload,
                    timeout_seconds=self.retry_config.timeout_seconds,
                )
            except ConnectionError as exc:
                last_error = exc
                if attempt < max_attempts - 1:
                    self._sleep_fn(self.retry_config.backoff_seconds * (attempt + 1))
                    continue
                raise KickbaseRequestError(f"Network error for {method} {url}: {exc}") from exc

            parsed: dict[str, Any] | list[Any]
            if response.body.strip():
                try:
                    parsed = json.loads(response.body)
                except json.JSONDecodeError as exc:
                    raise KickbaseRequestError(
                        f"Invalid JSON response for {method} {url}: {response.body[:200]}"
                    ) from exc
            else:
                parsed = {}

            if 200 <= response.status_code < 300:
                return parsed

            if response.status_code in {429, 500, 502, 503, 504} and attempt < max_attempts - 1:
                self._sleep_fn(self.retry_config.backoff_seconds * (attempt + 1))
                continue

            if response.status_code in {401, 403}:
                raise KickbaseAuthError(
                    f"Authentication failed for {method} {url} (status {response.status_code})"
                )

            raise KickbaseRequestError(
                f"Request failed for {method} {url} (status {response.status_code})"
            )

        if last_error is not None:
            raise KickbaseRequestError(str(last_error)) from last_error
        raise KickbaseRequestError(f"Request failed for {method} {url}")

    def _join_url(self, path: str) -> str:
        if path.startswith("http://") or path.startswith("https://"):
            return path
        if not path.startswith("/"):
            path = f"/{path}"
        return f"{self.base_url}{path}"

    def _extract_rows(self, payload: dict[str, Any] | list[Any]) -> list[dict[str, Any]]:
        if isinstance(payload, list):
            rows = payload
        elif isinstance(payload, dict):
            rows_any = None
            for key in ("data", "items", "players", "rows", "result"):
                candidate = payload.get(key)
                if isinstance(candidate, list):
                    rows_any = candidate
                    break
            if rows_any is None:
                raise KickbaseRequestError("Response does not contain a list payload")
            rows = rows_any
        else:
            raise KickbaseRequestError(f"Unexpected payload type: {type(payload)!r}")

        out: list[dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                raise KickbaseRequestError(
                    f"Expected object rows in response, got {type(row)!r}"
                )
            out.append(row)
        return out
