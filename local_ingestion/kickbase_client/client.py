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
# - competition_players = client.fetch_all_competition_players(...)
# - details = client.fetch_player_details(token, league_id, player_id)
# ------------------------------------

from __future__ import annotations

from dataclasses import dataclass
import json
import time
from typing import Any, Protocol
from urllib import error, request
from urllib.parse import urlencode

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
        competition_players_search_path: str = "",
        match_stats_path: str = "",
        player_details_path: str = "",
        player_market_value_history_path: str = "",
        player_performance_path: str = "",
        player_transfers_path: str = "",
        email: str,
        password: str,
        user_agent: str = "okhttp/4.11.0",
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
        self.competition_players_search_path = competition_players_search_path
        self.match_stats_path = match_stats_path
        self.player_details_path = player_details_path
        self.player_market_value_history_path = player_market_value_history_path
        self.player_performance_path = player_performance_path
        self.player_transfers_path = player_transfers_path
        self.email = email
        self.password = password
        self.user_agent = user_agent
        self.retry_config = retry_config
        self.cache = cache
        self.cache_ttl_seconds = cache_ttl_seconds
        self.transport = transport or UrllibTransport()
        self._sleep_fn = sleep_fn or time.sleep
        self._now_fn = now_fn or time.monotonic
        self._next_request_ts = 0.0
        self._last_auth_payload: dict[str, Any] | None = None

    def authenticate(self) -> str:
        auth_payload: dict[str, Any] = {
            self.auth_email_field: self.email,
            self.auth_password_field: self.password,
        }
        if self.auth_email_field == "em" and self.auth_password_field == "pass":
            auth_payload["loy"] = False
            auth_payload["rep"] = {}

        payload = self._request_json(
            method="POST",
            path=self.auth_path,
            payload=auth_payload,
            token=None,
        )
        self._last_auth_payload = payload if isinstance(payload, dict) else None

        token_candidates = [
            payload.get("token") if isinstance(payload, dict) else None,
            payload.get("access_token") if isinstance(payload, dict) else None,
            payload.get("jwt") if isinstance(payload, dict) else None,
            payload.get("tkn") if isinstance(payload, dict) else None,
            payload.get("chttkn") if isinstance(payload, dict) else None,
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

    def discover_competition_id(self, *, league_id: str) -> str | None:
        payload = self._last_auth_payload
        if not isinstance(payload, dict):
            return None

        leagues = payload.get("srvl")
        if not isinstance(leagues, list):
            return None

        league_id_text = str(league_id).strip()
        for item in leagues:
            if not isinstance(item, dict):
                continue
            item_league_id = str(item.get("id", "")).strip()
            if item_league_id != league_id_text:
                continue
            competition_id = item.get("cpi") or item.get("competition_id") or item.get("competitionId")
            if competition_id is None:
                return None
            competition_text = str(competition_id).strip()
            if competition_text:
                return competition_text
        return None

    def fetch_player_snapshot(self, *, token: str, league_id: str) -> list[dict[str, Any]]:
        path = self.player_snapshot_path.format(league_id=league_id)
        return self._fetch_list(path=path, token=token, cache_key=f"player_snapshot:{league_id}")

    def fetch_competition_players_page(
        self,
        *,
        token: str,
        competition_id: str,
        league_id: str,
        query: str,
        start: int,
        page_size: int,
    ) -> list[dict[str, Any]]:
        if not self.competition_players_search_path.strip():
            return []

        base_path = self.competition_players_search_path.format(competition_id=competition_id)
        query_params = urlencode(
            {
                "leagueId": league_id,
                "query": query,
                "start": max(0, start),
                "max": max(1, page_size),
            }
        )
        separator = "&" if "?" in base_path else "?"
        path = f"{base_path}{separator}{query_params}"
        cache_key = (
            f"competition_players:{competition_id}:{league_id}:"
            f"{query}:{max(0, start)}:{max(1, page_size)}"
        )
        return self._fetch_list(path=path, token=token, cache_key=cache_key)

    def fetch_all_competition_players(
        self,
        *,
        token: str,
        competition_id: str,
        league_id: str,
        query: str,
        page_size: int = 100,
    ) -> list[dict[str, Any]]:
        players: list[dict[str, Any]] = []
        seen_ids: set[str] = set()
        max_pages = 50

        for page_index in range(max_pages):
            start = page_index * max(1, page_size)
            page_rows = self.fetch_competition_players_page(
                token=token,
                competition_id=competition_id,
                league_id=league_id,
                query=query,
                start=start,
                page_size=page_size,
            )
            if not page_rows:
                break

            for row in page_rows:
                player_id = self._extract_player_id(row)
                if player_id and player_id in seen_ids:
                    continue
                if player_id:
                    seen_ids.add(player_id)
                players.append(row)

            if len(page_rows) < page_size:
                break

        return players

    def fetch_match_stats(self, *, token: str, league_id: str) -> list[dict[str, Any]]:
        if not self.match_stats_path.strip():
            return []
        path = self.match_stats_path.format(league_id=league_id)
        return self._fetch_list(path=path, token=token, cache_key=f"match_stats:{league_id}")

    def fetch_player_details(
        self, *, token: str, league_id: str, player_id: str
    ) -> dict[str, Any]:
        if not self.player_details_path.strip():
            return {}
        path = self.player_details_path.format(league_id=league_id, player_id=player_id)
        payload = self._fetch_optional_payload(
            path=path,
            token=token,
            cache_key=f"player_details:{league_id}:{player_id}",
        )
        return self._extract_object(payload)

    def fetch_player_market_value_history(
        self, *, token: str, player_id: str
    ) -> list[dict[str, Any]]:
        if not self.player_market_value_history_path.strip():
            return []
        path = self.player_market_value_history_path.format(player_id=player_id)
        payload = self._fetch_optional_payload(
            path=path,
            token=token,
            cache_key=f"player_market_values:{player_id}",
        )
        return self._extract_rows_or_empty(payload)

    def fetch_player_performance(self, *, token: str, player_id: str) -> dict[str, Any]:
        if not self.player_performance_path.strip():
            return {}
        path = self.player_performance_path.format(player_id=player_id)
        payload = self._fetch_optional_payload(
            path=path,
            token=token,
            cache_key=f"player_performance:{player_id}",
        )
        return self._extract_object(payload)

    def fetch_player_transfers(
        self, *, token: str, league_id: str, player_id: str
    ) -> list[dict[str, Any]]:
        if not self.player_transfers_path.strip():
            return []
        path = self.player_transfers_path.format(league_id=league_id, player_id=player_id)
        payload = self._fetch_optional_payload(
            path=path,
            token=token,
            cache_key=f"player_transfers:{league_id}:{player_id}",
        )
        return self._extract_rows_or_empty(payload)

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

    def _fetch_optional_payload(
        self, *, path: str, token: str, cache_key: str
    ) -> dict[str, Any] | list[Any]:
        if self.cache is not None:
            cached = self.cache.get(cache_key, ttl_seconds=self.cache_ttl_seconds)
            if cached is not None:
                return cached

        try:
            payload = self._request_json(method="GET", path=path, payload=None, token=token)
        except KickbaseAuthError:
            raise
        except KickbaseRequestError:
            payload = {}

        if self.cache is not None:
            self.cache.set(cache_key, payload)
        return payload

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
        headers = {
            "Accept": "application/json",
            "User-Agent": self.user_agent,
        }
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
            for key in ("data", "items", "players", "rows", "result", "it"):
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

    def _extract_rows_or_empty(self, payload: dict[str, Any] | list[Any]) -> list[dict[str, Any]]:
        if isinstance(payload, list):
            return [row for row in payload if isinstance(row, dict)]

        if isinstance(payload, dict):
            for key in ("data", "items", "players", "rows", "result", "it", "history", "transfers"):
                candidate = payload.get(key)
                if isinstance(candidate, list):
                    return [row for row in candidate if isinstance(row, dict)]
            return []

        return []

    def _extract_object(self, payload: dict[str, Any] | list[Any]) -> dict[str, Any]:
        if isinstance(payload, dict):
            for key in ("data", "item", "player", "result", "it"):
                candidate = payload.get(key)
                if isinstance(candidate, dict):
                    return candidate
            return payload

        return {}

    @staticmethod
    def _extract_player_id(row: dict[str, Any]) -> str:
        for key in ("kickbase_player_id", "player_id", "id", "i", "pi"):
            value = row.get(key)
            if value is None:
                continue
            text = str(value).strip()
            if text:
                return text
        return ""
