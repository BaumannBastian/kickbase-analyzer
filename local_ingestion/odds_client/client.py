# ------------------------------------
# client.py
#
# Dieses Modul implementiert einen HTTP-Client fuer The Odds API
# (Bundesliga Wettquoten) mit Retry, Rate-Limit und optionalem
# Dateicache.
#
# Outputs
# ------------------------------------
# 1) Keine direkten Files; liefert normalisierte Odds-Events.
#
# Usage
# ------------------------------------
# - client = OddsApiClient(...)
# - rows = client.fetch_upcoming_events(limit=9)
# ------------------------------------

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
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
        timeout_seconds: float,
    ) -> HttpResponse:
        req = request.Request(url=url, method=method, headers=headers)

        try:
            with request.urlopen(req, timeout=timeout_seconds) as resp:
                body = resp.read().decode("utf-8")
                return HttpResponse(status_code=resp.status, body=body)
        except error.HTTPError as exc:
            body = exc.read().decode("utf-8")
            return HttpResponse(status_code=exc.code, body=body)
        except error.URLError as exc:
            raise ConnectionError(str(exc)) from exc


class OddsApiClientError(RuntimeError):
    pass


class OddsApiClient:
    def __init__(
        self,
        *,
        api_key: str,
        base_url: str,
        sport_key: str,
        regions: str,
        markets: str,
        odds_format: str,
        date_format: str,
        bookmakers: str | None,
        retry_config: RetryConfig,
        cache: JsonFileCache | None = None,
        cache_ttl_seconds: int = 300,
        transport: HttpTransport | None = None,
        sleep_fn: Any | None = None,
        now_fn: Any | None = None,
    ) -> None:
        self.api_key = api_key.strip()
        self.base_url = base_url.rstrip("/")
        self.sport_key = sport_key.strip()
        self.regions = regions.strip()
        self.markets = markets.strip()
        self.odds_format = odds_format.strip()
        self.date_format = date_format.strip()
        self.bookmakers = bookmakers.strip() if isinstance(bookmakers, str) and bookmakers.strip() else None
        self.retry_config = retry_config
        self.cache = cache
        self.cache_ttl_seconds = cache_ttl_seconds
        self.transport = transport or UrllibTransport()
        self._sleep_fn = sleep_fn or time.sleep
        self._now_fn = now_fn or time.monotonic
        self._next_request_ts = 0.0

    def fetch_upcoming_events(self, *, limit: int = 9) -> list[dict[str, Any]]:
        if not self.api_key:
            raise OddsApiClientError("Missing ODDS_API_KEY")
        if not self.sport_key:
            raise OddsApiClientError("Missing ODDS_SPORT_KEY")

        limit_value = max(1, int(limit))
        cache_key = (
            f"odds:{self.sport_key}:{self.regions}:{self.markets}:"
            f"{self.odds_format}:{self.date_format}:{self.bookmakers or ''}:{limit_value}"
        )

        if self.cache is not None:
            cached = self.cache.get(cache_key, ttl_seconds=self.cache_ttl_seconds)
            if cached is not None:
                return self._normalize_events(cached, limit=limit_value)

        query = {
            "apiKey": self.api_key,
            "regions": self.regions,
            "markets": self.markets,
            "oddsFormat": self.odds_format,
            "dateFormat": self.date_format,
        }
        if self.bookmakers is not None:
            query["bookmakers"] = self.bookmakers

        path = f"/sports/{self.sport_key}/odds/?{urlencode(query)}"
        payload = self._request_json(method="GET", path=path)
        events = self._normalize_events(payload, limit=limit_value)

        if self.cache is not None:
            self.cache.set(cache_key, events)
        return events

    def _apply_rate_limit(self) -> None:
        now = float(self._now_fn())
        if now < self._next_request_ts:
            self._sleep_fn(self._next_request_ts - now)
        self._next_request_ts = float(self._now_fn()) + self.retry_config.rate_limit_seconds

    def _request_json(self, *, method: str, path: str) -> dict[str, Any] | list[Any]:
        url = self._join_url(path)
        headers = {
            "Accept": "application/json",
            "User-Agent": "kickbase-analyzer/0.1 (+private-use)",
        }

        last_error: Exception | None = None
        max_attempts = self.retry_config.max_retries + 1

        for attempt in range(max_attempts):
            self._apply_rate_limit()
            try:
                response = self.transport.request(
                    method=method,
                    url=url,
                    headers=headers,
                    timeout_seconds=self.retry_config.timeout_seconds,
                )
            except ConnectionError as exc:
                last_error = exc
                if attempt < max_attempts - 1:
                    self._sleep_fn(self.retry_config.backoff_seconds * (attempt + 1))
                    continue
                raise OddsApiClientError(f"Network error for {method} {url}: {exc}") from exc

            parsed: dict[str, Any] | list[Any]
            if response.body.strip():
                try:
                    parsed = json.loads(response.body)
                except json.JSONDecodeError as exc:
                    raise OddsApiClientError(
                        f"Invalid JSON response for {method} {url}: {response.body[:200]}"
                    ) from exc
            else:
                parsed = {}

            if 200 <= response.status_code < 300:
                return parsed

            if response.status_code in {429, 500, 502, 503, 504} and attempt < max_attempts - 1:
                self._sleep_fn(self.retry_config.backoff_seconds * (attempt + 1))
                continue

            raise OddsApiClientError(
                f"Request failed for {method} {url} (status {response.status_code})"
            )

        if last_error is not None:
            raise OddsApiClientError(str(last_error)) from last_error
        raise OddsApiClientError(f"Request failed for {method} {url}")

    def _join_url(self, path: str) -> str:
        if path.startswith("http://") or path.startswith("https://"):
            return path
        if not path.startswith("/"):
            path = f"/{path}"
        return f"{self.base_url}{path}"

    @staticmethod
    def _normalize_events(payload: dict[str, Any] | list[Any], *, limit: int) -> list[dict[str, Any]]:
        if isinstance(payload, list):
            rows = payload
        elif isinstance(payload, dict):
            rows_any = None
            for key in ("data", "items", "events", "result"):
                candidate = payload.get(key)
                if isinstance(candidate, list):
                    rows_any = candidate
                    break
            if rows_any is None:
                return []
            rows = rows_any
        else:
            return []

        parsed_rows: list[tuple[datetime | None, dict[str, Any]]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            dt = OddsApiClient._parse_event_time(row.get("commence_time"))
            parsed_rows.append((dt, row))

        parsed_rows.sort(key=lambda item: item[0] or datetime.max.replace(tzinfo=UTC))
        out = [row for _, row in parsed_rows]
        return out[:limit]

    @staticmethod
    def _parse_event_time(value: Any) -> datetime | None:
        if not isinstance(value, str):
            return None
        text = value.strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)
