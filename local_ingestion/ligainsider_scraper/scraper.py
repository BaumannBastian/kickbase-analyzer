# ------------------------------------
# scraper.py
#
# Dieses Modul implementiert einen LigaInsider HTML-Scraper mit
# Retry, Rate-Limit und optionalem Dateicache.
# Es extrahiert pro Spieler Status/Lineup-Felder aus HTML/JSON-Daten.
#
# Outputs
# ------------------------------------
# 1) Keine direkte Dateiausgabe; liefert normalisierte Row-Objekte.
#
# Usage
# ------------------------------------
# - scraper = LigaInsiderScraper(...)
# - rows = scraper.fetch_status_snapshot("https://www.ligainsider.de/...")
# ------------------------------------

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import html
import json
import re
import time
from typing import Any, Protocol
from urllib import error, request

from local_ingestion.core.cache import JsonFileCache
from local_ingestion.core.config import RetryConfig


SCRIPT_NEXT_DATA_RE = re.compile(
    r'<script[^>]*id=["\']__NEXT_DATA__["\'][^>]*>(?P<body>.*?)</script>',
    flags=re.IGNORECASE | re.DOTALL,
)
DATA_TAG_RE = re.compile(
    r"<(?P<tag>[a-zA-Z0-9]+)\s+(?P<attrs>[^>]*data-player-name=[^>]*)>",
    flags=re.IGNORECASE | re.DOTALL,
)
DATA_ATTR_RE = re.compile(
    r'(?P<key>data-[a-zA-Z0-9_-]+)=["\'](?P<value>[^"\']*)["\']',
    flags=re.IGNORECASE,
)
PLAYER_LINK_RE = re.compile(
    r'<a\s+href="/(?P<slug>[a-z0-9\-]+)_(?P<pid>\d+)/"[^>]*>(?P<body>.*?)</a>',
    flags=re.IGNORECASE | re.DOTALL,
)
PLAYER_PHOTO_LINK_RE = re.compile(
    r'<div\s+class="player_position_photo[^"]*"[^>]*>\s*'
    r'<a\s+href="/(?P<slug>[a-z0-9\-]+)_(?P<pid>\d+)/"[^>]*>(?P<body>.*?)</a>',
    flags=re.IGNORECASE | re.DOTALL,
)
HTML_TAG_RE = re.compile(r"<[^>]+>")
ALT_ATTR_RE = re.compile(r'alt="(?P<alt>[^"]+)"', flags=re.IGNORECASE)


@dataclass(frozen=True)
class HtmlResponse:
    status_code: int
    body: str


class HtmlTransport(Protocol):
    def request(
        self,
        *,
        method: str,
        url: str,
        headers: dict[str, str],
        timeout_seconds: float,
    ) -> HtmlResponse:
        ...


class UrllibHtmlTransport:
    def request(
        self,
        *,
        method: str,
        url: str,
        headers: dict[str, str],
        timeout_seconds: float,
    ) -> HtmlResponse:
        req = request.Request(url=url, method=method, headers=headers)
        try:
            with request.urlopen(req, timeout=timeout_seconds) as resp:
                body = resp.read().decode("utf-8", errors="replace")
                return HtmlResponse(status_code=resp.status, body=body)
        except error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            return HtmlResponse(status_code=exc.code, body=body)
        except error.URLError as exc:
            raise ConnectionError(str(exc)) from exc


class LigaInsiderScraperError(RuntimeError):
    pass


class LigaInsiderScraper:
    def __init__(
        self,
        *,
        user_agent: str,
        retry_config: RetryConfig,
        cache: JsonFileCache | None = None,
        cache_ttl_seconds: int = 300,
        transport: HtmlTransport | None = None,
        sleep_fn: Any | None = None,
        now_fn: Any | None = None,
    ) -> None:
        self.user_agent = user_agent
        self.retry_config = retry_config
        self.cache = cache
        self.cache_ttl_seconds = cache_ttl_seconds
        self.transport = transport or UrllibHtmlTransport()
        self._sleep_fn = sleep_fn or time.sleep
        self._now_fn = now_fn or time.monotonic
        self._next_request_ts = 0.0

    def fetch_status_snapshot(self, status_url: str) -> list[dict[str, Any]]:
        cache_key = f"ligainsider_status:{status_url}"
        if self.cache is not None:
            cached = self.cache.get(cache_key, ttl_seconds=self.cache_ttl_seconds)
            if cached is not None:
                return self._coerce_rows(cached)

        html_text = self._request_html(method="GET", url=status_url)
        rows = self.parse_status_rows(html_text)
        scraped_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")

        normalized: list[dict[str, Any]] = []
        for row in rows:
            out = dict(row)
            out["scraped_at"] = scraped_at
            out["source_url"] = status_url
            normalized.append(out)

        if self.cache is not None:
            self.cache.set(cache_key, normalized)

        return normalized

    def _apply_rate_limit(self) -> None:
        now = float(self._now_fn())
        if now < self._next_request_ts:
            self._sleep_fn(self._next_request_ts - now)
        self._next_request_ts = float(self._now_fn()) + self.retry_config.rate_limit_seconds

    def _request_html(self, *, method: str, url: str) -> str:
        headers = {
            "Accept": "text/html,application/xhtml+xml,application/json;q=0.9,*/*;q=0.8",
            "User-Agent": self.user_agent,
        }
        max_attempts = self.retry_config.max_retries + 1
        last_error: Exception | None = None

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
                raise LigaInsiderScraperError(f"Network error for {method} {url}: {exc}") from exc

            if 200 <= response.status_code < 300:
                return response.body

            if response.status_code in {429, 500, 502, 503, 504} and attempt < max_attempts - 1:
                self._sleep_fn(self.retry_config.backoff_seconds * (attempt + 1))
                continue

            raise LigaInsiderScraperError(
                f"HTTP error for {method} {url} (status {response.status_code})"
            )

        if last_error is not None:
            raise LigaInsiderScraperError(str(last_error)) from last_error
        raise LigaInsiderScraperError(f"Request failed for {method} {url}")

    @classmethod
    def parse_status_rows(cls, html_text: str) -> list[dict[str, Any]]:
        rows = cls._parse_next_data_rows(html_text)
        if rows:
            return rows

        rows = cls._parse_team_page_rows(html_text)
        if rows:
            return rows

        rows = cls._parse_data_attribute_rows(html_text)
        if rows:
            return rows

        raise LigaInsiderScraperError("No parseable player rows found in LigaInsider HTML.")

    @classmethod
    def _parse_next_data_rows(cls, html_text: str) -> list[dict[str, Any]]:
        match = SCRIPT_NEXT_DATA_RE.search(html_text)
        if match is None:
            return []

        raw_body = html.unescape(match.group("body").strip())
        if not raw_body:
            return []

        try:
            payload = json.loads(raw_body)
        except json.JSONDecodeError:
            return []

        found: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()

        def walk(node: Any) -> None:
            if isinstance(node, dict):
                candidate = cls._to_row_candidate(node)
                if candidate is not None:
                    key = (
                        str(candidate.get("ligainsider_player_slug", "")).strip(),
                        str(candidate.get("player_name", "")).strip(),
                    )
                    if key not in seen:
                        seen.add(key)
                        found.append(candidate)
                for value in node.values():
                    walk(value)
                return

            if isinstance(node, list):
                for item in node:
                    walk(item)

        walk(payload)
        return found

    @classmethod
    def _parse_data_attribute_rows(cls, html_text: str) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()

        for tag_match in DATA_TAG_RE.finditer(html_text):
            attrs_raw = tag_match.group("attrs")
            attrs: dict[str, str] = {}
            for attr_match in DATA_ATTR_RE.finditer(attrs_raw):
                key = attr_match.group("key").strip().lower()
                value = html.unescape(attr_match.group("value").strip())
                attrs[key] = value

            row = cls._row_from_attribute_map(attrs)
            if row is None:
                continue

            key = (
                str(row.get("ligainsider_player_slug", "")).strip(),
                str(row.get("player_name", "")).strip(),
            )
            if key in seen:
                continue
            seen.add(key)
            out.append(row)

        return out

    @classmethod
    def _parse_team_page_rows(cls, html_text: str) -> list[dict[str, Any]]:
        team_marker = 'class="team_squad_area"'
        league_marker = 'class="league_name_holder"'
        split_idx = html_text.find(team_marker)
        league_idx = html_text.find(league_marker)
        start_idx = html_text.find('<div class="player_position_row')

        if start_idx < 0:
            return []

        starter_end_idx = split_idx if split_idx > start_idx else league_idx
        if starter_end_idx < 0 or starter_end_idx <= start_idx:
            starter_end_idx = len(html_text)
        starter_fragment = html_text[start_idx:starter_end_idx]

        bench_fragment = ""
        if split_idx > start_idx:
            bench_end_idx = league_idx if league_idx > split_idx else len(html_text)
            bench_fragment = html_text[split_idx:bench_end_idx]

        rows: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()

        def append_from_fragment(fragment: str, lineup: str) -> None:
            for link in PLAYER_PHOTO_LINK_RE.finditer(fragment):
                slug = link.group("slug").strip().lower()
                body = link.group("body")
                name = cls._extract_name_from_link_body(body)
                if not name and slug:
                    name = cls._name_from_slug(slug)
                if not slug or not name:
                    continue
                key = (slug, "")
                if key in seen:
                    continue
                seen.add(key)
                rows.append(
                    {
                        "ligainsider_player_slug": slug,
                        "player_name": name,
                        "status": "unknown",
                        "predicted_lineup": lineup,
                        "competition_risk": "unknown",
                    }
                )

        append_from_fragment(starter_fragment, "starter")
        append_from_fragment(bench_fragment, "bench")
        return rows

    @staticmethod
    def _extract_name_from_link_body(body: str) -> str:
        alt_match = ALT_ATTR_RE.search(body)
        if alt_match is not None:
            candidate = html.unescape(alt_match.group("alt").strip())
            if candidate:
                return candidate

        text = HTML_TAG_RE.sub(" ", body)
        text = html.unescape(text).strip()
        return re.sub(r"\s+", " ", text)

    @staticmethod
    def _name_from_slug(slug: str) -> str:
        words = [part for part in slug.replace("_", "-").split("-") if part]
        if not words:
            return ""
        return " ".join(word.capitalize() for word in words)

    @classmethod
    def _to_row_candidate(cls, node: dict[str, Any]) -> dict[str, Any] | None:
        name = cls._first_str(
            node,
            ["player_name", "name", "display_name", "displayName", "full_name", "fullname"],
        )
        status = cls._first_str(node, ["status", "injury_status", "availability"])
        lineup = cls._normalize_lineup(
            cls._first_any(node, ["predicted_lineup", "lineup", "lineUp", "starting11", "is_starting"])
        )
        competition_risk = cls._first_str(node, ["competition_risk", "rotation_risk", "risk"])
        slug = cls._first_str(node, ["slug", "player_slug", "playerSlug", "id", "player_id", "playerId"])

        if not name:
            return None
        if not status and not lineup and not competition_risk:
            return None

        return {
            "ligainsider_player_slug": slug or "",
            "player_name": name,
            "status": status or "unknown",
            "predicted_lineup": lineup or "unknown",
            "competition_risk": competition_risk or "unknown",
        }

    @classmethod
    def _row_from_attribute_map(cls, attrs: dict[str, str]) -> dict[str, Any] | None:
        name = attrs.get("data-player-name", "").strip()
        status = attrs.get("data-status", "").strip() or "unknown"
        lineup = cls._normalize_lineup(attrs.get("data-predicted-lineup", "").strip()) or "unknown"
        competition_risk = attrs.get("data-competition-risk", "").strip() or "unknown"
        slug = attrs.get("data-player-slug", "").strip() or attrs.get("data-player-id", "").strip()

        if not name:
            return None

        return {
            "ligainsider_player_slug": slug,
            "player_name": name,
            "status": status,
            "predicted_lineup": lineup,
            "competition_risk": competition_risk,
        }

    @staticmethod
    def _first_any(node: dict[str, Any], keys: list[str]) -> Any:
        for key in keys:
            if key in node:
                return node[key]
        return None

    @staticmethod
    def _first_str(node: dict[str, Any], keys: list[str]) -> str:
        for key in keys:
            value = node.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
            if value is not None and not isinstance(value, (dict, list)):
                text = str(value).strip()
                if text:
                    return text
        return ""

    @staticmethod
    def _normalize_lineup(value: Any) -> str:
        if isinstance(value, bool):
            return "starter" if value else "bench"
        if value is None:
            return ""

        text = str(value).strip().lower()
        if text in {"starter", "startelf", "starting", "starting11", "first11", "1"}:
            return "starter"
        if text in {"bench", "bank", "reserve", "0", "false"}:
            return "bench"
        return text

    @staticmethod
    def _coerce_rows(payload: Any) -> list[dict[str, Any]]:
        if not isinstance(payload, list):
            raise LigaInsiderScraperError("Cached LigaInsider payload is not a list.")
        rows: list[dict[str, Any]] = []
        for item in payload:
            if not isinstance(item, dict):
                raise LigaInsiderScraperError("Cached LigaInsider row is not an object.")
            rows.append(dict(item))
        return rows
