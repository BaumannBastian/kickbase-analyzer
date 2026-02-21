# ------------------------------------
# test_private_ingestion.py
#
# Tests fuer private Ingestion, Config-Parsing und Kickbase-Client.
#
# Usage
# ------------------------------------
# - python3 -m unittest tests.test_private_ingestion -v
# ------------------------------------

from __future__ import annotations

import os
from pathlib import Path
import tempfile
import unittest

from local_ingestion.core.cache import JsonFileCache
from local_ingestion.core.config import PrivateIngestionConfig, RetryConfig, load_private_ingestion_config
from local_ingestion.core.private_ingestion import run_private_ingestion
from local_ingestion.kickbase_client.client import HttpResponse, KickbaseClient
from local_ingestion.ligainsider_scraper.scraper import HtmlResponse


class FakeTransport:
    def __init__(self, responses: dict[tuple[str, str], list[HttpResponse]]) -> None:
        self.responses = responses
        self.calls: list[tuple[str, str]] = []

    def request(
        self,
        *,
        method: str,
        url: str,
        headers: dict[str, str],
        payload: dict[str, object] | None,
        timeout_seconds: float,
    ) -> HttpResponse:
        del headers, payload, timeout_seconds
        key = (method, url)
        self.calls.append(key)
        queue = self.responses.get(key)
        if not queue:
            raise AssertionError(f"No fake response configured for {method} {url}")
        return queue.pop(0)


class FakeHtmlTransport:
    def __init__(self, responses: dict[tuple[str, str], list[HtmlResponse]]) -> None:
        self.responses = responses
        self.calls: list[tuple[str, str]] = []

    def request(
        self,
        *,
        method: str,
        url: str,
        headers: dict[str, str],
        timeout_seconds: float,
    ) -> HtmlResponse:
        del headers, timeout_seconds
        key = (method, url)
        self.calls.append(key)
        queue = self.responses.get(key)
        if not queue:
            raise AssertionError(f"No fake response configured for {method} {url}")
        return queue.pop(0)


class PrivateIngestionTests(unittest.TestCase):
    def _base_config(self, root: Path) -> PrivateIngestionConfig:
        return PrivateIngestionConfig(
            base_url="https://api.kickbase.test",
            league_id="league-42",
            email="user@example.com",
            password="secret",
            source_version="private-test",
            auth_path="/auth/login",
            auth_email_field="email",
            auth_password_field="password",
            player_snapshot_path="/leagues/{league_id}/players/snapshot",
            match_stats_path="/leagues/{league_id}/players/match-stats",
            cache_dir=root / "cache",
            cache_ttl_seconds=300,
            ligainsider_status_url=None,
            ligainsider_user_agent="kickbase-analyzer-test",
            ligainsider_retry=RetryConfig(
                timeout_seconds=5.0,
                max_retries=1,
                backoff_seconds=0.0,
                rate_limit_seconds=0.0,
            ),
            ligainsider_status_file=None,
            retry=RetryConfig(
                timeout_seconds=5.0,
                max_retries=2,
                backoff_seconds=0.0,
                rate_limit_seconds=0.0,
            ),
        )

    def test_private_ingestion_writes_expected_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            out_dir = root / "bronze"
            config = self._base_config(root)

            transport = FakeTransport(
                responses={
                    ("POST", "https://api.kickbase.test/auth/login"): [
                        HttpResponse(status_code=200, body='{"token":"abc"}')
                    ],
                    ("GET", "https://api.kickbase.test/leagues/league-42/players/snapshot"): [
                        HttpResponse(
                            status_code=200,
                            body='[{"kickbase_player_id":"kb_1","market_value":100}]',
                        )
                    ],
                    ("GET", "https://api.kickbase.test/leagues/league-42/players/match-stats"): [
                        HttpResponse(
                            status_code=200,
                            body='{"data":[{"kickbase_player_id":"kb_1","matchday":21}]}',
                        )
                    ],
                }
            )

            summary = run_private_ingestion(config, out_dir, transport=transport)
            self.assertEqual(summary["status"], "success")
            self.assertEqual(summary["mode"], "private")
            self.assertEqual(summary["rows_written"], 2)
            self.assertTrue((out_dir / "ingestion_runs.ndjson").exists())

    def test_client_uses_cache_for_repeated_snapshot_calls(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = self._base_config(root)

            transport = FakeTransport(
                responses={
                    ("POST", "https://api.kickbase.test/auth/login"): [
                        HttpResponse(status_code=200, body='{"token":"abc"}')
                    ],
                    ("GET", "https://api.kickbase.test/leagues/league-42/players/snapshot"): [
                        HttpResponse(
                            status_code=200,
                            body='[{"kickbase_player_id":"kb_1","market_value":100}]',
                        )
                    ],
                }
            )

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
                cache=JsonFileCache(config.cache_dir),
                cache_ttl_seconds=config.cache_ttl_seconds,
                transport=transport,
            )

            token = client.authenticate()
            rows_a = client.fetch_player_snapshot(token=token, league_id=config.league_id)
            rows_b = client.fetch_player_snapshot(token=token, league_id=config.league_id)

            self.assertEqual(rows_a, rows_b)
            snapshot_calls = [
                call for call in transport.calls if call[0] == "GET" and call[1].endswith("/players/snapshot")
            ]
            self.assertEqual(len(snapshot_calls), 1)

    def test_client_retries_transient_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = self._base_config(root)

            transport = FakeTransport(
                responses={
                    ("POST", "https://api.kickbase.test/auth/login"): [
                        HttpResponse(status_code=200, body='{"token":"abc"}')
                    ],
                    ("GET", "https://api.kickbase.test/leagues/league-42/players/match-stats"): [
                        HttpResponse(status_code=503, body='{"error":"temporary"}'),
                        HttpResponse(status_code=200, body='{"data":[{"kickbase_player_id":"kb_1"}]}'),
                    ],
                }
            )

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
                cache=None,
                transport=transport,
            )

            token = client.authenticate()
            rows = client.fetch_match_stats(token=token, league_id=config.league_id)
            self.assertEqual(len(rows), 1)

            match_calls = [
                call
                for call in transport.calls
                if call[0] == "GET" and call[1].endswith("/players/match-stats")
            ]
            self.assertEqual(len(match_calls), 2)

    def test_private_config_loads_from_env_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            env_file = root / ".env"
            env_file.write_text(
                "\n".join(
                    [
                        "KICKBASE_BASE_URL=https://api.kickbase.test",
                        "KICKBASE_LEAGUE_ID=league-42",
                        "KICKBASE_EMAIL=user@example.com",
                        "KICKBASE_PASSWORD=secret",
                        "KICKBASE_MAX_RETRIES=3",
                        "KICKBASE_RATE_LIMIT_SECONDS=0.5",
                    ]
                ),
                encoding="utf-8",
            )

            original_env = dict(os.environ)
            try:
                for key in list(os.environ):
                    if key.startswith("KICKBASE_") or key.startswith("LIGAINSIDER_") or key in {
                        "SOURCE_VERSION",
                    }:
                        os.environ.pop(key)

                config = load_private_ingestion_config(env_file)
                self.assertEqual(config.base_url, "https://api.kickbase.test")
                self.assertEqual(config.league_id, "league-42")
                self.assertEqual(config.retry.max_retries, 3)
                self.assertEqual(config.retry.rate_limit_seconds, 0.5)
                self.assertIsNone(config.ligainsider_status_url)
            finally:
                os.environ.clear()
                os.environ.update(original_env)

    def test_private_ingestion_uses_ligainsider_scraper_when_url_set(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            out_dir = root / "bronze"
            config = self._base_config(root)
            config = PrivateIngestionConfig(
                **{
                    **config.__dict__,
                    "ligainsider_status_url": "https://www.ligainsider.test/status",
                    "ligainsider_retry": RetryConfig(
                        timeout_seconds=5.0,
                        max_retries=1,
                        backoff_seconds=0.0,
                        rate_limit_seconds=0.0,
                    ),
                }
            )

            transport = FakeTransport(
                responses={
                    ("POST", "https://api.kickbase.test/auth/login"): [
                        HttpResponse(status_code=200, body='{"token":"abc"}')
                    ],
                    ("GET", "https://api.kickbase.test/leagues/league-42/players/snapshot"): [
                        HttpResponse(
                            status_code=200,
                            body='[{"kickbase_player_id":"kb_1","market_value":100}]',
                        )
                    ],
                    ("GET", "https://api.kickbase.test/leagues/league-42/players/match-stats"): [
                        HttpResponse(
                            status_code=200,
                            body='{"data":[{"kickbase_player_id":"kb_1","matchday":21}]}',
                        )
                    ],
                }
            )
            ligainsider_transport = FakeHtmlTransport(
                responses={
                    ("GET", "https://www.ligainsider.test/status"): [
                        HtmlResponse(
                            status_code=200,
                            body=(
                                '<div data-player-name="Max Beispiel" '
                                'data-status="fit" data-predicted-lineup="starter" '
                                'data-competition-risk="low" data-player-slug="max-beispiel"></div>'
                            ),
                        )
                    ]
                }
            )

            summary = run_private_ingestion(
                config,
                out_dir,
                transport=transport,
                ligainsider_transport=ligainsider_transport,
            )
            self.assertEqual(summary["status"], "success")
            self.assertEqual(summary["rows_written"], 3)
            ligainsider_path = next(
                out_dir.glob("ligainsider_status_snapshot_*.ndjson"),
                None,
            )
            self.assertIsNotNone(ligainsider_path)
            self.assertEqual(len(ligainsider_transport.calls), 1)


if __name__ == "__main__":
    unittest.main()
