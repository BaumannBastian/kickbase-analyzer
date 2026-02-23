# ------------------------------------
# test_ligainsider_scraper.py
#
# Tests fuer LigaInsider-Scraper Parsing, Retry und Snapshot-Abruf.
#
# Outputs
# ------------------------------------
# 1) Kein Dateioutput; Assertions fuer Parser-/HTTP-Verhalten.
#
# Usage
# ------------------------------------
# - python -m unittest tests.test_ligainsider_scraper
# - pytest tests/test_ligainsider_scraper.py
# ------------------------------------

from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from local_ingestion.core.cache import JsonFileCache
from local_ingestion.core.config import RetryConfig
from local_ingestion.ligainsider_scraper.scraper import HtmlResponse, LigaInsiderScraper


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


class LigaInsiderScraperTests(unittest.TestCase):
    def test_parse_rows_from_next_data_script(self) -> None:
        html_text = """
        <html>
          <body>
            <script id="__NEXT_DATA__" type="application/json">
            {
              "props": {
                "pageProps": {
                  "players": [
                    {"name":"Max Beispiel","status":"fit","predicted_lineup":"starter","competition_risk":"low","slug":"max-beispiel"},
                    {"name":"Luca Test","status":"questionable","predicted_lineup":"bench","competition_risk":"high","slug":"luca-test"}
                  ]
                }
              }
            }
            </script>
          </body>
        </html>
        """
        rows = LigaInsiderScraper.parse_status_rows(html_text)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["player_name"], "Max Beispiel")
        self.assertEqual(rows[0]["predicted_lineup"], "starter")
        self.assertEqual(rows[1]["competition_risk"], "high")

    def test_fetch_snapshot_retries_transient_http_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cache = JsonFileCache(Path(tmp) / "cache")
            transport = FakeHtmlTransport(
                responses={
                    ("GET", "https://www.ligainsider.test/status"): [
                        HtmlResponse(status_code=503, body="temporary error"),
                        HtmlResponse(
                            status_code=200,
                            body=(
                                '<div data-player-name="Max Beispiel" data-status="fit" '
                                'data-predicted-lineup="starter" data-competition-risk="low" '
                                'data-player-slug="max-beispiel"></div>'
                            ),
                        ),
                    ]
                }
            )
            scraper = LigaInsiderScraper(
                user_agent="kickbase-analyzer-test",
                retry_config=RetryConfig(
                    timeout_seconds=5.0,
                    max_retries=1,
                    backoff_seconds=0.0,
                    rate_limit_seconds=0.0,
                ),
                cache=cache,
                cache_ttl_seconds=300,
                transport=transport,
            )

            rows = scraper.fetch_status_snapshot("https://www.ligainsider.test/status")
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["player_name"], "Max Beispiel")
            self.assertIn("scraped_at", rows[0])
            self.assertEqual(len(transport.calls), 2)

    def test_parse_rows_from_team_page_markup(self) -> None:
        html_text = """
        <div class="player_position_row">
          <div class="player_position_photo">
            <a href="/max-beispiel_1234/"><img alt="Max Beispiel" /></a>
          </div>
        </div>
        <div class="team_squad_area">
          <div class="player_position_photo pull-left">
            <a href="/luca-test_5678/"><img alt="Luca Test" /></a>
          </div>
        </div>
        <div class="league_name_holder"></div>
        """
        rows = LigaInsiderScraper.parse_status_rows(html_text)
        self.assertEqual(len(rows), 2)
        by_slug = {str(row["ligainsider_player_slug"]): row for row in rows}
        self.assertEqual(by_slug["max-beispiel"]["predicted_lineup"], "starter")
        self.assertEqual(by_slug["luca-test"]["predicted_lineup"], "bench")

    def test_parse_rows_from_team_page_without_bench_marker(self) -> None:
        html_text = """
        <div class="player_position_row text-center">
          <div class="player_position_photo">
            <a href="/gregor-kobel_9357/"><img alt="" /></a>
          </div>
          <div class="player_position_photo">
            <a href="/julian-brandt_1111/"><img alt="" /></a>
          </div>
        </div>
        <div class="league_name_holder"></div>
        """
        rows = LigaInsiderScraper.parse_status_rows(html_text)
        self.assertEqual(len(rows), 2)
        by_slug = {str(row["ligainsider_player_slug"]): row for row in rows}
        self.assertEqual(by_slug["gregor-kobel"]["player_name"], "Gregor Kobel")
        self.assertEqual(by_slug["julian-brandt"]["predicted_lineup"], "starter")

    def test_parse_rows_from_team_page_with_sub_child_competitors(self) -> None:
        html_text = """
        <div class="player_position_row player_position_row2 text-center">
          <div class="player_position_column center-block">
            <div class="sub_child" style="display: block;">
              <div class="player_position_photo">
                <div class="sub_pic"><a href="/leonidas-stergiou_36303/"><img alt="" /></a></div>
                <div class="player_no img-circle"><div class="next_sub"><span class="next_sub_button"></span></div></div>
              </div>
              <div class="player_name"><a href="/leonidas-stergiou_36303/">Stergiou</a></div>
            </div>
            <div class="sub_child" style="display: none;">
              <div class="player_position_photo">
                <div class="sub_pic"><a href="/marnon-busch_3599/"><img alt="" /></a></div>
                <div class="player_no img-circle"><div class="next_sub"><span class="prev_sub_button"></span></div></div>
              </div>
              <div class="player_name"><a href="/marnon-busch_3599/">Busch</a></div>
            </div>
          </div>
        </div>
        <div class="league_name_holder"></div>
        """
        rows = LigaInsiderScraper.parse_status_rows(html_text)
        self.assertEqual(len(rows), 2)
        by_slug = {str(row["ligainsider_player_slug"]): row for row in rows}
        self.assertEqual(by_slug["leonidas-stergiou"]["competition_player_names"], ["Busch"])
        self.assertEqual(by_slug["leonidas-stergiou"]["competition_player_count"], 1)
        self.assertEqual(by_slug["marnon-busch"]["competition_player_names"], ["Stergiou"])
        self.assertEqual(by_slug["marnon-busch"]["competition_player_count"], 1)

    def test_parse_rows_from_kader_page_markup(self) -> None:
        html_text = """
        <div class="leg_column_row">
          <div class="player_img">
            <a href="/manuel-neuer_114/">
              <img src="https://cdn.ligainsider.de/images/player/team/minor/manuel-neuer.jpg" alt="Manuel Neuer" />
            </a>
          </div>
          <small>27.03.1986</small>
        </div>
        <div class="leg_column_row">
          <div class="player_img">
            <a href="/jonas-urbig_26101/">
              <img src="/images/player/team/minor/jonas-urbig.jpg" alt="Jonas Urbig" />
            </a>
          </div>
          <small>08.08.2003</small>
        </div>
        """
        rows = LigaInsiderScraper.parse_squad_rows(
            html_text,
            page_url="https://www.ligainsider.de/fc-bayern-muenchen/1/kader/",
        )
        self.assertEqual(len(rows), 2)
        by_slug = {str(row["ligainsider_player_slug"]): row for row in rows}

        self.assertEqual(by_slug["manuel-neuer"]["predicted_lineup"], "unknown")
        self.assertEqual(by_slug["manuel-neuer"]["birthdate"], "27.03.1986")
        self.assertEqual(
            by_slug["manuel-neuer"]["ligainsider_profile_url"],
            "https://www.ligainsider.de/manuel-neuer_114/",
        )
        self.assertIn("player_image_url", by_slug["manuel-neuer"])

        self.assertEqual(by_slug["jonas-urbig"]["birthdate"], "08.08.2003")
        self.assertEqual(
            by_slug["jonas-urbig"]["player_image_url"],
            "https://www.ligainsider.de/images/player/team/minor/jonas-urbig.jpg",
        )


if __name__ == "__main__":
    unittest.main()
