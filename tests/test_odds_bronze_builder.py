# ------------------------------------
# test_odds_bronze_builder.py
#
# Tests fuer die Normalisierung von The-Odds-API Events in
# Bronze-Tabellenzeilen.
#
# Usage
# ------------------------------------
# - python3 -m unittest tests.test_odds_bronze_builder -v
# ------------------------------------

from __future__ import annotations

import unittest

from local_ingestion.core.odds_bronze_builder import build_odds_rows


class OddsBronzeBuilderTests(unittest.TestCase):
    def test_build_odds_rows_extracts_h2h_and_totals(self) -> None:
        events = [
            {
                "id": "event_1",
                "sport_key": "soccer_germany_bundesliga",
                "sport_title": "Soccer Bundesliga",
                "commence_time": "2026-03-01T14:30:00Z",
                "home_team": "FC A",
                "away_team": "FC B",
                "bookmakers": [
                    {
                        "key": "book_a",
                        "markets": [
                            {
                                "key": "h2h",
                                "outcomes": [
                                    {"name": "FC A", "price": 1.8},
                                    {"name": "Draw", "price": 3.8},
                                    {"name": "FC B", "price": 4.5},
                                ],
                            },
                            {
                                "key": "totals",
                                "outcomes": [
                                    {"name": "Over", "price": 1.9, "point": 2.5},
                                    {"name": "Under", "price": 1.95, "point": 2.5},
                                ],
                            },
                        ],
                    },
                    {
                        "key": "book_b",
                        "markets": [
                            {
                                "key": "h2h",
                                "outcomes": [
                                    {"name": "FC A", "price": 1.85},
                                    {"name": "Draw", "price": 3.7},
                                    {"name": "FC B", "price": 4.4},
                                ],
                            }
                        ],
                    },
                ],
            }
        ]

        rows = build_odds_rows(events)
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["odds_event_id"], "event_1")
        self.assertEqual(row["bookmaker_count"], 2)
        self.assertAlmostEqual(float(row["h2h_home_odds"]), 1.825, places=3)
        self.assertAlmostEqual(float(row["h2h_draw_odds"]), 3.75, places=2)
        self.assertAlmostEqual(float(row["h2h_away_odds"]), 4.45, places=2)
        self.assertEqual(row["totals_line"], 2.5)
        self.assertAlmostEqual(float(row["totals_over_odds"]), 1.9, places=2)
        self.assertAlmostEqual(float(row["totals_under_odds"]), 1.95, places=2)

        implied_sum = (
            float(row["h2h_home_implied_prob"])
            + float(row["h2h_draw_implied_prob"])
            + float(row["h2h_away_implied_prob"])
        )
        self.assertAlmostEqual(implied_sum, 1.0, places=5)


if __name__ == "__main__":
    unittest.main()
