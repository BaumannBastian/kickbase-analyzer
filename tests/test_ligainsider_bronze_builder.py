# ------------------------------------
# test_ligainsider_bronze_builder.py
#
# Tests fuer LigaInsider-Bronze-Anreicherung und Konkurrenz-Logik.
#
# Usage
# ------------------------------------
# - python3 -m unittest tests.test_ligainsider_bronze_builder -v
# ------------------------------------

from __future__ import annotations

import unittest

from local_ingestion.core.ligainsider_bronze_builder import build_ligainsider_rows


class LigaInsiderBronzeBuilderTests(unittest.TestCase):
    def test_uses_explicit_competitors_from_ligainsider_column(self) -> None:
        raw_rows = [
            {
                "ligainsider_player_slug": "harry-kane",
                "player_name": "Harry Kane",
                "predicted_lineup": "starter",
                "status": "unknown",
                "source_url": "https://www.ligainsider.de/fc-bayern-muenchen/1/",
                "competition_player_names": ["Luis Diaz", "Nicolas Jackson"],
            },
            {
                "ligainsider_player_slug": "luis-diaz",
                "player_name": "Luis Diaz",
                "predicted_lineup": "starter",
                "status": "unknown",
                "source_url": "https://www.ligainsider.de/fc-bayern-muenchen/1/",
                "competition_player_names": ["Harry Kane", "Nicolas Jackson"],
            },
        ]

        kickbase_rows = [
            {
                "kickbase_player_id": "7226",
                "player_name": "Harry Kane",
                "team_id": "1",
                "position_code": 4,
                "position": "FWD",
                "injury_status": "fit",
                "market_value": 10_000_000,
            },
            {
                "kickbase_player_id": "11675",
                "player_name": "Luis Diaz",
                "team_id": "1",
                "position_code": 4,
                "position": "FWD",
                "injury_status": "fit",
                "market_value": 9_000_000,
            },
        ]

        rows = build_ligainsider_rows(
            raw_rows=raw_rows,
            kickbase_rows=kickbase_rows,
            previous_rows=[],
        )
        by_slug = {str(row["ligainsider_player_slug"]): row for row in rows}
        kane = by_slug["harry-kane"]
        self.assertEqual(kane["competition_scope"], "ligainsider_column")
        self.assertEqual(kane["competition_player_count"], 2)
        self.assertEqual(
            kane["competition_player_names"],
            ["Luis Diaz", "Nicolas Jackson"],
        )
        self.assertTrue(kane["has_position_competition"])


if __name__ == "__main__":
    unittest.main()
