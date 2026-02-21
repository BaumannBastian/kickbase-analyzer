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
                "ligainsider_player_id": "4828",
                "player_name": "Harry Kane",
                "predicted_lineup": "starter",
                "status": "unknown",
                "source_url": "https://www.ligainsider.de/fc-bayern-muenchen/1/",
                "competition_player_names": ["Luis Diaz", "Nicolas Jackson"],
            },
            {
                "ligainsider_player_slug": "luis-diaz",
                "ligainsider_player_id": "22566",
                "player_name": "Luis Diaz",
                "predicted_lineup": "starter",
                "status": "unknown",
                "source_url": "https://www.ligainsider.de/fc-bayern-muenchen/1/",
                "competition_player_names": ["Harry Kane", "Nicolas Jackson"],
            },
        ]

        rows = build_ligainsider_rows(
            raw_rows=raw_rows,
            previous_rows=[],
        )
        by_slug = {str(row["ligainsider_player_slug"]): row for row in rows}
        kane = by_slug["harry-kane"]
        self.assertEqual(kane["competition_player_count"], 2)
        self.assertEqual(
            kane["competition_player_names"],
            ["Luis Diaz", "Nicolas Jackson"],
        )
        self.assertEqual(kane["predicted_lineup"], "Potential Starter")

    def test_lineup_bench_and_safe_starter_mapping(self) -> None:
        raw_rows = [
            {
                "ligainsider_player_slug": "starter-safe",
                "ligainsider_player_id": "100",
                "player_name": "Starter Safe",
                "predicted_lineup": "starter",
                "status": "fit",
                "competition_player_names": [],
            },
            {
                "ligainsider_player_slug": "bench-player",
                "ligainsider_player_id": "101",
                "player_name": "Bench Player",
                "predicted_lineup": "bench",
                "status": "fit",
                "competition_player_names": ["Any Competitor"],
            },
        ]

        rows = build_ligainsider_rows(raw_rows=raw_rows, previous_rows=[])
        by_slug = {str(row["ligainsider_player_slug"]): row for row in rows}
        self.assertEqual(by_slug["starter-safe"]["predicted_lineup"], "Safe Starter")
        self.assertEqual(by_slug["bench-player"]["predicted_lineup"], "Bench")


if __name__ == "__main__":
    unittest.main()
