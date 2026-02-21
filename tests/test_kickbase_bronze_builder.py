# ------------------------------------
# test_kickbase_bronze_builder.py
#
# Tests fuer Kickbase-Bronze-Mapping (insb. Starts/Einsaetze
# und average_minutes Berechnung).
#
# Usage
# ------------------------------------
# - python3 -m unittest tests.test_kickbase_bronze_builder -v
# ------------------------------------

from __future__ import annotations

from datetime import UTC, datetime
import unittest

from local_ingestion.core.kickbase_bronze_builder import build_kickbase_player_row


class KickbaseBronzeBuilderTests(unittest.TestCase):
    def test_average_minutes_uses_appearances_not_starts(self) -> None:
        row = build_kickbase_player_row(
            market_row={
                "i": "kb_1",
                "fn": "Max",
                "n": "Beispiel",
                "mv": 10_000_000,
                "pos": 3,
                "st": 0,
                "prob": 1,
            },
            details_payload={
                "i": "kb_1",
                "smc": 11,
                "ismc": 22,
                "sec": 79_530,  # 1325.5 Minuten
                "g": 5,
                "a": 2,
                "y": 1,
                "r": 0,
            },
            market_value_history_payload={},
            performance_payload={},
            transfers_payload={},
            snapshot_ts=datetime(2026, 2, 21, tzinfo=UTC),
        )

        self.assertEqual(row["starts_total"], 11)
        self.assertEqual(row["appearances_total"], 22)
        self.assertAlmostEqual(float(row["average_minutes"]), 60.25, places=2)

    def test_average_minutes_handles_zero_appearances(self) -> None:
        row = build_kickbase_player_row(
            market_row={
                "i": "kb_2",
                "fn": "Luca",
                "n": "Test",
                "mv": 500_000,
                "pos": 2,
            },
            details_payload={
                "i": "kb_2",
                "smc": 0,
                "ismc": 0,
                "sec": 0,
            },
            market_value_history_payload={},
            performance_payload={},
            transfers_payload={},
            snapshot_ts=datetime(2026, 2, 21, tzinfo=UTC),
        )

        self.assertEqual(row["starts_total"], 0)
        self.assertEqual(row["appearances_total"], 0)
        self.assertIsNone(row["average_minutes"])


if __name__ == "__main__":
    unittest.main()
