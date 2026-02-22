# ------------------------------------
# test_history_etl_parsers.py
#
# Parser-Tests fuer die lokale History-ETL:
# - Eventtype-Mapping aus /v4/live/eventtypes
# - Match-Performance inkl. Home/Away und Resultat
#
# Usage
# ------------------------------------
# - pytest tests/test_history_etl_parsers.py
# ------------------------------------

import unittest

from src.etl_history import parse_event_types, parse_performance_rows


class HistoryEtlParserTests(unittest.TestCase):
    def test_parse_event_types_supports_i_ti_shape(self) -> None:
        payload = {
            "lcud": "2025-10-27T15:39:25Z",
            "it": [
                {"i": 3337, "ti": "Forward zone pass"},
                {"i": -8, "ti": "Von Anfang an gespielt"},
            ],
        }

        rows = parse_event_types(payload)
        by_id = {row["event_type_id"]: row for row in rows}

        self.assertIn(3337, by_id)
        self.assertEqual(by_id[3337]["event_name"], "Forward zone pass")
        self.assertIn(-8, by_id)
        self.assertEqual(by_id[-8]["event_name"], "Von Anfang an gespielt")

    def test_parse_performance_rows_derives_home_away_and_result(self) -> None:
        payload = {
            "it": [
                {
                    "ti": "2025/2026",
                    "ph": [
                        {
                            "day": 23,
                            "p": 126,
                            "md": "2026-02-21T17:30:00Z",
                            "t1": "43",
                            "t2": "3",
                            "t1g": 2,
                            "t2g": 2,
                            "pt": "43",
                        },
                        {
                            "day": 22,
                            "p": 124,
                            "md": "2026-02-15T14:30:00Z",
                            "t1": "11",
                            "t2": "43",
                            "t1g": 0,
                            "t2g": 1,
                            "pt": "43",
                        },
                    ],
                }
            ]
        }

        rows = parse_performance_rows(
            payload,
            player_uid=1246,
            active_season_label="2025/2026",
            team_code_by_team_id={43: "RBL", 3: "BVB", 11: "WOB"},
        )
        by_day = {row["matchday"]: row for row in rows}

        self.assertEqual(by_day[23]["player_uid"], 1246)
        self.assertEqual(by_day[23]["season_label"], "2025/2026")
        self.assertTrue(by_day[23]["is_home"])
        self.assertEqual(by_day[23]["match_result"], "D")
        self.assertEqual(by_day[23]["match_uid"], "25/26-MD23-RBLBVB")

        self.assertFalse(by_day[22]["is_home"])
        self.assertEqual(by_day[22]["match_result"], "W")

    def test_parse_performance_rows_uses_default_team_id_mapping_without_legacy_txx(self) -> None:
        payload = {
            "it": [
                {
                    "ti": "2025/2026",
                    "ph": [
                        {
                            "day": 9,
                            "p": 118,
                            "md": "2026-01-01T14:30:00Z",
                            "t1": "42",
                            "t2": "43",
                            "t1g": 0,
                            "t2g": 2,
                            "pt": "43",
                        },
                        {
                            "day": 10,
                            "p": 110,
                            "md": "2026-01-08T14:30:00Z",
                            "t1": "999",
                            "t2": "43",
                            "t1g": 1,
                            "t2g": 1,
                            "pt": "43",
                        },
                    ],
                }
            ]
        }

        rows = parse_performance_rows(
            payload,
            player_uid=1246,
            active_season_label="2025/2026",
            team_code_by_team_id={},
        )
        by_day = {row["matchday"]: row for row in rows}

        self.assertEqual(by_day[9]["match_uid"], "25/26-MD09-D98RBL")
        self.assertEqual(by_day[10]["match_uid"], "25/26-MD10-TNARBL")
        self.assertNotIn("T42", by_day[9]["match_uid"])
        self.assertNotIn("T999", by_day[10]["match_uid"])


if __name__ == "__main__":
    unittest.main()
