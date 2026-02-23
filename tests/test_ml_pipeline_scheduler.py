# ------------------------------------
# test_ml_pipeline_scheduler.py
#
# Tests fuer den ML-Pipeline-Scheduler.
#
# Usage
# ------------------------------------
# - python3 -m unittest tests.test_ml_pipeline_scheduler -v
# ------------------------------------

from __future__ import annotations

from unittest.mock import patch
import unittest

from scripts.ml.run_ml_pipeline_scheduler import run_scheduler


class MlPipelineSchedulerTests(unittest.TestCase):
    def test_scheduler_runs_expected_amount(self) -> None:
        calls: list[list[str]] = []
        sleep_calls: list[float] = []

        def fake_pipeline_main(argv: list[str]) -> int:
            calls.append(list(argv))
            return 0

        with patch("scripts.ml.run_ml_pipeline_scheduler.run_pipeline_main", side_effect=fake_pipeline_main):
            with patch("scripts.ml.run_ml_pipeline_scheduler.time.sleep", side_effect=sleep_calls.append):
                runs = run_scheduler(
                    pipeline_args=["--", "--env-file", ".env", "--skip-bq-load"],
                    interval_seconds=1.5,
                    max_runs=3,
                )

        self.assertEqual(runs, 3)
        self.assertEqual(len(calls), 3)
        self.assertEqual(calls[0], ["--env-file", ".env", "--skip-bq-load"])
        self.assertEqual(sleep_calls, [1.5, 1.5])


if __name__ == "__main__":
    unittest.main()
