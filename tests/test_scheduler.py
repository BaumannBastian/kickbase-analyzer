# ------------------------------------
# test_scheduler.py
#
# Tests fuer den lokalen Ingestion-Scheduler.
#
# Usage
# ------------------------------------
# - python3 -m unittest tests.test_scheduler -v
# ------------------------------------

from __future__ import annotations

import unittest

from local_ingestion.runners.run_scheduler import run_scheduler


class SchedulerTests(unittest.TestCase):
    def test_scheduler_runs_expected_amount(self) -> None:
        counter = {"value": 0}
        sleep_calls: list[float] = []

        def run_once() -> dict[str, object]:
            counter["value"] += 1
            return {"run": counter["value"], "status": "success"}

        runs = run_scheduler(
            run_once=run_once,
            interval_seconds=0.25,
            max_runs=3,
            sleep_fn=sleep_calls.append,
        )

        self.assertEqual(runs, 3)
        self.assertEqual(counter["value"], 3)
        self.assertEqual(sleep_calls, [0.25, 0.25])


if __name__ == "__main__":
    unittest.main()
