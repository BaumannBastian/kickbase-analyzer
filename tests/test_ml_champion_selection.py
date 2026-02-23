# ------------------------------------
# test_ml_champion_selection.py
#
# Tests fuer die Champion-Auswahl aus sklearn- und torch-CV-Metriken.
#
# Usage
# ------------------------------------
# - python3 -m unittest tests.test_ml_champion_selection -v
# ------------------------------------

from __future__ import annotations

import unittest

from scripts.ml.train_hierarchical_models import _select_champion_model


class MlChampionSelectionTests(unittest.TestCase):
    def test_prefers_torch_when_rmse_is_lower(self) -> None:
        sklearn_summary = {"metrics_mean": {"metric_points_total_rmse": 90.0}}
        torch_summary = {"metrics_mean": {"metric_points_total_rmse": 80.0}}

        champion = _select_champion_model(sklearn_summary, torch_summary)
        self.assertEqual(champion["champion_model"], "torch")
        self.assertEqual(champion["metric_key"], "metric_points_total_rmse")

    def test_falls_back_to_sklearn_without_torch_metrics(self) -> None:
        sklearn_summary = {"metrics_mean": {"metric_points_total_rmse": 90.0}}
        torch_summary = {"status": "skipped", "reason": "torch missing"}

        champion = _select_champion_model(sklearn_summary, torch_summary)
        self.assertEqual(champion["champion_model"], "sklearn")


if __name__ == "__main__":
    unittest.main()
