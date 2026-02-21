# ------------------------------------
# test_powerbi_api.py
#
# Basistests fuer Power BI REST API CLI Argument-Handling.
# ------------------------------------

from __future__ import annotations

import io
from contextlib import redirect_stdout
import unittest

from scripts.powerbi.powerbi_api import PowerBIClientError, main


class PowerBIApiTests(unittest.TestCase):
    def test_dry_run_list_workspaces(self) -> None:
        buf = io.StringIO()
        with redirect_stdout(buf):
            rc = main(["--dry-run", "list-workspaces"])
        self.assertEqual(rc, 0)
        self.assertIn('"status": "dry_run"', buf.getvalue())

    def test_workspace_id_required_for_dataset_calls(self) -> None:
        with self.assertRaises(PowerBIClientError):
            main(["--dry-run", "list-datasets"])


if __name__ == "__main__":
    unittest.main()
