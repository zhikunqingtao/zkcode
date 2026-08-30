from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).parents[1] / "inspect.py"
SPEC = importlib.util.spec_from_file_location("zkcode_dev_inspect", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
INSPECT = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(INSPECT)


class InspectTests(unittest.TestCase):
    def test_running_unhealthy_service_fails_health_check(self) -> None:
        result = INSPECT.service_health_check(
            "backend", {"running": True, "healthy": False}
        )
        self.assertFalse(result["ok"])
        self.assertEqual(result["detail"], "running but unhealthy")
        self.assertEqual(result["repair"], "./dev restart backend")

    def test_stopped_service_is_allowed_for_doctor(self) -> None:
        result = INSPECT.service_health_check(
            "frontend", {"running": False, "healthy": False}
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["detail"], "not running (allowed for doctor)")


if __name__ == "__main__":
    unittest.main()
