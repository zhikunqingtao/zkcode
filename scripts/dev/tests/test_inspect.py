from __future__ import annotations

import importlib.util
import tempfile
import unittest
from pathlib import Path
from unittest import mock


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

    def test_process_uses_the_supplied_identity_helper(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            pid_file = Path(directory) / "python.pid"
            pid_file.write_text("123\n", encoding="utf-8")
            command = "/framework/Python -m uvicorn src.main:app --uds /tmp/python.sock"
            identity_command = ["/venv/python", "/identity.py", "match-sidecar"]
            with mock.patch.object(
                INSPECT,
                "run",
                side_effect=((True, command), (True, "")),
            ) as run:
                result = INSPECT.process(pid_file, "/venv/python", identity_command)
            self.assertTrue(result["running"])
            self.assertTrue(result["commandMatches"])
            run.assert_called_with(identity_command, timeout=5, input_text=command)

    def test_process_fails_closed_when_identity_helper_rejects_command(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            pid_file = Path(directory) / "python.pid"
            pid_file.write_text("123\n", encoding="utf-8")
            with mock.patch.object(
                INSPECT,
                "run",
                side_effect=((True, "/framework/idle3.11"), (False, "")),
            ):
                result = INSPECT.process(
                    pid_file,
                    "/venv/python",
                    ["/venv/python", "/identity.py", "match-sidecar"],
                )
            self.assertFalse(result["running"])
            self.assertFalse(result["commandMatches"])

    def test_service_status_runs_python_matcher_under_project_venv(self) -> None:
        root = Path("/workspace")
        with mock.patch.object(
            INSPECT,
            "process",
            return_value={"running": False, "pid": None, "recorded": False},
        ) as process, mock.patch.object(INSPECT, "http_json", return_value=None), mock.patch.object(
            INSPECT, "http_ready", return_value=False
        ):
            INSPECT.service_status(root, 8082)
        python_call = process.call_args_list[2]
        expected_python = str(root / "python-service/.venv/bin/python")
        self.assertEqual(python_call.args[1], expected_python)
        self.assertEqual(python_call.args[2][0], expected_python)
        self.assertIn("match-sidecar", python_call.args[2])
        socket_index = python_call.args[2].index("--socket")
        self.assertEqual(
            python_call.args[2][socket_index + 1],
            str(root / ".runtime/python.sock"),
        )


if __name__ == "__main__":
    unittest.main()
