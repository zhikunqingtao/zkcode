from __future__ import annotations

import importlib.util
import subprocess
import sys
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).parents[1] / "python-process-identity.py"
SPEC = importlib.util.spec_from_file_location("zkcode_python_process_identity", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
IDENTITY = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(IDENTITY)


class PythonProcessIdentityTests(unittest.TestCase):
    def setUp(self) -> None:
        framework = (
            "/opt/homebrew/Cellar/python@3.11/3.11.15_1/Frameworks/"
            "Python.framework/Versions/3.11"
        )
        self.expected = "/workspace/python-service/.venv/bin/python"
        self.binary = f"{framework}/bin/python3.11"
        self.shim = f"{framework}/Resources/Python.app/Contents/MacOS/Python"
        self.socket = "/workspace with spaces/.runtime/python.sock"

    def matches(self, command: str, socket: str) -> bool:
        return IDENTITY.command_matches_sidecar(
            command,
            self.expected,
            socket,
            executable=self.expected,
            base_executable=self.binary,
            original_argv=(self.shim,),
        )

    def test_accepts_framework_shim_and_binary_forms(self) -> None:
        for python in (self.shim, self.binary, self.expected):
            with self.subTest(python=python):
                command = f"{python} -m uvicorn src.main:app --uds {self.socket}"
                self.assertTrue(self.matches(command, self.socket))

    def test_rejects_other_interpreters_and_non_sidecar_commands(self) -> None:
        other_version = self.shim.replace("3.11", "3.12")
        cases = (
            f"{other_version} -m uvicorn src.main:app --uds {self.socket}",
            f"{self.binary.replace('python3.11', 'idle3.11')} -m uvicorn src.main:app --uds {self.socket}",
            f"{self.shim} -m uvicorn other.main:app --uds {self.socket}",
            f"{self.shim} -m uvicorn src.main:app --uds",
            f"{self.shim} -m uvicorn src.main:app --uds {self.socket} --reload",
        )
        for command in cases:
            with self.subTest(command=command):
                self.assertFalse(self.matches(command, self.socket))

    def test_socket_is_always_exact(self) -> None:
        command = f"{self.shim} -m uvicorn src.main:app --uds {self.socket}"
        self.assertTrue(self.matches(command, self.socket))
        self.assertFalse(self.matches(command, "/workspace/.runtime/other.sock"))

    def test_runtime_must_be_the_expected_interpreter(self) -> None:
        with self.assertRaises(RuntimeError):
            IDENTITY.runtime_python_forms(
                self.expected,
                executable="/usr/bin/python3",
                base_executable="/usr/bin/python3",
                original_argv=("/usr/bin/python3",),
            )

    def test_cli_exit_status_matches_current_interpreter(self) -> None:
        socket = "/tmp/zkcode-python.sock"
        command = f"{sys.executable} -m uvicorn src.main:app --uds {socket}"
        result = subprocess.run(
            [
                sys.executable,
                str(MODULE_PATH),
                "match-sidecar",
                "--expected-python",
                sys.executable,
                "--socket",
                socket,
            ],
            input=command,
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main()
