from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


REPOSITORY = Path(__file__).resolve().parents[3]
EXEC_ENV = REPOSITORY / "scripts/dev/exec-env.py"
FLAG = "ZK_DEV_ALLOW_DEMO_CREDENTIAL"


class DemoFlagLauncherTests(unittest.TestCase):
    def run_exec_env(
        self, content: str, *, inherited: str | None = None
    ) -> subprocess.CompletedProcess[str]:
        with tempfile.TemporaryDirectory() as directory:
            env_file = Path(directory) / ".env"
            env_file.write_text(content, encoding="utf-8")
            environment = os.environ.copy()
            if inherited is None:
                environment.pop(FLAG, None)
            else:
                environment[FLAG] = inherited
            return subprocess.run(
                [
                    sys.executable,
                    str(EXEC_ENV),
                    "--file",
                    str(env_file),
                    "--canonical-zero-one",
                    f"{FLAG}=0",
                    "--",
                    sys.executable,
                    "-c",
                    f"import os; print(repr(os.environ[{FLAG!r}]))",
                ],
                check=False,
                capture_output=True,
                text=True,
                env=environment,
            )

    def test_accepts_only_canonical_values(self) -> None:
        for value in ("0", "1"):
            with self.subTest(value=value):
                result = self.run_exec_env(f"{FLAG}={value}\n")
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(result.stdout, f"'{value}'\n")

    def test_absent_value_defaults_to_zero_and_overrides_parent_environment(self) -> None:
        result = self.run_exec_env("", inherited="1")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "'0'\n")

    def test_rejects_empty_and_noncanonical_values(self) -> None:
        for value in ("", "true", "01", '"1\\n"', '"0\\r\\n"'):
            with self.subTest(value=value):
                result = self.run_exec_env(f"{FLAG}={value}\n")
                self.assertEqual(result.returncode, 2)
                self.assertIn(f"{FLAG} must be exactly 0 or 1", result.stderr)
                self.assertEqual(result.stdout, "")

    def test_launcher_uses_single_pass_validation_without_command_substitution(self) -> None:
        launcher = (REPOSITORY / "scripts/run-backend-macos.sh").read_text(
            encoding="utf-8"
        )
        self.assertIn(
            "--canonical-zero-one ZK_DEV_ALLOW_DEMO_CREDENTIAL=0", launcher
        )
        self.assertNotIn("DEMO_ALLOWED=$(", launcher)


if __name__ == "__main__":
    unittest.main()
