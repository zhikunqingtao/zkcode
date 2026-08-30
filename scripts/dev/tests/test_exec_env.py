from __future__ import annotations

import importlib.util
import tempfile
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).parents[1] / "exec-env.py"
SPEC = importlib.util.spec_from_file_location("zkcode_exec_env", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
EXEC_ENV = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(EXEC_ENV)


class ExecEnvTests(unittest.TestCase):
    def parse(self, content: str):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / ".env"
            path.write_text(content, encoding="utf-8")
            return EXEC_ENV.parse_env(path)

    def test_supported_values_are_data(self) -> None:
        values, warnings = self.parse(
            "ZK_PORT=8082\n"
            "ZK_DEFAULT_MODEL='model name'\n"
            'ZK_LLM_BASE_URL="https://example.test/v1"\n'
        )
        self.assertEqual(values["ZK_PORT"], "8082")
        self.assertEqual(values["ZK_DEFAULT_MODEL"], "model name")
        self.assertEqual(values["ZK_LLM_BASE_URL"], "https://example.test/v1")
        self.assertEqual(warnings, [])

    def test_command_substitution_is_never_evaluated(self) -> None:
        values, _ = self.parse("ZK_DEFAULT_MODEL=$(touch /tmp/never-run)\n")
        self.assertEqual(values["ZK_DEFAULT_MODEL"], "$(touch /tmp/never-run)")

    def test_export_syntax_is_rejected(self) -> None:
        with self.assertRaises(EXEC_ENV.EnvSyntaxError):
            self.parse("export ZK_PORT=8082\n")

    def test_invalid_name_is_rejected_without_value(self) -> None:
        with self.assertRaisesRegex(EXEC_ENV.EnvSyntaxError, "invalid variable name"):
            self.parse("ZK-PORT=secret-value\n")

    def test_unknown_key_is_ignored(self) -> None:
        values, warnings = self.parse("UNRELATED_SECRET=not-forwarded\n")
        self.assertEqual(values, {})
        self.assertEqual(len(warnings), 1)
        self.assertNotIn("not-forwarded", warnings[0])

    def test_empty_value_is_supported(self) -> None:
        values, _ = self.parse("LLM_PROVIDER_OPENAI_API_KEY=\n")
        self.assertEqual(values["LLM_PROVIDER_OPENAI_API_KEY"], "")


if __name__ == "__main__":
    unittest.main()
