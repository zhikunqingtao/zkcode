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

    def test_documented_summary_and_speech_configuration_is_forwarded_as_data(self) -> None:
        values, warnings = self.parse(
            "LLM_COMPACT_PROVIDER=deepseek\n"
            "LLM_COMPACT_MODEL=deepseek-flash\n"
            "LLM_COMPACT_MAX_COMPLETION_TOKENS=8192\n"
            "ASR_CORRECTIONS=zkcode:z k code,$(not-executed)\n"
        )
        self.assertEqual(values["LLM_COMPACT_PROVIDER"], "deepseek")
        self.assertEqual(values["LLM_COMPACT_MAX_COMPLETION_TOKENS"], "8192")
        self.assertEqual(values["ASR_CORRECTIONS"], "zkcode:z k code,$(not-executed)")
        self.assertEqual(warnings, [])

    def test_current_env_example_has_no_silently_ignored_keys(self) -> None:
        values, warnings = EXEC_ENV.parse_env(Path(__file__).parents[3] / ".env.example")
        self.assertTrue(values)
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
