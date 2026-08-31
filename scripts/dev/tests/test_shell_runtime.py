from __future__ import annotations

import json
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path


REPOSITORY = Path(__file__).resolve().parents[3]
COMMON = REPOSITORY / "scripts/dev/common.sh"
LIFECYCLE = REPOSITORY / "scripts/dev/lifecycle.sh"
SYNC = REPOSITORY / "scripts/dev/sync.sh"
TOOLCHAINS = REPOSITORY / "scripts/dev/toolchains-macos.sh"
DOCTOR = REPOSITORY / "scripts/dev/doctor.sh"
PYTHON_PROCESS_IDENTITY = REPOSITORY / "scripts/dev/python-process-identity.py"


def run_shell(script: str, *arguments: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["/bin/sh", "-c", script, "zkcode-test", *arguments],
        check=False,
        capture_output=True,
        text=True,
        timeout=15,
    )


def link_python_process_identity(root: Path) -> None:
    helper = root / "scripts/dev/python-process-identity.py"
    helper.parent.mkdir(parents=True, exist_ok=True)
    helper.symlink_to(PYTHON_PROCESS_IDENTITY)


def install_fake_project_python(root: Path) -> Path:
    python = root / "python-service/.venv/bin/python"
    python.parent.mkdir(parents=True, exist_ok=True)
    python.symlink_to(sys.executable)
    link_python_process_identity(root)
    return python


class ShellRuntimeTests(unittest.TestCase):
    def test_missing_env_uses_default_port_without_creating_a_file(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            result = run_shell(
                f"""
                set -eu
                ROOT_DIR=$1
                . {COMMON!s}
                dev_backend_port
                test ! -e "$ROOT_DIR/.env"
                """,
                str(root),
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stdout.strip(), "8082")

    def test_toolchain_values_are_loaded_from_toml(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = root / "configuration/dev-toolchain.toml"
            config.parent.mkdir(parents=True)
            config.write_text(
                """schema_version = 1
platform = "darwin"
arch = "arm64"
minimum_macos = "14.4"
rust = "1.80.1"
node = ">=20.1.0,<21.0.0"
npm = ">=10.2.0,<11.0.0"
python = ">=3.12.1,<3.13.0"
""",
                encoding="utf-8",
            )
            result = run_shell(
                f"""
                set -eu
                ROOT_DIR=$1
                . {COMMON!s}
                . {TOOLCHAINS!s}
                dev_load_toolchain_config
                printf '%s|%s|%s|%s|%s\n' "$DEV_REQUIRED_RUST" "$DEV_REQUIRED_NODE_RANGE" "$ZK_DEV_NODE_FORMULA" "$ZK_DEV_PYTHON_FORMULA" "$DEV_REQUIRED_MACOS"
                """,
                str(root),
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(
                result.stdout.strip(),
                "1.80.1|>=20.1.0,<21.0.0|node@20|python@3.12|14.4",
            )

    def test_bounded_runner_stops_a_hung_process(self) -> None:
        started = time.monotonic()
        result = run_shell(
            f"""
            set -u
            ROOT_DIR=$1
            . {COMMON!s}
            dev_run_bounded 1 deliberate-timeout /bin/sh -c 'sleep 30'
            printf '%s\n' "$?"
            """,
            str(REPOSITORY),
        )
        elapsed = time.monotonic() - started
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "124")
        self.assertLess(elapsed, 8)
        self.assertIn("exceeded 1s", result.stderr)

    def test_browser_smoke_executes_embedded_python(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            python_path = root / "python-service/.venv/bin/python"
            python_path.parent.mkdir(parents=True)
            python_path.symlink_to(sys.executable)
            browser_path = root / ".runtime/playwright"
            browser_path.mkdir(parents=True)
            module_root = root / "fake-modules"
            playwright = module_root / "playwright"
            playwright.mkdir(parents=True)
            (playwright / "__init__.py").write_text("", encoding="utf-8")
            (playwright / "sync_api.py").write_text(
                """class Page:
    def set_content(self, value):
        self.value = value

    def title(self):
        return "zkcode-dev-smoke"


class Browser:
    def new_page(self):
        return Page()

    def close(self):
        pass


class Playwright:
    chromium = type("Chromium", (), {"launch": lambda self, headless: Browser()})()


class Context:
    def __enter__(self):
        return Playwright()

    def __exit__(self, *args):
        pass


def sync_playwright():
    return Context()
""",
                encoding="utf-8",
            )
            result = run_shell(
                f"""
                set -eu
                ROOT_DIR=$1
                export PYTHONPATH=$2
                . {SYNC!s}
                dev_browser_smoke "$ROOT_DIR/.runtime/playwright"
                """,
                str(root),
                str(module_root),
            )
            self.assertEqual(result.returncode, 0, result.stderr)

    def test_diagnostic_port_falls_back_when_env_is_invalid(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            parser = root / "scripts/dev/exec-env.py"
            parser.parent.mkdir(parents=True)
            parser.symlink_to(REPOSITORY / "scripts/dev/exec-env.py")
            (root / ".env").write_text("export ZK_PORT=9000\n", encoding="utf-8")
            result = run_shell(
                f"""
                set -eu
                ROOT_DIR=$1
                DEV_PYTHON=$2
                export DEV_PYTHON
                . {COMMON!s}
                dev_backend_port_for_diagnostics
                """,
                str(root),
                sys.executable,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stdout.strip(), "8082")

    def test_doctor_json_survives_an_invalid_env(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            scripts = root / "scripts/dev"
            scripts.mkdir(parents=True)
            for name in ("exec-env.py", "inspect.py", "state.py"):
                (scripts / name).symlink_to(REPOSITORY / "scripts/dev" / name)
            policy = root / "configuration/dev-toolchain.toml"
            policy.parent.mkdir(parents=True)
            policy.symlink_to(REPOSITORY / "configuration/dev-toolchain.toml")
            (root / ".env").write_text("export ZK_PORT=9000\n", encoding="utf-8")
            result = run_shell(
                f"""
                set -u
                ROOT_DIR=$1
                DEV_PYTHON=$2
                export DEV_PYTHON
                . {COMMON!s}
                . {DOCTOR!s}
                dev_doctor 0 1
                """,
                str(root),
                sys.executable,
            )
            self.assertEqual(result.returncode, 1, result.stderr)
            report = json.loads(result.stdout)
            self.assertFalse(report["ok"])
            env_check = next(item for item in report["checks"] if item["name"] == "env")
            self.assertFalse(env_check["ok"])

    def test_python_enabled_value_matches_server_config_semantics(self) -> None:
        cases = (
            ("__missing__", "true", 0),
            ("", "true", 0),
            ("TRUE", "true", 0),
            ("False", "false", 0),
            ("  TrUe  ", "true", 0),
            ("\nTRUE\n", "true", 0),
            ("TR\nUE", "", 2),
            ("yes", "", 2),
        )
        for raw_value, expected, expected_status in cases:
            with self.subTest(raw_value=raw_value):
                result = run_shell(
                    f"""
                    set -u
                    ROOT_DIR=$1
                    DEV_TEST_VALUE=$2
                    . {COMMON!s}
                    . {LIFECYCLE!s}
                    dev_env_get() {{
                        if [ "$DEV_TEST_VALUE" = __missing__ ]; then
                            printf '%s\n' "$2"
                        else
                            printf '%s\n' "$DEV_TEST_VALUE"
                        fi
                    }}
                    dev_python_enabled_value
                    """,
                    str(REPOSITORY),
                    raw_value,
                )
                self.assertEqual(result.returncode, expected_status, result.stderr)
                self.assertEqual(result.stdout.strip(), expected)
                if expected_status == 2:
                    self.assertIn("ZK_PYTHON_ENABLED must be true or false", result.stderr)

    def test_start_backend_passes_only_canonical_python_values_to_reuse(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime = root / ".runtime"
            runtime.mkdir()
            (runtime / "backend.pid").write_text("123\n", encoding="utf-8")
            for raw_value, expected in (("TRUE", "true"), ("False", "false"), ("", "true")):
                with self.subTest(raw_value=raw_value):
                    result = run_shell(
                        f"""
                        set -eu
                        ROOT_DIR=$1
                        DEV_RUNTIME_DIR="$ROOT_DIR/.runtime"
                        DEV_TEST_VALUE=$2
                        . {COMMON!s}
                        . {LIFECYCLE!s}
                        dev_backend_port() {{ printf '8082\n'; }}
                        dev_env_get() {{ printf '%s\n' "$DEV_TEST_VALUE"; }}
                        dev_reuse_backend_if_ready() {{ printf '%s\n' "$2"; return 0; }}
                        dev_start_backend
                        """,
                        str(root),
                        raw_value,
                    )
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertEqual(result.stdout.strip(), expected)

            result = run_shell(
                f"""
                set -eu
                ROOT_DIR=$1
                DEV_RUNTIME_DIR="$ROOT_DIR/.runtime"
                . {COMMON!s}
                . {LIFECYCLE!s}
                dev_backend_port() {{ printf '8082\n'; }}
                dev_env_get() {{ printf 'invalid\n'; }}
                dev_reuse_backend_if_ready() {{ exit 9; }}
                dev_start_backend
                """,
                str(root),
            )
            self.assertEqual(result.returncode, 2, result.stderr)
            self.assertIn("ZK_PYTHON_ENABLED must be true or false", result.stderr)

    def test_new_backend_treats_uppercase_true_as_python_enabled(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime = root / ".runtime"
            runtime.mkdir()
            fake_python = install_fake_project_python(root)
            fake_launcher = root / "scripts/spawn-detached.py"
            fake_launcher.parent.mkdir(parents=True, exist_ok=True)
            fake_launcher.write_text("print(456)\n", encoding="utf-8")
            health_marker = root / "python-health-checked"
            result = run_shell(
                f"""
                set -eu
                ROOT_DIR=$1
                DEV_RUNTIME_DIR="$ROOT_DIR/.runtime"
                DEV_TEST_HEALTH_MARKER=$2
                . {COMMON!s}
                . {LIFECYCLE!s}
                dev_backend_port() {{ printf '8082\n'; }}
                dev_env_get() {{ printf 'TRUE\n'; }}
                dev_port_owner() {{ return 1; }}
                dev_python_socket() {{ printf '%s\n' "$ROOT_DIR/.runtime/python.sock"; }}
                dev_wait_backend_health() {{
                    [ "$2" = true ]
                    printf 'checked\n' >>"$DEV_TEST_HEALTH_MARKER"
                }}
                lsof() {{ printf '789\n'; }}
                ps() {{ printf '%s\n' "$ROOT_DIR/python-service/.venv/bin/python -m uvicorn src.main:app --uds $ROOT_DIR/.runtime/python.sock"; }}
                dev_start_backend
                """,
                str(root),
                str(health_marker),
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertTrue(health_marker.exists())
            self.assertEqual((runtime / "python.pid").read_text(encoding="utf-8"), "789\n")

    def test_backend_reuse_requires_exact_python_up(self) -> None:
        health_cases = (
            ('{"subsystems":{"python":{"status":"UP"}}}', True, True),
            ('{"subsystems":{"python":{"status":"DOWN"}}}', True, False),
            ('{"subsystems":{"python":{"status":"DEGRADED"}}}', True, False),
            ('{"subsystems":{}}', True, False),
            ("not-json", True, False),
            ('{"subsystems":{"python":{"status":"DOWN"}}}', False, False),
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            python_path = root / "python-service/.venv/bin/python"
            python_path.parent.mkdir(parents=True)
            python_path.symlink_to(sys.executable)
            for health_json, curl_succeeds, expected in health_cases:
                with self.subTest(health_json=health_json, curl_succeeds=curl_succeeds):
                    result = run_shell(
                        f"""
                        set -u
                        ROOT_DIR=$1
                        DEV_TEST_HEALTH_JSON=$2
                        DEV_TEST_CURL_SUCCEEDS=$3
                        curl() {{
                            [ "$DEV_TEST_CURL_SUCCEEDS" = 1 ] || return 22
                            printf '%s' "$DEV_TEST_HEALTH_JSON"
                        }}
                        . {LIFECYCLE!s}
                        dev_backend_health_ready http://127.0.0.1:8082 true
                        """,
                        str(root),
                        health_json,
                        "1" if curl_succeeds else "0",
                    )
                    self.assertEqual(result.returncode == 0, expected, result.stderr)

    def test_backend_reuse_requires_exact_python_disabled_status(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            python_path = root / "python-service/.venv/bin/python"
            python_path.parent.mkdir(parents=True)
            python_path.symlink_to(sys.executable)
            health_cases = (
                ('{"subsystems":{"python":{"status":"DISABLED"}}}', True, True),
                ('{"subsystems":{"python":{"status":"UP"}}}', True, False),
                ('{"subsystems":{"python":{"status":"DOWN"}}}', True, False),
                ('{"subsystems":{"python":{"status":"DEGRADED"}}}', True, False),
                ('{"subsystems":{}}', True, False),
                ("not-json", True, False),
                ("", False, False),
            )
            for health_json, curl_succeeds, expected in health_cases:
                with self.subTest(health_json=health_json, curl_succeeds=curl_succeeds):
                    result = run_shell(
                        f"""
                        set -u
                        ROOT_DIR=$1
                        DEV_TEST_HEALTH_JSON=$2
                        DEV_TEST_CURL_SUCCEEDS=$3
                        curl() {{
                            [ "$DEV_TEST_CURL_SUCCEEDS" = 1 ] || return 22
                            printf '%s' "$DEV_TEST_HEALTH_JSON"
                        }}
                        . {LIFECYCLE!s}
                        dev_backend_health_ready http://127.0.0.1:8082 false
                        """,
                        str(root),
                        health_json,
                        "1" if curl_succeeds else "0",
                    )
                    self.assertEqual(result.returncode == 0, expected, result.stderr)

    def test_unhealthy_recorded_backend_restarts_only_backend_and_sidecar(self) -> None:
        unhealthy_cases = (
            ('{"subsystems":{"python":{"status":"DOWN"}}}', True),
            ('{"subsystems":{"python":{"status":"DEGRADED"}}}', True),
            ('{"subsystems":{}}', True),
            ("not-json", True),
            ("", False),
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime = root / ".runtime"
            runtime.mkdir()
            (runtime / "backend.pid").write_text("123\n", encoding="utf-8")
            python_path = root / "python-service/.venv/bin/python"
            python_path.parent.mkdir(parents=True)
            python_path.symlink_to(sys.executable)
            for health_json, curl_succeeds in unhealthy_cases:
                with self.subTest(health_json=health_json, curl_succeeds=curl_succeeds):
                    marker = root / "stopped"
                    marker.unlink(missing_ok=True)
                    result = run_shell(
                        f"""
                        set -eu
                        ROOT_DIR=$1
                        DEV_RUNTIME_DIR="$ROOT_DIR/.runtime"
                        DEV_TEST_MARKER=$2
                        DEV_TEST_HEALTH_JSON=$3
                        DEV_TEST_CURL_SUCCEEDS=$4
                        . {COMMON!s}
                        . {LIFECYCLE!s}
                        dev_pid_is_live() {{ DEV_PID=123; return 0; }}
                        curl() {{
                            [ "$DEV_TEST_CURL_SUCCEEDS" = 1 ] || return 22
                            printf '%s' "$DEV_TEST_HEALTH_JSON"
                        }}
                        dev_stop_backend_for_recovery() {{
                            printf 'backend\npython-sidecar\n' >>"$DEV_TEST_MARKER"
                        }}
                        if dev_reuse_backend_if_ready http://127.0.0.1:8082 true; then
                            exit 9
                        fi
                        """,
                        str(root),
                        str(marker),
                        health_json,
                        "1" if curl_succeeds else "0",
                    )
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertEqual(
                        marker.read_text(encoding="utf-8").splitlines(),
                        ["backend", "python-sidecar"],
                    )
                    self.assertNotIn("frontend", marker.read_text(encoding="utf-8"))

    def test_healthy_recorded_backend_is_reused_without_stopping_services(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime = root / ".runtime"
            runtime.mkdir()
            (runtime / "backend.pid").write_text("123\n", encoding="utf-8")
            result = run_shell(
                f"""
                set -eu
                ROOT_DIR=$1
                DEV_RUNTIME_DIR="$ROOT_DIR/.runtime"
                . {COMMON!s}
                . {LIFECYCLE!s}
                dev_pid_is_live() {{ DEV_PID=123; return 0; }}
                dev_backend_health_ready() {{ return 0; }}
                dev_stop_services() {{ exit 9; }}
                dev_reuse_backend_if_ready http://127.0.0.1:8082 true
                """,
                str(root),
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("backend already healthy (PID 123)", result.stdout)

    def test_unhealthy_recorded_backend_continues_through_full_startup(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime = root / ".runtime"
            runtime.mkdir()
            (runtime / "backend.pid").write_text("123\n", encoding="utf-8")
            fake_python = install_fake_project_python(root)
            fake_launcher = root / "scripts/spawn-detached.py"
            fake_launcher.parent.mkdir(parents=True, exist_ok=True)
            fake_launcher.write_text("print(456)\n", encoding="utf-8")
            stopped_marker = root / "stopped"
            result = run_shell(
                f"""
                set -eu
                ROOT_DIR=$1
                DEV_RUNTIME_DIR="$ROOT_DIR/.runtime"
                DEV_TEST_STOPPED_MARKER=$2
                . {COMMON!s}
                . {LIFECYCLE!s}
                dev_backend_port() {{ printf '8082\n'; }}
                dev_env_get() {{ printf 'true\n'; }}
                dev_pid_is_live() {{ DEV_PID=123; return 0; }}
                dev_backend_health_ready() {{ return 1; }}
                dev_stop_backend_for_recovery() {{ printf 'backend\n' >>"$DEV_TEST_STOPPED_MARKER"; }}
                dev_port_owner() {{ return 1; }}
                dev_python_socket() {{ printf '%s\n' "$ROOT_DIR/.runtime/python.sock"; }}
                dev_wait_backend_health() {{ return 0; }}
                lsof() {{ printf '789\n'; }}
                ps() {{ printf '%s\n' "$ROOT_DIR/python-service/.venv/bin/python -m uvicorn src.main:app --uds $ROOT_DIR/.runtime/python.sock"; }}
                dev_start_backend
                """,
                str(root),
                str(stopped_marker),
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(stopped_marker.read_text(encoding="utf-8"), "backend\n")
            self.assertEqual((runtime / "backend.pid").read_text(encoding="utf-8"), "456\n")
            self.assertEqual((runtime / "python.pid").read_text(encoding="utf-8"), "789\n")

    def test_backend_recovery_failure_cannot_report_ready_or_start_frontend(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime = root / ".runtime"
            runtime.mkdir()
            (runtime / "backend.pid").write_text("123\n", encoding="utf-8")
            frontend_marker = root / "frontend-started"
            result = run_shell(
                f"""
                set -eu
                ROOT_DIR=$1
                DEV_RUNTIME_DIR="$ROOT_DIR/.runtime"
                . {COMMON!s}
                . {LIFECYCLE!s}
                dev_release_lock() {{ :; }}
                dev_backend_port() {{ printf '8082\n'; }}
                dev_env_get() {{ printf 'true\n'; }}
                dev_pid_is_live() {{ DEV_PID=123; return 0; }}
                dev_backend_health_ready() {{ return 1; }}
                dev_stop_backend_for_recovery() {{ return 1; }}
                DEV_TEST_FRONTEND_MARKER=$2
                dev_start_frontend() {{ : >"$DEV_TEST_FRONTEND_MARKER"; }}
                dev_start_services all
                """,
                str(root),
                str(frontend_marker),
            )
            self.assertEqual(result.returncode, 18)
            self.assertFalse(frontend_marker.exists())
            self.assertNotIn("zkcode is ready", result.stdout)

    def test_backend_readiness_uses_a_deadline_budget_for_curl(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            marker = root / "health-attempts"
            result = run_shell(
                f"""
                set -eu
                ROOT_DIR=$1
                DEV_TEST_MARKER=$2
                DEV_TEST_NOW=100
                . {LIFECYCLE!s}
                date() {{ printf '%s\n' "$DEV_TEST_NOW"; }}
                sleep() {{ DEV_TEST_NOW=$((DEV_TEST_NOW + $1)); }}
                dev_backend_health_ready() {{
                    printf '%s\n' "$3" >>"$DEV_TEST_MARKER"
                    DEV_TEST_NOW=$((DEV_TEST_NOW + $3))
                    return 1
                }}
                if dev_wait_backend_health http://127.0.0.1:8082 true 5; then
                    exit 9
                fi
                printf '%s\n' "$DEV_TEST_NOW"
                """,
                str(root),
                str(marker),
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(marker.read_text(encoding="utf-8").splitlines(), ["3", "1"])
            self.assertEqual(result.stdout.strip(), "105")

    def test_recovery_resolves_validated_uds_owner_over_stale_pid(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime = root / ".runtime"
            runtime.mkdir()
            (runtime / "backend.pid").write_text("123\n", encoding="utf-8")
            (runtime / "python.pid").write_text("999\n", encoding="utf-8")
            python_path = install_fake_project_python(root)
            marker = root / "stopped"
            result = run_shell(
                f"""
                set -eu
                ROOT_DIR=$1
                DEV_RUNTIME_DIR="$ROOT_DIR/.runtime"
                DEV_TEST_MARKER=$2
                . {COMMON!s}
                . {LIFECYCLE!s}
                dev_stop_one() {{ printf '%s\n' "$1" >>"$DEV_TEST_MARKER"; }}
                dev_python_socket() {{ printf '%s\n' "$ROOT_DIR/.runtime/python.sock"; }}
                lsof() {{ printf '456\n'; }}
                DEV_TEST_SIDECAR_LIVE=1
                kill() {{
                    case "$1" in
                        -0) [ "$DEV_TEST_SIDECAR_LIVE" -eq 1 ] ;;
                        -TERM)
                            printf '%s|%s\n' "$*" "$(sed -n '1p' "$ROOT_DIR/.runtime/python.pid")" >>"$DEV_TEST_MARKER"
                            DEV_TEST_SIDECAR_LIVE=0
                            ;;
                        *) exit 8 ;;
                    esac
                }}
                ps() {{
                    printf '%s\n' "$ROOT_DIR/python-service/.venv/bin/python -m uvicorn src.main:app --uds $ROOT_DIR/.runtime/python.sock"
                }}
                dev_stop_backend_for_recovery
                """,
                str(root),
                str(marker),
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(
                marker.read_text(encoding="utf-8").splitlines(),
                ["backend", "-TERM 456|456"],
            )
            self.assertFalse((runtime / "python.pid").exists())

    def test_recovery_never_signals_mismatched_uds_owner(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime = root / ".runtime"
            runtime.mkdir()
            (runtime / "backend.pid").write_text("123\n", encoding="utf-8")
            (runtime / "python.pid").write_text("999\n", encoding="utf-8")
            python_path = install_fake_project_python(root)
            marker = root / "signals"
            result = run_shell(
                f"""
                set -eu
                ROOT_DIR=$1
                DEV_RUNTIME_DIR="$ROOT_DIR/.runtime"
                DEV_TEST_MARKER=$2
                . {COMMON!s}
                . {LIFECYCLE!s}
                dev_stop_one() {{ :; }}
                dev_python_socket() {{ printf '%s\n' "$ROOT_DIR/.runtime/python.sock"; }}
                lsof() {{ printf '456\n'; }}
                kill() {{
                    [ "$1" = -0 ] && return 0
                    printf '%s\n' "$*" >>"$DEV_TEST_MARKER"
                }}
                ps() {{
                    printf '%s\n' "$ROOT_DIR/python-service/.venv/bin/python -m uvicorn src.main:app --uds $ROOT_DIR/.runtime/other.sock"
                }}
                if dev_stop_backend_for_recovery; then
                    exit 9
                fi
                test ! -e "$DEV_TEST_MARKER"
                """,
                str(root),
                str(marker),
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("does not match the configured Python sidecar identity", result.stderr)
            self.assertEqual((runtime / "python.pid").read_text(encoding="utf-8"), "999\n")

    def test_recovery_discards_stale_python_pid_without_signalling_it(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime = root / ".runtime"
            runtime.mkdir()
            (runtime / "backend.pid").write_text("123\n", encoding="utf-8")
            (runtime / "python.pid").write_text("999\n", encoding="utf-8")
            marker = root / "signals"
            result = run_shell(
                f"""
                set -eu
                ROOT_DIR=$1
                DEV_RUNTIME_DIR="$ROOT_DIR/.runtime"
                DEV_TEST_MARKER=$2
                . {COMMON!s}
                . {LIFECYCLE!s}
                dev_stop_one() {{ :; }}
                dev_python_socket() {{ printf '%s\n' "$ROOT_DIR/.runtime/python.sock"; }}
                lsof() {{ return 1; }}
                kill() {{ printf '%s\n' "$*" >>"$DEV_TEST_MARKER"; }}
                dev_stop_backend_for_recovery
                test ! -e "$ROOT_DIR/.runtime/python.pid"
                test ! -e "$DEV_TEST_MARKER"
                """,
                str(root),
                str(marker),
            )
            self.assertEqual(result.returncode, 0, result.stderr)

    def test_normal_sidecar_stop_uses_token_aware_identity_check(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            python = install_fake_project_python(root)
            pid_file = root / ".runtime/python.pid"
            pid_file.parent.mkdir(parents=True)
            pid_file.write_text("456\n", encoding="utf-8")
            marker = root / "signals"
            result = run_shell(
                f"""
                set -eu
                ROOT_DIR=$1
                DEV_TEST_MARKER=$2
                DEV_TEST_PYTHON=$3
                DEV_TEST_LIVE=1
                . {COMMON!s}
                . {LIFECYCLE!s}
                dev_python_socket() {{ printf '%s\n' "$ROOT_DIR/.runtime/python.sock"; }}
                kill() {{
                    case "$1" in
                        -0) [ "$DEV_TEST_LIVE" -eq 1 ] ;;
                        -TERM)
                            printf '%s\n' "$*" >>"$DEV_TEST_MARKER"
                            DEV_TEST_LIVE=0
                            ;;
                        *) exit 8 ;;
                    esac
                }}
                ps() {{
                    printf '%s\n' "$DEV_TEST_PYTHON -m uvicorn src.main:app --uds $ROOT_DIR/.runtime/python.sock"
                }}
                dev_stop_one python-sidecar "$ROOT_DIR/.runtime/python.pid" "$DEV_TEST_PYTHON" src.main:app --uds
                test ! -e "$ROOT_DIR/.runtime/python.pid"
                """,
                str(root),
                str(marker),
                str(python),
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(marker.read_text(encoding="utf-8"), "-TERM 456\n")

    def test_normal_sidecar_stop_rejects_a_different_socket(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            python = install_fake_project_python(root)
            pid_file = root / ".runtime/python.pid"
            pid_file.parent.mkdir(parents=True)
            pid_file.write_text("456\n", encoding="utf-8")
            marker = root / "signals"
            result = run_shell(
                f"""
                set -eu
                ROOT_DIR=$1
                DEV_TEST_MARKER=$2
                DEV_TEST_PYTHON=$3
                . {COMMON!s}
                . {LIFECYCLE!s}
                dev_python_socket() {{ printf '%s\n' "$ROOT_DIR/.runtime/python.sock"; }}
                kill() {{
                    [ "$1" = -0 ] && return 0
                    printf '%s\n' "$*" >>"$DEV_TEST_MARKER"
                }}
                ps() {{
                    printf '%s\n' "$DEV_TEST_PYTHON -m uvicorn src.main:app --uds $ROOT_DIR/.runtime/other.sock"
                }}
                if dev_stop_one python-sidecar "$ROOT_DIR/.runtime/python.pid" "$DEV_TEST_PYTHON" src.main:app --uds; then
                    exit 9
                fi
                test ! -e "$DEV_TEST_MARKER"
                test -e "$ROOT_DIR/.runtime/python.pid"
                """,
                str(root),
                str(marker),
                str(python),
            )
            self.assertEqual(result.returncode, 0, result.stderr)

    def test_sidecar_stop_rejects_an_unrelated_venv_python_process(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            link_python_process_identity(root)
            pid_file = root / ".runtime/python.pid"
            pid_file.parent.mkdir(parents=True)
            process = subprocess.Popen(
                [sys.executable, "-c", "import time; time.sleep(30)"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            try:
                pid_file.write_text(f"{process.pid}\n", encoding="utf-8")
                result = run_shell(
                    f"""
                    set -eu
                    ROOT_DIR=$1
                    . {COMMON!s}
                    . {LIFECYCLE!s}
                    dev_python_socket() {{ printf '%s\n' "$ROOT_DIR/.runtime/python.sock"; }}
                    if dev_stop_one python-sidecar "$ROOT_DIR/.runtime/python.pid" "$2" src.main:app --uds; then
                        exit 9
                    fi
                    kill -0 "$3"
                    """,
                    str(root),
                    sys.executable,
                    str(process.pid),
                )
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIsNone(process.poll())
            finally:
                process.terminate()
                process.wait(timeout=5)

    def test_recovery_journal_rejects_parent_traversal(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            target = root / "frontend/node_modules"
            target.mkdir(parents=True)
            outside = root / "outside"
            outside.mkdir()
            journal = root / ".runtime/dev/frontend.journal"
            journal.parent.mkdir(parents=True)
            journal.write_text(
                str(root / ".runtime/dev/previous/frontend-../../outside") + "\n",
                encoding="utf-8",
            )
            result = run_shell(
                f"""
                set -eu
                ROOT_DIR=$1
                . {COMMON!s}
                . {SYNC!s}
                dev_recover_component frontend "$ROOT_DIR/frontend/node_modules"
                """,
                str(root),
            )
            self.assertEqual(result.returncode, 13)
            self.assertTrue(target.is_dir())
            self.assertTrue(outside.is_dir())
            self.assertIn("invalid recovery journal", result.stderr)

    def test_operation_lock_symlink_does_not_touch_external_files(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            external = root / "external"
            external.mkdir()
            external_pid = external / "pid"
            external_pid.write_text("do-not-delete\n", encoding="utf-8")
            state_dir = root / ".runtime/dev"
            state_dir.mkdir(parents=True)
            (state_dir / "operation.lock").symlink_to(external, target_is_directory=True)
            result = run_shell(
                f"""
                set -eu
                ROOT_DIR=$1
                . {COMMON!s}
                dev_acquire_lock
                """,
                str(root),
            )
            self.assertEqual(result.returncode, 13)
            self.assertEqual(external_pid.read_text(encoding="utf-8"), "do-not-delete\n")
            self.assertIn("symbolic link", result.stderr)

    def test_failed_start_rolls_back_only_new_processes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            marker = root / "stopped"
            result = run_shell(
                f"""
                set -eu
                ROOT_DIR=$1
                . {COMMON!s}
                . {LIFECYCLE!s}
                dev_release_lock() {{ :; }}
                dev_start_backend() {{ DEV_START_STARTED_BACKEND=1; }}
                dev_start_frontend() {{ dev_fail 19 'frontend failed'; }}
                DEV_TEST_MARKER=$2
                dev_stop_one() {{ printf '%s\n' "$1" >>"$DEV_TEST_MARKER"; }}
                dev_start_services all
                """,
                str(root),
                str(marker),
            )
            self.assertEqual(result.returncode, 19)
            self.assertEqual(marker.read_text(encoding="utf-8").splitlines(), ["backend", "python-sidecar"])

    def test_targeted_restart_restores_a_non_target_stopped_by_sync(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            result = run_shell(
                f"""
                set -eu
                ROOT_DIR=$1
                DEV_RUNTIME_DIR="$ROOT_DIR/.runtime"
                . {LIFECYCLE!s}
                dev_pid_is_live() {{ return 1; }}

                DEV_RESTART_FRONTEND_WAS_LIVE=1
                DEV_RESTART_BACKEND_WAS_LIVE=0
                dev_select_restart_start_target backend
                printf '%s\n' "$DEV_RESTART_START_TARGET"

                DEV_RESTART_FRONTEND_WAS_LIVE=0
                DEV_RESTART_BACKEND_WAS_LIVE=1
                dev_select_restart_start_target frontend
                printf '%s\n' "$DEV_RESTART_START_TARGET"
                """,
                str(root),
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stdout.splitlines(), ["all", "all"])

    @unittest.skipUnless(hasattr(socket, "AF_UNIX"), "Unix sockets are required")
    def test_live_external_socket_is_preserved(self) -> None:
        with tempfile.TemporaryDirectory(prefix="zkcode-uds-") as directory:
            root = Path(directory)
            socket_path = root / "external.sock"
            listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            listener.bind(str(socket_path))
            listener.listen(1)
            try:
                result = run_shell(
                    f"""
                    set -eu
                    ROOT_DIR=$1
                    . {COMMON!s}
                    . {LIFECYCLE!s}
                    dev_pid_is_live() {{ return 1; }}
                    dev_backend_port() {{ printf '65432\n'; }}
                    dev_port_owner() {{ return 1; }}
                    DEV_TEST_SOCKET=$2
                    dev_python_socket() {{ printf '%s\n' "$DEV_TEST_SOCKET"; }}
                    dev_start_backend
                    """,
                    str(root),
                    str(socket_path),
                )
                self.assertEqual(result.returncode, 18, result.stderr)
                self.assertTrue(socket_path.exists())
                self.assertIn("live listener", result.stderr)
            finally:
                listener.close()


if __name__ == "__main__":
    unittest.main()
