from __future__ import annotations

import os
import subprocess
import tempfile
import time
import unittest
from pathlib import Path


REPOSITORY = Path(__file__).resolve().parents[3]
COMMON = REPOSITORY / "scripts/dev/common.sh"
TOOLCHAINS = REPOSITORY / "scripts/dev/toolchains-macos.sh"
TEST_TEMP_ROOT = REPOSITORY / ".runtime/test-tmp"


def run_shell(
    script: str,
    *arguments: str,
    environment: dict[str, str] | None = None,
    start_new_session: bool = False,
) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    if environment:
        env.update(environment)
    return subprocess.run(
        ["/bin/sh", "-c", script, "zkcode-homebrew-test", *arguments],
        check=False,
        capture_output=True,
        text=True,
        timeout=15,
        env=env,
        start_new_session=start_new_session,
    )


class HomebrewBootstrapTests(unittest.TestCase):
    def setUp(self) -> None:
        TEST_TEMP_ROOT.mkdir(parents=True, exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(dir=TEST_TEMP_ROOT)
        self.root = Path(self.temporary.name)
        self.log = self.root / "calls.log"
        self.ticket = self.root / "sudo-ticket"
        self.revoked = self.root / "sudo-revoked"
        self.brew_ready = self.root / "brew-ready"
        self.tmpdir = self.root / "tmp"
        self.tmpdir.mkdir()
        self.installer = self.root / "fake-homebrew-installer.sh"
        self.installer.write_text(
            """#!/bin/bash
set -u
printf 'installer:NONINTERACTIVE=%s:SUDO_ASKPASS=%s\\n' \
    "${NONINTERACTIVE:-}" "${SUDO_ASKPASS:-}" >>"$FAKE_CALL_LOG"
printf 'installer-env:INTERACTIVE=%s:POSIXLY_CORRECT=%s\\n' \
    "${INTERACTIVE:-}" "${POSIXLY_CORRECT:-}" >>"$FAKE_CALL_LOG"
if [ "${FAKE_SCENARIO:-}" = ticket-expiring ]; then
    sleep 2
    refresh_count=$(grep -c 'sudo:-n -v' "$FAKE_CALL_LOG" || true)
    [ "$refresh_count" -ge 3 ] || exit 75
fi
touch "$FAKE_BREW_READY"
""",
            encoding="utf-8",
        )
        self.askpass = self.root / "askpass.sh"
        self.askpass.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        self.askpass.chmod(0o700)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def environment(self, scenario: str, *, yes: bool = True) -> dict[str, str]:
        environment = {
            "DEV_YES": "1" if yes else "0",
            "FAKE_SCENARIO": scenario,
            "FAKE_CALL_LOG": str(self.log),
            "FAKE_SUDO_TICKET": str(self.ticket),
            "FAKE_SUDO_REVOKED": str(self.revoked),
            "FAKE_BREW_READY": str(self.brew_ready),
            "FAKE_INSTALLER_SOURCE": str(self.installer),
            "TMPDIR": str(self.tmpdir),
        }
        if scenario in {"slow-download", "ticket-expiring"}:
            environment["DEV_HOMEBREW_KEEPALIVE_SECONDS"] = "1"
        return environment

    @staticmethod
    def mocks() -> str:
        return r"""
        dev_find_brew() {
            if [ -f "$FAKE_BREW_READY" ]; then
                printf '%s\n' /opt/homebrew/bin/brew
                return 0
            fi
            return 1
        }

        dev_sudo_available() {
            [ "$FAKE_SCENARIO" != no-sudo ]
        }

        dev_homebrew_has_tty() {
            [ "$FAKE_SCENARIO" != no-tty ]
        }

        dev_homebrew_sudo_from_tty() {
            dev_sudo -v
        }

        dev_sudo() {
            printf 'sudo:%s\n' "$*" >>"$FAKE_CALL_LOG"
            case "$*" in
                '-n -v')
                    [ -f "$FAKE_SUDO_TICKET" ]
                    ;;
                '-n -l mkdir')
                    if [ "$FAKE_SCENARIO" = nopasswd ] || [ -f "$FAKE_SUDO_TICKET" ]; then
                        [ "$FAKE_SCENARIO" != permission-denied ]
                    else
                        return 1
                    fi
                    ;;
                '-v')
                    case "$FAKE_SCENARIO" in
                        interactive|curl-fail|installer-fail|installer-timeout|permission-denied|sudo-timeout-after-ticket)
                            touch "$FAKE_SUDO_TICKET"
                            ;;
                        *) return 1 ;;
                    esac
                    ;;
                '-A -v')
                    [ "$FAKE_SCENARIO" = askpass ] || return 1
                    ;;
                '-A -l mkdir')
                    [ "$FAKE_SCENARIO" = askpass ]
                    ;;
                '-k')
                    rm -f "$FAKE_SUDO_TICKET"
                    touch "$FAKE_SUDO_REVOKED"
                    ;;
                *) return 99 ;;
            esac
        }

        dev_run_bounded() {
            DEV_TEST_BOUND_SECONDS=$1
            DEV_TEST_BOUND_LABEL=$2
            shift 2
            printf 'bounded:%s:%s\n' "$DEV_TEST_BOUND_SECONDS" "$DEV_TEST_BOUND_LABEL" >>"$FAKE_CALL_LOG"
            if [ "$FAKE_SCENARIO" = sudo-timeout ] && [ "$DEV_TEST_BOUND_LABEL" = 'sudo authorization' ]; then
                return 124
            fi
            if [ "$FAKE_SCENARIO" = sudo-timeout-after-ticket ] && [ "$DEV_TEST_BOUND_LABEL" = 'sudo authorization' ]; then
                "$@" || return $?
                return 124
            fi
            if [ "$FAKE_SCENARIO" = installer-fail ] && [ "$DEV_TEST_BOUND_LABEL" = 'Homebrew installation' ]; then
                return 42
            fi
            if [ "$FAKE_SCENARIO" = installer-timeout ] && [ "$DEV_TEST_BOUND_LABEL" = 'Homebrew installation' ]; then
                return 124
            fi
            "$@"
        }

        curl() {
            printf 'curl:start\n' >>"$FAKE_CALL_LOG"
            [ "$FAKE_SCENARIO" != curl-fail ] || return 22
            if [ "$FAKE_SCENARIO" = slow-download ]; then
                sleep 2
            fi
            DEV_TEST_OUTPUT=
            while [ "$#" -gt 0 ]; do
                if [ "$1" = --output ]; then
                    shift
                    DEV_TEST_OUTPUT=$1
                    break
                fi
                shift
            done
            [ -n "$DEV_TEST_OUTPUT" ] || return 2
            cp "$FAKE_INSTALLER_SOURCE" "$DEV_TEST_OUTPUT"
            printf 'curl:end\n' >>"$FAKE_CALL_LOG"
        }
        """

    def invoke_install(
        self,
        scenario: str,
        *,
        yes: bool = True,
        askpass: Path | None = None,
        existing_ticket: bool = False,
        existing_brew: bool = False,
        environment_overrides: dict[str, str] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        if existing_ticket:
            self.ticket.touch()
        if existing_brew:
            self.brew_ready.touch()
        env = self.environment(scenario, yes=yes)
        if askpass is not None:
            env["SUDO_ASKPASS"] = str(askpass)
        else:
            env.pop("SUDO_ASKPASS", None)
        if environment_overrides:
            env.update(environment_overrides)
        return run_shell(
            f"""
            set -u
            ROOT_DIR=$1
            . {COMMON!s}
            . {TOOLCHAINS!s}
            {self.mocks()}
            dev_install_homebrew
            """,
            str(REPOSITORY),
            environment=env,
        )

    def calls(self) -> str:
        return self.log.read_text(encoding="utf-8") if self.log.exists() else ""

    def assert_no_install_temp(self) -> None:
        self.assertEqual(list(self.tmpdir.glob("zkcode-dev-tools.*")), [])

    def test_existing_native_homebrew_never_checks_sudo_or_downloads(self) -> None:
        result = self.invoke_install("no-sudo", existing_brew=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.calls(), "")

    def test_cached_sudo_is_preserved_and_installer_is_noninteractive(self) -> None:
        result = self.invoke_install("cached", existing_ticket=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        calls = self.calls()
        self.assertIn("sudo:-n -v", calls)
        self.assertIn("sudo:-n -l mkdir", calls)
        self.assertNotIn("sudo:-k", calls)
        self.assertIn("bounded:1200:Homebrew installation", calls)
        self.assertIn("installer:NONINTERACTIVE=1:SUDO_ASKPASS=", calls)
        self.assertTrue(self.ticket.exists())
        self.assert_no_install_temp()

    def test_cached_sudo_is_preserved_when_installation_fails(self) -> None:
        result = self.invoke_install("installer-fail", existing_ticket=True)
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertNotIn("sudo:-k", self.calls())
        self.assertTrue(self.ticket.exists())
        self.assert_no_install_temp()

    def test_cached_sudo_without_mkdir_permission_is_not_revoked(self) -> None:
        result = self.invoke_install("permission-denied", existing_ticket=True)
        self.assertEqual(result.returncode, 13, result.stderr)
        calls = self.calls()
        self.assertNotIn("bounded:300:sudo authorization", calls)
        self.assertNotIn("sudo:-k", calls)
        self.assertTrue(self.ticket.exists())
        self.assertNotIn("curl", calls)

    def test_yes_accepts_nopasswd_without_creating_a_ticket(self) -> None:
        result = self.invoke_install("nopasswd")
        self.assertEqual(result.returncode, 0, result.stderr)
        calls = self.calls()
        self.assertIn("sudo:-n -l mkdir", calls)
        self.assertNotIn("bounded:300:sudo authorization", calls)
        self.assertNotIn("sudo:-k", calls)
        self.assert_no_install_temp()

    def test_interactive_mode_authorizes_on_tty_then_revokes_new_ticket(self) -> None:
        result = self.invoke_install("interactive", yes=False)
        self.assertEqual(result.returncode, 0, result.stderr)
        calls = self.calls()
        self.assertIn("bounded:300:sudo authorization", calls)
        self.assertIn("sudo:-v", calls)
        self.assertIn("sudo:-n -l mkdir", calls)
        self.assertIn("sudo:-k", calls)
        self.assertFalse(self.ticket.exists())
        self.assertTrue(self.revoked.exists())
        self.assert_no_install_temp()

    def test_yes_keeps_trusted_askpass_for_official_installer(self) -> None:
        result = self.invoke_install("askpass", askpass=self.askpass)
        self.assertEqual(result.returncode, 0, result.stderr)
        calls = self.calls()
        self.assertIn("bounded:300:sudo authorization", calls)
        self.assertIn("sudo:-A -v", calls)
        self.assertIn("sudo:-A -l mkdir", calls)
        self.assertIn(
            f"installer:NONINTERACTIVE=1:SUDO_ASKPASS={self.askpass}", calls
        )
        self.assertIn("sudo:-k", calls)
        self.assert_no_install_temp()

    def test_askpass_under_a_world_writable_parent_is_rejected(self) -> None:
        unsafe_parent = self.root / "unsafe"
        unsafe_parent.mkdir()
        unsafe_parent.chmod(0o777)
        unsafe_askpass = unsafe_parent / "askpass.sh"
        unsafe_askpass.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        unsafe_askpass.chmod(0o700)
        result = self.invoke_install("askpass", askpass=unsafe_askpass)
        self.assertEqual(result.returncode, 13, result.stderr)
        self.assertNotIn("curl", self.calls())
        self.assertNotIn("sudo:-A -v", self.calls())
        self.assert_no_install_temp()

    def test_untrusted_askpass_is_rejected_without_downloading(self) -> None:
        self.askpass.chmod(0o777)
        result = self.invoke_install("askpass", askpass=self.askpass)
        self.assertEqual(result.returncode, 13, result.stderr)
        self.assertNotIn("curl", self.calls())
        self.assertIn("trusted SUDO_ASKPASS", result.stderr)
        self.assert_no_install_temp()

    def test_untrusted_askpass_is_stripped_when_cached_sudo_is_available(self) -> None:
        self.askpass.chmod(0o777)
        result = self.invoke_install("cached", askpass=self.askpass, existing_ticket=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("installer:NONINTERACTIVE=1:SUDO_ASKPASS=", self.calls())
        self.assertNotIn(f"SUDO_ASKPASS={self.askpass}", self.calls())

    def test_yes_without_noninteractive_authorization_returns_13(self) -> None:
        result = self.invoke_install("denied")
        self.assertEqual(result.returncode, 13, result.stderr)
        self.assertNotIn("curl", self.calls())
        self.assert_no_install_temp()

    def test_missing_sudo_returns_13_without_downloading(self) -> None:
        result = self.invoke_install("no-sudo")
        self.assertEqual(result.returncode, 13, result.stderr)
        self.assertNotIn("curl", self.calls())
        self.assert_no_install_temp()

    def test_interactive_mode_requires_a_controlling_terminal(self) -> None:
        result = self.invoke_install("no-tty", yes=False)
        self.assertEqual(result.returncode, 13, result.stderr)
        self.assertNotIn("sudo:-v\n", self.calls())
        self.assertNotIn("curl", self.calls())
        self.assert_no_install_temp()

    def test_tty_probe_opens_the_controlling_terminal(self) -> None:
        result = run_shell(
            f"""
            set -u
            ROOT_DIR=$1
            . {COMMON!s}
            . {TOOLCHAINS!s}
            dev_homebrew_has_tty
            """,
            str(REPOSITORY),
            start_new_session=True,
        )
        self.assertEqual(result.returncode, 1, result.stderr)

    def test_confirmation_uses_the_controlling_terminal_probe(self) -> None:
        result = run_shell(
            f"""
            set -u
            ROOT_DIR=$1
            . {COMMON!s}
            . {TOOLCHAINS!s}
            dev_homebrew_has_tty() {{ return 1; }}
            DEV_YES=0
            dev_confirm_external_install
            """,
            str(REPOSITORY),
        )
        self.assertEqual(result.returncode, 13, result.stderr)
        self.assertIn("interactive terminal", result.stderr)

    def test_cancelled_interactive_authorization_returns_13(self) -> None:
        result = self.invoke_install("interactive-denied", yes=False)
        self.assertEqual(result.returncode, 13, result.stderr)
        self.assertIn("bounded:300:sudo authorization", self.calls())
        self.assertNotIn("curl", self.calls())
        self.assert_no_install_temp()

    def test_timed_out_authorization_returns_13(self) -> None:
        result = self.invoke_install("sudo-timeout", yes=False)
        self.assertEqual(result.returncode, 13, result.stderr)
        self.assertIn("bounded:300:sudo authorization", self.calls())
        self.assertNotIn("curl", self.calls())
        self.assert_no_install_temp()

    def test_ticket_created_just_before_authorization_timeout_is_revoked(self) -> None:
        result = self.invoke_install("sudo-timeout-after-ticket", yes=False)
        self.assertEqual(result.returncode, 13, result.stderr)
        self.assertIn("sudo:-v", self.calls())
        self.assertIn("sudo:-k", self.calls())
        self.assertFalse(self.ticket.exists())
        self.assertTrue(self.revoked.exists())
        self.assertNotIn("curl", self.calls())
        self.assert_no_install_temp()

    def test_permission_check_failure_revokes_new_ticket(self) -> None:
        result = self.invoke_install("permission-denied", yes=False)
        self.assertEqual(result.returncode, 13, result.stderr)
        calls = self.calls()
        self.assertGreaterEqual(calls.count("sudo:-n -l mkdir"), 2)
        self.assertIn("sudo:-k", calls)
        self.assertNotIn("curl", calls)
        self.assert_no_install_temp()

    def test_download_failure_cleans_temp_and_revokes_new_ticket(self) -> None:
        result = self.invoke_install(
            "curl-fail",
            yes=False,
            environment_overrides={"DEV_HOMEBREW_KEEPALIVE_SECONDS": "1"},
        )
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("sudo:-k", self.calls())
        self.assert_no_install_temp()
        refreshes_after_failure = self.calls().count("sudo:-n -v")
        time.sleep(1.2)
        self.assertEqual(self.calls().count("sudo:-n -v"), refreshes_after_failure)

    def test_keepalive_covers_a_slow_installer_download(self) -> None:
        result = self.invoke_install("slow-download", existing_ticket=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        calls = self.calls().splitlines()
        download_start = calls.index("curl:start")
        download_end = calls.index("curl:end")
        self.assertIn("sudo:-n -v", calls[download_start + 1 : download_end])
        self.assertGreater(calls.index("bounded:1200:Homebrew installation"), download_end)
        self.assertNotIn("sudo:-k", calls)
        self.assert_no_install_temp()

    def test_installer_failure_and_timeout_are_normalized_and_cleaned(self) -> None:
        for scenario in ("installer-fail", "installer-timeout"):
            with self.subTest(scenario=scenario):
                self.log.unlink(missing_ok=True)
                self.revoked.unlink(missing_ok=True)
                result = self.invoke_install(scenario, yes=False)
                self.assertEqual(result.returncode, 1, result.stderr)
                self.assertIn("bounded:1200:Homebrew installation", self.calls())
                self.assertIn("sudo:-k", self.calls())
                self.assert_no_install_temp()

    def test_installer_keeps_a_cached_ticket_alive_and_stops_refreshing(self) -> None:
        result = self.invoke_install("ticket-expiring", existing_ticket=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        refreshes_after_install = self.calls().count("sudo:-n -v")
        self.assertGreaterEqual(refreshes_after_install, 3)
        self.assertNotIn("sudo:-k", self.calls())
        time.sleep(1.2)
        self.assertEqual(self.calls().count("sudo:-n -v"), refreshes_after_install)
        self.assertTrue(self.ticket.exists())
        self.assert_no_install_temp()

    def test_installer_unsets_conflicting_noninteractive_environment(self) -> None:
        result = self.invoke_install(
            "cached",
            askpass=self.askpass,
            existing_ticket=True,
            environment_overrides={"INTERACTIVE": "1", "POSIXLY_CORRECT": "1"},
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        calls = self.calls()
        self.assertIn("installer:NONINTERACTIVE=1:SUDO_ASKPASS=", calls)
        self.assertIn("installer-env:INTERACTIVE=:POSIXLY_CORRECT=", calls)
        self.assertNotIn(f"SUDO_ASKPASS={self.askpass}", calls)

    def test_caller_preserves_auth_13_and_maps_install_failure_to_11(self) -> None:
        script = f"""
            set -u
            ROOT_DIR=$1
            . {COMMON!s}
            . {TOOLCHAINS!s}
            {self.mocks()}
            dev_confirm_external_install() {{ :; }}
            dev_wait_for_clt() {{ :; }}
            dev_node_is_supported() {{ return 1; }}
            dev_python_is_supported() {{ return 1; }}
            dev_rust_is_supported() {{ return 0; }}
            dev_install_missing_toolchains
        """
        auth_result = run_shell(
            script,
            str(REPOSITORY),
            environment=self.environment("denied"),
        )
        self.assertEqual(auth_result.returncode, 13, auth_result.stderr)

        install_result = run_shell(
            script,
            str(REPOSITORY),
            environment=self.environment("curl-fail", yes=False),
        )
        self.assertEqual(install_result.returncode, 11, install_result.stderr)


if __name__ == "__main__":
    unittest.main()
