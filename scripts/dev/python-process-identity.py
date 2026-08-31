#!/usr/bin/env python3
"""Fail-closed identity checks for the managed Python sidecar process."""

from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Sequence


def path_forms(path: str) -> set[str]:
    if not path:
        return set()
    return {path, os.path.realpath(path)}


def runtime_python_forms(
    expected_python: str,
    *,
    executable: str | None = None,
    base_executable: str | None = None,
    original_argv: Sequence[str] | None = None,
) -> set[str]:
    """Return identities exposed by the expected interpreter at runtime."""
    running_executable = sys.executable if executable is None else executable
    running_original_argv = (
        getattr(sys, "orig_argv", ()) if original_argv is None else original_argv
    )
    running_base_executable = (
        getattr(sys, "_base_executable", "")
        if base_executable is None
        else base_executable
    )
    expected_forms = path_forms(expected_python)
    running_forms = path_forms(running_executable)
    if not expected_forms & running_forms:
        raise RuntimeError("identity helper is not running under the expected Python")
    forms = expected_forms | running_forms
    forms.update(path_forms(running_base_executable))
    if running_original_argv and running_original_argv[0]:
        forms.update(path_forms(running_original_argv[0]))
    return forms


def command_matches_sidecar(
    command: str,
    expected_python: str,
    expected_socket: str,
    *,
    executable: str | None = None,
    base_executable: str | None = None,
    original_argv: Sequence[str] | None = None,
) -> bool:
    expected_forms = runtime_python_forms(
        expected_python,
        executable=executable,
        base_executable=base_executable,
        original_argv=original_argv,
    )
    command = command.rstrip("\n")
    for python in expected_forms:
        prefix = f"{python} -m uvicorn src.main:app --uds "
        if command == prefix + expected_socket:
            return True
    return False


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("match-sidecar",))
    parser.add_argument("--expected-python", required=True)
    parser.add_argument("--socket", required=True)
    args = parser.parse_args()
    try:
        matches = command_matches_sidecar(
            sys.stdin.read(),
            args.expected_python,
            args.socket,
        )
    except RuntimeError:
        return 2
    return 0 if matches else 1


if __name__ == "__main__":
    raise SystemExit(main())
