#!/usr/bin/env python3
"""Parse zkcode's .env as data and optionally exec a child process.

This parser deliberately implements a small grammar. It never invokes a shell,
performs interpolation, or evaluates command substitutions.
"""

from __future__ import annotations

import argparse
import ast
import os
import re
import sys
from pathlib import Path


KEY_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
ALLOWED_PREFIXES = (
    "ZK_",
    "LLM_PROVIDER_",
    "FEATURE_",
)
ALLOWED_EXACT = {
    "ALL_PROXY",
    "BROWSER_CHANNEL",
    "BROWSER_HEADLESS",
    "BROWSER_TYPE",
    "DASHSCOPE_API_KEY",
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "MCP_REGISTRY_PATH",
    "NO_PROXY",
    "PLAYWRIGHT_BROWSERS_PATH",
    "PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD",
    "RUST_LOG",
    "SELF_CORRECTION_LOOP",
    "ZHIKUN_COORDINATOR_MODE",
}


class EnvSyntaxError(ValueError):
    """A safe, value-free syntax diagnostic."""


def is_allowed_key(key: str) -> bool:
    return key in ALLOWED_EXACT or key.startswith(ALLOWED_PREFIXES)


def parse_value(raw: str, line_number: int, key: str) -> str:
    value = raw.strip()
    if not value:
        return ""
    if value[0] in {"'", '"'}:
        try:
            parsed = ast.literal_eval(value)
        except (SyntaxError, ValueError) as error:
            raise EnvSyntaxError(
                f"line {line_number}: invalid quoted value for {key}"
            ) from error
        if not isinstance(parsed, str):
            raise EnvSyntaxError(f"line {line_number}: {key} must be a string")
        return parsed
    return value


def parse_env(path: Path) -> tuple[dict[str, str], list[str]]:
    values: dict[str, str] = {}
    warnings: list[str] = []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as error:
        raise EnvSyntaxError(f"cannot read {path}: {error.strerror}") from error

    for line_number, raw_line in enumerate(lines, 1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            raise EnvSyntaxError(
                f"line {line_number}: shell 'export' syntax is not supported"
            )
        if "=" not in line:
            raise EnvSyntaxError(f"line {line_number}: expected KEY=VALUE")
        raw_key, raw_value = line.split("=", 1)
        key = raw_key.strip()
        if not KEY_RE.fullmatch(key):
            raise EnvSyntaxError(f"line {line_number}: invalid variable name")
        if not is_allowed_key(key):
            warnings.append(f"line {line_number}: ignoring unsupported key {key}")
            continue
        values[key] = parse_value(raw_value, line_number, key)
    return values, warnings


def canonicalize_zero_one(values: dict[str, str], specifications: list[str]) -> None:
    """Validate exact 0/1 values and install an explicit default when absent."""
    for specification in specifications:
        if "=" not in specification:
            raise EnvSyntaxError(
                "--canonical-zero-one requires KEY=DEFAULT"
            )
        key, default = specification.split("=", 1)
        if not KEY_RE.fullmatch(key) or not is_allowed_key(key):
            raise EnvSyntaxError(
                f"unsupported --canonical-zero-one key {key!r}"
            )
        if default not in {"0", "1"}:
            raise EnvSyntaxError(
                f"--canonical-zero-one default for {key} must be exactly 0 or 1"
            )
        value = values.get(key, default)
        if value not in {"0", "1"}:
            raise EnvSyntaxError(f"{key} must be exactly 0 or 1")
        values[key] = value


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", required=True, type=Path)
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--get")
    parser.add_argument("--default", default="")
    parser.add_argument(
        "--canonical-zero-one",
        action="append",
        default=[],
        metavar="KEY=DEFAULT",
        help="require an exact 0/1 value and set DEFAULT when KEY is absent",
    )
    parser.add_argument(
        "--set",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="trusted launcher override applied after the file",
    )
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()

    try:
        values, warnings = parse_env(args.file)
    except EnvSyntaxError as error:
        print(f"error: {error}", file=sys.stderr)
        return 2

    for warning in warnings:
        print(f"warning: {warning}", file=sys.stderr)

    for override in args.set:
        if "=" not in override:
            print("error: --set requires KEY=VALUE", file=sys.stderr)
            return 2
        key, value = override.split("=", 1)
        if not KEY_RE.fullmatch(key) or not is_allowed_key(key):
            print(f"error: unsupported --set key {key!r}", file=sys.stderr)
            return 2
        values[key] = value

    try:
        canonicalize_zero_one(values, args.canonical_zero_one)
    except EnvSyntaxError as error:
        print(f"error: {error}", file=sys.stderr)
        return 2

    if args.get is not None:
        print(values.get(args.get, args.default))
        return 0
    if args.check and not args.command:
        print(f"env-check: ok ({len(values)} supported keys)")
        return 0
    if not args.command:
        parser.error("provide --check, --get, or a command after --")
    command = args.command
    if command[0] == "--":
        command = command[1:]
    if not command:
        parser.error("the command after -- is empty")

    child_env = os.environ.copy()
    child_env.update(values)
    os.execvpe(command[0], command, child_env)
    return 127


if __name__ == "__main__":
    raise SystemExit(main())
