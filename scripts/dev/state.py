#!/usr/bin/env python3
"""Content fingerprints and atomic state storage for ./dev."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable


SCHEMA_VERSION = 1


def tracked_files(root: Path, component: str) -> Iterable[Path]:
    if component == "frontend":
        yield root / "configuration/dev-toolchain.toml"
        yield root / "frontend/package.json"
        yield root / "frontend/package-lock.json"
    elif component == "python":
        yield root / "configuration/dev-toolchain.toml"
        yield root / "python-service/pyproject.toml"
        yield root / "python-service/requirements.lock"
        yield root / "python-service/build-requirements.lock"
    elif component == "browser":
        yield root / "configuration/dev-toolchain.toml"
        yield root / "python-service/requirements.lock"
    elif component == "rust":
        yield root / "configuration/dev-toolchain.toml"
        yield root / "rust-toolchain.toml"
        yield root / "Cargo.lock"
        # The workspace layout is intentionally shallow. Avoid rglob here:
        # walking target/node_modules just to discard their manifests made a
        # no-op sync spend seconds scanning generated dependency trees.
        yield root / "Cargo.toml"
        yield from sorted((root / "crates").glob("*/Cargo.toml"))
    else:
        raise ValueError(f"unknown component: {component}")


def fingerprint(root: Path, component: str, versions: list[str]) -> str:
    digest = hashlib.sha256()
    for path in tracked_files(root, component):
        relative = path.relative_to(root).as_posix()
        digest.update(relative.encode())
        digest.update(b"\0")
        try:
            digest.update(path.read_bytes())
        except OSError as error:
            raise RuntimeError(f"cannot fingerprint {relative}: {error}") from error
        digest.update(b"\0")
    for version in versions:
        digest.update(version.encode())
        digest.update(b"\0")
    digest.update(platform.system().encode())
    digest.update(platform.machine().encode())
    return digest.hexdigest()


def load_state(path: Path) -> dict:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    except (OSError, json.JSONDecodeError):
        return {}
    if data.get("schemaVersion") != SCHEMA_VERSION:
        return {}
    return data


def verify_npm_lock(root: Path) -> tuple[bool, str]:
    """Verify that the installed hidden npm lock exactly matches the source lock."""
    source_path = root / "frontend/package-lock.json"
    installed_path = root / "frontend/node_modules/.package-lock.json"
    try:
        source = json.loads(source_path.read_text(encoding="utf-8"))
        installed = json.loads(installed_path.read_text(encoding="utf-8"))
    except FileNotFoundError as error:
        return False, f"missing npm lock: {Path(error.filename).relative_to(root)}"
    except (OSError, json.JSONDecodeError, ValueError) as error:
        return False, f"cannot compare npm locks: {error}"

    source_packages = source.get("packages")
    installed_packages = installed.get("packages")
    if not isinstance(source_packages, dict) or not isinstance(installed_packages, dict):
        return False, "npm lock is missing a packages object"
    expected_names = set(source_packages) - {""}
    installed_names = set(installed_packages)
    if expected_names != installed_names:
        missing = sorted(expected_names - installed_names)
        extra = sorted(installed_names - expected_names)
        detail = []
        if missing:
            detail.append(f"missing {missing[0]}")
        if extra:
            detail.append(f"extra {extra[0]}")
        return False, "; ".join(detail)

    identity_keys = ("version", "resolved", "integrity", "link")
    for package in sorted(expected_names):
        expected = source_packages[package]
        actual = installed_packages[package]
        if not isinstance(expected, dict) or not isinstance(actual, dict):
            return False, f"invalid npm package entry: {package}"
        for key in identity_keys:
            if expected.get(key) != actual.get(key):
                return False, f"{package} has a different {key}"
    return True, f"{len(expected_names)} locked packages match"


def atomic_write(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(data, handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


def command_version(command: list[str]) -> str:
    try:
        return subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        ).stdout.strip().splitlines()[0]
    except (OSError, subprocess.SubprocessError, IndexError):
        return "unavailable"


def update_component(path: Path, component: str, value: str, metadata: dict) -> None:
    state = load_state(path)
    state.update(
        {
            "schemaVersion": SCHEMA_VERSION,
            "platform": platform.system().lower(),
            "arch": platform.machine(),
            "updatedAt": datetime.now(timezone.utc).isoformat(),
        }
    )
    state["toolchains"] = {
        "rust": command_version(["rustc", "--version"]),
        "node": command_version(["node", "--version"]),
        "npm": command_version(["npm", "--version"]),
        "python": command_version([sys.executable, "--version"]),
        "pythonPath": sys.executable,
    }
    components = state.setdefault("components", {})
    components[component] = {
        "fingerprint": value,
        "verifiedAt": datetime.now(timezone.utc).isoformat(),
        **metadata,
    }
    atomic_write(path, state)


def main() -> int:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="action", required=True)

    fp_parser = subparsers.add_parser("fingerprint")
    fp_parser.add_argument("--root", required=True, type=Path)
    fp_parser.add_argument("--component", required=True, choices=("frontend", "python", "browser", "rust"))
    fp_parser.add_argument("--version", action="append", default=[])

    fps_parser = subparsers.add_parser("fingerprints")
    fps_parser.add_argument("--root", required=True, type=Path)
    fps_parser.add_argument("--node-version", required=True)
    fps_parser.add_argument("--npm-version", required=True)
    fps_parser.add_argument("--python-version", required=True)
    fps_parser.add_argument("--python-identity", required=True)
    fps_parser.add_argument("--rust-version", required=True)
    fps_parser.add_argument("--rust-host", required=True)

    get_parser = subparsers.add_parser("get")
    get_parser.add_argument("--state", required=True, type=Path)
    get_parser.add_argument("--component", required=True)

    set_parser = subparsers.add_parser("set")
    set_parser.add_argument("--state", required=True, type=Path)
    set_parser.add_argument("--component", required=True)
    set_parser.add_argument("--fingerprint", required=True)
    set_parser.add_argument("--metadata", default="{}")

    show_parser = subparsers.add_parser("show")
    show_parser.add_argument("--state", required=True, type=Path)

    plan_parser = subparsers.add_parser("plan")
    plan_parser.add_argument("--path", required=True, type=Path)
    plan_parser.add_argument("--entry", action="append", default=[])

    verify_npm_parser = subparsers.add_parser("verify-npm-lock")
    verify_npm_parser.add_argument("--root", required=True, type=Path)

    args = parser.parse_args()
    try:
        if args.action == "fingerprint":
            print(fingerprint(args.root.resolve(), args.component, args.version))
        elif args.action == "fingerprints":
            root = args.root.resolve()
            values = {
                "frontend": fingerprint(root, "frontend", [args.node_version, args.npm_version]),
                "python": fingerprint(root, "python", [args.python_version, args.python_identity]),
                "browser": fingerprint(root, "browser", [args.python_version, "only-shell"]),
                "rust": fingerprint(root, "rust", [args.rust_version, args.rust_host]),
            }
            for component, value in values.items():
                print(f"{component}\t{value}")
        elif args.action == "get":
            state = load_state(args.state)
            print(state.get("components", {}).get(args.component, {}).get("fingerprint", ""))
        elif args.action == "set":
            metadata = json.loads(args.metadata)
            if not isinstance(metadata, dict):
                raise ValueError("metadata must be a JSON object")
            update_component(args.state, args.component, args.fingerprint, metadata)
        elif args.action == "show":
            print(json.dumps(load_state(args.state), ensure_ascii=False, indent=2, sort_keys=True))
        elif args.action == "plan":
            components: dict[str, str] = {}
            for entry in args.entry:
                if "=" not in entry:
                    raise ValueError("plan entries must use component=action")
                component, action = entry.split("=", 1)
                if component not in {"frontend", "python", "browser", "rust"}:
                    raise ValueError(f"unknown plan component: {component}")
                if action not in {"reuse", "verify-or-sync"}:
                    raise ValueError(f"unknown plan action: {action}")
                components[component] = action
            atomic_write(
                args.path,
                {
                    "schemaVersion": SCHEMA_VERSION,
                    "createdAt": datetime.now(timezone.utc).isoformat(),
                    "components": components,
                },
            )
        elif args.action == "verify-npm-lock":
            passed, detail = verify_npm_lock(args.root.resolve())
            print(detail)
            if not passed:
                return 1
    except (RuntimeError, ValueError, json.JSONDecodeError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
