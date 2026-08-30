#!/usr/bin/env python3
"""Human and JSON diagnostics for the source-development environment."""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import re
import subprocess
import sys
import tomllib
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any


def run(command: list[str], cwd: Path | None = None, timeout: int = 30) -> tuple[bool, str]:
    try:
        result = subprocess.run(
            command,
            cwd=cwd,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except (OSError, subprocess.SubprocessError) as error:
        return False, str(error)
    output = (result.stdout or result.stderr).strip()
    return result.returncode == 0, output


def first_line(value: str) -> str:
    return value.splitlines()[0] if value else "unavailable"


def load_toolchain_policy(root: Path) -> dict[str, Any]:
    path = root / "configuration/dev-toolchain.toml"
    try:
        policy = tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError) as error:
        raise RuntimeError(f"cannot read toolchain policy: {error}") from error
    required = {
        "schema_version",
        "platform",
        "arch",
        "minimum_macos",
        "rust",
        "node",
        "npm",
        "python",
    }
    if policy.get("schema_version") != 1 or not required.issubset(policy):
        raise RuntimeError("toolchain policy is missing required schema 1 fields")
    return policy


def semver_tuple(value: str) -> tuple[int, int, int] | None:
    match = re.search(r"(\d+)\.(\d+)(?:\.(\d+))?", value)
    if match is None:
        return None
    return tuple(int(part or 0) for part in match.groups())


def semver_in_range(value: str, constraint: str) -> bool:
    actual = semver_tuple(value)
    bounds = constraint.split(",")
    if actual is None or len(bounds) != 2 or not bounds[0].startswith(">=") or not bounds[1].startswith("<"):
        return False
    lower = semver_tuple(bounds[0][2:])
    upper = semver_tuple(bounds[1][1:])
    return lower is not None and upper is not None and lower <= actual < upper


def process(pid_file: Path, expected: str) -> dict[str, Any]:
    try:
        pid = int(pid_file.read_text(encoding="utf-8").splitlines()[0])
    except (FileNotFoundError, OSError, ValueError, IndexError):
        return {"running": False, "pid": None, "recorded": False}
    ok, command = run(["ps", "-p", str(pid), "-o", "command="], timeout=5)
    return {
        "running": ok and expected in command,
        "pid": pid,
        "recorded": True,
        "commandMatches": ok and expected in command,
    }


def http_json(url: str) -> dict[str, Any] | None:
    try:
        with urllib.request.urlopen(url, timeout=2) as response:  # noqa: S310 - loopback URL only
            return json.load(response)
    except (OSError, ValueError, urllib.error.URLError):
        return None


def http_ready(url: str) -> bool:
    try:
        with urllib.request.urlopen(url, timeout=2):  # noqa: S310 - loopback URL only
            return True
    except (OSError, urllib.error.URLError):
        return False


def load_state(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def load_state_module(root: Path):
    module_path = root / "scripts/dev/state.py"
    spec = importlib.util.spec_from_file_location("zkcode_dev_state", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load the component fingerprint module")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def service_status(root: Path, port: int) -> dict[str, Any]:
    runtime = root / ".runtime"
    health = http_json(f"http://127.0.0.1:{port}/api/health")
    python_status = None
    if health:
        python_status = health.get("subsystems", {}).get("python", {}).get("status")
    return {
        "backend": {
            **process(runtime / "backend.pid", str(root / "target/debug/zk-server")),
            "healthy": health is not None,
            "url": f"http://127.0.0.1:{port}",
        },
        "frontend": {
            **process(runtime / "frontend.pid", str(root / "frontend/node_modules/.bin/vite")),
            "healthy": http_ready("http://127.0.0.1:5273/"),
            "url": "http://127.0.0.1:5273",
        },
        "python": {
            **process(runtime / "python.pid", str(root / "python-service/.venv/bin/python")),
            "healthStatus": python_status,
        },
    }


def print_status(root: Path, port: int, as_json: bool) -> int:
    data = {
        "schemaVersion": 1,
        "root": str(root),
        "services": service_status(root, port),
        "dependencyState": load_state(root / ".runtime/dev/dev-state.json"),
    }
    if as_json:
        print(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True))
    else:
        print("zkcode source development status")
        for name, status in data["services"].items():
            state = "healthy" if status.get("healthy") or status.get("healthStatus") == "UP" else (
                "running" if status.get("running") else "stopped"
            )
            pid = f" (PID {status['pid']})" if status.get("pid") else ""
            print(f"{name:9} {state}{pid}")
        components = data["dependencyState"].get("components", {})
        print("components " + (", ".join(sorted(components)) if components else "not synchronized"))
        print("open       http://127.0.0.1:5273")
    return 0


def load_env_parser(root: Path):
    module_path = root / "scripts/dev/exec-env.py"
    spec = importlib.util.spec_from_file_location("zkcode_exec_env", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load the safe .env parser")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def check(name: str, passed: bool, detail: str, repair: str | None = None) -> dict[str, Any]:
    result: dict[str, Any] = {"name": name, "ok": passed, "detail": detail}
    if repair:
        result["repair"] = repair
    return result


def service_health_check(name: str, service: dict[str, Any]) -> dict[str, Any]:
    healthy = bool(service.get("healthy"))
    running = bool(service.get("running"))
    if healthy:
        return check(f"{name}-health", True, "healthy")
    if running:
        return check(f"{name}-health", False, "running but unhealthy", f"./dev restart {name}")
    return check(f"{name}-health", True, "not running (allowed for doctor)")


def doctor(root: Path, port: int, deep: bool, as_json: bool) -> int:
    checks: list[dict[str, Any]] = []
    try:
        policy = load_toolchain_policy(root)
        checks.append(check("toolchain-policy", True, "schema 1 loaded"))
    except RuntimeError as error:
        checks.append(check("toolchain-policy", False, str(error), "restore configuration/dev-toolchain.toml"))
        policy = {
            "rust": "unavailable",
            "node": ">=0.0.0,<0.0.0",
            "npm": ">=0.0.0,<0.0.0",
            "python": ">=0.0.0,<0.0.0",
        }
    ok, rust = run(["rustc", "--version"])
    rust_parts = first_line(rust).split()
    rust_matches = ok and len(rust_parts) > 1 and rust_parts[1] == policy["rust"]
    checks.append(check("rust", rust_matches, first_line(rust), "./dev bootstrap"))
    ok, node = run(["node", "--version"])
    checks.append(check("node", ok and semver_in_range(first_line(node), policy["node"]), first_line(node), "./dev bootstrap"))
    ok, npm = run(["npm", "--version"])
    checks.append(check("npm", ok and semver_in_range(first_line(npm), policy["npm"]), first_line(npm), "./dev bootstrap"))
    python = Path(os.environ.get("DEV_PYTHON", sys.executable))
    ok, py_version = run([str(python), "--version"])
    checks.append(check("python", ok and semver_in_range(first_line(py_version), policy["python"]), f"{first_line(py_version)} ({python})", "./dev bootstrap"))

    env_path = root / ".env"
    try:
        parser = load_env_parser(root)
        values, warnings = parser.parse_env(env_path)
        checks.append(check("env", True, f"valid ({len(values)} supported keys, {len(warnings)} warnings)"))
    except (RuntimeError, ValueError) as error:
        checks.append(check("env", False, str(error), "fix the reported .env line"))

    assets = {
        "frontend": root / "frontend/node_modules/.bin/vite",
        "python-venv": root / "python-service/.venv/bin/python",
        "browser": root / ".runtime/playwright",
        "backend-build": root / "target/debug/zk-server",
    }
    for name, path in assets.items():
        if name == "browser":
            passed = bool(list(path.glob("chromium_headless_shell-*/**/chrome-headless-shell"))) and bool(
                list(path.glob("ffmpeg-*/ffmpeg*"))
            )
        else:
            passed = path.is_file()
        repair = "./dev repair browser" if name == "browser" else (
            "./dev repair frontend" if name == "frontend" else (
                "./dev repair python" if name == "python-venv" else "./dev sync --build"
            )
        )
        checks.append(check(name, passed, str(path.relative_to(root)), repair))

    state = load_state(root / ".runtime/dev/dev-state.json")
    components = state.get("components", {})
    try:
        state_module = load_state_module(root)
        _, python_identity = run(
            [str(python), "-c", "import sys; print(sys.executable); print(sys.implementation.cache_tag)"],
            timeout=10,
        )
        _, rust_host_output = run(["rustc", "-vV"], timeout=10)
        rust_host = next(
            (line.removeprefix("host: ") for line in rust_host_output.splitlines() if line.startswith("host: ")),
            "unavailable",
        )
        expected = {
            "frontend": state_module.fingerprint(root, "frontend", [first_line(node), first_line(npm)]),
            "python": state_module.fingerprint(root, "python", [first_line(py_version), python_identity]),
            "browser": state_module.fingerprint(root, "browser", [first_line(py_version), "only-shell"]),
            "rust": state_module.fingerprint(root, "rust", [first_line(rust), rust_host]),
        }
    except (OSError, RuntimeError, ValueError) as error:
        expected = {}
        checks.append(check("fingerprints", False, str(error), "./dev sync"))
    for component in ("frontend", "python", "browser", "rust"):
        saved = components.get(component, {}).get("fingerprint")
        current = expected.get(component)
        passed = bool(saved and current and saved == current)
        detail = "verified/current" if passed else ("stale" if saved else "not verified")
        checks.append(check(f"state-{component}", passed, detail, f"./dev repair {component}"))

    services = service_status(root, port)
    for service_name, service in services.items():
        if service.get("recorded"):
            checks.append(
                check(
                    f"pid-{service_name}",
                    bool(service.get("commandMatches")),
                    "matches this repository" if service.get("commandMatches") else "stale or belongs to another command",
                    "./dev stop",
                )
            )
    checks.append(service_health_check("backend", services["backend"]))
    checks.append(service_health_check("frontend", services["frontend"]))

    if deep:
        deep_commands = [
            ("npm-tree", ["npm", "ls", "--depth=0", "--silent"], root / "frontend", 120, "./dev repair frontend"),
            (
                "npm-lock",
                [str(python), str(root / "scripts/dev/state.py"), "verify-npm-lock", "--root", str(root)],
                root,
                120,
                "./dev repair frontend",
            ),
            ("pip-check", [str(root / "python-service/.venv/bin/python"), "-m", "pip", "check"], root, 120, "./dev repair python"),
            (
                "pip-runtime-lock",
                [
                    str(root / "python-service/.venv/bin/python"),
                    "-m",
                    "pip",
                    "install",
                    "--disable-pip-version-check",
                    "--dry-run",
                    "--no-index",
                    "--no-deps",
                    "-r",
                    str(root / "python-service/requirements.lock"),
                ],
                root,
                120,
                "./dev repair python",
            ),
            (
                "pip-build-lock",
                [
                    str(root / "python-service/.venv/bin/python"),
                    "-m",
                    "pip",
                    "install",
                    "--disable-pip-version-check",
                    "--dry-run",
                    "--no-index",
                    "--no-deps",
                    "-r",
                    str(root / "python-service/build-requirements.lock"),
                ],
                root,
                120,
                "./dev repair python",
            ),
            ("cargo-metadata", ["cargo", "metadata", "--locked", "--no-deps", "--format-version", "1"], root, 120, "./dev repair rust"),
            ("frontend-build", ["npm", "run", "build"], root / "frontend", 600, "./dev repair frontend"),
        ]
        for name, command, cwd, timeout, repair in deep_commands:
            passed, output = run(command, cwd=cwd, timeout=timeout)
            checks.append(check(name, passed, "passed" if passed else first_line(output), repair))
        browser_env = os.environ.copy()
        browser_env["PLAYWRIGHT_BROWSERS_PATH"] = str(root / ".runtime/playwright")
        browser_script = (
            "from playwright.sync_api import sync_playwright; "
            "p=sync_playwright().start(); b=p.chromium.launch(headless=True); "
            "page=b.new_page(); page.set_content('<title>ok</title>'); "
            "assert page.title() == 'ok'; b.close(); p.stop()"
        )
        try:
            result = subprocess.run(
                [str(root / "python-service/.venv/bin/python"), "-c", browser_script],
                cwd=root,
                env=browser_env,
                check=False,
                capture_output=True,
                text=True,
                timeout=60,
            )
            checks.append(check("browser-smoke", result.returncode == 0, "launch/page/close", "./dev repair browser"))
        except (OSError, subprocess.SubprocessError) as error:
            checks.append(check("browser-smoke", False, str(error), "./dev repair browser"))

    failed = [item for item in checks if not item["ok"]]
    result = {"schemaVersion": 1, "ok": not failed, "deep": deep, "checks": checks, "services": services}
    if as_json:
        print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    else:
        print("zkcode source development doctor")
        for item in checks:
            label = "ok  " if item["ok"] else "fail"
            print(f"{label} {item['name']:<18} {item['detail']}")
            if not item["ok"] and item.get("repair"):
                print(f"     repair: {item['repair']}")
    return 0 if not failed else 1


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("status", "doctor"))
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--port", required=True, type=int)
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--deep", action="store_true")
    args = parser.parse_args()
    root = args.root.resolve()
    if args.action == "status":
        return print_status(root, args.port, args.json)
    return doctor(root, args.port, args.deep, args.json)


if __name__ == "__main__":
    raise SystemExit(main())
