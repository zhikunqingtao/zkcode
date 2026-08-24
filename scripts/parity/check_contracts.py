#!/usr/bin/env python3
"""Fast, deterministic parity-contract gate. It never invokes Git."""

from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path
from urllib.parse import unquote


ROOT = Path(__file__).resolve().parents[2]
PARITY = ROOT / "docs" / "parity"


def load(name: str) -> dict:
    with (PARITY / name).open(encoding="utf-8") as handle:
        return json.load(handle)


def quoted_kinds(path: Path, pattern: str) -> set[str]:
    return set(re.findall(pattern, path.read_text(encoding="utf-8")))


def fail(message: str) -> None:
    print(f"parity-contract: {message}", file=sys.stderr)
    raise SystemExit(1)


def parse_env_example(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for line_number, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            fail(f"invalid .env.example line {line_number}")
        key, value = line.split("=", 1)
        if not re.fullmatch(r"[A-Z][A-Z0-9_]*", key):
            fail(f"invalid .env.example key on line {line_number}: {key}")
        if key in values:
            fail(f"duplicate .env.example key: {key}")
        values[key] = value
    return values


def toml_section_version(path: Path, section: str) -> str:
    content = path.read_text(encoding="utf-8")
    section_match = re.search(
        rf"^\[{re.escape(section)}\]\s*$([\s\S]*?)(?=^\[|\Z)",
        content,
        re.MULTILINE,
    )
    if section_match is None:
        fail(f"TOML section [{section}] is missing in {path.relative_to(ROOT)}")
    version_match = re.search(
        r'^version\s*=\s*"([^"]+)"\s*$',
        section_match.group(1),
        re.MULTILINE,
    )
    if version_match is None:
        fail(f"version is missing from [{section}] in {path.relative_to(ROOT)}")
    return version_match.group(1)


def check_release_metadata() -> None:
    cargo_version = toml_section_version(ROOT / "Cargo.toml", "workspace.package")
    frontend = json.loads((ROOT / "frontend" / "package.json").read_text(encoding="utf-8"))
    frontend_lock = json.loads((ROOT / "frontend" / "package-lock.json").read_text(encoding="utf-8"))
    python_version = toml_section_version(ROOT / "python-service" / "pyproject.toml", "project")

    versions = {
        "Cargo workspace": cargo_version,
        "frontend package": frontend["version"],
        "frontend lock root": frontend_lock["version"],
        "frontend lock package": frontend_lock["packages"][""]["version"],
        "Python package": python_version,
    }
    if set(versions.values()) != {"0.1.0"}:
        fail(f"release versions differ: {versions}")
    if not (ROOT / "CHANGELOG.md").is_file():
        fail("CHANGELOG.md is missing")


def check_supported_env() -> None:
    values = parse_env_example(ROOT / ".env.example")
    expected = {
        "ZK_HOST": "127.0.0.1",
        "ZK_PORT": "8081",
        "ZK_AUTH_MODE": "localhost",
        "ZK_LOCAL_PICKER_ENABLED": "true",
        "ZK_PYTHON_ENABLED": "true",
        "ZK_AGENT_ENABLED": "true",
        "ZK_AGENT_WRITE_ENABLED": "true",
        "ZK_SWARM_ENABLED": "true",
        "ZK_WORKTREE_ENABLED": "false",
        "ZK_FEATURE_THINKING_MODE": "true",
        "ZK_FEATURE_COORDINATOR_MODE": "true",
        "ZK_FEATURE_WEB_BROWSER_TOOL": "true",
        "ZK_FEATURE_GIT_ENHANCED_TOOL": "true",
        "ZK_FEATURE_RUNTIME_VERIFICATION": "true",
        "MCP_REGISTRY_PATH": "configuration/mcp/mcp_capability_registry.json",
    }
    mismatches = {
        key: {"expected": expected_value, "actual": values.get(key)}
        for key, expected_value in expected.items()
        if values.get(key) != expected_value
    }
    if mismatches:
        fail(f"supported .env.example defaults differ: {mismatches}")
    registry = ROOT / values["MCP_REGISTRY_PATH"]
    if not registry.is_file():
        fail(f"MCP registry is missing: {registry.relative_to(ROOT)}")

    setup_script = (ROOT / "scripts" / "setup-python-macos.sh").read_text(encoding="utf-8")
    if '"$VENV_PYTHON" -m playwright install chromium' not in setup_script:
        fail("macOS setup does not install Playwright Chromium")
    ci_workflow = (ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
    if "python -m playwright install chromium" not in ci_workflow:
        fail("macOS CI does not install Playwright Chromium")

    installer_path = ROOT / "install-zkcode.command"
    if not installer_path.is_file() or not os.access(installer_path, os.X_OK):
        fail("executable macOS one-command installer is missing")
    installer = installer_path.read_text(encoding="utf-8")
    required_installer_markers = {
        "official Homebrew installer": "https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh",
        "official rustup installer": "https://sh.rustup.rs",
        "bounded execution": "run_bounded",
        "network connection timeout": "--connect-timeout",
        "locked project setup": '"$ROOT_DIR/scripts/setup-macos.sh"',
        "service startup": '"$ROOT_DIR/start.sh"',
        "browser opening": '/usr/bin/open "$FRONTEND_URL"',
        "bounded administrator authorization": 'run_bounded 300 "administrator authorization"',
        "native Apple Silicon Homebrew": '[ -x /opt/homebrew/bin/brew ]',
    }
    missing_installer_markers = [
        label for label, marker in required_installer_markers.items() if marker not in installer
    ]
    if missing_installer_markers:
        fail(f"macOS one-command installer is incomplete: {missing_installer_markers}")
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    if "./install-zkcode.command" not in readme:
        fail("README does not document the macOS one-command installer")
    if "./stop.sh\n./start.sh" not in readme:
        fail("README does not explain how to reload newly configured LLM credentials")
    start_script = (ROOT / "start.sh").read_text(encoding="utf-8")
    if "scripts/spawn-detached.py" not in start_script:
        fail("macOS services are not detached from the installation terminal")
    detached_spawner = (ROOT / "scripts" / "spawn-detached.py").read_text(encoding="utf-8")
    if "start_new_session=True" not in detached_spawner or "subprocess.Popen" not in detached_spawner:
        fail("macOS service spawner does not create an independent process session")
    if 'subsystems"]["python"]["status"] == "UP"' not in start_script:
        fail("macOS startup does not require a healthy Python sidecar")


def check_local_markdown_links() -> None:
    markdown_files = [
        ROOT / "README.md",
        ROOT / "CONTRIBUTING.md",
        ROOT / "SECURITY.md",
        ROOT / "THIRD_PARTY_NOTICES.md",
        ROOT / "CHANGELOG.md",
        *sorted((ROOT / "docs").glob("*.md")),
    ]
    link_pattern = re.compile(r"\[[^\]]*\]\(([^)]+)\)")
    missing: list[str] = []
    for markdown_file in markdown_files:
        content = markdown_file.read_text(encoding="utf-8")
        for raw_target in link_pattern.findall(content):
            target = raw_target.strip().strip("<>")
            if not target or target.startswith("#"):
                continue
            if re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*:", target):
                continue
            target = unquote(target.split("#", 1)[0])
            if not target:
                continue
            resolved = (markdown_file.parent / target).resolve()
            if not resolved.exists():
                missing.append(f"{markdown_file.relative_to(ROOT)} -> {raw_target}")
    if missing:
        fail(f"local Markdown links are missing: {missing}")


def main() -> None:
    check_release_metadata()
    check_supported_env()
    check_local_markdown_links()

    rest = load("rest-contract.json")
    ws = load("ws-contract.json")
    tools = load("tool-contract.json")
    ddl = load("ddl-consumers.json")

    if len(ws["upstream"]) != ws["upstreamTargetCount"]:
        fail("upstream count does not match contract")
    if len(ws["downstream"]) != ws["downstreamTargetCount"]:
        fail("downstream count does not match contract")
    if "evidence_decision" in ws["upstream"]:
        fail("removed evidence_decision is active")
    if len(tools["frozenDefault"]) != tools["frozenDefaultCount"]:
        fail("frozen tool count does not match contract")
    tool_schema_hash = tools.get("frozenDefaultSchemaSha256", "")
    if not re.fullmatch(r"[0-9a-f]{64}", tool_schema_hash):
        fail("frozen tool schema digest is missing or malformed")
    engine_bridge = (
        ROOT / "crates" / "zk-server" / "src" / "engine_bridge.rs"
    ).read_text(encoding="utf-8")
    if f'"{tool_schema_hash}"' not in engine_bridge:
        fail("frozen tool schema digest differs from the Rust gate")
    if len(ddl["tables"]) != ddl["tableCount"] or ddl["databaseCount"] != 1:
        fail("DDL consumer inventory is inconsistent")
    if not rest["requiredEndpoints"]:
        fail("REST contract is empty")

    routes_source = (ROOT / "crates" / "zk-server" / "src" / "routes.rs").read_text(encoding="utf-8")
    enforced_through = int(rest.get("enforcedThroughWorkPackage", 0))
    for endpoint in rest["requiredEndpoints"]:
        packages = [int(value) for value in re.findall(r"WP-(\d+)", endpoint["workPackage"])]
        if packages and min(packages) > enforced_through:
            continue
        path = endpoint["path"]
        marker = f'"{path}"'
        offset = routes_source.find(marker)
        if offset < 0:
            fail(f"REST route missing: {endpoint['method']} {path}")
        nearby = routes_source[offset:offset + 500]
        method = endpoint["method"].lower()
        if method not in {"get", "post", "put", "patch", "delete"} or not re.search(rf"\b{method}\s*\(", nearby):
            fail(f"REST route method missing: {endpoint['method']} {path}")

    server_source = ROOT / "crates" / "zk-protocol" / "src" / "server_message.rs"
    server_kinds = quoted_kinds(server_source, r'=>\s*"([a-z0-9_]+)"')
    missing_downstream = set(ws["downstream"]) - server_kinds
    if missing_downstream:
        fail(f"server message kinds missing: {sorted(missing_downstream)}")

    migration = (ROOT / "crates" / "zk-db" / "migrations" / "V2__init_session_message.sql").read_text(encoding="utf-8")
    missing_tables = [name for name in ddl["tables"] if not re.search(rf"CREATE TABLE IF NOT EXISTS\s+{re.escape(name)}\b", migration)]
    if missing_tables:
        fail(f"DDL tables missing: {missing_tables}")

    print("parity-contract: ok")


if __name__ == "__main__":
    main()
