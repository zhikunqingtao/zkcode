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


def flat_toml_values(path: Path) -> dict[str, str | int]:
    values: dict[str, str | int] = {}
    for line_number, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        match = re.fullmatch(r'([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(?:"([^"]*)"|(\d+))', line)
        if match is None:
            fail(f"unsupported flat TOML line in {path.relative_to(ROOT)}:{line_number}")
        key, string_value, integer_value = match.groups()
        if key in values:
            fail(f"duplicate TOML key in {path.relative_to(ROOT)}: {key}")
        values[key] = string_value if string_value is not None else int(integer_value)
    return values


def toml_quoted_value(path: Path, key: str) -> str:
    matches = re.findall(
        rf'^{re.escape(key)}\s*=\s*"([^"]+)"\s*$',
        path.read_text(encoding="utf-8"),
        re.MULTILINE,
    )
    if len(matches) != 1:
        fail(f"expected one quoted {key} in {path.relative_to(ROOT)}")
    return matches[0]


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
        "ZK_PORT": "8082",
        "ZK_AUTH_MODE": "localhost",
        "ZK_LOCAL_PICKER_ENABLED": "true",
        "ZK_PYTHON_ENABLED": "true",
        "ZK_PYTHON_UDS": ".runtime/python.sock",
        "ZK_DEV_ALLOW_DEMO_CREDENTIAL": "0",
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

    sync_script = (ROOT / "scripts" / "dev" / "sync.sh").read_text(encoding="utf-8")
    if "playwright install --only-shell chromium" not in sync_script:
        fail("source bootstrap does not install the locked Playwright Headless Shell")
    ci_workflow = (ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
    if "python -m playwright install --only-shell chromium" not in ci_workflow:
        fail("macOS CI does not install the locked Playwright Headless Shell")

    installer_path = ROOT / "install-zkcode.command"
    if not installer_path.is_file() or not os.access(installer_path, os.X_OK):
        fail("executable macOS one-command installer is missing")
    installer = installer_path.read_text(encoding="utf-8")
    required_installer_markers = {
        "unified source bootstrap": '"$ROOT_DIR/dev" bootstrap --start',
    }
    missing_installer_markers = [
        label for label, marker in required_installer_markers.items() if marker not in installer
    ]
    if missing_installer_markers:
        fail(f"macOS one-command installer is incomplete: {missing_installer_markers}")
    dev_path = ROOT / "dev"
    if not dev_path.is_file() or not os.access(dev_path, os.X_OK):
        fail("executable ./dev source bootstrap is missing")
    toolchains = (ROOT / "scripts" / "dev" / "toolchains-macos.sh").read_text(
        encoding="utf-8"
    )
    for marker in (
        "https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh",
        "https://sh.rustup.rs",
        "dev_load_toolchain_config",
        "ZK_DEV_NODE_FORMULA",
        "ZK_DEV_PYTHON_FORMULA",
        "dev_run_bounded",
        "dev_homebrew_authorize_sudo",
        "dev_sudo -n -l mkdir",
        'dev_run_bounded 300 "sudo authorization"',
        "NONINTERACTIVE=1",
        "DEV_HOMEBREW_CREATED_SUDO_TICKET",
    ):
        if marker not in toolchains:
            fail(f"source toolchain resolver is missing marker: {marker}")
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    for marker, message in (
        (
            "git clone https://github.com/zhikunqingtao/zkcode.git",
            "README does not document Git acquisition",
        ),
        ("Download ZIP", "README does not document the no-Git ZIP fallback"),
        ("./dev bootstrap --start", "README does not document the source bootstrap"),
        ("./dev doctor --deep", "README does not document deep first-run verification"),
        ("sudo ./dev", "README does not warn against running the project as root"),
        ("./dev restart", "README does not explain how to reload newly configured LLM credentials"),
        ("docs/troubleshooting.md", "README does not link first-run troubleshooting"),
    ):
        if marker not in readme:
            fail(message)
    backend_launcher = (ROOT / "scripts" / "run-backend-macos.sh").read_text(
        encoding="utf-8"
    )
    if (
        "--canonical-zero-one ZK_DEV_ALLOW_DEMO_CREDENTIAL=0"
        not in backend_launcher
    ):
        fail("backend launcher does not enforce a canonical demo-credential gate")
    if "demo-credential-disabled" in backend_launcher:
        fail("backend launcher still bypasses demo credentials with a fake seed path")
    server_config = (ROOT / "crates" / "zk-server" / "src" / "config.rs").read_text(
        encoding="utf-8"
    )
    if 'parse_zero_one_env("ZK_DEV_ALLOW_DEMO_CREDENTIAL")' not in server_config:
        fail("server does not enforce the canonical 0/1 demo-credential gate")
    start_script = (ROOT / "start.sh").read_text(encoding="utf-8")
    if '"$ROOT_DIR/dev" up --no-open' not in start_script:
        fail("legacy start.sh does not forward to ./dev")
    lifecycle = (ROOT / "scripts" / "dev" / "lifecycle.sh").read_text(encoding="utf-8")
    if "scripts/spawn-detached.py" not in lifecycle:
        fail("source lifecycle does not detach services from the terminal")
    detached_spawner = (ROOT / "scripts" / "spawn-detached.py").read_text(encoding="utf-8")
    if "start_new_session=True" not in detached_spawner or "subprocess.Popen" not in detached_spawner:
        fail("macOS service spawner does not create an independent process session")
    for marker, message in (
        ("DEV_HEALTH_EXPECTED_PYTHON_STATUS=UP", "enabled Python must require UP"),
        (
            "DEV_HEALTH_EXPECTED_PYTHON_STATUS=DISABLED",
            "disabled Python must require DISABLED",
        ),
        (
            'data["subsystems"]["python"]["status"] == sys.argv[1]',
            "Python subsystem health must be parsed from JSON",
        ),
    ):
        if marker not in lifecycle:
            fail(f"source lifecycle contract is incomplete: {message}")


def check_source_toolchain_policy() -> None:
    policy_path = ROOT / "configuration" / "dev-toolchain.toml"
    policy = flat_toml_values(policy_path)
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
        fail("source toolchain policy is missing required schema 1 fields")

    if toml_quoted_value(ROOT / "rust-toolchain.toml", "channel") != policy["rust"]:
        fail("Rust differs between dev-toolchain.toml and rust-toolchain.toml")

    def lower_major(constraint: str) -> str:
        match = re.fullmatch(r">=(\d+)(?:\.\d+){1,2},<\d+(?:\.\d+){1,2}", constraint)
        if match is None:
            fail(f"unsupported toolchain range: {constraint}")
        return match.group(1)

    python_match = re.fullmatch(
        r">=(\d+\.\d+)\.\d+,<\d+\.\d+\.\d+", policy["python"]
    )
    if python_match is None:
        fail(f"unsupported Python toolchain range: {policy['python']}")

    ci = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    expected_ci_markers = (
        f"dtolnay/rust-toolchain@{policy['rust']}",
        f"node-version: '{lower_major(policy['node'])}'",
        f"python-version: '{python_match.group(1)}'",
        "run: ./dev sync --build",
        "run: ./dev doctor --deep --json",
        "PLAYWRIGHT_BROWSERS_PATH: ${{ github.workspace }}/.runtime/playwright",
    )
    for marker in expected_ci_markers:
        if marker not in ci:
            fail(f"CI is not aligned with source toolchain policy: missing {marker}")
    real_smoke = (ROOT / ".github/workflows/real-smoke.yml").read_text(encoding="utf-8")
    if f"dtolnay/rust-toolchain@{policy['rust']}" not in real_smoke:
        fail("real-smoke Rust toolchain differs from source toolchain policy")

    inspect_script = (ROOT / "scripts/dev/inspect.py").read_text(encoding="utf-8")
    if "load_toolchain_policy(root)" not in inspect_script:
        fail("doctor does not load configuration/dev-toolchain.toml")
    main_script = (ROOT / "scripts/dev/main.sh").read_text(encoding="utf-8")
    if 'PLAYWRIGHT_BROWSERS_PATH="$ROOT_DIR/.runtime/playwright"' not in main_script:
        fail("source tests do not use the repository-local Playwright runtime")


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
    check_source_toolchain_policy()
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
