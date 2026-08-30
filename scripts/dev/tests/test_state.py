from __future__ import annotations

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock


MODULE_PATH = Path(__file__).parents[1] / "state.py"
SPEC = importlib.util.spec_from_file_location("zkcode_dev_state", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
STATE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(STATE)


class StateTests(unittest.TestCase):
    def make_rust_tree(self, root: Path) -> None:
        files = {
            "configuration/dev-toolchain.toml": 'rust = "1.97.1"\n',
            "rust-toolchain.toml": '[toolchain]\nchannel = "1.97.1"\n',
            "Cargo.lock": "version = 4\n",
            "Cargo.toml": '[workspace]\nmembers = ["crates/example"]\n',
            "crates/example/Cargo.toml": '[package]\nname = "example"\nversion = "0.1.0"\n',
        }
        for relative, content in files.items():
            path = root / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content, encoding="utf-8")

    def test_generated_manifests_do_not_change_rust_fingerprint(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.make_rust_tree(root)
            original = STATE.fingerprint(root, "rust", ["rustc 1.97.1", "aarch64"])
            generated = root / "target/debug/build/generated/Cargo.toml"
            generated.parent.mkdir(parents=True)
            generated.write_text("generated", encoding="utf-8")
            self.assertEqual(
                original,
                STATE.fingerprint(root, "rust", ["rustc 1.97.1", "aarch64"]),
            )

    def test_workspace_manifest_change_invalidates_fingerprint(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.make_rust_tree(root)
            original = STATE.fingerprint(root, "rust", ["rustc 1.97.1", "aarch64"])
            (root / "crates/example/Cargo.toml").write_text(
                '[package]\nname = "example"\nversion = "0.2.0"\n',
                encoding="utf-8",
            )
            self.assertNotEqual(
                original,
                STATE.fingerprint(root, "rust", ["rustc 1.97.1", "aarch64"]),
            )

    def test_atomic_state_update_preserves_other_components(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state_path = Path(directory) / "dev-state.json"
            STATE.update_component(state_path, "frontend", "front", {})
            STATE.update_component(state_path, "python", "python", {"abi": "cp311"})
            value = STATE.load_state(state_path)
            self.assertEqual(value["components"]["frontend"]["fingerprint"], "front")
            self.assertEqual(value["components"]["python"]["fingerprint"], "python")
            self.assertEqual(value["components"]["python"]["abi"], "cp311")

    def test_npm_lock_verification_accepts_exact_install(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = {
                "lockfileVersion": 3,
                "packages": {
                    "": {"name": "example"},
                    "node_modules/example": {
                        "version": "1.2.3",
                        "resolved": "https://example.test/example.tgz",
                        "integrity": "sha512-example",
                    },
                },
            }
            installed = {
                "lockfileVersion": 3,
                "packages": {
                    "node_modules/example": source["packages"]["node_modules/example"]
                },
            }
            source_path = root / "frontend/package-lock.json"
            installed_path = root / "frontend/node_modules/.package-lock.json"
            installed_path.parent.mkdir(parents=True)
            source_path.write_text(json.dumps(source), encoding="utf-8")
            installed_path.write_text(json.dumps(installed), encoding="utf-8")
            self.assertEqual(STATE.verify_npm_lock(root), (True, "1 locked packages match"))

    def test_npm_lock_verification_rejects_version_drift(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source_path = root / "frontend/package-lock.json"
            installed_path = root / "frontend/node_modules/.package-lock.json"
            installed_path.parent.mkdir(parents=True)
            source_path.write_text(
                json.dumps({"packages": {"": {}, "node_modules/example": {"version": "1.2.3"}}}),
                encoding="utf-8",
            )
            installed_path.write_text(
                json.dumps({"packages": {"node_modules/example": {"version": "1.2.4"}}}),
                encoding="utf-8",
            )
            self.assertEqual(
                STATE.verify_npm_lock(root),
                (False, "node_modules/example has a different version"),
            )

    def test_npm_lock_verification_ignores_other_platform_optional_packages(self) -> None:
        with tempfile.TemporaryDirectory() as directory, mock.patch.object(
            STATE.platform, "system", return_value="Darwin"
        ), mock.patch.object(STATE.platform, "machine", return_value="arm64"):
            root = Path(directory)
            current = {"version": "1.0.0", "os": ["darwin"], "cpu": ["arm64"]}
            source = {
                "packages": {
                    "": {},
                    "node_modules/current": current,
                    "node_modules/other": {
                        "version": "1.0.0",
                        "optional": True,
                        "os": ["aix"],
                        "cpu": ["ppc64"],
                    },
                }
            }
            installed = {"packages": {"node_modules/current": current}}
            source_path = root / "frontend/package-lock.json"
            installed_path = root / "frontend/node_modules/.package-lock.json"
            installed_path.parent.mkdir(parents=True)
            source_path.write_text(json.dumps(source), encoding="utf-8")
            installed_path.write_text(json.dumps(installed), encoding="utf-8")

            self.assertEqual(STATE.verify_npm_lock(root), (True, "1 locked packages match"))


if __name__ == "__main__":
    unittest.main()
