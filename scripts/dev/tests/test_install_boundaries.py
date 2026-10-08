"""Exercise install entrypoints with local command stubs; never install host tools."""
import importlib.util
import pathlib
import shlex
import subprocess
import tempfile
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[3]
# This bounds local stub processes, not production installation. Leave room for
# process startup and filesystem contention during the complete native gates.
STUB_TIMEOUT_SECONDS = 30

class InstallBoundaries(unittest.TestCase):
    def test_sync_never_bootstraps_homebrew(self):
        with tempfile.TemporaryDirectory() as directory:
            marker = pathlib.Path(directory) / 'installer-called'
            script = f"""
set -eu
DEV_PYTHON=/usr/bin/false
DEV_OFFLINE=0
ROOT_DIR={shlex.quote(str(ROOT))}
dev_find_brew() {{ return 1; }}
dev_install_homebrew() {{ touch {shlex.quote(str(marker))}; return 0; }}
dev_fail() {{ exit "$1"; }}
. {shlex.quote(str(ROOT / 'scripts/dev/documents.sh'))}
dev_sync_documents
"""
            result = subprocess.run(['/bin/sh', '-c', script], capture_output=True, timeout=STUB_TIMEOUT_SECONDS)
            self.assertNotEqual(result.returncode, 0)
            self.assertFalse(marker.exists(), 'sync invoked the global bootstrap installer')

    def test_python_floor_supports_safe_tar_filter(self):
        import tomllib
        policy = tomllib.loads((ROOT/'configuration/dev-toolchain.toml').read_text())
        self.assertEqual(policy['python'], '>=3.11.4,<3.12.0')

    def test_ocr_requires_successful_nonempty_absolute_brew_prefix(self):
        for prefix, status, allowed in [('', 0, False), ('relative', 0, False),
                                        ('/valid-but-failed', 1, False), ('/valid', 0, True)]:
            with self.subTest(prefix=prefix, status=status), tempfile.TemporaryDirectory() as directory:
                work = pathlib.Path(directory)
                marker = work / 'ocr-called'
                python = work / 'python'
                python.write_text(f'''#!/bin/sh
case "$1" in
 */ocr-models.py) touch {shlex.quote(str(marker))}; exit 0;;
 *) [ -f {shlex.quote(str(marker))} ];;
esac
''')
                python.chmod(0o755)
                brew = work / 'brew'
                brew.write_text(f'#!/bin/sh\nprintf "%s" {shlex.quote(prefix)}\nexit {status}\n')
                brew.chmod(0o755)
                script = f'''
set -eu
DEV_PYTHON={shlex.quote(str(python))}
DEV_OFFLINE=0
ROOT_DIR={shlex.quote(str(ROOT))}
dev_find_brew() {{ printf '%s' {shlex.quote(str(brew))}; }}
dev_toml_string() {{ return 0; }}
dev_activate_toolchains() {{ return 0; }}
dev_fail() {{ exit "$1"; }}
. {shlex.quote(str(ROOT / 'scripts/dev/documents.sh'))}
dev_sync_documents
'''
                result = subprocess.run(['/bin/sh', '-c', script], capture_output=True, timeout=STUB_TIMEOUT_SECONDS)
                self.assertEqual(marker.exists(), allowed)
                self.assertEqual(result.returncode == 0, allowed, result.stderr)

if __name__ == '__main__':
    unittest.main()
