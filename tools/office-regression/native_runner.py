#!/usr/bin/env python3
"""Run the complete native Office/HTML suite and retain results outside the repo."""
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
import tempfile
import time


def run(root: Path, out: Path, args: list[str]) -> int:
    root, out = root.resolve(), out.resolve()
    if out == root or root in out.parents:
        raise ValueError('OFFICE_REGRESSION_OUT must be outside the repository')
    if not (root/'Cargo.toml').is_file():
        raise ValueError('zkcode repository root was not found')
    out.mkdir(parents=True, exist_ok=True)
    lock = out/'.office-active'
    try:
        lock.mkdir()
    except FileExistsError as error:
        raise ValueError('output directory is occupied') from error
    try:
        if any(path != lock for path in out.iterdir()):
            raise ValueError('output directory must be empty')
        env = {**os.environ, 'OFFICE_REGRESSION_OUT': str(out), 'R12_REPO_ROOT':str(root),
               'R12_TOOLS_DIR':str(root/'tools/office-regression'),
               'R12_PYTHON_SERVICE_SRC':str(root/'python-service/src'),
               'PLAYWRIGHT_BROWSERS_PATH':str(root/'.runtime/playwright'),
               'BROWSER_TYPE':'chromium', 'BROWSER_HEADLESS':'true', 'BROWSER_CHANNEL':''}
        command = [sys.executable, '-m', 'pytest', *args, str(root/'tools/office-regression/tests')]
        identity = {'started_at':time.time(), 'python':sys.executable, 'command':command, 'platform':sys.platform}
        status = 1
        try:
            with (out/'pytest.log').open('w') as log:
                process = subprocess.Popen(command, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
                try:
                    for line in process.stdout:
                        log.write(line); log.flush(); print(line, end='', flush=True)
                    status = process.wait()
                except BaseException:
                    process.terminate()
                    try: process.wait(timeout=10)
                    except subprocess.TimeoutExpired: process.kill(); process.wait()
                    raise
            return status
        finally:
            identity.update(exit_code=status, result='passed' if status == 0 else 'failed', finished_at=time.time())
            (out/'run-identity.json').write_text(json.dumps(identity, indent=2)+'\n')
            (out/'listing.txt').write_text('\n'.join(str(p.relative_to(out)) for p in sorted(out.rglob('*')) if p.is_file())+'\n')
    finally:
        lock.rmdir()


def main() -> int:
    root = Path(__file__).resolve().parents[2]
    out = Path(os.environ.get('OFFICE_REGRESSION_OUT') or tempfile.mkdtemp(prefix='zkcode-office-'))
    args = sys.argv[1:] or shlex.split(os.environ.get('R12_PYTEST_ARGS', '-q -p no:cacheprovider --timeout=600'))
    try:
        result = run(root, out, args)
        print('Office evidence: ' + str(out))
        return result
    except (ValueError, OSError) as error:
        print('Office regression blocked: ' + str(error), file=sys.stderr)
        return 2

if __name__ == '__main__':
    sys.exit(main())
