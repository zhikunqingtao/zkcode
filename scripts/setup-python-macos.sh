#!/bin/sh
set -eu

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
. "$ROOT_DIR/scripts/macos-toolchain-env.sh"
zk_use_macos_toolchain
SERVICE_DIR="$ROOT_DIR/python-service"
VENV_DIR="$SERVICE_DIR/.venv"
PYCACHE_DIR="$VENV_DIR/pycache"

PYTHON_BIN=""
for candidate in python3.11 python3.12; do
    if command -v "$candidate" >/dev/null 2>&1; then
        PYTHON_BIN=$(command -v "$candidate")
        break
    fi
done
if [ -z "$PYTHON_BIN" ]; then
    echo "Python 3.11 or 3.12 is required." >&2
    exit 1
fi

VERSION=$($PYTHON_BIN -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
case "$VERSION" in
    3.11|3.12) ;;
    *) echo "Unsupported Python version: $VERSION" >&2; exit 1 ;;
esac

"$PYTHON_BIN" -m venv "$VENV_DIR"
VENV_PYTHON="$VENV_DIR/bin/python"
"$VENV_PYTHON" -m pip install --disable-pip-version-check -r "$SERVICE_DIR/requirements.lock"
(cd "$SERVICE_DIR" && "$VENV_PYTHON" -m pip install --disable-pip-version-check --no-deps -e '.[full,test]')
echo "Installing the Playwright Chromium runtime pinned by requirements.lock..."
"$VENV_PYTHON" -m playwright install chromium
(cd "$SERVICE_DIR" && PYTHONPATH=./src PYTHONPYCACHEPREFIX="$PYCACHE_DIR" "$VENV_PYTHON" -m compileall -q src)
(cd "$SERVICE_DIR" && PYTHONPATH=./src PYTHONPYCACHEPREFIX="$PYCACHE_DIR" "$VENV_PYTHON" -c 'import importlib, uvicorn, fastapi, pydantic; import src.main; [importlib.import_module(name) for name in ("routers.code_intel", "routers.file_processing", "routers.git_enhanced", "routers.browser", "routers.code_quality", "routers.analysis", "routers.http_api")]; print("python-import-smoke: ok")')

echo "Python sidecar environment ready: $VENV_DIR"
