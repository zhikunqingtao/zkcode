#!/bin/sh
set -eu

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
SERVICE_DIR="$ROOT_DIR/python-service"
VENV_PYTHON="$SERVICE_DIR/.venv/bin/python"
PYCACHE_DIR="$SERVICE_DIR/.venv/pycache"

if [ ! -x "$VENV_PYTHON" ]; then
    echo "Missing project Python environment. Run scripts/setup-python-macos.sh." >&2
    exit 1
fi
VERSION=$($VENV_PYTHON -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
case "$VERSION" in
    3.11|3.12) ;;
    *) echo "Unsupported project Python version: $VERSION" >&2; exit 1 ;;
esac
(cd "$SERVICE_DIR" && PYTHONPATH=./src PYTHONPYCACHEPREFIX="$PYCACHE_DIR" "$VENV_PYTHON" -c 'import importlib, uvicorn, fastapi, pydantic; import src.main; [importlib.import_module(name) for name in ("routers.code_intel", "routers.file_processing", "routers.git_enhanced", "routers.browser", "routers.code_quality", "routers.analysis", "routers.http_api")]')
echo "python-sidecar-check: ok ($VERSION)"
