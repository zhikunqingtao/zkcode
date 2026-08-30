#!/bin/sh
set -eu

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
RUNTIME_DIR="$ROOT_DIR/.runtime"

. "$ROOT_DIR/scripts/macos-toolchain-env.sh"
zk_use_macos_toolchain

ENV_PYTHON="$ROOT_DIR/python-service/.venv/bin/python"
[ -x "$ENV_PYTHON" ] || {
    echo "Missing project Python environment. Run ./dev sync." >&2
    exit 16
}

PYTHON_SOCKET=${ZK_DEV_PYTHON_SOCKET:-$RUNTIME_DIR/python.sock}

cd "$ROOT_DIR"
exec "$ENV_PYTHON" "$ROOT_DIR/scripts/dev/exec-env.py" --file "$ROOT_DIR/.env" \
    --canonical-zero-one ZK_DEV_ALLOW_DEMO_CREDENTIAL=0 \
    --set ZK_HOST=127.0.0.1 \
    --set ZK_AUTH_MODE=localhost \
    --set ZK_PYTHON_SERVICE_DIR="$ROOT_DIR/python-service" \
    --set ZK_PYTHON_CMD="$ENV_PYTHON" \
    --set ZK_PYTHON_UDS="$PYTHON_SOCKET" \
    --set PLAYWRIGHT_BROWSERS_PATH="$ROOT_DIR/.runtime/playwright" \
    -- "$ROOT_DIR/target/debug/zk-server"
