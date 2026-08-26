#!/bin/sh
set -eu

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
RUNTIME_DIR="$ROOT_DIR/.runtime"

. "$ROOT_DIR/scripts/macos-toolchain-env.sh"
zk_use_macos_toolchain

set -a
. "$ROOT_DIR/.env"
set +a

export ZK_HOST=127.0.0.1
export ZK_PORT=${ZK_PORT:-8082}
export ZK_AUTH_MODE=localhost
export ZK_PYTHON_SERVICE_DIR="$ROOT_DIR/python-service"
export ZK_PYTHON_CMD="$ROOT_DIR/python-service/.venv/bin/python"
export ZK_PYTHON_UDS=${ZK_PYTHON_UDS:-$RUNTIME_DIR/python.sock}

cd "$ROOT_DIR"
exec "$ROOT_DIR/target/debug/zk-server"
