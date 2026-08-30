#!/bin/sh
set -eu

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
. "$ROOT_DIR/scripts/macos-toolchain-env.sh"
zk_use_macos_toolchain

cd "$ROOT_DIR/frontend"
export VITE_API_URL=${ZK_DEV_BACKEND_URL:-http://127.0.0.1:8082}
exec "$ROOT_DIR/frontend/node_modules/.bin/vite" --host 127.0.0.1 --port 5273 --strictPort
