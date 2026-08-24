#!/bin/sh
set -eu

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
. "$ROOT_DIR/scripts/macos-toolchain-env.sh"
zk_use_macos_toolchain

cd "$ROOT_DIR/frontend"
exec "$ROOT_DIR/frontend/node_modules/.bin/vite" --host 127.0.0.1 --port 5273
