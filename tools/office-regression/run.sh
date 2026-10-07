#!/bin/sh
set -eu
TOOLS_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
ROOT_DIR=$(CDPATH= cd -- "$TOOLS_DIR/../.." && pwd)
. "$ROOT_DIR/scripts/macos-toolchain-env.sh"
zk_use_macos_toolchain
exec "$ROOT_DIR/python-service/.venv/bin/python" "$TOOLS_DIR/native_runner.py" "$@"
