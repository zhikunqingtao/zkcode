#!/bin/sh
set -eu

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
printf 'zkcode source checkout detected; using the unified developer bootstrap.\n'
exec "$ROOT_DIR/dev" bootstrap --start "$@"
