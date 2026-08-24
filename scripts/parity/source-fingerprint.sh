#!/bin/sh
set -eu

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
{
  find "$ROOT_DIR/crates" "$ROOT_DIR/frontend/src" "$ROOT_DIR/frontend/tests" \
    "$ROOT_DIR/frontend/e2e" "$ROOT_DIR/python-service" "$ROOT_DIR/scripts" \
    -type f \
    ! -path '*/node_modules/*' ! -path '*/.venv/*' ! -path '*/__pycache__/*' \
    ! -path '*/dist/*' ! -path '*/coverage/*' \
    \( -name '*.rs' -o -name '*.ts' -o -name '*.tsx' -o -name '*.py' \
       -o -name '*.sql' -o -name '*.toml' -o -name '*.sh' \) -print0 2>/dev/null
  for FILE in \
    "$ROOT_DIR/Cargo.toml" "$ROOT_DIR/Cargo.lock" "$ROOT_DIR/rust-toolchain.toml" \
    "$ROOT_DIR/rustfmt.toml" "$ROOT_DIR/frontend/package.json" \
    "$ROOT_DIR/frontend/package-lock.json" "$ROOT_DIR/frontend/eslint.config.js" \
    "$ROOT_DIR/frontend/tsconfig.json" "$ROOT_DIR/frontend/vite.config.ts" \
    "$ROOT_DIR/frontend/vitest.config.ts" "$ROOT_DIR/python-service/pyproject.toml"
  do
    if [ -f "$FILE" ]; then
      printf '%s\0' "$FILE"
    fi
  done
} | LC_ALL=C sort -zu | xargs -0 shasum -a 256
