#!/bin/sh
set -eu

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
for COMMAND in cargo-deny gitleaks; do
  if ! command -v "$COMMAND" >/dev/null 2>&1; then
    echo "missing required release tool: $COMMAND" >&2
    exit 2
  fi
done

"$ROOT_DIR/scripts/parity/check-contracts.sh"
"$ROOT_DIR/scripts/parity/scan-release-secrets.sh"
(cd "$ROOT_DIR" && cargo fmt --all -- --check)
(cd "$ROOT_DIR" && cargo clippy --workspace --all-targets --locked -- -D warnings)
(cd "$ROOT_DIR" && cargo test --workspace --locked)
(cd "$ROOT_DIR" && cargo deny check)
(cd "$ROOT_DIR" && cargo build --workspace --release --locked)
(cd "$ROOT_DIR/frontend" && npm run lint)
(cd "$ROOT_DIR/frontend" && npm run test:run)
(cd "$ROOT_DIR/frontend" && npm run build)
"$ROOT_DIR/scripts/parity/npm-audit.sh"
(cd "$ROOT_DIR/python-service" && .venv/bin/python -m pytest --cov=src --cov-fail-under=70)
