#!/bin/sh
set -eu

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
export PLAYWRIGHT_BROWSERS_PATH=${PLAYWRIGHT_BROWSERS_PATH:-$ROOT_DIR/.runtime/playwright}
for COMMAND in cargo-deny gitleaks; do
  if ! command -v "$COMMAND" >/dev/null 2>&1; then
    echo "missing required release tool: $COMMAND" >&2
    exit 2
  fi
done

"$ROOT_DIR/scripts/parity/check-contracts.sh"
"$ROOT_DIR/scripts/parity/scan-release-secrets.sh"
(cd "$ROOT_DIR" && cargo fmt --all -- --check)
(cd "$ROOT_DIR" && cargo clippy --workspace --all-targets --all-features --locked -- -D warnings)
(cd "$ROOT_DIR" && ZK_RUN_GIT_TESTS=true cargo test --workspace --locked)
(cd "$ROOT_DIR" && ZK_RUN_GIT_TESTS=true cargo test -p zk-engine --no-default-features --locked)
(cd "$ROOT_DIR" && cargo test -p zk-tools --test lsp_native --locked -- --ignored)
(cd "$ROOT_DIR" && cargo test -p zk-server --test verify_journey_native --locked -- --ignored)
(cd "$ROOT_DIR" && cargo test -p zk-mcp --lib oauth::tests::native_keychain_round_trip --locked -- --ignored --exact --nocapture)
(cd "$ROOT_DIR" && cargo deny check)
(cd "$ROOT_DIR" && cargo build --workspace --release --locked)
(cd "$ROOT_DIR/frontend" && npm run lint)
(cd "$ROOT_DIR/frontend" && npm run test:run)
(cd "$ROOT_DIR/frontend" && npm run build)
(cd "$ROOT_DIR/frontend" && npm run test:theme-regression)
(cd "$ROOT_DIR/frontend" && npm run test:jelly-regression)
(cd "$ROOT_DIR/frontend" && ZK_E2E_SERVER_BINARY="$ROOT_DIR/target/release/zk-server" npm run test:e2e:production)
(cd "$ROOT_DIR/frontend" && ZK_E2E_SERVER_BINARY="$ROOT_DIR/target/release/zk-server" npm run test:e2e:analysis)
"$ROOT_DIR/scripts/parity/npm-audit.sh"
(cd "$ROOT_DIR/python-service" && .venv/bin/python -m pytest --cov=src --cov-fail-under=70)
"$ROOT_DIR/dev" test office
"$ROOT_DIR/dev" doctor --deep --json
