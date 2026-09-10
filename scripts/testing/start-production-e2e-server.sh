#!/bin/sh
set -eu

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
SERVER_PORT=${ZK_E2E_SERVER_PORT:?ZK_E2E_SERVER_PORT is required}
PROVIDER_PORT=${ZK_E2E_PROVIDER_PORT:?ZK_E2E_PROVIDER_PORT is required}
FRONTEND_PORT=${ZK_E2E_FRONTEND_PORT:?ZK_E2E_FRONTEND_PORT is required}
FIXTURE_ROOT=$(mktemp -d "${TMPDIR:-/tmp}/zkcode-production-e2e.XXXXXX")
SERVER_PID=

cleanup() {
  trap - EXIT INT TERM
  if [ -n "$SERVER_PID" ]; then
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  rm -rf "$FIXTURE_ROOT"
}
trap cleanup EXIT INT TERM

mkdir -p "$FIXTURE_ROOT/workspace" "$FIXTURE_ROOT/snapshots"

SERVER_BINARY=${ZK_E2E_SERVER_BINARY:-$ROOT_DIR/target/debug/zk-server}
if [ ! -x "$SERVER_BINARY" ] && [ -x "$ROOT_DIR/target/release/zk-server" ]; then
  SERVER_BINARY=$ROOT_DIR/target/release/zk-server
fi
if [ ! -x "$SERVER_BINARY" ]; then
  (cd "$ROOT_DIR" && cargo build -p zk-server --locked)
  SERVER_BINARY=$ROOT_DIR/target/debug/zk-server
fi

# Session creation resolves an unbound workspace from the server process CWD.
# Run the fixture from its isolated workspace so even an accidental tool call
# cannot address the repository checkout.
cd "$FIXTURE_ROOT/workspace"

ZK_HOST=127.0.0.1 \
ZK_PORT="$SERVER_PORT" \
ZK_DB_PATH="$FIXTURE_ROOT/runtime.sqlite3" \
ZK_DEMO_CREDENTIAL_DB="$ROOT_DIR/configuration/bootstrap/demo-credentials.db" \
ZK_DEV_ALLOW_DEMO_CREDENTIAL=0 \
ZK_DEFAULT_MODEL=qwen3.8-max-0902 \
ZK_LLM_BASE_URL="http://127.0.0.1:$PROVIDER_PORT/v1" \
ZK_LLM_API_KEY=local-fixture-token \
ZK_WORKSPACE_DEFAULT_ROOT="$FIXTURE_ROOT/workspace" \
ZK_WORKSPACE_ALLOWED_ROOTS="$FIXTURE_ROOT/workspace" \
ZK_CORS_ALLOWED_ORIGINS="http://127.0.0.1:$FRONTEND_PORT" \
ZK_SNAPSHOT_DIR="$FIXTURE_ROOT/snapshots" \
ZK_SCRATCHPAD_SYSTEM_ROOT="$FIXTURE_ROOT/scratchpad" \
MCP_REGISTRY_PATH="$FIXTURE_ROOT/mcp-capabilities.json" \
ZK_STATIC_DIR="$ROOT_DIR/crates/zk-server/resources/static" \
ZK_LOG=warn \
ZK_LOG_FILE="$FIXTURE_ROOT/zk-server.jsonl" \
ZK_PYTHON_ENABLED=false \
ZK_AGENT_ENABLED=false \
ZK_AGENT_WRITE_ENABLED=false \
ZK_SHARED_WORKSPACE_ENABLED=false \
ZK_AUTO_RESUME_SAFE_TASKS=false \
ZK_CRON_ENABLED=false \
ZK_WORKTREE_ENABLED=false \
ZK_SWARM_ENABLED=false \
"$SERVER_BINARY" &
SERVER_PID=$!
wait "$SERVER_PID"
