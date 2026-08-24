#!/bin/sh
set -eu

# Real, bounded Kimi connectivity gate. The selected .env is parsed as data:
# it is never sourced, and only the exact Moonshot allowlist key is accepted.
ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
REFERENCE_ENV=${ZK_REFERENCE_ENV:-$ROOT_DIR/.env}
MODEL=${ZK_REAL_MOONSHOT_MODEL:-kimi-k3}
MODE=${1:-connectivity}

if [ ! -f "$REFERENCE_ENV" ]; then
    echo "Kimi smoke failed: env file not found; set ZK_REFERENCE_ENV or create .env" >&2
    exit 2
fi

KIMI_KEY=$(awk '
BEGIN { count = 0 }
{
    line = $0
    sub(/\r$/, "", line)
    if (line ~ /^[[:space:]]*LLM_PROVIDER_MOONSHOT_API_KEY[[:space:]]*=/) {
        count++
        sub(/^[[:space:]]*LLM_PROVIDER_MOONSHOT_API_KEY[[:space:]]*=[[:space:]]*/, "", line)
        value = line
    }
}
END {
    if (count != 1) exit 3
    print value
}
' "$REFERENCE_ENV") || {
    echo "Kimi smoke failed: expected exactly one Moonshot allowlist key" >&2
    exit 2
}

case "$KIMI_KEY" in
    \"*\") KIMI_KEY=${KIMI_KEY#\"}; KIMI_KEY=${KIMI_KEY%\"} ;;
    \'*\') KIMI_KEY=${KIMI_KEY#\'}; KIMI_KEY=${KIMI_KEY%\'} ;;
esac

if [ -z "$KIMI_KEY" ]; then
    echo "Kimi smoke failed: Moonshot allowlist key is blank" >&2
    exit 2
fi

case "$KIMI_KEY" in
    *'\n'*|*'\r'*)
        echo "Kimi smoke failed: multiline credentials are not supported" >&2
        exit 2
        ;;
esac

export LLM_PROVIDER_MOONSHOT_API_KEY=$KIMI_KEY
export ZK_REAL_MOONSHOT_MODEL=$MODEL

if [ "$MODE" = "server" ]; then
    RUN_ROOT=${ZK_KIMI_RUN_ROOT:-}
    if [ -z "$RUN_ROOT" ] || [ ! -d "$RUN_ROOT/home" ] || [ ! -d "$RUN_ROOT/workspace" ]; then
        echo "Kimi server failed: ZK_KIMI_RUN_ROOT must contain home/ and workspace/" >&2
        exit 2
    fi
    RUN_ROOT=$(cd "$RUN_ROOT" && pwd -P)
    WORKSPACE=$RUN_ROOT/workspace
    export HOME=$RUN_ROOT/home
    export ZK_HOST=127.0.0.1
    export ZK_PORT=${ZK_KIMI_PORT:-18081}
    export ZK_DB_PATH=$RUN_ROOT/zkcode.sqlite
    export ZK_SNAPSHOT_DIR=$RUN_ROOT/snapshots
    export ZK_LLM_BASE_URL=https://api.moonshot.cn/v1
    export ZK_LLM_API_KEY=$KIMI_KEY
    export ZK_DEFAULT_MODEL=$MODEL
    export ZK_WORKSPACE_DEFAULT_ROOT=$WORKSPACE
    export ZK_WORKSPACE_ALLOWED_ROOTS=$WORKSPACE
    export ZK_PYTHON_ENABLED=false
    export ZK_PYTHON_UDS=$RUN_ROOT/python.sock
    export ZK_AGENT_ENABLED=true
    export ZK_AGENT_WRITE_ENABLED=true
    export ZK_SWARM_ENABLED=true
    export ZK_WORKTREE_ENABLED=false
    export MCP_REGISTRY_PATH=$RUN_ROOT/mcp-capabilities.json
    echo "[kimi server] isolated root=$RUN_ROOT port=$ZK_PORT model=$MODEL"
    exec "$PWD/target/debug/zk-server"
fi

if [ "$MODE" != "connectivity" ]; then
    echo "Kimi smoke failed: mode must be connectivity or server" >&2
    exit 2
fi

echo "[kimi 1/1] real Moonshot streaming connectivity (30s request limit)"
(cd "$ROOT_DIR" && cargo test -p zk-llm --test real_connectivity moonshot_kimi_live -- \
    --ignored --exact --nocapture
)

if [ -n "${ZK_E2E_BASE:-}" ]; then
    export ZK_E2E_MODEL=$MODEL
    echo "[kimi optional] real zk-server WS/REST/SQLite smoke (90s hard limit)"
    (cd "$ROOT_DIR" && python3 scripts/e2e_chat_smoke.py)
fi
