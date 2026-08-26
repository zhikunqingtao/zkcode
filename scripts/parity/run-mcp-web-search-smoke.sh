#!/bin/sh
set -eu

# One real, bounded DashScope MCP WebSearch request. The credential is read as
# data and never printed. By default this validates the exact tracked
# first-run credential; set ZK_MCP_USE_DEMO_DB=0 to use the local .env instead.
ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
REFERENCE_ENV=${ZK_REFERENCE_ENV:-$ROOT_DIR/.env}
DEMO_DB="$ROOT_DIR/configuration/bootstrap/demo-credentials.db"

if [ "${ZK_MCP_USE_DEMO_DB:-1}" = "1" ]; then
    "$ROOT_DIR/scripts/parity/check-demo-credential-db.sh" >/dev/null
    MCP_KEY=$(sqlite3 -readonly "$DEMO_DB" \
        "SELECT api_key FROM public_demo_credentials WHERE provider='dashscope-token-plan' AND purpose='public-first-run-demo';")
    CREDENTIAL_LABEL="tracked public demo database"
else
    if [ ! -f "$REFERENCE_ENV" ]; then
        echo "MCP WebSearch smoke failed: env file not found" >&2
        exit 2
    fi
    MCP_KEY=$(awk '
BEGIN { count = 0 }
{
    line = $0
    sub(/\r$/, "", line)
    if (line ~ /^[[:space:]]*LLM_PROVIDER_DASHSCOPE_TOKEN_PLAN_API_KEY[[:space:]]*=/) {
        count++
        sub(/^[[:space:]]*LLM_PROVIDER_DASHSCOPE_TOKEN_PLAN_API_KEY[[:space:]]*=[[:space:]]*/, "", line)
        value = line
    }
}
END {
    if (count != 1) exit 3
    print value
}
' "$REFERENCE_ENV") || {
        echo "MCP WebSearch smoke failed: expected exactly one Token Plan key" >&2
        exit 2
    }
    CREDENTIAL_LABEL="local .env"
fi

case "$MCP_KEY" in
    \"*\") MCP_KEY=${MCP_KEY#\"}; MCP_KEY=${MCP_KEY%\"} ;;
    \'*\') MCP_KEY=${MCP_KEY#\'}; MCP_KEY=${MCP_KEY%\'} ;;
esac
if [ -z "$MCP_KEY" ]; then
    echo "MCP WebSearch smoke failed: selected key is blank" >&2
    exit 2
fi

export LLM_PROVIDER_DASHSCOPE_TOKEN_PLAN_API_KEY=$MCP_KEY
echo "[mcp-web-search 1/1] real DashScope search (source=$CREDENTIAL_LABEL, 45s total limit)"
(cd "$ROOT_DIR" && cargo test -p zk-server --lib \
    mcp_search::tests::dashscope_web_search_live --locked -- \
    --ignored --exact --nocapture)
