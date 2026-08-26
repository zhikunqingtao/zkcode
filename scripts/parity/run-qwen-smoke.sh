#!/bin/sh
set -eu

# Real, bounded qwen3.8-max connectivity gate. The selected credential source
# is read as data and its value is never printed. Set ZK_QWEN_USE_DEMO_DB=1 to
# verify the exact tracked first-run credential instead of the local .env.
ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
REFERENCE_ENV=${ZK_REFERENCE_ENV:-$ROOT_DIR/.env}
DEMO_DB="$ROOT_DIR/configuration/bootstrap/demo-credentials.db"
MODEL=${ZK_REAL_DASHSCOPE_TOKEN_PLAN_MODEL:-qwen3.8-max}

if [ "${ZK_QWEN_USE_DEMO_DB:-0}" = "1" ]; then
    "$ROOT_DIR/scripts/parity/check-demo-credential-db.sh" >/dev/null
    QWEN_KEY=$(sqlite3 -readonly "$DEMO_DB" \
        "SELECT api_key FROM public_demo_credentials WHERE provider='dashscope-token-plan' AND purpose='public-first-run-demo';")
    CREDENTIAL_LABEL="tracked public demo database"
else
    if [ ! -f "$REFERENCE_ENV" ]; then
        echo "Qwen smoke failed: env file not found; set ZK_REFERENCE_ENV or create .env" >&2
        exit 2
    fi
    QWEN_KEY=$(awk '
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
        echo "Qwen smoke failed: expected exactly one Token Plan allowlist key" >&2
        exit 2
    }
    CREDENTIAL_LABEL="local .env"
fi

case "$QWEN_KEY" in
    \"*\") QWEN_KEY=${QWEN_KEY#\"}; QWEN_KEY=${QWEN_KEY%\"} ;;
    \'*\') QWEN_KEY=${QWEN_KEY#\'}; QWEN_KEY=${QWEN_KEY%\'} ;;
esac

if [ -z "$QWEN_KEY" ]; then
    echo "Qwen smoke failed: Token Plan allowlist key is blank" >&2
    exit 2
fi

case "$QWEN_KEY" in
    *'\n'*|*'\r'*)
        echo "Qwen smoke failed: multiline credentials are not supported" >&2
        exit 2
        ;;
esac

export LLM_PROVIDER_DASHSCOPE_TOKEN_PLAN_API_KEY=$QWEN_KEY
export ZK_REAL_DASHSCOPE_TOKEN_PLAN_MODEL=$MODEL

echo "[qwen 1/1] real DashScope Token Plan stream (source=$CREDENTIAL_LABEL, 30s request limit, model=$MODEL)"
(cd "$ROOT_DIR" && cargo test -p zk-llm --test real_connectivity \
    dashscope_token_plan_live -- --ignored --exact --nocapture)
