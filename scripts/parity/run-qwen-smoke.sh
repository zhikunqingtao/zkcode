#!/bin/sh
set -eu

# Real, bounded qwen3.8-max connectivity gate. The selected .env is parsed as
# data: it is never sourced, and only the exact Token Plan allowlist key is read.
ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
REFERENCE_ENV=${ZK_REFERENCE_ENV:-$ROOT_DIR/.env}
MODEL=${ZK_REAL_DASHSCOPE_TOKEN_PLAN_MODEL:-qwen3.8-max}

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

echo "[qwen 1/1] real DashScope Token Plan stream (30s request limit, model=$MODEL)"
(cd "$ROOT_DIR" && cargo test -p zk-llm --test real_connectivity \
    dashscope_token_plan_live -- --ignored --exact --nocapture)
