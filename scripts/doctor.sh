#!/bin/sh
set -eu

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
. "$ROOT_DIR/scripts/macos-toolchain-env.sh"
zk_use_macos_toolchain
FAILED=0
DIAGNOSTIC_PORT=8081

check_command() {
    if command -v "$1" >/dev/null 2>&1; then
        printf 'ok   %-18s %s\n' "$1" "$(command -v "$1")"
    else
        printf 'fail %-18s missing\n' "$1" >&2
        FAILED=1
    fi
}

echo "zkcode macOS local diagnostics"
echo "system: $(sw_vers -productVersion 2>/dev/null || uname -r) / $(uname -m)"
check_command rustc
check_command cargo
check_command node
check_command npm
check_command curl

if [ -x "$ROOT_DIR/python-service/.venv/bin/python" ]; then
    PY_VERSION=$($ROOT_DIR/python-service/.venv/bin/python -c 'import sys; print(".".join(map(str, sys.version_info[:3])))')
    echo "ok   python venv        $PY_VERSION"
else
    echo "fail python venv        run ./scripts/setup-macos.sh" >&2
    FAILED=1
fi

if [ -f "$ROOT_DIR/.env" ]; then
    echo "ok   .env               present"
    if grep -Eq '^(ZK_LLM_API_KEY|LLM_PROVIDER_[A-Z0-9_]+_API_KEY)=.+$' "$ROOT_DIR/.env"; then
        echo "ok   LLM credentials    configured (value hidden)"
    else
        echo "warn LLM credentials    none configured; chat requests will fail"
    fi
    set -a
    . "$ROOT_DIR/.env"
    set +a
    DIAGNOSTIC_PORT=${ZK_PORT:-8081}
    case "$DIAGNOSTIC_PORT" in
        *[!0-9]*|'') echo "fail ZK_PORT            must be numeric" >&2; FAILED=1; DIAGNOSTIC_PORT=8081 ;;
    esac
else
    echo "fail .env               run ./scripts/setup-macos.sh" >&2
    FAILED=1
fi

if curl -fsS --max-time 2 "http://127.0.0.1:$DIAGNOSTIC_PORT/api/health" >/dev/null 2>&1; then
    echo "ok   backend            http://127.0.0.1:$DIAGNOSTIC_PORT"
else
    echo "info backend            not running"
fi
if curl -fsS --max-time 2 http://127.0.0.1:5273/ >/dev/null 2>&1; then
    echo "ok   frontend           http://127.0.0.1:5273"
else
    echo "info frontend           not running"
fi

if [ "$FAILED" -ne 0 ]; then
    exit 1
fi
