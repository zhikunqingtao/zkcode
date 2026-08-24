#!/bin/sh
set -eu

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
. "$ROOT_DIR/scripts/macos-toolchain-env.sh"
zk_use_macos_toolchain
RUNTIME_DIR="$ROOT_DIR/.runtime"
BACKEND_PID_FILE="$RUNTIME_DIR/backend.pid"
FRONTEND_PID_FILE="$RUNTIME_DIR/frontend.pid"
PYTHON_PID_FILE="$RUNTIME_DIR/python.pid"
BACKEND_LOG="$RUNTIME_DIR/backend.log"
FRONTEND_LOG="$RUNTIME_DIR/frontend.log"

if [ "$(uname -s)" != "Darwin" ]; then
    echo "zkcode Beta supports local macOS installation only." >&2
    exit 1
fi
if [ ! -f "$ROOT_DIR/.env" ]; then
    echo "Missing .env. Run ./scripts/setup-macos.sh first." >&2
    exit 1
fi
if [ ! -x "$ROOT_DIR/python-service/.venv/bin/python" ] || [ ! -d "$ROOT_DIR/frontend/node_modules" ]; then
    echo "Dependencies are incomplete. Run ./scripts/setup-macos.sh first." >&2
    exit 1
fi

is_live_pid_file() {
    [ -f "$1" ] || return 1
    PID=$(sed -n '1p' "$1")
    case "$PID" in *[!0-9]*|'') return 1 ;; esac
    kill -0 "$PID" 2>/dev/null
}

if is_live_pid_file "$BACKEND_PID_FILE" || is_live_pid_file "$FRONTEND_PID_FILE"; then
    echo "zkcode already appears to be running. Run ./stop.sh first." >&2
    exit 1
fi

mkdir -p "$RUNTIME_DIR"
set -a
# .env is a user-owned local configuration file. It must contain shell-style KEY=VALUE entries.
. "$ROOT_DIR/.env"
set +a
ZK_PORT=${ZK_PORT:-8081}
PYTHON_ENABLED=${ZK_PYTHON_ENABLED:-true}
PYTHON_SOCKET=${ZK_PYTHON_UDS:-$RUNTIME_DIR/python.sock}
case "$PYTHON_SOCKET" in
    '~/'*) PYTHON_SOCKET="$HOME/${PYTHON_SOCKET#\~/}" ;;
esac

cleanup_failed_start() {
    "$ROOT_DIR/stop.sh" >/dev/null 2>&1 || true
}
trap cleanup_failed_start INT TERM HUP

echo "Starting zkcode backend on http://127.0.0.1:$ZK_PORT ..."
BACKEND_PID=$("$ROOT_DIR/python-service/.venv/bin/python" "$ROOT_DIR/scripts/spawn-detached.py" \
    --working-directory "$ROOT_DIR" --log "$BACKEND_LOG" \
    "$ROOT_DIR/scripts/run-backend-macos.sh")
case "$BACKEND_PID" in
    *[!0-9]*|'') echo "Could not resolve the backend PID." >&2; cleanup_failed_start; exit 1 ;;
esac
echo "$BACKEND_PID" >"$BACKEND_PID_FILE"

READY=0
COUNT=0
while [ "$COUNT" -lt 90 ]; do
    if curl -fsS --max-time 2 "http://127.0.0.1:$ZK_PORT/api/health" >/dev/null 2>&1; then
        READY=1
        break
    fi
    COUNT=$((COUNT + 1))
    sleep 1
done
if [ "$READY" -ne 1 ]; then
    echo "Backend did not become healthy. See $BACKEND_LOG" >&2
    cleanup_failed_start
    exit 1
fi

if [ "$PYTHON_ENABLED" = "true" ]; then
    READY=0
    COUNT=0
    while [ "$COUNT" -lt 30 ]; do
        HEALTH_JSON=$(curl -fsS --max-time 2 "http://127.0.0.1:$ZK_PORT/api/health" 2>/dev/null || true)
        if printf '%s' "$HEALTH_JSON" | "$ROOT_DIR/python-service/.venv/bin/python" -c \
            'import json, sys; raise SystemExit(0 if json.load(sys.stdin)["subsystems"]["python"]["status"] == "UP" else 1)' \
            >/dev/null 2>&1; then
            READY=1
            break
        fi
        COUNT=$((COUNT + 1))
        sleep 1
    done
    if [ "$READY" -ne 1 ]; then
        echo "Python sidecar did not become healthy. See $BACKEND_LOG" >&2
        cleanup_failed_start
        exit 1
    fi
    PYTHON_PID=$(lsof -t "$PYTHON_SOCKET" 2>/dev/null | sed -n '1p')
    case "$PYTHON_PID" in
        *[!0-9]*|'') echo "Could not resolve the Python sidecar PID." >&2; cleanup_failed_start; exit 1 ;;
    esac
    PYTHON_COMMAND=$(ps -p "$PYTHON_PID" -o command= 2>/dev/null || true)
    case "$PYTHON_COMMAND" in
        *"src.main:app"*"--uds"*) ;;
        *) echo "Refusing unexpected process at Python socket: $PYTHON_COMMAND" >&2; cleanup_failed_start; exit 1 ;;
    esac
    echo "$PYTHON_PID" >"$PYTHON_PID_FILE"
fi

echo "Starting zkcode frontend on http://127.0.0.1:5273 ..."
FRONTEND_PID=$("$ROOT_DIR/python-service/.venv/bin/python" "$ROOT_DIR/scripts/spawn-detached.py" \
    --working-directory "$ROOT_DIR/frontend" --log "$FRONTEND_LOG" \
    "$ROOT_DIR/scripts/run-frontend-macos.sh")
case "$FRONTEND_PID" in
    *[!0-9]*|'') echo "Could not resolve the frontend PID." >&2; cleanup_failed_start; exit 1 ;;
esac
echo "$FRONTEND_PID" >"$FRONTEND_PID_FILE"

READY=0
COUNT=0
while [ "$COUNT" -lt 30 ]; do
    if curl -fsS --max-time 2 http://127.0.0.1:5273/ >/dev/null 2>&1; then
        READY=1
        break
    fi
    COUNT=$((COUNT + 1))
    sleep 1
done
if [ "$READY" -ne 1 ]; then
    echo "Frontend did not become healthy. See $FRONTEND_LOG" >&2
    cleanup_failed_start
    exit 1
fi
trap - INT TERM HUP
echo "zkcode is ready: http://127.0.0.1:5273"
echo "Logs: $RUNTIME_DIR"
