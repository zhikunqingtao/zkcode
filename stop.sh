#!/bin/sh
set -eu

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
RUNTIME_DIR="$ROOT_DIR/.runtime"

stop_one() {
    NAME=$1
    PID_FILE=$2
    EXPECTED=$3
    if [ ! -f "$PID_FILE" ]; then
        echo "$NAME is not recorded as running."
        return
    fi
    PID=$(sed -n '1p' "$PID_FILE")
    case "$PID" in
        *[!0-9]*|'') echo "Refusing invalid PID in $PID_FILE" >&2; return 1 ;;
    esac
    if ! kill -0 "$PID" 2>/dev/null; then
        rm -f "$PID_FILE"
        echo "$NAME was already stopped."
        return
    fi
    COMMAND=$(ps -p "$PID" -o command= 2>/dev/null || true)
    case "$COMMAND" in
        *"$EXPECTED"*) ;;
        *) echo "Refusing to stop PID $PID: command does not match $EXPECTED" >&2; return 1 ;;
    esac
    kill -TERM "$PID"
    COUNT=0
    while kill -0 "$PID" 2>/dev/null && [ "$COUNT" -lt 10 ]; do
        COUNT=$((COUNT + 1))
        sleep 1
    done
    if kill -0 "$PID" 2>/dev/null; then
        kill -KILL "$PID"
    fi
    rm -f "$PID_FILE"
    echo "Stopped $NAME."
}

stop_one frontend "$RUNTIME_DIR/frontend.pid" "vite"
stop_one backend "$RUNTIME_DIR/backend.pid" "/target/debug/zk-server"
stop_one python-sidecar "$RUNTIME_DIR/python.pid" "src.main:app"
