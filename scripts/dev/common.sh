#!/bin/sh

# Shared primitives for the source-development CLI. This file is sourced by
# the other scripts; it intentionally does not enable shell options itself.

if [ -z "${ROOT_DIR:-}" ]; then
    ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
fi

DEV_RUNTIME_DIR="$ROOT_DIR/.runtime"
DEV_STATE_DIR="$DEV_RUNTIME_DIR/dev"
DEV_STATE_FILE="$DEV_STATE_DIR/dev-state.json"
DEV_LOCK_DIR="$DEV_STATE_DIR/operation.lock"
DEV_PREVIOUS_DIR="$DEV_STATE_DIR/previous"
DEV_FRONTEND_PORT=5273
DEV_DEFAULT_BACKEND_PORT=8082
DEV_PYTHON_READY_SECONDS=90
DEV_LOCK_HELD=0
DEV_START_TRANSACTION_ACTIVE=0

dev_info() {
    printf '\n==> %s\n' "$*"
}

dev_note() {
    printf '    %s\n' "$*"
}

dev_warn() {
    printf 'warning: %s\n' "$*" >&2
}

dev_fail() {
    DEV_FAIL_CODE=$1
    shift
    printf 'error: %s\n' "$*" >&2
    exit "$DEV_FAIL_CODE"
}

dev_require_file() {
    [ -f "$1" ] || dev_fail 2 "required source file is missing: ${1#$ROOT_DIR/}"
}

dev_now_id() {
    date -u '+%Y%m%dT%H%M%SZ'
}

dev_terminate_process_tree() {
    DEV_TREE_PID=$1
    DEV_TREE_SIGNAL=$2
    if command -v pgrep >/dev/null 2>&1; then
        for DEV_TREE_CHILD in $(pgrep -P "$DEV_TREE_PID" 2>/dev/null || true); do
            dev_terminate_process_tree "$DEV_TREE_CHILD" "$DEV_TREE_SIGNAL"
        done
    fi
    kill -"$DEV_TREE_SIGNAL" "$DEV_TREE_PID" 2>/dev/null || true
}

dev_run_bounded() {
    DEV_BOUND_SECONDS=$1
    DEV_BOUND_LABEL=$2
    shift 2
    DEV_BOUND_MARKER=$(mktemp "${TMPDIR:-/tmp}/zkcode-dev-timeout.XXXXXX") || {
        dev_warn "could not create timeout marker for $DEV_BOUND_LABEL"
        return 1
    }

    "$@" &
    DEV_BOUND_PID=$!
    (
        sleep "$DEV_BOUND_SECONDS"
        if kill -0 "$DEV_BOUND_PID" 2>/dev/null; then
            printf 'timeout\n' >"$DEV_BOUND_MARKER"
            dev_terminate_process_tree "$DEV_BOUND_PID" TERM
            sleep 5
            if kill -0 "$DEV_BOUND_PID" 2>/dev/null; then
                dev_terminate_process_tree "$DEV_BOUND_PID" KILL
            fi
        fi
    ) &
    DEV_BOUND_WATCHDOG=$!

    if wait "$DEV_BOUND_PID"; then
        DEV_BOUND_STATUS=0
    else
        DEV_BOUND_STATUS=$?
    fi
    kill "$DEV_BOUND_WATCHDOG" 2>/dev/null || true
    wait "$DEV_BOUND_WATCHDOG" 2>/dev/null || true

    if [ -s "$DEV_BOUND_MARKER" ]; then
        dev_warn "$DEV_BOUND_LABEL exceeded ${DEV_BOUND_SECONDS}s and was stopped"
        DEV_BOUND_STATUS=124
    fi
    rm -f "$DEV_BOUND_MARKER"
    return "$DEV_BOUND_STATUS"
}

dev_release_lock() {
    if [ "$DEV_LOCK_HELD" -eq 1 ] && [ -d "$DEV_LOCK_DIR" ]; then
        DEV_LOCK_OWNER=$(sed -n '1p' "$DEV_LOCK_DIR/pid" 2>/dev/null || true)
        if [ "$DEV_LOCK_OWNER" = "$$" ]; then
            rm -f "$DEV_LOCK_DIR/pid"
            rmdir "$DEV_LOCK_DIR" 2>/dev/null || true
        fi
    fi
    DEV_LOCK_HELD=0
}

dev_assert_runtime_directory() {
    DEV_RUNTIME_PATH=$1
    if [ -L "$DEV_RUNTIME_PATH" ]; then
        dev_fail 13 "refusing a symbolic link in the runtime control path: $DEV_RUNTIME_PATH"
    fi
    if [ -e "$DEV_RUNTIME_PATH" ] && [ ! -d "$DEV_RUNTIME_PATH" ]; then
        dev_fail 13 "runtime control path is not a directory: $DEV_RUNTIME_PATH"
    fi
}

dev_acquire_lock() {
    dev_assert_runtime_directory "$DEV_RUNTIME_DIR"
    dev_assert_runtime_directory "$DEV_STATE_DIR"
    dev_assert_runtime_directory "$DEV_PREVIOUS_DIR"
    mkdir -p "$DEV_STATE_DIR" "$DEV_PREVIOUS_DIR"
    if [ -L "$DEV_LOCK_DIR" ]; then
        dev_fail 13 "refusing a symbolic link as the ./dev operation lock: $DEV_LOCK_DIR"
    fi
    if mkdir "$DEV_LOCK_DIR" 2>/dev/null; then
        printf '%s\n' "$$" >"$DEV_LOCK_DIR/pid"
        DEV_LOCK_HELD=1
        trap 'dev_release_lock' EXIT HUP INT TERM
        return 0
    fi

    DEV_EXISTING_OWNER=$(sed -n '1p' "$DEV_LOCK_DIR/pid" 2>/dev/null || true)
    case "$DEV_EXISTING_OWNER" in
        ''|*[!0-9]*) DEV_EXISTING_OWNER= ;;
    esac
    if [ -n "$DEV_EXISTING_OWNER" ] && kill -0 "$DEV_EXISTING_OWNER" 2>/dev/null; then
        dev_fail 20 "another ./dev operation is running (PID $DEV_EXISTING_OWNER)"
    fi

    dev_warn "removing a stale ./dev operation lock"
    rm -f "$DEV_LOCK_DIR/pid"
    rmdir "$DEV_LOCK_DIR" 2>/dev/null || dev_fail 20 "cannot recover stale operation lock: $DEV_LOCK_DIR"
    mkdir "$DEV_LOCK_DIR" || dev_fail 20 "cannot acquire operation lock"
    printf '%s\n' "$$" >"$DEV_LOCK_DIR/pid"
    DEV_LOCK_HELD=1
    trap 'dev_release_lock' EXIT HUP INT TERM
}

dev_assert_generated_path() {
    case "$1" in
        "$ROOT_DIR/frontend/node_modules"|\
        "$ROOT_DIR/python-service/.venv"|\
        "$ROOT_DIR/.runtime/playwright"|\
        "$ROOT_DIR/frontend/dist"|\
        "$ROOT_DIR/target") return 0 ;;
        *) dev_fail 13 "refusing generated-directory operation outside the allowlist: $1" ;;
    esac
}

dev_remove_generated() {
    dev_assert_generated_path "$1"
    [ ! -e "$1" ] || rm -rf -- "$1"
}

dev_remove_previous() {
    DEV_PREVIOUS_PATH=$1
    case "$DEV_PREVIOUS_PATH" in
        "$DEV_PREVIOUS_DIR"/*) ;;
        *) dev_fail 13 "refusing cleanup outside the previous-component directory: $DEV_PREVIOUS_PATH" ;;
    esac
    DEV_PREVIOUS_NAME=${DEV_PREVIOUS_PATH#"$DEV_PREVIOUS_DIR"/}
    case "$DEV_PREVIOUS_NAME" in
        ''|.|..|*/*) dev_fail 13 "refusing a non-direct previous-component path: $DEV_PREVIOUS_PATH" ;;
    esac
    [ ! -e "$DEV_PREVIOUS_PATH" ] || rm -rf -- "$DEV_PREVIOUS_PATH"
}

dev_ensure_env() {
    if [ ! -f "$ROOT_DIR/.env" ]; then
        cp "$ROOT_DIR/.env.example" "$ROOT_DIR/.env"
        dev_note "created .env from .env.example; configure your own API key before real chat validation"
    fi
}

dev_env_python() {
    if [ -x "$ROOT_DIR/python-service/.venv/bin/python" ]; then
        printf '%s\n' "$ROOT_DIR/python-service/.venv/bin/python"
    elif [ -n "${DEV_PYTHON:-}" ] && [ -x "$DEV_PYTHON" ]; then
        printf '%s\n' "$DEV_PYTHON"
    elif command -v python3.11 >/dev/null 2>&1; then
        command -v python3.11
    else
        return 1
    fi
}

dev_env_get() {
    DEV_ENV_READER=$(dev_env_python) || dev_fail 14 "Python 3.11 is required to parse .env safely"
    "$DEV_ENV_READER" "$ROOT_DIR/scripts/dev/exec-env.py" \
        --file "$ROOT_DIR/.env" --get "$1" --default "${2:-}"
}

dev_backend_port() {
    if [ ! -f "$ROOT_DIR/.env" ]; then
        printf '%s\n' "$DEV_DEFAULT_BACKEND_PORT"
        return 0
    fi
    DEV_PORT=$(dev_env_get ZK_PORT "$DEV_DEFAULT_BACKEND_PORT")
    case "$DEV_PORT" in
        ''|*[!0-9]*) dev_fail 2 "ZK_PORT must be a numeric TCP port" ;;
    esac
    [ "$DEV_PORT" -ge 1 ] 2>/dev/null && [ "$DEV_PORT" -le 65535 ] 2>/dev/null || \
        dev_fail 2 "ZK_PORT must be between 1 and 65535"
    printf '%s\n' "$DEV_PORT"
}

dev_backend_port_for_diagnostics() {
    if [ ! -f "$ROOT_DIR/.env" ]; then
        printf '%s\n' "$DEV_DEFAULT_BACKEND_PORT"
        return 0
    fi
    DEV_DIAGNOSTIC_READER=$(dev_env_python) || {
        printf '%s\n' "$DEV_DEFAULT_BACKEND_PORT"
        return 0
    }
    DEV_DIAGNOSTIC_PORT=$(
        "$DEV_DIAGNOSTIC_READER" "$ROOT_DIR/scripts/dev/exec-env.py" \
            --file "$ROOT_DIR/.env" --get ZK_PORT \
            --default "$DEV_DEFAULT_BACKEND_PORT" 2>/dev/null
    ) || DEV_DIAGNOSTIC_PORT=$DEV_DEFAULT_BACKEND_PORT
    case "$DEV_DIAGNOSTIC_PORT" in
        ''|*[!0-9]*) DEV_DIAGNOSTIC_PORT=$DEV_DEFAULT_BACKEND_PORT ;;
        *)
            if [ "$DEV_DIAGNOSTIC_PORT" -lt 1 ] 2>/dev/null || \
               [ "$DEV_DIAGNOSTIC_PORT" -gt 65535 ] 2>/dev/null; then
                DEV_DIAGNOSTIC_PORT=$DEV_DEFAULT_BACKEND_PORT
            fi
            ;;
    esac
    printf '%s\n' "$DEV_DIAGNOSTIC_PORT"
}

dev_python_socket() {
    DEV_SOCKET=$(dev_env_get ZK_PYTHON_UDS "$DEV_RUNTIME_DIR/python.sock")
    case "$DEV_SOCKET" in
        '~/'*) DEV_SOCKET="$HOME/${DEV_SOCKET#\~/}" ;;
        /*) ;;
        *) DEV_SOCKET="$ROOT_DIR/$DEV_SOCKET" ;;
    esac
    printf '%s\n' "$DEV_SOCKET"
}

dev_command_version() {
    if command -v "$1" >/dev/null 2>&1; then
        "$1" --version 2>/dev/null | sed -n '1p'
    else
        printf '%s\n' missing
    fi
}
