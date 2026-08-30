#!/bin/sh

dev_pid_is_live() {
    DEV_PID_FILE=$1
    DEV_PID_EXPECTED=$2
    [ -f "$DEV_PID_FILE" ] || return 1
    DEV_PID=$(sed -n '1p' "$DEV_PID_FILE")
    case "$DEV_PID" in
        ''|*[!0-9]*) return 1 ;;
    esac
    kill -0 "$DEV_PID" 2>/dev/null || return 1
    DEV_PID_COMMAND=$(ps -p "$DEV_PID" -o command= 2>/dev/null || true)
    case "$DEV_PID_COMMAND" in
        *"$DEV_PID_EXPECTED"*) return 0 ;;
        *) return 1 ;;
    esac
}

dev_port_owner() {
    lsof -nP -iTCP:"$1" -sTCP:LISTEN 2>/dev/null | sed -n '2p'
}

dev_wait_http() {
    DEV_WAIT_URL=$1
    DEV_WAIT_SECONDS=$2
    DEV_WAIT_NOW=$(date +%s) || return 1
    case "$DEV_WAIT_NOW" in ''|*[!0-9]*) return 1 ;; esac
    case "$DEV_WAIT_SECONDS" in ''|*[!0-9]*) return 1 ;; esac
    DEV_WAIT_DEADLINE=$((DEV_WAIT_NOW + DEV_WAIT_SECONDS))
    while [ "$DEV_WAIT_NOW" -lt "$DEV_WAIT_DEADLINE" ]; do
        DEV_WAIT_REMAINING=$((DEV_WAIT_DEADLINE - DEV_WAIT_NOW))
        DEV_WAIT_MAX_TIME=3
        DEV_WAIT_CONNECT_TIME=2
        [ "$DEV_WAIT_REMAINING" -ge "$DEV_WAIT_MAX_TIME" ] || DEV_WAIT_MAX_TIME=$DEV_WAIT_REMAINING
        [ "$DEV_WAIT_REMAINING" -ge "$DEV_WAIT_CONNECT_TIME" ] || DEV_WAIT_CONNECT_TIME=$DEV_WAIT_REMAINING
        curl -fsS --connect-timeout "$DEV_WAIT_CONNECT_TIME" --max-time "$DEV_WAIT_MAX_TIME" \
            "$DEV_WAIT_URL" >/dev/null 2>&1 && return 0
        DEV_WAIT_NOW=$(date +%s) || return 1
        [ "$DEV_WAIT_NOW" -lt "$DEV_WAIT_DEADLINE" ] || break
        sleep 1
        DEV_WAIT_NOW=$(date +%s) || return 1
    done
    return 1
}

dev_python_enabled_value() {
    DEV_PYTHON_ENABLED_RAW=$(dev_env_get ZK_PYTHON_ENABLED true)
    DEV_PYTHON_ENABLED_NORMALIZED=$(printf '%s' "$DEV_PYTHON_ENABLED_RAW" | awk '
        {
            value = NR == 1 ? $0 : value "\n" $0
        }
        END {
            sub(/^[[:space:]]+/, "", value)
            sub(/[[:space:]]+$/, "", value)
            print tolower(value)
        }
    ') || dev_fail 2 "ZK_PYTHON_ENABLED must be true or false"
    case "$DEV_PYTHON_ENABLED_NORMALIZED" in
        ''|true) printf '%s\n' true ;;
        false) printf '%s\n' false ;;
        *) dev_fail 2 "ZK_PYTHON_ENABLED must be true or false" ;;
    esac
}

dev_backend_health_ready() {
    DEV_HEALTH_URL=$1
    DEV_HEALTH_PYTHON_ENABLED=$2
    case "$DEV_HEALTH_PYTHON_ENABLED" in
        false) DEV_HEALTH_EXPECTED_PYTHON_STATUS=DISABLED ;;
        true) DEV_HEALTH_EXPECTED_PYTHON_STATUS=UP ;;
        *) return 1 ;;
    esac

    DEV_HEALTH_MAX_TIME=${3:-3}
    case "$DEV_HEALTH_MAX_TIME" in
        ''|*[!0-9]*) return 1 ;;
    esac
    [ "$DEV_HEALTH_MAX_TIME" -gt 0 ] || return 1
    DEV_HEALTH_CONNECT_TIME=2
    [ "$DEV_HEALTH_MAX_TIME" -ge "$DEV_HEALTH_CONNECT_TIME" ] || \
        DEV_HEALTH_CONNECT_TIME=$DEV_HEALTH_MAX_TIME
    DEV_HEALTH_JSON=$(curl -fsS --connect-timeout "$DEV_HEALTH_CONNECT_TIME" \
        --max-time "$DEV_HEALTH_MAX_TIME" \
        "$DEV_HEALTH_URL/api/health" 2>/dev/null) || return 1
    printf '%s' "$DEV_HEALTH_JSON" | "$ROOT_DIR/python-service/.venv/bin/python" -c \
        'import json, sys; data=json.load(sys.stdin); raise SystemExit(0 if data["subsystems"]["python"]["status"] == sys.argv[1] else 1)' \
        "$DEV_HEALTH_EXPECTED_PYTHON_STATUS" \
        >/dev/null 2>&1
}

dev_wait_backend_health() {
    DEV_BACKEND_WAIT_URL=$1
    DEV_BACKEND_WAIT_PYTHON_ENABLED=$2
    DEV_BACKEND_WAIT_SECONDS=$3
    DEV_BACKEND_WAIT_NOW=$(date +%s) || return 1
    case "$DEV_BACKEND_WAIT_NOW" in ''|*[!0-9]*) return 1 ;; esac
    case "$DEV_BACKEND_WAIT_SECONDS" in ''|*[!0-9]*) return 1 ;; esac
    DEV_BACKEND_WAIT_DEADLINE=$((DEV_BACKEND_WAIT_NOW + DEV_BACKEND_WAIT_SECONDS))
    while [ "$DEV_BACKEND_WAIT_NOW" -lt "$DEV_BACKEND_WAIT_DEADLINE" ]; do
        DEV_BACKEND_WAIT_REMAINING=$((DEV_BACKEND_WAIT_DEADLINE - DEV_BACKEND_WAIT_NOW))
        DEV_BACKEND_WAIT_MAX_TIME=3
        [ "$DEV_BACKEND_WAIT_REMAINING" -ge "$DEV_BACKEND_WAIT_MAX_TIME" ] || \
            DEV_BACKEND_WAIT_MAX_TIME=$DEV_BACKEND_WAIT_REMAINING
        dev_backend_health_ready "$DEV_BACKEND_WAIT_URL" "$DEV_BACKEND_WAIT_PYTHON_ENABLED" \
            "$DEV_BACKEND_WAIT_MAX_TIME" && return 0
        DEV_BACKEND_WAIT_NOW=$(date +%s) || return 1
        [ "$DEV_BACKEND_WAIT_NOW" -lt "$DEV_BACKEND_WAIT_DEADLINE" ] || break
        sleep 1
        DEV_BACKEND_WAIT_NOW=$(date +%s) || return 1
    done
    return 1
}

dev_python_health_up() {
    dev_backend_health_ready "$1" true
}

dev_command_matches_all() {
    DEV_MATCH_COMMAND=$1
    shift
    [ "$#" -gt 0 ] || return 1
    for DEV_MATCH_EXPECTED do
        case "$DEV_MATCH_COMMAND" in
            *"$DEV_MATCH_EXPECTED"*) ;;
            *) return 1 ;;
        esac
    done
}

dev_sidecar_command_matches_socket() {
    DEV_SIDECAR_COMMAND=$1
    DEV_SIDECAR_SOCKET=$2
    printf '%s\n' "$DEV_SIDECAR_COMMAND" | "$ROOT_DIR/python-service/.venv/bin/python" -c '
import shlex
import sys

expected_python, expected_socket = sys.argv[1:]
try:
    argv = shlex.split(sys.stdin.read())
except ValueError:
    raise SystemExit(1)

if not argv or argv[0] != expected_python or "src.main:app" not in argv:
    raise SystemExit(1)
try:
    uds_index = argv.index("--uds")
except ValueError:
    raise SystemExit(1)
raise SystemExit(0 if uds_index + 1 < len(argv) and argv[uds_index + 1] == expected_socket else 1)
' "$ROOT_DIR/python-service/.venv/bin/python" "$DEV_SIDECAR_SOCKET" >/dev/null 2>&1
}

dev_stop_sidecar_owner() {
    DEV_SIDECAR_STOP_PID=$1
    DEV_SIDECAR_STOP_SOCKET=$2
    DEV_SIDECAR_STOP_PID_FILE=$3

    DEV_SIDECAR_STOP_COMMAND=$(ps -p "$DEV_SIDECAR_STOP_PID" -o command= 2>/dev/null || true)
    if ! dev_sidecar_command_matches_socket "$DEV_SIDECAR_STOP_COMMAND" "$DEV_SIDECAR_STOP_SOCKET"; then
        dev_warn "refusing to stop PID $DEV_SIDECAR_STOP_PID: command does not match the configured Python sidecar identity"
        return 1
    fi
    if ! kill -TERM "$DEV_SIDECAR_STOP_PID" 2>/dev/null; then
        kill -0 "$DEV_SIDECAR_STOP_PID" 2>/dev/null || {
            rm -f "$DEV_SIDECAR_STOP_PID_FILE"
            return 0
        }
        return 1
    fi

    DEV_SIDECAR_STOP_WAIT=0
    while kill -0 "$DEV_SIDECAR_STOP_PID" 2>/dev/null && [ "$DEV_SIDECAR_STOP_WAIT" -lt 10 ]; do
        sleep 1
        DEV_SIDECAR_STOP_WAIT=$((DEV_SIDECAR_STOP_WAIT + 1))
    done
    if kill -0 "$DEV_SIDECAR_STOP_PID" 2>/dev/null; then
        DEV_SIDECAR_STOP_COMMAND=$(ps -p "$DEV_SIDECAR_STOP_PID" -o command= 2>/dev/null || true)
        if dev_sidecar_command_matches_socket "$DEV_SIDECAR_STOP_COMMAND" "$DEV_SIDECAR_STOP_SOCKET"; then
            kill -KILL "$DEV_SIDECAR_STOP_PID"
        else
            dev_warn "refusing SIGKILL for PID $DEV_SIDECAR_STOP_PID after its command identity changed"
            return 1
        fi
    fi
    rm -f "$DEV_SIDECAR_STOP_PID_FILE"
    dev_note "stopped python-sidecar"
}

dev_stop_one() {
    DEV_STOP_NAME=$1
    DEV_STOP_PID_FILE=$2
    shift 2
    [ "$#" -gt 0 ] || {
        dev_warn "refusing to stop $DEV_STOP_NAME without an expected command identity"
        return 1
    }
    if [ ! -f "$DEV_STOP_PID_FILE" ]; then
        return 0
    fi
    DEV_STOP_PID=$(sed -n '1p' "$DEV_STOP_PID_FILE")
    case "$DEV_STOP_PID" in
        ''|*[!0-9]*)
            dev_warn "refusing invalid PID in $DEV_STOP_PID_FILE"
            return 1
            ;;
    esac
    if ! kill -0 "$DEV_STOP_PID" 2>/dev/null; then
        rm -f "$DEV_STOP_PID_FILE"
        return 0
    fi
    DEV_STOP_COMMAND=$(ps -p "$DEV_STOP_PID" -o command= 2>/dev/null || true)
    if ! dev_command_matches_all "$DEV_STOP_COMMAND" "$@"; then
        dev_warn "refusing to stop PID $DEV_STOP_PID: command does not match the expected $DEV_STOP_NAME identity"
        return 1
    fi
    kill -TERM "$DEV_STOP_PID"
    DEV_STOP_WAIT=0
    while kill -0 "$DEV_STOP_PID" 2>/dev/null && [ "$DEV_STOP_WAIT" -lt 10 ]; do
        sleep 1
        DEV_STOP_WAIT=$((DEV_STOP_WAIT + 1))
    done
    if kill -0 "$DEV_STOP_PID" 2>/dev/null; then
        DEV_STOP_COMMAND=$(ps -p "$DEV_STOP_PID" -o command= 2>/dev/null || true)
        if dev_command_matches_all "$DEV_STOP_COMMAND" "$@"; then
            kill -KILL "$DEV_STOP_PID"
        else
            dev_warn "refusing SIGKILL for PID $DEV_STOP_PID after its command identity changed"
            return 1
        fi
    fi
    rm -f "$DEV_STOP_PID_FILE"
    dev_note "stopped $DEV_STOP_NAME"
}

dev_cleanup_failed_start() {
    [ "${DEV_START_TRANSACTION_ACTIVE:-0}" -eq 1 ] || return 0
    DEV_START_TRANSACTION_ACTIVE=0
    if [ "${DEV_START_STARTED_FRONTEND:-0}" -eq 1 ]; then
        dev_stop_one frontend "$DEV_RUNTIME_DIR/frontend.pid" "$ROOT_DIR/frontend/node_modules/.bin/vite" || true
    fi
    if [ "${DEV_START_STARTED_BACKEND:-0}" -eq 1 ]; then
        dev_stop_one backend "$DEV_RUNTIME_DIR/backend.pid" "$ROOT_DIR/target/debug/zk-server" || true
        dev_stop_one python-sidecar "$DEV_RUNTIME_DIR/python.pid" \
            "$ROOT_DIR/python-service/.venv/bin/python" src.main:app --uds || true
    fi
}

dev_stop_services() {
    DEV_STOP_TARGET=${1:-all}
    DEV_STOP_FAILED=0
    case "$DEV_STOP_TARGET" in
        all)
            dev_stop_one frontend "$DEV_RUNTIME_DIR/frontend.pid" "$ROOT_DIR/frontend/node_modules/.bin/vite" || DEV_STOP_FAILED=1
            dev_stop_one backend "$DEV_RUNTIME_DIR/backend.pid" "$ROOT_DIR/target/debug/zk-server" || DEV_STOP_FAILED=1
            dev_stop_one python-sidecar "$DEV_RUNTIME_DIR/python.pid" \
                "$ROOT_DIR/python-service/.venv/bin/python" src.main:app --uds || DEV_STOP_FAILED=1
            ;;
        backend)
            dev_stop_one backend "$DEV_RUNTIME_DIR/backend.pid" "$ROOT_DIR/target/debug/zk-server" || DEV_STOP_FAILED=1
            dev_stop_one python-sidecar "$DEV_RUNTIME_DIR/python.pid" \
                "$ROOT_DIR/python-service/.venv/bin/python" src.main:app --uds || DEV_STOP_FAILED=1
            ;;
        frontend)
            dev_stop_one frontend "$DEV_RUNTIME_DIR/frontend.pid" "$ROOT_DIR/frontend/node_modules/.bin/vite" || DEV_STOP_FAILED=1
            ;;
        *) dev_fail 2 "stop target must be all, backend, or frontend" ;;
    esac
    [ "$DEV_STOP_FAILED" -eq 0 ] || dev_fail 18 "one or more recorded processes failed identity validation"
}

dev_stop_backend_for_recovery() {
    dev_stop_one backend "$DEV_RUNTIME_DIR/backend.pid" "$ROOT_DIR/target/debug/zk-server" || return 1

    DEV_RECOVERY_PYTHON_PID_FILE="$DEV_RUNTIME_DIR/python.pid"
    DEV_RECOVERY_SOCKET=$(dev_python_socket) || return 1
    DEV_RECOVERY_SOCKET_OWNERS=$(lsof -t "$DEV_RECOVERY_SOCKET" 2>/dev/null | awk '!seen[$0]++ { print }')
    DEV_RECOVERY_SOCKET_OWNER=$(printf '%s\n' "$DEV_RECOVERY_SOCKET_OWNERS" | sed -n '1p')
    DEV_RECOVERY_EXTRA_OWNER=$(printf '%s\n' "$DEV_RECOVERY_SOCKET_OWNERS" | sed -n '2p')

    if [ -z "$DEV_RECOVERY_SOCKET_OWNER" ]; then
        # A recorded PID that does not own the configured UDS is stale. It is
        # deliberately not signalled, even if that PID now belongs to another process.
        rm -f "$DEV_RECOVERY_PYTHON_PID_FILE"
        return 0
    fi
    case "$DEV_RECOVERY_SOCKET_OWNER" in
        *[!0-9]*)
            dev_warn "refusing to stop an invalid owner at Python socket $DEV_RECOVERY_SOCKET"
            return 1
            ;;
    esac
    if [ -n "$DEV_RECOVERY_EXTRA_OWNER" ]; then
        dev_warn "refusing to stop multiple owners at Python socket $DEV_RECOVERY_SOCKET"
        return 1
    fi
    kill -0 "$DEV_RECOVERY_SOCKET_OWNER" 2>/dev/null || {
        rm -f "$DEV_RECOVERY_PYTHON_PID_FILE"
        return 0
    }

    DEV_RECOVERY_PYTHON_COMMAND=$(ps -p "$DEV_RECOVERY_SOCKET_OWNER" -o command= 2>/dev/null || true)
    if ! dev_sidecar_command_matches_socket "$DEV_RECOVERY_PYTHON_COMMAND" "$DEV_RECOVERY_SOCKET"; then
        dev_warn "refusing to stop PID $DEV_RECOVERY_SOCKET_OWNER: command does not match the configured Python sidecar identity"
        return 1
    fi

    # The UDS owner is the authoritative PID. Replace a stale record only after
    # validating the full command identity; the dedicated stop path repeats that
    # exact token validation immediately before TERM and any later KILL.
    printf '%s\n' "$DEV_RECOVERY_SOCKET_OWNER" >"$DEV_RECOVERY_PYTHON_PID_FILE"
    dev_stop_sidecar_owner "$DEV_RECOVERY_SOCKET_OWNER" "$DEV_RECOVERY_SOCKET" \
        "$DEV_RECOVERY_PYTHON_PID_FILE"
}

dev_select_restart_start_target() {
    DEV_RESTART_START_TARGET=$1
    case "$DEV_RESTART_START_TARGET" in
        backend)
            if [ "${DEV_RESTART_FRONTEND_WAS_LIVE:-0}" -eq 1 ] && \
               ! dev_pid_is_live "$DEV_RUNTIME_DIR/frontend.pid" "$ROOT_DIR/frontend/node_modules/.bin/vite"; then
                DEV_RESTART_START_TARGET=all
            fi
            ;;
        frontend)
            if [ "${DEV_RESTART_BACKEND_WAS_LIVE:-0}" -eq 1 ] && \
               ! dev_pid_is_live "$DEV_RUNTIME_DIR/backend.pid" "$ROOT_DIR/target/debug/zk-server"; then
                DEV_RESTART_START_TARGET=all
            fi
            ;;
    esac
}

dev_reuse_backend_if_ready() {
    DEV_REUSE_BACKEND_URL=$1
    DEV_REUSE_PYTHON_ENABLED=$2
    DEV_REUSE_BACKEND_PID_FILE="$DEV_RUNTIME_DIR/backend.pid"
    DEV_REUSE_PYTHON_PID_FILE="$DEV_RUNTIME_DIR/python.pid"

    if dev_pid_is_live "$DEV_REUSE_BACKEND_PID_FILE" "$ROOT_DIR/target/debug/zk-server"; then
        if dev_backend_health_ready "$DEV_REUSE_BACKEND_URL" "$DEV_REUSE_PYTHON_ENABLED"; then
            dev_note "backend already healthy (PID $DEV_PID)"
            return 0
        fi
        dev_warn "recorded backend is not fully healthy; restarting backend and Python sidecar"
    elif [ -f "$DEV_REUSE_BACKEND_PID_FILE" ] || [ -f "$DEV_REUSE_PYTHON_PID_FILE" ]; then
        dev_warn "recorded backend processes are incomplete; restarting backend and Python sidecar"
    else
        return 1
    fi

    # Recovery intentionally excludes the frontend. The backend PID is validated
    # first; the sidecar is then resolved from the configured UDS instead of a
    # potentially stale python.pid record.
    dev_stop_backend_for_recovery || \
        dev_fail 18 "one or more recorded backend processes failed identity validation"
    return 1
}

dev_start_backend() {
    DEV_BACKEND_PORT=$(dev_backend_port)
    DEV_BACKEND_URL="http://127.0.0.1:$DEV_BACKEND_PORT"
    DEV_PYTHON_ENABLED=
    if [ -f "$DEV_RUNTIME_DIR/backend.pid" ] || [ -f "$DEV_RUNTIME_DIR/python.pid" ]; then
        DEV_PYTHON_ENABLED=$(dev_python_enabled_value)
        if dev_reuse_backend_if_ready "$DEV_BACKEND_URL" "$DEV_PYTHON_ENABLED"; then
            return 0
        fi
    fi
    if DEV_OWNER=$(dev_port_owner "$DEV_BACKEND_PORT") && [ -n "$DEV_OWNER" ]; then
        dev_fail 18 "backend port $DEV_BACKEND_PORT is occupied: $DEV_OWNER"
    fi

    DEV_SOCKET=$(dev_python_socket)
    mkdir -p "$DEV_RUNTIME_DIR"
    if [ -L "$DEV_SOCKET" ] || { [ -e "$DEV_SOCKET" ] && [ ! -S "$DEV_SOCKET" ]; }; then
        dev_fail 13 "refusing to replace a non-socket path configured as ZK_PYTHON_UDS: $DEV_SOCKET"
    fi
    if [ -S "$DEV_SOCKET" ]; then
        DEV_SOCKET_OWNER=$(lsof -t "$DEV_SOCKET" 2>/dev/null | sed -n '1p')
        case "$DEV_SOCKET_OWNER" in
            '') ;;
            *[!0-9]*) dev_fail 18 "could not validate the listener at Python socket $DEV_SOCKET" ;;
            *) dev_fail 18 "Python socket is owned by a live listener (PID $DEV_SOCKET_OWNER): $DEV_SOCKET" ;;
        esac
    fi
    export ZK_DEV_PYTHON_SOCKET=$DEV_SOCKET
    export ZK_DEV_BACKEND_URL=$DEV_BACKEND_URL
    export PLAYWRIGHT_BROWSERS_PATH="$ROOT_DIR/.runtime/playwright"
    export PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD=1
    if [ -z "$DEV_PYTHON_ENABLED" ]; then
        DEV_PYTHON_ENABLED=$(dev_python_enabled_value)
    fi

    dev_info "Starting zk-server on $DEV_BACKEND_URL"
    DEV_BACKEND_PID=$("$ROOT_DIR/python-service/.venv/bin/python" "$ROOT_DIR/scripts/spawn-detached.py" \
        --working-directory "$ROOT_DIR" --log "$DEV_RUNTIME_DIR/backend.log" \
        "$ROOT_DIR/scripts/run-backend-macos.sh")
    case "$DEV_BACKEND_PID" in
        ''|*[!0-9]*) dev_fail 19 "could not resolve the backend PID" ;;
    esac
    printf '%s\n' "$DEV_BACKEND_PID" >"$DEV_RUNTIME_DIR/backend.pid"
    DEV_START_STARTED_BACKEND=1
    if ! dev_wait_backend_health "$DEV_BACKEND_URL" "$DEV_PYTHON_ENABLED" "$DEV_PYTHON_READY_SECONDS"; then
        dev_stop_services backend || true
        dev_fail 19 "backend and configured Python mode did not become healthy; see .runtime/backend.log"
    fi
    if [ "$DEV_PYTHON_ENABLED" = true ]; then
        DEV_PY_PID=$(lsof -t "$DEV_SOCKET" 2>/dev/null | sed -n '1p')
        case "$DEV_PY_PID" in
            ''|*[!0-9]*) dev_fail 19 "could not resolve the Python sidecar PID at $DEV_SOCKET" ;;
        esac
        DEV_PY_COMMAND=$(ps -p "$DEV_PY_PID" -o command= 2>/dev/null || true)
        dev_sidecar_command_matches_socket "$DEV_PY_COMMAND" "$DEV_SOCKET" || \
            dev_fail 19 "unexpected process owns the Python socket"
        printf '%s\n' "$DEV_PY_PID" >"$DEV_RUNTIME_DIR/python.pid"
    fi
}

dev_start_frontend() {
    if dev_pid_is_live "$DEV_RUNTIME_DIR/frontend.pid" "$ROOT_DIR/frontend/node_modules/.bin/vite" && \
        curl -fsS --max-time 2 http://127.0.0.1:5273/ >/dev/null 2>&1; then
        dev_note "frontend already healthy (PID $DEV_PID)"
        return 0
    fi
    if DEV_OWNER=$(dev_port_owner "$DEV_FRONTEND_PORT") && [ -n "$DEV_OWNER" ]; then
        dev_fail 18 "frontend port $DEV_FRONTEND_PORT is occupied: $DEV_OWNER"
    fi
    DEV_BACKEND_PORT=$(dev_backend_port)
    export ZK_DEV_BACKEND_URL="http://127.0.0.1:$DEV_BACKEND_PORT"
    dev_info "Starting Vite on http://127.0.0.1:$DEV_FRONTEND_PORT"
    DEV_FRONTEND_PID=$("$ROOT_DIR/python-service/.venv/bin/python" "$ROOT_DIR/scripts/spawn-detached.py" \
        --working-directory "$ROOT_DIR/frontend" --log "$DEV_RUNTIME_DIR/frontend.log" \
        "$ROOT_DIR/scripts/run-frontend-macos.sh")
    case "$DEV_FRONTEND_PID" in
        ''|*[!0-9]*) dev_fail 19 "could not resolve the frontend PID" ;;
    esac
    printf '%s\n' "$DEV_FRONTEND_PID" >"$DEV_RUNTIME_DIR/frontend.pid"
    DEV_START_STARTED_FRONTEND=1
    if ! dev_wait_http http://127.0.0.1:5273/ 30; then
        dev_stop_services frontend || true
        dev_fail 19 "frontend did not become healthy; see .runtime/frontend.log"
    fi
}

dev_start_services() {
    DEV_START_TARGET=${1:-all}
    DEV_START_TRANSACTION_ACTIVE=1
    DEV_START_STARTED_BACKEND=0
    DEV_START_STARTED_FRONTEND=0
    trap 'dev_cleanup_failed_start; dev_release_lock' EXIT HUP INT TERM
    case "$DEV_START_TARGET" in
        all) dev_start_backend; dev_start_frontend ;;
        backend) dev_start_backend ;;
        frontend) dev_start_frontend ;;
        *) dev_fail 2 "start target must be all, backend, or frontend" ;;
    esac
    DEV_START_TRANSACTION_ACTIVE=0
    trap 'dev_release_lock' EXIT HUP INT TERM
    dev_note "zkcode is ready: http://127.0.0.1:5273"
    dev_note "logs: $DEV_RUNTIME_DIR"
}

dev_open_frontend() {
    /usr/bin/open http://127.0.0.1:5273/ || dev_warn "could not open the browser; services remain running"
}

dev_logs() {
    DEV_LOG_TARGET=${1:-backend}
    case "$DEV_LOG_TARGET" in
        backend|frontend|python) ;;
        *) dev_fail 2 "logs expects backend, frontend, or python" ;;
    esac
    DEV_LOG_FILE="$DEV_RUNTIME_DIR/$DEV_LOG_TARGET.log"
    if [ "$DEV_LOG_TARGET" = python ]; then
        DEV_LOG_FILE="$DEV_RUNTIME_DIR/backend.log"
        dev_warn "Python sidecar logs are currently multiplexed into backend.log"
    fi
    [ -f "$DEV_LOG_FILE" ] || dev_fail 2 "log does not exist yet: $DEV_LOG_FILE"
    tail -n 100 -f "$DEV_LOG_FILE"
}
