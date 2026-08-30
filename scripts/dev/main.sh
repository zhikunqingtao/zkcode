#!/bin/sh
set -eu

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)

. "$ROOT_DIR/scripts/dev/common.sh"
. "$ROOT_DIR/scripts/dev/toolchains-macos.sh"
. "$ROOT_DIR/scripts/dev/fingerprint.sh"
. "$ROOT_DIR/scripts/dev/lifecycle.sh"
. "$ROOT_DIR/scripts/dev/sync.sh"
. "$ROOT_DIR/scripts/dev/doctor.sh"

export HOMEBREW_NO_AUTO_UPDATE=${HOMEBREW_NO_AUTO_UPDATE:-1}
export NPM_CONFIG_FETCH_RETRIES=${NPM_CONFIG_FETCH_RETRIES:-2}
export NPM_CONFIG_FETCH_TIMEOUT=${NPM_CONFIG_FETCH_TIMEOUT:-120000}
export PIP_DEFAULT_TIMEOUT=${PIP_DEFAULT_TIMEOUT:-30}
export PIP_RETRIES=${PIP_RETRIES:-2}
export PLAYWRIGHT_DOWNLOAD_CONNECTION_TIMEOUT=${PLAYWRIGHT_DOWNLOAD_CONNECTION_TIMEOUT:-120000}
export CARGO_HTTP_TIMEOUT=${CARGO_HTTP_TIMEOUT:-120}
export CARGO_NET_RETRY=${CARGO_NET_RETRY:-2}

dev_help() {
    cat <<'EOF'
Usage: ./dev <command> [options]

Commands:
  bootstrap [--start] [--yes]       install/repair the complete environment
  sync [--offline] [--build]        synchronize changed locked dependencies
  up [--open|--no-open]             sync, build, and start all services
  restart [all|backend|frontend]    build first, then safely restart
  stop [all|backend|frontend]       stop only verified repository processes
  status [--json]                   show processes, URLs, and dependency state
  doctor [--deep] [--json]          diagnose environment and capabilities
  repair <component>                repair frontend/python/browser/rust/build
  logs [backend|frontend|python]    follow a service log
  test [quick|full|browser|real]    run a validation tier

Source, lock files, .env, and user data are never reset, checked out, or
committed by this command.

bootstrap --yes skips the install confirmation and never prompts for a sudo
password; missing Homebrew requires cached/passwordless sudo or a trusted
SUDO_ASKPASS helper.
EOF
}

dev_validate_env() {
    "$DEV_PYTHON" "$ROOT_DIR/scripts/dev/exec-env.py" --file "$ROOT_DIR/.env" --check
}

dev_bootstrap_command() {
    DEV_BOOTSTRAP_START=0
    DEV_YES=0
    while [ "$#" -gt 0 ]; do
        case "$1" in
            --start) DEV_BOOTSTRAP_START=1 ;;
            --yes) DEV_YES=1 ;;
            -h|--help) dev_help; return 0 ;;
            *) dev_fail 2 "unknown bootstrap option: $1" ;;
        esac
        shift
    done
    export DEV_YES
    dev_acquire_lock
    dev_resolve_toolchains 1
    dev_ensure_env
    dev_validate_env
    DEV_OFFLINE=0
    export DEV_OFFLINE
    dev_toolchain_report
    dev_sync_all
    dev_build_backend
    dev_doctor 0 0
    if [ "$DEV_BOOTSTRAP_START" -eq 1 ]; then
        dev_start_services all
        dev_open_frontend
    fi
}

dev_sync_command() {
    DEV_OFFLINE=0
    DEV_SYNC_BUILD=0
    while [ "$#" -gt 0 ]; do
        case "$1" in
            --offline) DEV_OFFLINE=1 ;;
            --build) DEV_SYNC_BUILD=1 ;;
            -h|--help) dev_help; return 0 ;;
            *) dev_fail 2 "unknown sync option: $1" ;;
        esac
        shift
    done
    export DEV_OFFLINE
    dev_acquire_lock
    dev_resolve_toolchains 0
    dev_ensure_env
    dev_validate_env
    dev_sync_all
    [ "$DEV_SYNC_BUILD" -eq 0 ] || dev_build_backend
}

dev_up_command() {
    DEV_UP_OPEN=0
    DEV_OFFLINE=0
    while [ "$#" -gt 0 ]; do
        case "$1" in
            --open) DEV_UP_OPEN=1 ;;
            --no-open) DEV_UP_OPEN=0 ;;
            --offline) DEV_OFFLINE=1 ;;
            -h|--help) dev_help; return 0 ;;
            *) dev_fail 2 "unknown up option: $1" ;;
        esac
        shift
    done
    export DEV_OFFLINE
    dev_acquire_lock
    dev_resolve_toolchains 0
    dev_ensure_env
    dev_validate_env
    dev_sync_all
    dev_build_backend
    dev_start_services all
    [ "$DEV_UP_OPEN" -eq 0 ] || dev_open_frontend
}

dev_restart_command() {
    DEV_RESTART_TARGET=${1:-all}
    [ "$#" -le 1 ] || dev_fail 2 "restart accepts at most one target"
    case "$DEV_RESTART_TARGET" in
        all|backend|frontend) ;;
        *) dev_fail 2 "restart target must be all, backend, or frontend" ;;
    esac
    DEV_OFFLINE=0
    export DEV_OFFLINE
    dev_acquire_lock
    dev_resolve_toolchains 0
    dev_ensure_env
    dev_validate_env
    DEV_RESTART_BACKEND_WAS_LIVE=0
    DEV_RESTART_FRONTEND_WAS_LIVE=0
    dev_pid_is_live "$DEV_RUNTIME_DIR/backend.pid" "$ROOT_DIR/target/debug/zk-server" && \
        DEV_RESTART_BACKEND_WAS_LIVE=1
    dev_pid_is_live "$DEV_RUNTIME_DIR/frontend.pid" "$ROOT_DIR/frontend/node_modules/.bin/vite" && \
        DEV_RESTART_FRONTEND_WAS_LIVE=1
    dev_sync_all
    dev_select_restart_start_target "$DEV_RESTART_TARGET"
    if [ "$DEV_RESTART_TARGET" != frontend ]; then
        dev_python_smoke_bounded || dev_fail 16 "Python import smoke failed; the running instance was not stopped"
        dev_build_backend
    else
        dev_frontend_smoke_bounded || dev_fail 15 "frontend dependency smoke failed; Vite was not stopped"
    fi
    dev_stop_services "$DEV_RESTART_TARGET"
    dev_start_services "$DEV_RESTART_START_TARGET"
}

dev_stop_command() {
    DEV_STOP_TARGET=${1:-all}
    [ "$#" -le 1 ] || dev_fail 2 "stop accepts at most one target"
    dev_acquire_lock
    dev_preflight_platform
    dev_stop_services "$DEV_STOP_TARGET"
}

dev_status_command() {
    DEV_STATUS_JSON=0
    while [ "$#" -gt 0 ]; do
        case "$1" in
            --json) DEV_STATUS_JSON=1 ;;
            *) dev_fail 2 "unknown status option: $1" ;;
        esac
        shift
    done
    dev_activate_toolchains
    dev_preflight_platform
    dev_python_is_supported || {
        if [ -x "$ROOT_DIR/python-service/.venv/bin/python" ]; then
            DEV_PYTHON="$ROOT_DIR/python-service/.venv/bin/python"
        else
            dev_fail 14 "status needs Python 3.11 or the project venv; run ./dev bootstrap"
        fi
    }
    export DEV_PYTHON
    [ ! -f "$ROOT_DIR/.env" ] || dev_validate_env >/dev/null
    dev_status "$DEV_STATUS_JSON"
}

dev_doctor_command() {
    DEV_DOCTOR_DEEP=0
    DEV_DOCTOR_JSON=0
    while [ "$#" -gt 0 ]; do
        case "$1" in
            --deep) DEV_DOCTOR_DEEP=1 ;;
            --json) DEV_DOCTOR_JSON=1 ;;
            *) dev_fail 2 "unknown doctor option: $1" ;;
        esac
        shift
    done
    dev_activate_toolchains
    dev_preflight_platform
    dev_python_is_supported || {
        if [ -x "$ROOT_DIR/python-service/.venv/bin/python" ]; then
            DEV_PYTHON="$ROOT_DIR/python-service/.venv/bin/python"
        else
            dev_fail 14 "doctor needs Python 3.11 or the project venv; run ./dev bootstrap"
        fi
    }
    export DEV_PYTHON
    if [ -f "$ROOT_DIR/.env" ]; then
        if [ "$DEV_DOCTOR_JSON" -eq 1 ]; then
            dev_validate_env >/dev/null 2>&1 || true
        else
            dev_validate_env || true
        fi
    fi
    dev_doctor "$DEV_DOCTOR_DEEP" "$DEV_DOCTOR_JSON"
}

dev_repair_command() {
    [ "$#" -eq 1 ] || dev_fail 2 "repair requires exactly one component"
    DEV_OFFLINE=0
    export DEV_OFFLINE
    dev_acquire_lock
    dev_resolve_toolchains 0
    dev_ensure_env
    dev_validate_env
    dev_repair_component "$1"
}

dev_test_command() {
    DEV_TEST_TIER=${1:-quick}
    [ "$#" -le 1 ] || dev_fail 2 "test accepts at most one tier"
    dev_resolve_toolchains 0
    dev_ensure_env
    case "$DEV_TEST_TIER" in
        quick)
            dev_doctor 0 0
            (cd "$ROOT_DIR/frontend" && npm run test:run)
            (cd "$ROOT_DIR/python-service" && \
                PLAYWRIGHT_BROWSERS_PATH="$ROOT_DIR/.runtime/playwright" \
                .venv/bin/python -m pytest -q)
            (cd "$ROOT_DIR" && cargo test -p zk-server --lib --locked)
            ;;
        full)
            PLAYWRIGHT_BROWSERS_PATH="$ROOT_DIR/.runtime/playwright" \
                "$ROOT_DIR/scripts/parity/run-local-gates.sh"
            ;;
        browser)
            PLAYWRIGHT_BROWSERS_PATH="$ROOT_DIR/.runtime/playwright" \
                "$ROOT_DIR/python-service/.venv/bin/python" -m pytest \
                "$ROOT_DIR/python-service/tests/test_real_browser_service.py"
            ;;
        real)
            dev_fail 2 "choose an explicit bounded provider smoke: scripts/parity/run-qwen-smoke.sh or run-kimi-smoke.sh"
            ;;
        *) dev_fail 2 "test tier must be quick, full, browser, or real" ;;
    esac
}

DEV_COMMAND=${1:-help}
[ "$#" -eq 0 ] || shift
case "$DEV_COMMAND" in
    help|-h|--help) dev_help ;;
    bootstrap) dev_bootstrap_command "$@" ;;
    sync) dev_sync_command "$@" ;;
    up) dev_up_command "$@" ;;
    restart) dev_restart_command "$@" ;;
    stop) dev_stop_command "$@" ;;
    status) dev_status_command "$@" ;;
    doctor) dev_doctor_command "$@" ;;
    repair) dev_repair_command "$@" ;;
    logs)
        [ "$#" -le 1 ] || dev_fail 2 "logs accepts at most one target"
        dev_logs "${1:-backend}"
        ;;
    test) dev_test_command "$@" ;;
    *) dev_fail 2 "unknown command: $DEV_COMMAND (run ./dev help)" ;;
esac
