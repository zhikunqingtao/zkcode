#!/bin/sh
set -eu

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
RUNTIME_DIR="$ROOT_DIR/.runtime"
TEMP_DIR=""
RUN_SEQUENCE=0

info() {
    printf '\n==> %s\n' "$1"
}

warn() {
    printf 'warning: %s\n' "$1" >&2
}

fail() {
    printf '\nerror: %s\n' "$1" >&2
    printf 'The installer stopped safely. Fix the reported problem and run it again.\n' >&2
    printf 'Existing downloads and installed packages can be reused on the next run.\n' >&2
    exit 1
}

cleanup() {
    if [ -n "$TEMP_DIR" ] && [ -d "$TEMP_DIR" ]; then
        rm -rf "$TEMP_DIR"
    fi
}

on_signal() {
    warn "installation interrupted"
    exit 130
}

trap cleanup EXIT
trap on_signal INT TERM HUP

run_bounded() {
    TIME_LIMIT=$1
    LABEL=$2
    shift 2
    RUN_SEQUENCE=$((RUN_SEQUENCE + 1))
    TIMEOUT_MARKER="$TEMP_DIR/timeout-$RUN_SEQUENCE"

    "$@" &
    COMMAND_PID=$!
    (
        sleep "$TIME_LIMIT"
        if kill -0 "$COMMAND_PID" 2>/dev/null; then
            : >"$TIMEOUT_MARKER"
            terminate_process_tree "$COMMAND_PID" TERM
            sleep 5
            terminate_process_tree "$COMMAND_PID" KILL
        fi
    ) &
    WATCHDOG_PID=$!

    set +e
    wait "$COMMAND_PID"
    COMMAND_STATUS=$?
    set -e
    kill "$WATCHDOG_PID" 2>/dev/null || true
    wait "$WATCHDOG_PID" 2>/dev/null || true

    if [ -f "$TIMEOUT_MARKER" ]; then
        printf 'error: %s exceeded %s seconds and was stopped.\n' "$LABEL" "$TIME_LIMIT" >&2
        return 124
    fi
    if [ "$COMMAND_STATUS" -ne 0 ]; then
        printf 'error: %s failed with exit code %s.\n' "$LABEL" "$COMMAND_STATUS" >&2
        return "$COMMAND_STATUS"
    fi
}

terminate_process_tree() {
    if command -v pgrep >/dev/null 2>&1; then
        for TREE_CHILD_PID in $(pgrep -P "$1" 2>/dev/null || true); do
            terminate_process_tree "$TREE_CHILD_PID" "$2"
        done
    fi
    kill -"$2" "$1" 2>/dev/null || true
}

download_file() {
    URL=$1
    DESTINATION=$2
    LABEL=$3
    if ! curl --fail --location --show-error --silent \
        --connect-timeout 10 --max-time 120 \
        --retry 2 --retry-delay 3 --retry-connrefused \
        "$URL" --output "$DESTINATION"; then
        printf 'error: could not download %s from %s\n' "$LABEL" "$URL" >&2
        printf 'Check DNS/proxy/firewall settings. Trusted proxy and mirror variables are documented in docs/troubleshooting.md.\n' >&2
        return 1
    fi
    [ -s "$DESTINATION" ] || {
        printf 'error: the downloaded %s file is empty.\n' "$LABEL" >&2
        return 1
    }
}

wait_for_command_line_tools() {
    if xcode-select -p >/dev/null 2>&1; then
        return 0
    fi

    info "Requesting Apple Xcode Command Line Tools"
    printf 'Approve the Apple installation dialog. Waiting for at most 30 minutes.\n'
    xcode-select --install >/dev/null 2>&1 || true
    ELAPSED=0
    while [ "$ELAPSED" -lt 1800 ]; do
        if xcode-select -p >/dev/null 2>&1; then
            return 0
        fi
        sleep 10
        ELAPSED=$((ELAPSED + 10))
        if [ $((ELAPSED % 60)) -eq 0 ]; then
            printf 'Still waiting for Command Line Tools (%s/30 minutes)...\n' "$((ELAPSED / 60))"
        fi
    done
    return 1
}

find_brew() {
    # Apple Silicon must use native Homebrew. An Intel Homebrew migrated under
    # /usr/local can silently select x86_64 bottles or require Rosetta.
    [ -x /opt/homebrew/bin/brew ] || return 1
    [ "$(/opt/homebrew/bin/brew --prefix 2>/dev/null)" = "/opt/homebrew" ] || return 1
    printf '%s\n' /opt/homebrew/bin/brew
}

ensure_homebrew() {
    if BREW_BIN=$(find_brew); then
        printf 'Using Homebrew: %s\n' "$BREW_BIN"
        return 0
    fi

    info "Installing Homebrew from the official installer"
    printf 'macOS may request the administrator password once.\n'
    if [ ! -r /dev/tty ]; then
        fail "an interactive Terminal is required for Homebrew administrator authorization"
    fi
    if ! run_bounded 300 "administrator authorization" /usr/bin/sudo -v </dev/tty; then
        fail "administrator authorization is required to install Homebrew"
    fi
    HOMEBREW_INSTALLER="$TEMP_DIR/homebrew-install.sh"
    download_file \
        "https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh" \
        "$HOMEBREW_INSTALLER" "the Homebrew installer" || return 1
    run_bounded 1200 "Homebrew installation" \
        env NONINTERACTIVE=1 /bin/bash "$HOMEBREW_INSTALLER" || return 1
    BREW_BIN=$(find_brew) || return 1
}

ensure_brew_formula() {
    FORMULA=$1
    if "$BREW_BIN" list --versions "$FORMULA" >/dev/null 2>&1; then
        printf 'Using installed Homebrew formula: %s\n' "$FORMULA"
        return 0
    fi
    run_bounded 1800 "Homebrew installation of $FORMULA" \
        "$BREW_BIN" install "$FORMULA"
}

rust_minor() {
    rustc --version 2>/dev/null | awk '{split($2, v, "."); print v[2]}'
}

ensure_rust() {
    CURRENT_MINOR=0
    if command -v rustc >/dev/null 2>&1; then
        CURRENT_MINOR=$(rust_minor)
        case "$CURRENT_MINOR" in *[!0-9]*|'') CURRENT_MINOR=0 ;; esac
    fi
    if [ "$CURRENT_MINOR" -ge 97 ] && command -v cargo >/dev/null 2>&1; then
        printf 'Using Rust: %s\n' "$(rustc --version)"
        return 0
    fi

    info "Installing or updating the official Rust stable toolchain"
    if ! command -v rustup >/dev/null 2>&1; then
        RUSTUP_INSTALLER="$TEMP_DIR/rustup-init.sh"
        download_file "https://sh.rustup.rs" "$RUSTUP_INSTALLER" "the rustup installer" || return 1
        run_bounded 1200 "rustup installation" \
            /bin/sh "$RUSTUP_INSTALLER" -y --profile minimal --default-toolchain stable --no-modify-path || return 1
        . "$ROOT_DIR/scripts/macos-toolchain-env.sh"
        zk_use_macos_toolchain
    fi
    run_bounded 1200 "Rust stable update" rustup update stable || return 1
    run_bounded 120 "Rust stable selection" rustup default stable || return 1
    run_bounded 300 "Rust component installation" \
        rustup component add rustfmt clippy --toolchain stable || return 1

    CURRENT_MINOR=$(rust_minor)
    case "$CURRENT_MINOR" in *[!0-9]*|'') CURRENT_MINOR=0 ;; esac
    [ "$CURRENT_MINOR" -ge 97 ] || return 1
}

port_is_listening() {
    lsof -nP -iTCP:"$1" -sTCP:LISTEN >/dev/null 2>&1
}

http_is_ready() {
    curl -fsS --connect-timeout 2 --max-time 3 "$1" >/dev/null 2>&1
}

all_services_are_ready() {
    http_is_ready "$BACKEND_URL" || return 1
    http_is_ready "$FRONTEND_URL" || return 1
    if [ "${ZK_PYTHON_ENABLED:-true}" = "true" ]; then
        HEALTH_JSON=$(curl -fsS --connect-timeout 2 --max-time 3 "$BACKEND_URL" 2>/dev/null || true)
        printf '%s' "$HEALTH_JSON" | "$ROOT_DIR/python-service/.venv/bin/python" -c \
            'import json, sys; raise SystemExit(0 if json.load(sys.stdin)["subsystems"]["python"]["status"] == "UP" else 1)' \
            >/dev/null 2>&1 || return 1
    fi
}

wait_for_http() {
    URL=$1
    LABEL=$2
    MAX_SECONDS=$3
    ELAPSED=0
    while [ "$ELAPSED" -lt "$MAX_SECONDS" ]; do
        if http_is_ready "$URL"; then
            return 0
        fi
        sleep 1
        ELAPSED=$((ELAPSED + 1))
    done
    printf 'error: %s did not become ready within %s seconds.\n' "$LABEL" "$MAX_SECONDS" >&2
    return 1
}

if [ "$(uname -s)" != "Darwin" ]; then
    fail "zkcode Beta supports local macOS installation only"
fi
if [ "$(uname -m)" != "arm64" ]; then
    fail "zkcode Beta currently supports Apple Silicon Macs only (found $(uname -m))"
fi
if [ ! -f "$ROOT_DIR/Cargo.toml" ] || [ ! -d "$ROOT_DIR/frontend" ]; then
    fail "run this command from an intact zkcode source directory"
fi
command -v curl >/dev/null 2>&1 || fail "macOS curl is missing"
command -v lsof >/dev/null 2>&1 || fail "macOS lsof is missing"

mkdir -p "$RUNTIME_DIR"
TEMP_DIR=$(mktemp -d "${TMPDIR:-/tmp}/zkcode-install.XXXXXX") || fail "could not create a temporary directory"

# Bound the package managers' own retry behavior as well as the outer process.
export HOMEBREW_NO_AUTO_UPDATE=${HOMEBREW_NO_AUTO_UPDATE:-1}
export NPM_CONFIG_FETCH_RETRIES=${NPM_CONFIG_FETCH_RETRIES:-2}
export NPM_CONFIG_FETCH_TIMEOUT=${NPM_CONFIG_FETCH_TIMEOUT:-120000}
export PIP_DEFAULT_TIMEOUT=${PIP_DEFAULT_TIMEOUT:-30}
export PIP_RETRIES=${PIP_RETRIES:-2}
export PLAYWRIGHT_DOWNLOAD_CONNECTION_TIMEOUT=${PLAYWRIGHT_DOWNLOAD_CONNECTION_TIMEOUT:-120000}
export CARGO_HTTP_TIMEOUT=${CARGO_HTTP_TIMEOUT:-120}
export CARGO_NET_RETRY=${CARGO_NET_RETRY:-2}
# A clean release install does not benefit from incremental state, and disabling
# it avoids reusing a stale compiler cache left by an interrupted/older build.
export CARGO_INCREMENTAL=0

info "Checking Apple developer tools"
wait_for_command_line_tools || fail "Xcode Command Line Tools were not installed within 30 minutes"

ensure_homebrew || fail "Homebrew installation failed or timed out"
export PATH="$(dirname "$BREW_BIN"):$PATH"

info "Installing supported language runtimes"
ensure_brew_formula node@22 || fail "Node.js 22 installation failed or timed out"
ensure_brew_formula python@3.11 || fail "Python 3.11 installation failed or timed out"
. "$ROOT_DIR/scripts/macos-toolchain-env.sh"
zk_use_macos_toolchain
ensure_rust || fail "Rust 1.97 or newer could not be installed"

NODE_MAJOR=$(node -p 'process.versions.node.split(".")[0]')
[ "$NODE_MAJOR" -eq 22 ] || fail "version conflict: expected Node.js 22, found $(node --version) at $(command -v node)"
PYTHON_VERSION=$(python3.11 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
[ "$PYTHON_VERSION" = "3.11" ] || fail "version conflict: expected Python 3.11 at $(command -v python3.11)"
printf 'Node: %s\nPython: %s\nRust: %s\n' "$(node --version)" "$(python3.11 --version)" "$(rustc --version)"

info "Installing locked zkcode dependencies and building the backend"
if ! run_bounded 2700 "zkcode dependency installation and build" "$ROOT_DIR/scripts/setup-macos.sh"; then
    fail "project setup failed; review the package-manager error above before retrying"
fi

info "Running local installation diagnostics"
run_bounded 60 "zkcode diagnostics" "$ROOT_DIR/scripts/doctor.sh" || fail "installation diagnostics failed"

set -a
# setup-macos.sh creates this trusted, user-owned local configuration when absent.
. "$ROOT_DIR/.env"
set +a
EFFECTIVE_PORT=${ZK_PORT:-8081}
case "$EFFECTIVE_PORT" in
    *[!0-9]*|'') fail "ZK_PORT in .env must be a numeric TCP port" ;;
esac
if [ "${#EFFECTIVE_PORT}" -gt 5 ]; then
    fail "ZK_PORT in .env must be between 1 and 65535"
fi
if [ "$EFFECTIVE_PORT" -lt 1 ] || [ "$EFFECTIVE_PORT" -gt 65535 ]; then
    fail "ZK_PORT in .env must be between 1 and 65535"
fi

BACKEND_URL="http://127.0.0.1:$EFFECTIVE_PORT/api/health"
FRONTEND_URL=http://127.0.0.1:5273/
if all_services_are_ready; then
    info "zkcode services are already healthy"
else
    if [ -f "$RUNTIME_DIR/backend.pid" ] || [ -f "$RUNTIME_DIR/frontend.pid" ]; then
        info "Stopping an incomplete previous zkcode run"
        run_bounded 30 "stopping previous zkcode services" "$ROOT_DIR/stop.sh" || \
            fail "previous zkcode services could not be stopped; inspect $RUNTIME_DIR"
    fi
    port_is_listening "$EFFECTIVE_PORT" && fail "TCP port $EFFECTIVE_PORT is already occupied by another process"
    port_is_listening 5273 && fail "TCP port 5273 is already occupied by another process"

    info "Starting all zkcode services"
    run_bounded 180 "zkcode service startup" "$ROOT_DIR/start.sh" || \
        fail "service startup failed; inspect $RUNTIME_DIR/backend.log and $RUNTIME_DIR/frontend.log"
fi

wait_for_http "$BACKEND_URL" "zkcode backend" 60 || fail "backend health verification failed"
wait_for_http "$FRONTEND_URL" "zkcode frontend" 60 || fail "frontend health verification failed"
all_services_are_ready || fail "one or more required zkcode subsystems are not healthy"

info "Opening zkcode in the default browser"
if ! run_bounded 15 "opening the browser" /usr/bin/open "$FRONTEND_URL"; then
    fail "services are running, but macOS could not open the browser; open $FRONTEND_URL manually"
fi

printf '\nzkcode installation is complete and all services are healthy.\n'
printf 'Open: %s\n' "$FRONTEND_URL"
printf 'Stop later with: %s/stop.sh\n' "$ROOT_DIR"
if ! grep -Eq '^(ZK_LLM_API_KEY|LLM_PROVIDER_[A-Z0-9_]+_API_KEY)=.+$' "$ROOT_DIR/.env"; then
    warn "no LLM API key is configured; add one to $ROOT_DIR/.env, then run ./stop.sh and ./start.sh before chatting"
fi
