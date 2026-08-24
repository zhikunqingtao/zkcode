#!/bin/sh
set -eu

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
. "$ROOT_DIR/scripts/macos-toolchain-env.sh"
zk_use_macos_toolchain

if [ "$(uname -s)" != "Darwin" ]; then
    echo "zkcode Beta supports local macOS installation only." >&2
    exit 1
fi

require_command() {
    if ! command -v "$1" >/dev/null 2>&1; then
        echo "Missing required command: $1" >&2
        echo "$2" >&2
        exit 1
    fi
}

require_command rustc "Install Rust with rustup: https://rustup.rs"
require_command cargo "Install Rust with rustup: https://rustup.rs"
require_command node "Install Node.js 22 before continuing."
require_command npm "Install npm with Node.js 22 before continuing."

RUST_MINOR=$(rustc --version | awk '{split($2, v, "."); print v[2]}')
if [ "${RUST_MINOR:-0}" -lt 97 ]; then
    echo "Rust 1.97 or newer is required; found: $(rustc --version)" >&2
    exit 1
fi

NODE_MAJOR=$(node -p 'process.versions.node.split(".")[0]')
if [ "$NODE_MAJOR" -ne 22 ]; then
    echo "Node.js 22 is required; found: $(node --version)" >&2
    exit 1
fi

if [ ! -f "$ROOT_DIR/.env" ]; then
    cp "$ROOT_DIR/.env.example" "$ROOT_DIR/.env"
    echo "Created .env from .env.example. Add at least one LLM API key before chatting."
fi

echo "Installing locked frontend dependencies..."
(cd "$ROOT_DIR/frontend" && npm ci)

echo "Creating the locked Python 3.11/3.12 environment..."
"$ROOT_DIR/scripts/setup-python-macos.sh"

echo "Fetching locked Rust dependencies..."
(cd "$ROOT_DIR" && cargo fetch --locked)

echo "Building the zkcode backend..."
(cd "$ROOT_DIR" && cargo build --locked -p zk-server)

echo "Setup complete. Run ./scripts/doctor.sh, then ./start.sh."
