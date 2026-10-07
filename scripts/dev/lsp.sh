#!/bin/sh
# All five language families are installed at synchronization time, never on a tool call.
dev_sync_lsp() {
    DEV_LSP_FLAGS=
    [ "${DEV_OFFLINE:-0}" -eq 0 ] || DEV_LSP_FLAGS=--offline
    # shellcheck disable=SC2086 -- this is one controlled fixed flag.
    dev_run_bounded 1800 "private language-server toolchains" "$DEV_PYTHON" \
        "$ROOT_DIR/scripts/dev/lsp-toolchains.py" --root "$ROOT_DIR" --install $DEV_LSP_FLAGS || \
        dev_fail 14 "private language-server toolchains are incomplete; run ./dev bootstrap"
}
