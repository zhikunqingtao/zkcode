#!/bin/sh

# Prefer the project-supported toolchain without unlinking or replacing other
# versions installed on the Mac. This file is sourced by setup/start/doctor.
zk_prepend_path() {
    [ -d "$1" ] || return 0
    case ":${PATH:-}:" in
        *":$1:"*) ;;
        *) PATH="$1${PATH:+:$PATH}" ;;
    esac
}

zk_use_macos_toolchain() {
    zk_prepend_path "/usr/local/bin"
    zk_prepend_path "/opt/homebrew/bin"
    zk_prepend_path "$HOME/.cargo/bin"

    # Apple Silicon is the supported platform. The /usr/local fallbacks make
    # diagnostics clearer on older Homebrew layouts without changing support.
    zk_prepend_path "/usr/local/opt/python@3.11/bin"
    zk_prepend_path "/opt/homebrew/opt/python@3.11/bin"
    zk_prepend_path "/usr/local/opt/node@22/bin"
    zk_prepend_path "/opt/homebrew/opt/node@22/bin"
    export PATH
}
