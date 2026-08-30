#!/bin/sh

DEV_TOOLCHAIN_LOADED=0

dev_toml_string() {
    DEV_TOML_FILE=$1
    DEV_TOML_KEY=$2
    awk -v wanted="$DEV_TOML_KEY" '
        function trim(value) {
            sub(/^[[:space:]]+/, "", value)
            sub(/[[:space:]]+$/, "", value)
            return value
        }
        {
            line = $0
            if (line ~ "^[[:space:]]*" wanted "[[:space:]]*=") {
                count += 1
                sub(/^[^=]*=[[:space:]]*/, "", line)
                line = trim(line)
                if (line !~ /^"[^"]*"$/) invalid = 1
                sub(/^"/, "", line)
                sub(/"$/, "", line)
                result = line
            }
        }
        END {
            if (count != 1 || invalid) exit 2
            print result
        }
    ' "$DEV_TOML_FILE"
}

dev_semver_satisfies() {
    DEV_SEMVER_ACTUAL=$1
    DEV_SEMVER_RANGE=$2
    awk -v actual="$DEV_SEMVER_ACTUAL" -v constraint="$DEV_SEMVER_RANGE" '
        function parse(value, parts, count, idx) {
            sub(/^[^0-9]*/, "", value)
            sub(/[-+].*$/, "", value)
            count = split(value, parts, ".")
            if (count < 2 || count > 3) return 0
            for (idx = 1; idx <= 3; idx += 1) {
                if (idx > count) parts[idx] = 0
                if (parts[idx] !~ /^[0-9]+$/) return 0
                parts[idx] += 0
            }
            return 1
        }
        function compare(left, right, idx) {
            for (idx = 1; idx <= 3; idx += 1) {
                if (left[idx] < right[idx]) return -1
                if (left[idx] > right[idx]) return 1
            }
            return 0
        }
        BEGIN {
            if (split(constraint, bounds, ",") != 2) exit 1
            if (bounds[1] !~ /^>=/ || bounds[2] !~ /^</) exit 1
            lower = substr(bounds[1], 3)
            upper = substr(bounds[2], 2)
            if (!parse(actual, actual_parts) || !parse(lower, lower_parts) || !parse(upper, upper_parts)) exit 1
            exit !(compare(actual_parts, lower_parts) >= 0 && compare(actual_parts, upper_parts) < 0)
        }
    '
}

dev_load_toolchain_config() {
    [ "$DEV_TOOLCHAIN_LOADED" -eq 0 ] || return 0
    DEV_TOOLCHAIN_FILE="$ROOT_DIR/configuration/dev-toolchain.toml"
    [ -f "$DEV_TOOLCHAIN_FILE" ] || dev_fail 2 "required source file is missing: configuration/dev-toolchain.toml"
    [ "$(grep -Ec '^[[:space:]]*schema_version[[:space:]]*=[[:space:]]*1[[:space:]]*$' "$DEV_TOOLCHAIN_FILE")" -eq 1 ] || \
        dev_fail 2 "configuration/dev-toolchain.toml must use schema_version = 1"

    DEV_REQUIRED_PLATFORM=$(dev_toml_string "$DEV_TOOLCHAIN_FILE" platform) || dev_fail 2 "invalid platform in configuration/dev-toolchain.toml"
    DEV_REQUIRED_ARCH=$(dev_toml_string "$DEV_TOOLCHAIN_FILE" arch) || dev_fail 2 "invalid arch in configuration/dev-toolchain.toml"
    DEV_REQUIRED_MACOS=$(dev_toml_string "$DEV_TOOLCHAIN_FILE" minimum_macos) || dev_fail 2 "invalid minimum_macos in configuration/dev-toolchain.toml"
    DEV_REQUIRED_RUST=$(dev_toml_string "$DEV_TOOLCHAIN_FILE" rust) || dev_fail 2 "invalid rust in configuration/dev-toolchain.toml"
    DEV_REQUIRED_NODE_RANGE=$(dev_toml_string "$DEV_TOOLCHAIN_FILE" node) || dev_fail 2 "invalid node range in configuration/dev-toolchain.toml"
    DEV_REQUIRED_NPM_RANGE=$(dev_toml_string "$DEV_TOOLCHAIN_FILE" npm) || dev_fail 2 "invalid npm range in configuration/dev-toolchain.toml"
    DEV_REQUIRED_PYTHON_RANGE=$(dev_toml_string "$DEV_TOOLCHAIN_FILE" python) || dev_fail 2 "invalid python range in configuration/dev-toolchain.toml"

    DEV_NODE_LOWER=${DEV_REQUIRED_NODE_RANGE%%,*}
    DEV_NODE_LOWER=${DEV_NODE_LOWER#>=}
    DEV_REQUIRED_NODE_MAJOR=${DEV_NODE_LOWER%%.*}
    DEV_NPM_LOWER=${DEV_REQUIRED_NPM_RANGE%%,*}
    DEV_NPM_LOWER=${DEV_NPM_LOWER#>=}
    DEV_REQUIRED_NPM_MAJOR=${DEV_NPM_LOWER%%.*}
    DEV_PYTHON_LOWER=${DEV_REQUIRED_PYTHON_RANGE%%,*}
    DEV_PYTHON_LOWER=${DEV_PYTHON_LOWER#>=}
    DEV_REQUIRED_PYTHON_SERIES=${DEV_PYTHON_LOWER%.*}
    DEV_REQUIRED_MACOS_MAJOR=${DEV_REQUIRED_MACOS%%.*}
    DEV_REQUIRED_MACOS_MINOR=${DEV_REQUIRED_MACOS#*.}
    [ "$DEV_REQUIRED_MACOS_MINOR" != "$DEV_REQUIRED_MACOS" ] || DEV_REQUIRED_MACOS_MINOR=0

    case "$DEV_REQUIRED_PLATFORM:$DEV_REQUIRED_ARCH:$DEV_REQUIRED_NODE_MAJOR:$DEV_REQUIRED_NPM_MAJOR:$DEV_REQUIRED_PYTHON_SERIES:$DEV_REQUIRED_MACOS_MAJOR:$DEV_REQUIRED_MACOS_MINOR" in
        *[!A-Za-z0-9._:-]*) dev_fail 2 "unsupported value format in configuration/dev-toolchain.toml" ;;
    esac
    dev_semver_satisfies "$DEV_NODE_LOWER" "$DEV_REQUIRED_NODE_RANGE" || dev_fail 2 "invalid Node range in configuration/dev-toolchain.toml"
    dev_semver_satisfies "$DEV_NPM_LOWER" "$DEV_REQUIRED_NPM_RANGE" || dev_fail 2 "invalid npm range in configuration/dev-toolchain.toml"
    dev_semver_satisfies "$DEV_PYTHON_LOWER" "$DEV_REQUIRED_PYTHON_RANGE" || dev_fail 2 "invalid Python range in configuration/dev-toolchain.toml"

    ZK_DEV_NODE_FORMULA="node@$DEV_REQUIRED_NODE_MAJOR"
    ZK_DEV_PYTHON_FORMULA="python@$DEV_REQUIRED_PYTHON_SERIES"
    DEV_TOOLCHAIN_LOADED=1
    export DEV_REQUIRED_PLATFORM DEV_REQUIRED_ARCH DEV_REQUIRED_MACOS DEV_REQUIRED_RUST
    export DEV_REQUIRED_NODE_RANGE DEV_REQUIRED_NPM_RANGE DEV_REQUIRED_PYTHON_RANGE
    export DEV_REQUIRED_NODE_MAJOR DEV_REQUIRED_NPM_MAJOR DEV_REQUIRED_PYTHON_SERIES
    export ZK_DEV_NODE_FORMULA ZK_DEV_PYTHON_FORMULA DEV_TOOLCHAIN_LOADED
}

dev_activate_toolchains() {
    dev_load_toolchain_config
    # Reuse the repository's side-by-side PATH selection. It never changes a
    # login shell or a package manager's global default.
    . "$ROOT_DIR/scripts/macos-toolchain-env.sh"
    zk_use_macos_toolchain
}

dev_major_version() {
    printf '%s' "$1" | sed -E 's/^[^0-9]*([0-9]+).*/\1/'
}

dev_minor_version() {
    printf '%s' "$1" | sed -E 's/^[^0-9]*([0-9]+\.[0-9]+).*/\1/'
}

dev_preflight_platform() {
    dev_load_toolchain_config
    DEV_CURRENT_PLATFORM=$(uname -s | tr '[:upper:]' '[:lower:]')
    [ "$DEV_CURRENT_PLATFORM" = "$DEV_REQUIRED_PLATFORM" ] || dev_fail 10 "source bootstrap requires $DEV_REQUIRED_PLATFORM (found $DEV_CURRENT_PLATFORM)"
    [ "$(uname -m)" = "$DEV_REQUIRED_ARCH" ] || dev_fail 10 "source bootstrap requires $DEV_REQUIRED_ARCH (found $(uname -m))"
    DEV_MACOS_VERSION=$(sw_vers -productVersion 2>/dev/null || printf '0')
    DEV_MACOS_MAJOR=$(printf '%s' "$DEV_MACOS_VERSION" | cut -d. -f1)
    DEV_MACOS_MINOR=$(printf '%s' "$DEV_MACOS_VERSION" | cut -d. -f2)
    case "$DEV_MACOS_MAJOR" in
        ''|*[!0-9]*) dev_fail 10 "cannot determine the macOS version" ;;
    esac
    case "$DEV_MACOS_MINOR" in
        ''|*[!0-9]*) DEV_MACOS_MINOR=0 ;;
    esac
    if [ "$DEV_MACOS_MAJOR" -lt "$DEV_REQUIRED_MACOS_MAJOR" ] || \
       { [ "$DEV_MACOS_MAJOR" -eq "$DEV_REQUIRED_MACOS_MAJOR" ] && [ "$DEV_MACOS_MINOR" -lt "$DEV_REQUIRED_MACOS_MINOR" ]; }; then
        dev_fail 10 "macOS $DEV_REQUIRED_MACOS or newer is required (found $DEV_MACOS_VERSION)"
    fi

    dev_require_file "$ROOT_DIR/Cargo.toml"
    dev_require_file "$ROOT_DIR/Cargo.lock"
    dev_require_file "$ROOT_DIR/frontend/package-lock.json"
    dev_require_file "$ROOT_DIR/python-service/requirements.lock"
    dev_require_file "$ROOT_DIR/python-service/build-requirements.lock"
    dev_require_file "$ROOT_DIR/configuration/dev-toolchain.toml"
    DEV_RUST_TOOLCHAIN_CHANNEL=$(dev_toml_string "$ROOT_DIR/rust-toolchain.toml" channel) || \
        dev_fail 2 "rust-toolchain.toml must declare one quoted channel"
    [ "$DEV_RUST_TOOLCHAIN_CHANNEL" = "$DEV_REQUIRED_RUST" ] || \
        dev_fail 2 "Rust differs between configuration/dev-toolchain.toml and rust-toolchain.toml"
}

dev_rust_is_supported() {
    command -v rustc >/dev/null 2>&1 || return 1
    command -v cargo >/dev/null 2>&1 || return 1
    [ "$(rustc --version | awk '{print $2}')" = "$DEV_REQUIRED_RUST" ]
}

dev_node_is_supported() {
    command -v node >/dev/null 2>&1 || return 1
    command -v npm >/dev/null 2>&1 || return 1
    dev_semver_satisfies "$(node --version)" "$DEV_REQUIRED_NODE_RANGE" && \
        dev_semver_satisfies "$(npm --version)" "$DEV_REQUIRED_NPM_RANGE"
}

dev_python_is_supported() {
    DEV_PYTHON=
    for DEV_PYTHON_CANDIDATE in \
        "/opt/homebrew/opt/$ZK_DEV_PYTHON_FORMULA/bin/python$DEV_REQUIRED_PYTHON_SERIES" \
        "/usr/local/opt/$ZK_DEV_PYTHON_FORMULA/bin/python$DEV_REQUIRED_PYTHON_SERIES" \
        "$(command -v "python$DEV_REQUIRED_PYTHON_SERIES" 2>/dev/null || true)"
    do
        [ -n "$DEV_PYTHON_CANDIDATE" ] || continue
        [ -x "$DEV_PYTHON_CANDIDATE" ] || continue
        DEV_PYTHON_VERSION=$("$DEV_PYTHON_CANDIDATE" -c 'import sys; print(".".join(map(str, sys.version_info[:3])))')
        if dev_semver_satisfies "$DEV_PYTHON_VERSION" "$DEV_REQUIRED_PYTHON_RANGE"; then
            DEV_PYTHON=$DEV_PYTHON_CANDIDATE
            export DEV_PYTHON
            return 0
        fi
    done
    return 1
}

dev_toolchain_report() {
    dev_note "Rust:  $(dev_command_version rustc)"
    dev_note "Node:  $(dev_command_version node)"
    dev_note "npm:   $(dev_command_version npm)"
    if [ -n "${DEV_PYTHON:-}" ]; then
        dev_note "Python: $($DEV_PYTHON --version 2>&1) ($DEV_PYTHON)"
    else
        dev_note "Python: missing or outside $DEV_REQUIRED_PYTHON_RANGE"
    fi
}

dev_confirm_external_install() {
    [ "${DEV_YES:-0}" -eq 1 ] && return 0
    dev_homebrew_has_tty || dev_fail 13 "dependency installation needs an interactive terminal; rerun with --yes only after reviewing the plan"
    printf 'Install the missing supported toolchains from official sources? [y/N] ' >/dev/tty
    IFS= read -r DEV_CONFIRM </dev/tty || DEV_CONFIRM=
    case "$DEV_CONFIRM" in
        y|Y|yes|YES) ;;
        *) dev_fail 13 "toolchain installation was not approved" ;;
    esac
}

dev_wait_for_clt() {
    xcode-select -p >/dev/null 2>&1 && return 0
    dev_info "Requesting Xcode Command Line Tools"
    xcode-select --install >/dev/null 2>&1 || true
    DEV_CLT_WAIT=0
    while [ "$DEV_CLT_WAIT" -lt 1800 ]; do
        xcode-select -p >/dev/null 2>&1 && return 0
        sleep 10
        DEV_CLT_WAIT=$((DEV_CLT_WAIT + 10))
    done
    return 1
}

dev_find_brew() {
    [ -x /opt/homebrew/bin/brew ] || return 1
    [ "$(/opt/homebrew/bin/brew --prefix 2>/dev/null)" = /opt/homebrew ] || return 1
    printf '%s\n' /opt/homebrew/bin/brew
}

dev_sudo_available() {
    [ -x /usr/bin/sudo ]
}

dev_sudo() {
    /usr/bin/sudo "$@"
}

dev_homebrew_has_tty() {
    (: </dev/tty) 2>/dev/null && (: >/dev/tty) 2>/dev/null
}

dev_homebrew_sudo_from_tty() {
    dev_sudo -v </dev/tty
}

dev_homebrew_path_chain_is_trusted() {
    DEV_HOMEBREW_ASKPASS_PATH=$1
    DEV_HOMEBREW_ASKPASS_UID=$2
    while :; do
        /usr/bin/stat -f '%u %Sp' "$DEV_HOMEBREW_ASKPASS_PATH" 2>/dev/null | \
            awk -v uid="$DEV_HOMEBREW_ASKPASS_UID" '
                ($1 == 0 || $1 == uid) && substr($2, 6, 1) != "w" && substr($2, 9, 1) != "w" { valid = 1 }
                END { exit !valid }
            ' || return 1
        [ "$DEV_HOMEBREW_ASKPASS_PATH" = / ] && break
        DEV_HOMEBREW_ASKPASS_PATH=${DEV_HOMEBREW_ASKPASS_PATH%/*}
        [ -n "$DEV_HOMEBREW_ASKPASS_PATH" ] || DEV_HOMEBREW_ASKPASS_PATH=/
        [ -d "$DEV_HOMEBREW_ASKPASS_PATH" ] || return 1
    done
}

dev_homebrew_askpass_is_trusted() {
    DEV_HOMEBREW_ASKPASS=$1
    case "$DEV_HOMEBREW_ASKPASS" in
        /*) ;;
        *) return 1 ;;
    esac
    [ -f "$DEV_HOMEBREW_ASKPASS" ] || return 1
    [ ! -L "$DEV_HOMEBREW_ASKPASS" ] || return 1
    [ -x "$DEV_HOMEBREW_ASKPASS" ] || return 1

    DEV_HOMEBREW_ASKPASS_UID=$(id -u) || return 1
    dev_homebrew_path_chain_is_trusted \
        "$DEV_HOMEBREW_ASKPASS" "$DEV_HOMEBREW_ASKPASS_UID" || return 1
    [ -x /bin/realpath ] || return 1
    DEV_HOMEBREW_ASKPASS_REAL=$(/bin/realpath "$DEV_HOMEBREW_ASKPASS" 2>/dev/null) || return 1
    dev_homebrew_path_chain_is_trusted \
        "$DEV_HOMEBREW_ASKPASS_REAL" "$DEV_HOMEBREW_ASKPASS_UID"
}

dev_homebrew_sudo_with_askpass() (
    SUDO_ASKPASS=$DEV_HOMEBREW_SUDO_ASKPASS
    export SUDO_ASKPASS
    dev_sudo -A "$@"
)

dev_homebrew_check_sudo_permission() {
    if [ -n "${DEV_HOMEBREW_SUDO_ASKPASS:-}" ]; then
        dev_homebrew_sudo_with_askpass -l mkdir
    else
        dev_sudo -n -l mkdir
    fi
}

dev_homebrew_authorize_sudo() {
    DEV_HOMEBREW_HAD_SUDO_TICKET=0
    DEV_HOMEBREW_CREATED_SUDO_TICKET=0
    DEV_HOMEBREW_SUDO_ASKPASS=
    dev_sudo_available || {
        dev_warn "Homebrew installation requires /usr/bin/sudo and an administrator account"
        return 13
    }

    if dev_sudo -n -v >/dev/null 2>&1; then
        DEV_HOMEBREW_HAD_SUDO_TICKET=1
    fi

    # Match Homebrew's permission check: authorization alone is insufficient
    # unless sudoers also permits the mkdir command used by the installer.
    if dev_sudo -n -l mkdir >/dev/null 2>&1; then
        return 0
    fi
    if [ "$DEV_HOMEBREW_HAD_SUDO_TICKET" -eq 1 ]; then
        dev_warn "the current account is not allowed to install Homebrew with sudo"
        return 13
    fi

    if [ "${DEV_YES:-0}" -eq 1 ]; then
        if [ -z "${SUDO_ASKPASS:-}" ] || ! dev_homebrew_askpass_is_trusted "$SUDO_ASKPASS"; then
            dev_warn "Homebrew needs cached, passwordless, or trusted SUDO_ASKPASS sudo access when bootstrap uses --yes"
            return 13
        fi
        dev_info "Authorizing Homebrew installation with SUDO_ASKPASS"
        DEV_HOMEBREW_SUDO_ASKPASS=$SUDO_ASKPASS
        # The initial non-interactive probe proved that there was no existing
        # ticket. From this point on, cleanup owns any timestamp that appears,
        # including one created just before a timeout or signal.
        DEV_HOMEBREW_CREATED_SUDO_TICKET=1
        if ! dev_run_bounded 300 "sudo authorization" dev_homebrew_sudo_with_askpass -v; then
            dev_warn "administrator authorization for Homebrew failed"
            return 13
        fi
    else
        if ! dev_homebrew_has_tty; then
            dev_warn "Homebrew installation needs a controlling terminal for administrator authorization"
            return 13
        fi
        dev_info "Requesting administrator authorization for Homebrew (the password is read by sudo and is not echoed)"
        DEV_HOMEBREW_CREATED_SUDO_TICKET=1
        if ! dev_run_bounded 300 "sudo authorization" dev_homebrew_sudo_from_tty; then
            dev_warn "administrator authorization for Homebrew failed"
            return 13
        fi
    fi

    if ! dev_homebrew_check_sudo_permission >/dev/null 2>&1; then
        dev_warn "the current account is not allowed to install Homebrew with sudo"
        return 13
    fi
}

dev_homebrew_refresh_sudo() {
    if [ -n "${DEV_HOMEBREW_SUDO_ASKPASS:-}" ]; then
        dev_homebrew_sudo_with_askpass -v
    else
        dev_sudo -n -v
    fi
}

dev_homebrew_sudo_keepalive_loop() {
    while sleep "${DEV_HOMEBREW_KEEPALIVE_SECONDS:-45}"; do
        # NOPASSWD policies may allow Homebrew without providing a renewable
        # timestamp. A failed refresh must not turn that valid case into an
        # installation failure; the official installer remains authoritative.
        dev_homebrew_refresh_sudo >/dev/null 2>&1 || true
    done
}

dev_homebrew_start_sudo_keepalive() {
    DEV_HOMEBREW_KEEPALIVE_PID=
    # Cached/interactive tickets use -n; the trusted askpass path uses the same
    # -A semantics as Homebrew. NOPASSWD-only policies can continue without a
    # renewable timestamp when the refresh probe fails.
    dev_homebrew_refresh_sudo >/dev/null 2>&1 || return 0
    dev_homebrew_sudo_keepalive_loop &
    DEV_HOMEBREW_KEEPALIVE_PID=$!
}

dev_homebrew_stop_sudo_keepalive() {
    case "${DEV_HOMEBREW_KEEPALIVE_PID:-}" in
        ''|*[!0-9]*) DEV_HOMEBREW_KEEPALIVE_PID=; return 0 ;;
    esac
    if kill -0 "$DEV_HOMEBREW_KEEPALIVE_PID" 2>/dev/null; then
        dev_terminate_process_tree "$DEV_HOMEBREW_KEEPALIVE_PID" TERM
    fi
    wait "$DEV_HOMEBREW_KEEPALIVE_PID" 2>/dev/null || true
    DEV_HOMEBREW_KEEPALIVE_PID=
}

dev_homebrew_cleanup_install() {
    dev_homebrew_stop_sudo_keepalive
    if [ -n "${DEV_HOMEBREW_INSTALLER:-}" ]; then
        rm -f "$DEV_HOMEBREW_INSTALLER"
    fi
    if [ -n "${DEV_INSTALL_TMP:-}" ]; then
        rmdir "$DEV_INSTALL_TMP" 2>/dev/null || true
    fi

    # Match Homebrew's cleanup policy for a timestamp created during this run.
    # The explicit marker prevents cached and NOPASSWD authorization from being
    # mistaken for a timestamp owned by this bootstrap.
    if [ "${DEV_HOMEBREW_CREATED_SUDO_TICKET:-0}" -eq 1 ]; then
        dev_sudo -k >/dev/null 2>&1 || true
    fi
}

dev_homebrew_run_installer() {
    if [ -n "${DEV_HOMEBREW_SUDO_ASKPASS:-}" ]; then
        env -u INTERACTIVE -u POSIXLY_CORRECT \
            SUDO_ASKPASS="$DEV_HOMEBREW_SUDO_ASKPASS" NONINTERACTIVE=1 \
            /bin/bash "$DEV_HOMEBREW_INSTALLER"
    else
        env -u SUDO_ASKPASS -u INTERACTIVE -u POSIXLY_CORRECT \
            NONINTERACTIVE=1 /bin/bash "$DEV_HOMEBREW_INSTALLER"
    fi
}

dev_homebrew_run_installer_bounded() {
    if dev_run_bounded 1200 "Homebrew installation" dev_homebrew_run_installer; then
        DEV_HOMEBREW_INSTALL_STATUS=0
    else
        DEV_HOMEBREW_INSTALL_STATUS=$?
    fi
    return "$DEV_HOMEBREW_INSTALL_STATUS"
}

dev_install_homebrew() {
    if DEV_BREW=$(dev_find_brew); then
        export DEV_BREW
        return 0
    fi

    (
        DEV_INSTALL_TMP=
        DEV_HOMEBREW_INSTALLER=
        DEV_HOMEBREW_HAD_SUDO_TICKET=0
        DEV_HOMEBREW_CREATED_SUDO_TICKET=0
        DEV_HOMEBREW_SUDO_ASKPASS=
        DEV_HOMEBREW_KEEPALIVE_PID=
        trap 'dev_homebrew_cleanup_install' EXIT
        trap 'exit 130' HUP INT TERM

        if ! dev_homebrew_authorize_sudo; then
            exit 13
        fi
        dev_homebrew_start_sudo_keepalive
        DEV_INSTALL_TMP=$(mktemp -d "${TMPDIR:-/tmp}/zkcode-dev-tools.XXXXXX") || exit 1
        DEV_HOMEBREW_INSTALLER="$DEV_INSTALL_TMP/install-homebrew.sh"
        if ! curl --fail --location --show-error --silent \
            --connect-timeout 10 --max-time 120 --retry 2 \
            https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh \
            --output "$DEV_HOMEBREW_INSTALLER"; then
            exit 1
        fi
        if ! dev_homebrew_run_installer_bounded; then
            exit 1
        fi
    )
    DEV_INSTALL_STATUS=$?
    [ "$DEV_INSTALL_STATUS" -eq 0 ] || return "$DEV_INSTALL_STATUS"
    DEV_BREW=$(dev_find_brew) || return 1
    export DEV_BREW
}

dev_ensure_brew_formula() {
    DEV_FORMULA=$1
    "$DEV_BREW" list --versions "$DEV_FORMULA" >/dev/null 2>&1 || \
        dev_run_bounded 1800 "Homebrew installation of $DEV_FORMULA" \
            env HOMEBREW_NO_AUTO_UPDATE=1 "$DEV_BREW" install "$DEV_FORMULA"
}

dev_install_rust_toolchain() {
    if ! command -v rustup >/dev/null 2>&1; then
        DEV_RUST_TMP=$(mktemp -d "${TMPDIR:-/tmp}/zkcode-rustup.XXXXXX") || return 1
        DEV_RUST_INSTALLER="$DEV_RUST_TMP/rustup-init.sh"
        if ! curl --fail --location --show-error --silent \
            --connect-timeout 10 --max-time 120 --retry 2 \
            https://sh.rustup.rs --output "$DEV_RUST_INSTALLER"; then
            rm -f "$DEV_RUST_INSTALLER"
            rmdir "$DEV_RUST_TMP" 2>/dev/null || true
            return 1
        fi
        if dev_run_bounded 1200 "rustup installation" /bin/sh "$DEV_RUST_INSTALLER" \
            -y --profile minimal --default-toolchain none --no-modify-path; then
            DEV_RUST_STATUS=0
        else
            DEV_RUST_STATUS=$?
        fi
        rm -f "$DEV_RUST_INSTALLER"
        rmdir "$DEV_RUST_TMP" 2>/dev/null || true
        [ "$DEV_RUST_STATUS" -eq 0 ] || return "$DEV_RUST_STATUS"
        dev_activate_toolchains
    fi
    dev_run_bounded 1200 "Rust $DEV_REQUIRED_RUST installation" rustup toolchain install \
        "$DEV_REQUIRED_RUST" --profile minimal --component rustfmt --component clippy
}

dev_install_missing_toolchains() {
    dev_confirm_external_install
    dev_wait_for_clt || dev_fail 13 "Xcode Command Line Tools did not become available"

    DEV_NEED_BREW=0
    dev_node_is_supported || DEV_NEED_BREW=1
    dev_python_is_supported || DEV_NEED_BREW=1
    if [ "$DEV_NEED_BREW" -eq 1 ]; then
        dev_info "Installing supported Node and Python side-by-side with Homebrew"
        if dev_install_homebrew; then
            :
        else
            DEV_HOMEBREW_STATUS=$?
            [ "$DEV_HOMEBREW_STATUS" -ne 13 ] || \
                dev_fail 13 "Homebrew installation needs administrator authorization"
            dev_fail 11 "Homebrew installation failed"
        fi
        dev_ensure_brew_formula "$ZK_DEV_NODE_FORMULA" || dev_fail 11 "Homebrew $ZK_DEV_NODE_FORMULA installation failed"
        dev_ensure_brew_formula "$ZK_DEV_PYTHON_FORMULA" || dev_fail 11 "Homebrew $ZK_DEV_PYTHON_FORMULA installation failed"
        dev_activate_toolchains
    fi

    if ! dev_rust_is_supported; then
        dev_info "Installing the repository Rust toolchain with rustup"
        dev_install_rust_toolchain || dev_fail 11 "Rust $DEV_REQUIRED_RUST installation failed"
        dev_activate_toolchains
    fi
}

dev_resolve_toolchains() {
    DEV_ALLOW_INSTALL=${1:-0}
    dev_activate_toolchains
    dev_preflight_platform
    dev_python_is_supported || true

    if ! xcode-select -p >/dev/null 2>&1 || ! dev_node_is_supported || \
        ! dev_python_is_supported || ! dev_rust_is_supported; then
        if [ "$DEV_ALLOW_INSTALL" -eq 1 ]; then
            dev_install_missing_toolchains
            dev_python_is_supported || true
        else
            dev_toolchain_report
            dev_fail 14 "toolchains are missing or incompatible; run ./dev bootstrap"
        fi
    fi

    dev_node_is_supported || dev_fail 14 "Node $DEV_REQUIRED_NODE_RANGE and npm $DEV_REQUIRED_NPM_RANGE are required"
    dev_python_is_supported || dev_fail 14 "Python $DEV_REQUIRED_PYTHON_RANGE is required"
    dev_rust_is_supported || dev_fail 14 "Rust $DEV_REQUIRED_RUST is required"
    export DEV_PYTHON
}
