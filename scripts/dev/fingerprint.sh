#!/bin/sh

dev_fingerprint() {
    DEV_FP_COMPONENT=$1
    DEV_FP_PYTHON=${DEV_PYTHON:-$(dev_env_python)}
    case "$DEV_FP_COMPONENT" in
        frontend)
            "$DEV_FP_PYTHON" "$ROOT_DIR/scripts/dev/state.py" fingerprint \
                --root "$ROOT_DIR" --component frontend \
                --version "$(node --version)" --version "$(npm --version)"
            ;;
        python)
            "$DEV_FP_PYTHON" "$ROOT_DIR/scripts/dev/state.py" fingerprint \
                --root "$ROOT_DIR" --component python \
                --version "$($DEV_FP_PYTHON --version 2>&1)" \
                --version "$($DEV_FP_PYTHON -c 'import sys; print(sys.executable); print(sys.implementation.cache_tag)')"
            ;;
        browser)
            "$DEV_FP_PYTHON" "$ROOT_DIR/scripts/dev/state.py" fingerprint \
                --root "$ROOT_DIR" --component browser \
                --version "$($DEV_FP_PYTHON --version 2>&1)" --version only-shell
            ;;
        rust)
            "$DEV_FP_PYTHON" "$ROOT_DIR/scripts/dev/state.py" fingerprint \
                --root "$ROOT_DIR" --component rust --version "$(rustc --version)" \
                --version "$(rustc -vV | sed -n 's/^host: //p')"
            ;;
        *) dev_fail 2 "unknown fingerprint component: $DEV_FP_COMPONENT" ;;
    esac
}

dev_prepare_fingerprints() {
    DEV_FP_PYTHON=${DEV_PYTHON:-$(dev_env_python)}
    DEV_FP_BATCH=$("$DEV_FP_PYTHON" "$ROOT_DIR/scripts/dev/state.py" fingerprints \
        --root "$ROOT_DIR" \
        --node-version "$(node --version)" \
        --npm-version "$(npm --version)" \
        --python-version "$($DEV_FP_PYTHON --version 2>&1)" \
        --python-identity "$($DEV_FP_PYTHON -c 'import sys; print(sys.executable); print(sys.implementation.cache_tag)')" \
        --rust-version "$(rustc --version)" \
        --rust-host "$(rustc -vV | sed -n 's/^host: //p')")
    DEV_FP_FRONTEND=$(printf '%s\n' "$DEV_FP_BATCH" | sed -n 's/^frontend[[:space:]]//p')
    DEV_FP_PYTHON_COMPONENT=$(printf '%s\n' "$DEV_FP_BATCH" | sed -n 's/^python[[:space:]]//p')
    DEV_FP_BROWSER=$(printf '%s\n' "$DEV_FP_BATCH" | sed -n 's/^browser[[:space:]]//p')
    DEV_FP_RUST=$(printf '%s\n' "$DEV_FP_BATCH" | sed -n 's/^rust[[:space:]]//p')
    [ -n "$DEV_FP_FRONTEND" ] && [ -n "$DEV_FP_PYTHON_COMPONENT" ] && \
        [ -n "$DEV_FP_BROWSER" ] && [ -n "$DEV_FP_RUST" ] || \
        dev_fail 2 "could not calculate component fingerprints"
}

dev_state_fingerprint() {
    DEV_FP_PYTHON=${DEV_PYTHON:-$(dev_env_python)}
    "$DEV_FP_PYTHON" "$ROOT_DIR/scripts/dev/state.py" get \
        --state "$DEV_STATE_FILE" --component "$1"
}

dev_mark_component() {
    DEV_MARK_COMPONENT=$1
    DEV_MARK_FINGERPRINT=$2
    if [ "$#" -ge 3 ]; then
        DEV_MARK_METADATA=$3
    else
        DEV_MARK_METADATA='{}'
    fi
    DEV_FP_PYTHON=${DEV_PYTHON:-$(dev_env_python)}
    "$DEV_FP_PYTHON" "$ROOT_DIR/scripts/dev/state.py" set \
        --state "$DEV_STATE_FILE" --component "$DEV_MARK_COMPONENT" \
        --fingerprint "$DEV_MARK_FINGERPRINT" --metadata "$DEV_MARK_METADATA"
}
