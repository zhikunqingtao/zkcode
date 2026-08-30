#!/bin/sh

dev_npm_ci_command() {
    (cd "$ROOT_DIR/frontend" && npm ci $DEV_NPM_OFFLINE)
}

dev_pip_build_dependencies() {
    "$DEV_VENV_PYTHON" -m pip install --disable-pip-version-check $DEV_PIP_OFFLINE \
        -r "$ROOT_DIR/python-service/build-requirements.lock"
}

dev_pip_runtime_dependencies() {
    "$DEV_VENV_PYTHON" -m pip install --disable-pip-version-check $DEV_PIP_OFFLINE \
        -r "$ROOT_DIR/python-service/requirements.lock"
}

dev_pip_editable_project() {
    (cd "$ROOT_DIR/python-service" && "$DEV_VENV_PYTHON" -m pip install \
        --disable-pip-version-check --no-deps --no-build-isolation -e '.[full,test]')
}

dev_playwright_install_stage() {
    PLAYWRIGHT_BROWSERS_PATH="$DEV_BROWSER_STAGE" \
    PLAYWRIGHT_DOWNLOAD_CONNECTION_TIMEOUT=${PLAYWRIGHT_DOWNLOAD_CONNECTION_TIMEOUT:-120000} \
        "$ROOT_DIR/python-service/.venv/bin/python" -m playwright install --only-shell chromium
}

dev_cargo_fetch_command() {
    if [ "${DEV_OFFLINE:-0}" -eq 1 ]; then
        (cd "$ROOT_DIR" && cargo fetch --locked --offline)
    else
        (cd "$ROOT_DIR" && cargo fetch --locked)
    fi
}

dev_cargo_build_command() {
    if [ "${DEV_OFFLINE:-0}" -eq 1 ]; then
        (cd "$ROOT_DIR" && cargo build --locked --offline -p zk-server)
    else
        (cd "$ROOT_DIR" && cargo build --locked -p zk-server)
    fi
}

dev_component_current() {
    DEV_CURRENT_COMPONENT=$1
    DEV_CURRENT_FINGERPRINT=$2
    [ "$(dev_state_fingerprint "$DEV_CURRENT_COMPONENT")" = "$DEV_CURRENT_FINGERPRINT" ]
}

dev_journal_path() {
    printf '%s/%s.journal\n' "$DEV_STATE_DIR" "$1"
}

dev_recover_component() {
    DEV_RECOVER_COMPONENT=$1
    DEV_RECOVER_TARGET=$2
    DEV_RECOVER_JOURNAL=$(dev_journal_path "$DEV_RECOVER_COMPONENT")
    [ -f "$DEV_RECOVER_JOURNAL" ] || return 0
    DEV_RECOVER_BACKUP=$(sed -n '1p' "$DEV_RECOVER_JOURNAL")
    case "$DEV_RECOVER_BACKUP" in
        "$DEV_PREVIOUS_DIR"/"$DEV_RECOVER_COMPONENT"-*) ;;
        *) dev_fail 13 "invalid recovery journal for $DEV_RECOVER_COMPONENT" ;;
    esac
    DEV_RECOVER_NAME=${DEV_RECOVER_BACKUP#"$DEV_PREVIOUS_DIR"/}
    case "$DEV_RECOVER_NAME" in
        ''|.|..|*/*) dev_fail 13 "invalid recovery journal for $DEV_RECOVER_COMPONENT" ;;
    esac
    if [ -e "$DEV_RECOVER_BACKUP" ]; then
        dev_warn "recovering $DEV_RECOVER_COMPONENT from an interrupted sync"
        dev_remove_generated "$DEV_RECOVER_TARGET"
        mv "$DEV_RECOVER_BACKUP" "$DEV_RECOVER_TARGET"
    fi
    rm -f "$DEV_RECOVER_JOURNAL"
}

dev_begin_component_replace() {
    DEV_REPLACE_COMPONENT=$1
    DEV_REPLACE_TARGET=$2
    dev_assert_generated_path "$DEV_REPLACE_TARGET"
    dev_recover_component "$DEV_REPLACE_COMPONENT" "$DEV_REPLACE_TARGET"
    case "$DEV_REPLACE_COMPONENT" in
        frontend)
            if dev_pid_is_live "$DEV_RUNTIME_DIR/frontend.pid" "$ROOT_DIR/frontend/node_modules/.bin/vite"; then
                dev_warn "frontend dependencies changed; stopping Vite for a recoverable replacement"
                dev_stop_services frontend
            fi
            ;;
        python|browser)
            if dev_pid_is_live "$DEV_RUNTIME_DIR/backend.pid" "$ROOT_DIR/target/debug/zk-server"; then
                dev_warn "$DEV_REPLACE_COMPONENT dependencies changed; stopping backend and sidecar for a recoverable replacement"
                dev_stop_services backend
            fi
            ;;
    esac
    DEV_REPLACE_BACKUP=
    if [ -e "$DEV_REPLACE_TARGET" ]; then
        DEV_REPLACE_BACKUP="$DEV_PREVIOUS_DIR/$DEV_REPLACE_COMPONENT-$(dev_now_id)-$$"
        mv "$DEV_REPLACE_TARGET" "$DEV_REPLACE_BACKUP"
        printf '%s\n' "$DEV_REPLACE_BACKUP" >"$(dev_journal_path "$DEV_REPLACE_COMPONENT")"
    fi
}

dev_rollback_component_replace() {
    DEV_ROLLBACK_COMPONENT=$1
    DEV_ROLLBACK_TARGET=$2
    dev_remove_generated "$DEV_ROLLBACK_TARGET"
    if [ -n "${DEV_REPLACE_BACKUP:-}" ] && [ -e "$DEV_REPLACE_BACKUP" ]; then
        mv "$DEV_REPLACE_BACKUP" "$DEV_ROLLBACK_TARGET"
    fi
    rm -f "$(dev_journal_path "$DEV_ROLLBACK_COMPONENT")"
}

dev_finish_component_replace() {
    DEV_FINISH_COMPONENT=$1
    if [ -n "${DEV_REPLACE_BACKUP:-}" ] && [ -e "$DEV_REPLACE_BACKUP" ]; then
        dev_remove_previous "$DEV_REPLACE_BACKUP"
    fi
    rm -f "$(dev_journal_path "$DEV_FINISH_COMPONENT")"
    DEV_REPLACE_BACKUP=
}

dev_frontend_smoke() {
    [ -x "$ROOT_DIR/frontend/node_modules/.bin/vite" ] || return 1
    "$DEV_PYTHON" "$ROOT_DIR/scripts/dev/state.py" verify-npm-lock \
        --root "$ROOT_DIR" >/dev/null 2>&1 || return 1
    (cd "$ROOT_DIR/frontend" && npm ls --depth=0 --silent >/dev/null 2>&1)
}

dev_frontend_smoke_bounded() {
    dev_run_bounded 300 "frontend dependency smoke" dev_frontend_smoke
}

dev_frontend_assets_present() {
    [ -x "$ROOT_DIR/frontend/node_modules/.bin/vite" ] && \
        [ -f "$ROOT_DIR/frontend/node_modules/.package-lock.json" ]
}

dev_sync_frontend() {
    DEV_FRONTEND_FP=${DEV_FP_FRONTEND:-$(dev_fingerprint frontend)}
    if [ "${DEV_FORCE_COMPONENT:-}" != frontend ] && dev_component_current frontend "$DEV_FRONTEND_FP" && dev_frontend_smoke_bounded; then
        DEV_PLAN_FRONTEND=reuse
        dev_note "frontend dependencies: unchanged"
        return 0
    fi
    if [ "${DEV_FORCE_COMPONENT:-}" != frontend ] && [ -z "$(dev_state_fingerprint frontend)" ] && dev_frontend_smoke_bounded; then
        DEV_PLAN_FRONTEND=verify-or-sync
        dev_note "frontend dependencies: adopting the existing verified node_modules"
        dev_mark_component frontend "$DEV_FRONTEND_FP"
        return 0
    fi

    dev_info "Synchronizing locked frontend dependencies"
    DEV_PLAN_FRONTEND=verify-or-sync
    dev_begin_component_replace frontend "$ROOT_DIR/frontend/node_modules"
    DEV_NPM_OFFLINE=
    [ "${DEV_OFFLINE:-0}" -eq 0 ] || DEV_NPM_OFFLINE=--offline
    if ! dev_run_bounded 1800 "npm ci" dev_npm_ci_command; then
        dev_rollback_component_replace frontend "$ROOT_DIR/frontend/node_modules"
        dev_fail 15 "frontend dependency sync failed; retry with ./dev repair frontend"
    fi
    if ! dev_frontend_smoke_bounded; then
        dev_rollback_component_replace frontend "$ROOT_DIR/frontend/node_modules"
        dev_fail 15 "frontend dependency smoke failed after npm ci"
    fi
    dev_mark_component frontend "$DEV_FRONTEND_FP"
    dev_finish_component_replace frontend
}

dev_python_smoke() {
    DEV_VENV_PYTHON="$ROOT_DIR/python-service/.venv/bin/python"
    [ -x "$DEV_VENV_PYTHON" ] || return 1
    DEV_VENV_VERSION=$("$DEV_VENV_PYTHON" -c 'import sys; print(".".join(map(str, sys.version_info[:3])))')
    dev_semver_satisfies "$DEV_VENV_VERSION" "$DEV_REQUIRED_PYTHON_RANGE" || return 1
    "$DEV_VENV_PYTHON" -m pip check >/dev/null 2>&1 || return 1
    "$DEV_VENV_PYTHON" -m pip install --disable-pip-version-check --dry-run \
        --no-index --no-deps -r "$ROOT_DIR/python-service/build-requirements.lock" \
        >/dev/null 2>&1 || return 1
    "$DEV_VENV_PYTHON" -m pip install --disable-pip-version-check --dry-run \
        --no-index --no-deps -r "$ROOT_DIR/python-service/requirements.lock" \
        >/dev/null 2>&1 || return 1
    DEV_EDITABLE_INFO=$("$DEV_VENV_PYTHON" -m pip show zkcode-python-service 2>/dev/null) || return 1
    printf '%s\n' "$DEV_EDITABLE_INFO" | \
        grep -Fqx "Editable project location: $ROOT_DIR/python-service" || return 1
    (cd "$ROOT_DIR/python-service" && \
        PYTHONPATH=./src \
        "$DEV_VENV_PYTHON" -c \
        'import importlib, fastapi, pydantic, uvicorn; import src.main; [importlib.import_module(name) for name in ("routers.code_intel", "routers.file_processing", "routers.git_enhanced", "routers.browser", "routers.code_quality", "routers.analysis", "routers.http_api")]' \
        >/dev/null 2>&1)
}

dev_python_smoke_bounded() {
    dev_run_bounded 600 "Python dependency smoke" dev_python_smoke
}

dev_sync_python() {
    DEV_PYTHON_FP=${DEV_FP_PYTHON_COMPONENT:-$(dev_fingerprint python)}
    if [ "${DEV_FORCE_COMPONENT:-}" != python ] && dev_component_current python "$DEV_PYTHON_FP" && dev_python_smoke_bounded; then
        DEV_PLAN_PYTHON=reuse
        dev_note "Python environment: unchanged"
        return 0
    fi
    if [ "${DEV_FORCE_COMPONENT:-}" != python ] && [ -z "$(dev_state_fingerprint python)" ] && dev_python_smoke_bounded; then
        DEV_PLAN_PYTHON=verify-or-sync
        dev_note "Python environment: adopting the existing verified venv"
        dev_mark_component python "$DEV_PYTHON_FP"
        return 0
    fi

    dev_info "Rebuilding the locked Python 3.11 environment"
    DEV_PLAN_PYTHON=verify-or-sync
    dev_begin_component_replace python "$ROOT_DIR/python-service/.venv"
    if ! "$DEV_PYTHON" -m venv "$ROOT_DIR/python-service/.venv"; then
        dev_rollback_component_replace python "$ROOT_DIR/python-service/.venv"
        dev_fail 16 "could not create the Python virtual environment"
    fi
    DEV_VENV_PYTHON="$ROOT_DIR/python-service/.venv/bin/python"
    DEV_PIP_OFFLINE=
    [ "${DEV_OFFLINE:-0}" -eq 0 ] || DEV_PIP_OFFLINE=--no-index
    if ! dev_run_bounded 1800 "Python build dependency installation" dev_pip_build_dependencies || \
       ! dev_run_bounded 1800 "Python locked dependency installation" dev_pip_runtime_dependencies || \
       ! dev_run_bounded 600 "Python editable installation" dev_pip_editable_project; then
        dev_rollback_component_replace python "$ROOT_DIR/python-service/.venv"
        dev_fail 16 "Python dependency sync failed; the previous venv was restored"
    fi
    if ! dev_python_smoke_bounded; then
        dev_rollback_component_replace python "$ROOT_DIR/python-service/.venv"
        dev_fail 16 "Python import smoke failed; the previous venv was restored"
    fi
    dev_mark_component python "$DEV_PYTHON_FP"
    dev_finish_component_replace python
}

dev_browser_smoke() {
    DEV_BROWSER_SMOKE_PATH=${1:-$ROOT_DIR/.runtime/playwright}
    [ -d "$DEV_BROWSER_SMOKE_PATH" ] || return 1
    PLAYWRIGHT_BROWSERS_PATH="$DEV_BROWSER_SMOKE_PATH" \
        "$ROOT_DIR/python-service/.venv/bin/python" -c \
        'from playwright.sync_api import sync_playwright
with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page()
    page.set_content("<title>zkcode-dev-smoke</title>")
    assert page.title() == "zkcode-dev-smoke"
    browser.close()' >/dev/null 2>&1
}

dev_browser_smoke_bounded() {
    DEV_BROWSER_SMOKE_BOUND_PATH=${1:-$ROOT_DIR/.runtime/playwright}
    dev_run_bounded 180 "Playwright launch smoke" \
        dev_browser_smoke "$DEV_BROWSER_SMOKE_BOUND_PATH"
}

dev_browser_assets_present() {
    DEV_BROWSER_ASSET_PATH=${1:-$ROOT_DIR/.runtime/playwright}
    [ -d "$DEV_BROWSER_ASSET_PATH" ] || return 1
    DEV_HEADLESS_EXECUTABLE=$(find "$DEV_BROWSER_ASSET_PATH" -type f \
        -name 'chrome-headless-shell' -perm -111 -print 2>/dev/null | sed -n '1p')
    DEV_FFMPEG_EXECUTABLE=$(find "$DEV_BROWSER_ASSET_PATH" -type f \
        -name 'ffmpeg*' -perm -111 -print 2>/dev/null | sed -n '1p')
    [ -n "$DEV_HEADLESS_EXECUTABLE" ] && [ -n "$DEV_FFMPEG_EXECUTABLE" ]
}

dev_sync_browser() {
    DEV_BROWSER_FP=${DEV_FP_BROWSER:-$(dev_fingerprint browser)}
    if [ "${DEV_FORCE_COMPONENT:-}" != browser ] && dev_component_current browser "$DEV_BROWSER_FP" && dev_browser_assets_present; then
        DEV_PLAN_BROWSER=reuse
        dev_note "Playwright Headless Shell: unchanged"
        return 0
    fi
    if [ "${DEV_FORCE_COMPONENT:-}" != browser ] && [ -z "$(dev_state_fingerprint browser)" ] && dev_browser_smoke_bounded; then
        DEV_PLAN_BROWSER=verify-or-sync
        dev_note "Playwright Headless Shell: adopting the existing verified runtime"
        dev_mark_component browser "$DEV_BROWSER_FP" '{"distribution":"chromium-headless-shell"}'
        return 0
    fi

    dev_info "Installing the Playwright Headless Shell and FFmpeg"
    DEV_PLAN_BROWSER=verify-or-sync
    if [ "${DEV_OFFLINE:-0}" -eq 1 ]; then
        dev_fail 16 "Playwright runtime is missing and cannot be downloaded in --offline mode"
    fi
    dev_begin_component_replace browser "$ROOT_DIR/.runtime/playwright"

    # Playwright protects its cache with a frequently touched directory lock.
    # Desktop folders can be watched by sync/indexing software that changes
    # directory mtimes and makes proper-lockfile report a compromised lock.
    # Download in a private temporary directory, validate there, then move the
    # completed immutable runtime into the repository-local final path.
    DEV_BROWSER_TMP=$(mktemp -d "${TMPDIR:-/tmp}/zkcode-playwright.XXXXXX") || {
        dev_rollback_component_replace browser "$ROOT_DIR/.runtime/playwright"
        dev_fail 13 "could not create a Playwright staging directory"
    }
    DEV_BROWSER_STAGE="$DEV_BROWSER_TMP/runtime"
    mkdir -p "$DEV_BROWSER_STAGE"
    if ! dev_run_bounded 1800 "Playwright Headless Shell installation" dev_playwright_install_stage; then
        rm -rf -- "$DEV_BROWSER_TMP"
        dev_rollback_component_replace browser "$ROOT_DIR/.runtime/playwright"
        dev_fail 16 "Playwright Headless Shell installation failed"
    fi
    if ! dev_browser_smoke_bounded "$DEV_BROWSER_STAGE"; then
        rm -rf -- "$DEV_BROWSER_TMP"
        dev_rollback_component_replace browser "$ROOT_DIR/.runtime/playwright"
        dev_fail 16 "Playwright Headless Shell smoke failed"
    fi
    mv "$DEV_BROWSER_STAGE" "$ROOT_DIR/.runtime/playwright"
    rmdir "$DEV_BROWSER_TMP" 2>/dev/null || true
    dev_mark_component browser "$DEV_BROWSER_FP" '{"distribution":"chromium-headless-shell"}'
    dev_finish_component_replace browser
}

dev_sync_rust() {
    DEV_RUST_FP=${DEV_FP_RUST:-$(dev_fingerprint rust)}
    if [ "${DEV_FORCE_COMPONENT:-}" != rust ] && dev_component_current rust "$DEV_RUST_FP"; then
        DEV_PLAN_RUST=reuse
        dev_note "Rust dependency graph: unchanged"
        return 0
    fi
    dev_info "Fetching the locked Rust dependency graph"
    DEV_PLAN_RUST=verify-or-sync
    if ! dev_run_bounded 1200 "locked Rust dependency fetch" dev_cargo_fetch_command; then
        [ "${DEV_OFFLINE:-0}" -eq 0 ] || dev_fail 11 "locked Rust dependencies are incomplete in offline cache"
        dev_fail 11 "locked Rust dependency fetch failed"
    fi
    dev_mark_component rust "$DEV_RUST_FP"
}

dev_build_backend() {
    dev_info "Building zk-server from the current source"
    dev_run_bounded 2700 "zk-server build" dev_cargo_build_command || \
        dev_fail 17 "Rust build failed or timed out; the running instance was not stopped"
}

dev_sync_all() {
    DEV_PLAN_FRONTEND=verify-or-sync
    DEV_PLAN_PYTHON=verify-or-sync
    DEV_PLAN_BROWSER=verify-or-sync
    DEV_PLAN_RUST=verify-or-sync
    dev_prepare_fingerprints
    dev_sync_frontend
    dev_sync_python
    dev_sync_browser
    dev_sync_rust
    dev_write_sync_plan
}

dev_write_sync_plan() {
    "$DEV_PYTHON" "$ROOT_DIR/scripts/dev/state.py" plan \
        --path "$DEV_STATE_DIR/last-plan.json" \
        --entry "frontend=$DEV_PLAN_FRONTEND" \
        --entry "python=$DEV_PLAN_PYTHON" \
        --entry "browser=$DEV_PLAN_BROWSER" \
        --entry "rust=$DEV_PLAN_RUST"
}

dev_repair_component() {
    DEV_REPAIR_COMPONENT=$1
    case "$DEV_REPAIR_COMPONENT" in
        frontend)
            DEV_FORCE_COMPONENT=frontend
            dev_sync_frontend
            ;;
        python)
            DEV_FORCE_COMPONENT=python
            dev_sync_python
            ;;
        browser)
            DEV_FORCE_COMPONENT=browser
            dev_sync_browser
            ;;
        rust)
            DEV_FORCE_COMPONENT=rust
            dev_sync_rust
            ;;
        build)
            dev_build_backend
            ;;
        *) dev_fail 2 "repair expects frontend, python, browser, rust, or build" ;;
    esac
    DEV_FORCE_COMPONENT=
}
