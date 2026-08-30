#!/bin/sh

dev_doctor() {
    DEV_DOCTOR_DEEP=${1:-0}
    DEV_DOCTOR_JSON=${2:-0}
    DEV_DOCTOR_ARGS=""
    [ "$DEV_DOCTOR_DEEP" -eq 0 ] || DEV_DOCTOR_ARGS="$DEV_DOCTOR_ARGS --deep"
    [ "$DEV_DOCTOR_JSON" -eq 0 ] || DEV_DOCTOR_ARGS="$DEV_DOCTOR_ARGS --json"
    # shellcheck disable=SC2086 -- arguments are fixed flags assembled above.
    "$DEV_PYTHON" "$ROOT_DIR/scripts/dev/inspect.py" doctor \
        --root "$ROOT_DIR" --port "$(dev_backend_port_for_diagnostics)" $DEV_DOCTOR_ARGS
}

dev_status() {
    DEV_STATUS_JSON=${1:-0}
    if [ "$DEV_STATUS_JSON" -eq 1 ]; then
        "$DEV_PYTHON" "$ROOT_DIR/scripts/dev/inspect.py" status \
            --root "$ROOT_DIR" --port "$(dev_backend_port)" --json
    else
        "$DEV_PYTHON" "$ROOT_DIR/scripts/dev/inspect.py" status \
            --root "$ROOT_DIR" --port "$(dev_backend_port)"
    fi
}
