#!/bin/sh
set -eu

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
python3 "$ROOT_DIR/scripts/parity/check_contracts.py"
"$ROOT_DIR/scripts/parity/check-demo-credential-db.sh"
