#!/bin/sh
set -eu

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
SCAN_DIR=$(mktemp -d "${TMPDIR:-/tmp}/zkcode-gitleaks.XXXXXX")

cleanup() {
  case "$SCAN_DIR" in
    */zkcode-gitleaks.*) rm -rf -- "$SCAN_DIR" ;;
    *) echo "refusing to remove unexpected scan directory: $SCAN_DIR" >&2 ;;
  esac
}
trap cleanup EXIT HUP INT TERM

# `gitleaks dir .` traverses dependency and compiler outputs even when every
# finding under them is allowlisted. Build a temporary publication candidate
# using the repository's own ignore rules, then scan every byte that can be
# included in the first commit. Repository metadata is not publication input.
# File bytes and relative paths are the only inputs to secret detection. Avoid
# archive-mode owner/group/device metadata: macOS rsync can block while applying
# that metadata inside a managed temporary directory.
rsync -rlt \
  --exclude='.git/' \
  --exclude-from="$ROOT_DIR/.gitignore" \
  "$ROOT_DIR/" "$SCAN_DIR/"

cd "$ROOT_DIR"
gitleaks dir "$SCAN_DIR" --redact --config "$ROOT_DIR/.gitleaks-local.toml"
