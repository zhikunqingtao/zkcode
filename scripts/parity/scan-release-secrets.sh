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

# rsync's --exclude-from syntax does not implement gitignore negation.  Restore
# the single deliberate `*.db` publication exception explicitly so the local
# release scan examines the same public asset that the repository will track.
mkdir -p "$SCAN_DIR/configuration/bootstrap"
cp -p "$ROOT_DIR/configuration/bootstrap/demo-credentials.db" \
  "$SCAN_DIR/configuration/bootstrap/demo-credentials.db"

# Scan from inside the candidate so allowlist rules match publication-relative
# paths. This also keeps a caller-provided TMPDIR under `.runtime/` from making
# every candidate path look runtime-only and therefore allowlisted.
cd "$SCAN_DIR"
gitleaks dir . --redact --config "$ROOT_DIR/.gitleaks-local.toml"
