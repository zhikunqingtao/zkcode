#!/bin/sh
set -eu

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$ROOT_DIR/frontend"

# npm 10 still occasionally selects the retiring quick-audit endpoint, which
# can return a transient HTTP 400 even for a valid tree. Retry the official
# audit once; a real advisory remains non-zero on both attempts and still
# fails the release gate.
if npm audit --audit-level=high; then
  exit 0
fi
echo "npm audit failed; retrying the official registry once" >&2
npm audit --audit-level=high
