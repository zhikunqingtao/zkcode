#!/bin/sh
set -eu

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
SEED_DB="$ROOT_DIR/configuration/bootstrap/demo-credentials.db"

if ! command -v sqlite3 >/dev/null 2>&1; then
  echo "missing sqlite3; macOS ships it and the demo credential gate requires it" >&2
  exit 2
fi
if [ ! -f "$SEED_DB" ]; then
  echo "missing tracked public demo credential database" >&2
  exit 1
fi

APPLICATION_ID=$(sqlite3 -readonly "$SEED_DB" 'PRAGMA application_id;')
SCHEMA_VERSION=$(sqlite3 -readonly "$SEED_DB" 'PRAGMA user_version;')
ROW_COUNT=$(sqlite3 -readonly "$SEED_DB" \
  "SELECT count(*) FROM public_demo_credentials;")
VALID_ROW_COUNT=$(sqlite3 -readonly "$SEED_DB" \
  "SELECT count(*) FROM public_demo_credentials WHERE provider='dashscope-token-plan' AND purpose='public-first-run-demo' AND api_key GLOB 'sk-sp-*' AND length(api_key) BETWEEN 40 AND 512;")

if [ "$APPLICATION_ID" != "1514885956" ] || \
   [ "$SCHEMA_VERSION" != "1" ] || \
   [ "$ROW_COUNT" != "1" ] || \
   [ "$VALID_ROW_COUNT" != "1" ]; then
  echo "public demo credential database failed its fixed schema/identity checks" >&2
  exit 1
fi

echo "public demo credential database: valid (credential value not displayed)"
