#!/bin/bash
# One command to get from a fresh clone to a buildable indexer: check the
# tools are present and build. Postgres is bring-your-own — point
# --database-url (or RETRACER_TEST_DATABASE_URL for tests) at whatever
# instance you already run.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

DB_URL="${RETRACER_TEST_DATABASE_URL:-postgres://retracer:retracer@localhost:5433/retracer}"

say() { printf '\033[1;34m==>\033[0m %s\n' "$1"; }
die() { printf '\033[1;31merror:\033[0m %s\n' "$1" >&2; exit 1; }

say "Checking prerequisites"
command -v cargo >/dev/null || die "cargo not found — install Rust from https://rustup.rs"
echo "    rust   $(rustc --version | cut -d' ' -f2)"

if command -v pg_isready >/dev/null; then
  if pg_isready -d "$DB_URL" >/dev/null 2>&1; then
    echo "    postgres  reachable at $DB_URL"
  else
    printf '\033[1;33mwarning:\033[0m no Postgres reachable at %s yet — point --database-url at your own instance before running retracerd.\n' "$DB_URL"
  fi
else
  echo "    postgres  (pg_isready not installed — skipping reachability check)"
fi

say "Building the workspace"
cargo build --workspace

cat <<DONE

Setup complete.

  Database   $DB_URL (override with --database-url or RETRACER_TEST_DATABASE_URL)
  Run        cargo run -p retracerd -- --bootnodes <multiaddr> --database-url <your-postgres-url>
  Test       ./scripts/test.sh
  Reset DB   ./scripts/reset-db.sh

Migrations apply automatically on startup. With the indexer running:

  curl localhost:8080/v1/chains
DONE
