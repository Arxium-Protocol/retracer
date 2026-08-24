#!/bin/bash
# Runs the full suite *including* the Postgres integration tests.
#
# Those tests are opt-in behind RETRACER_TEST_DATABASE_URL because CI runs
# `cargo test --workspace` inside an image builder with no database, where
# they'd otherwise fail. That makes a plain `cargo test` quietly skip 11 of
# them — exactly the kind of thing you don't notice — so this script sets the
# variable and reports whether they actually ran.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

export RETRACER_TEST_DATABASE_URL="${RETRACER_TEST_DATABASE_URL:-postgres://retracer:retracer@localhost:5433/retracer}"

if ! command -v pg_isready >/dev/null || ! pg_isready -d "$RETRACER_TEST_DATABASE_URL" >/dev/null 2>&1; then
  printf '\033[1;33mwarning:\033[0m Postgres not reachable at %s — database tests will skip.\n' "$RETRACER_TEST_DATABASE_URL"
  printf '  Point RETRACER_TEST_DATABASE_URL at your own instance to include them.\n\n'
  unset RETRACER_TEST_DATABASE_URL
fi

cargo test --workspace "$@"

echo
# -D warnings keeps the tree clean at the cheapest point to fail — the moment
# a warning lands, not the moment someone finally notices.
cargo clippy --workspace --all-targets -- -D warnings
