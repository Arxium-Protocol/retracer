#!/bin/bash
# Runs the full suite *including* the Postgres integration tests.
#
# Those tests are opt-in behind RETRACER_TEST_DATABASE_URL because CI runs
# `cargo test --workspace` inside an image builder with no database, where they
# would otherwise fail. That makes a plain `cargo test` quietly skip 11 of them,
# which is exactly the kind of thing you don't notice — so this script sets the
# variable and reports whether they actually ran.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

export RETRACER_TEST_DATABASE_URL="${RETRACER_TEST_DATABASE_URL:-postgres://retracer:retracer@localhost:5433/retracer}"

if ! docker compose exec -T postgres pg_isready -U retracer -d retracer >/dev/null 2>&1; then
  printf '\033[1;33mwarning:\033[0m Postgres is not reachable — database tests will skip.\n'
  printf '         Run ./scripts/setup.sh first to include them.\n\n'
  unset RETRACER_TEST_DATABASE_URL
fi

cargo test --workspace "$@"

echo
# -D warnings because the tree is currently clean and the cheapest way to keep
# it that way is to fail the moment it isn't.
cargo clippy --workspace --all-targets -- -D warnings
