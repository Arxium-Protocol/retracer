#!/bin/bash
# One command to get from a fresh clone to a running indexer: check the tools
# are present, bring up Postgres, wait until it actually accepts connections,
# and build. Everything here is idempotent — re-running it is the intended way
# to recover a half-set-up machine.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

DB_URL="${RETRACER_TEST_DATABASE_URL:-postgres://retracer:retracer@localhost:5433/retracer}"

say() { printf '\033[1;34m==>\033[0m %s\n' "$1"; }
die() { printf '\033[1;31merror:\033[0m %s\n' "$1" >&2; exit 1; }

say "Checking prerequisites"
command -v cargo >/dev/null || die "cargo not found — install Rust from https://rustup.rs"
command -v docker >/dev/null || die "docker not found — install Docker Desktop"
docker info >/dev/null 2>&1 || die "docker is installed but not running — start Docker Desktop"
echo "    rust   $(rustc --version | cut -d' ' -f2)"
echo "    docker $(docker --version | cut -d' ' -f3 | tr -d ,)"

# The indexer needs the sibling Arxium checkout for xc-primitives (the wire
# types). Failing here with an explanation beats a wall of cargo path errors.
[ -d "$ROOT/../arxium/core/primitives" ] \
  || die "sibling Arxium checkout not found at $(cd "$ROOT/.." && pwd)/arxium
       Retracer depends on xc-primitives by path; clone Arxium alongside this repo."

say "Starting Postgres"
docker compose up -d

say "Waiting for Postgres to accept connections"
for _ in $(seq 1 60); do
  if docker compose exec -T postgres pg_isready -U retracer -d retracer >/dev/null 2>&1; then
    echo "    ready"
    break
  fi
  sleep 1
done
docker compose exec -T postgres pg_isready -U retracer -d retracer >/dev/null 2>&1 \
  || die "Postgres did not come up within 60s — check 'docker compose logs postgres'"

say "Building the workspace"
cargo build --workspace

cat <<DONE

Setup complete.

  Database   $DB_URL
  Run        cargo run -p retracerd -- --bootnodes <multiaddr>
  Test       ./scripts/test.sh
  Reset DB   ./scripts/reset-db.sh

Migrations apply automatically on startup. With the indexer running:

  curl localhost:8080/v1/chains
DONE
