#!/bin/bash
# Drops every table and lets the next startup re-apply migrations from scratch.
#
# Useful while the schema is still a single un-shipped 0001_init.sql that gets
# edited in place: SQLx verifies applied migrations by hashing their bytes, so
# editing that file makes an existing database refuse to start until it's reset.
# Once the schema ships this stops being a normal thing to do — you add a new
# numbered migration instead.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [ "${1:-}" != "--yes" ]; then
  printf '\033[1;33mThis deletes all indexed data in the local database.\033[0m\n'
  read -r -p "Continue? [y/N] " reply
  [ "$reply" = "y" ] || [ "$reply" = "Y" ] || { echo "Aborted."; exit 1; }
fi

docker compose exec -T postgres psql -U retracer -d retracer -q <<'SQL'
DROP SCHEMA public CASCADE;
CREATE SCHEMA public;
SQL

echo "Database reset. Migrations re-apply on the next start."
