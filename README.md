# Retracer

A read-only blockchain indexer for Arxium chains. It joins the P2P network as an
ordinary peer, writes blocks and actions into Postgres, and serves them back
over HTTP and gRPC — so explorers, wallet backends and dashboards don't have to
hit node RPC or replay chain logic themselves.

- **No privileged access.** Same gossipsub subscription and public sync protocol
  any peer can use.
- **Any Arxium-stack chain.** The payload type is yours; the indexer is generic
  over it. No forking required.
- **Many chains, one endpoint.** Follow a Hub and its Spokes from one process
  and query them all from one API.

---

## Quickstart

A Postgres database is required either way — bring your own; Retracer doesn't
run one for you.

### From a release (no Rust toolchain, no Docker)

```bash
curl -fsSL https://raw.githubusercontent.com/Arxium-Protocol/retracer/main/scripts/install.sh | bash
```

Downloads the latest `retracerd` release (checksum-verified), prompts for
your bootnodes/database URL/auth token, and offers to install it as a
systemd service. Read it before piping to `bash` if you'd rather:

```bash
curl -fsSL .../install.sh -o install.sh && less install.sh && bash install.sh
```

Non-interactive install with defaults: `install.sh --yes`. See
`install.sh --help` for `--version`/`--base-path`/`--dry-run`.

Releases only ship `x86_64-linux-gnu` binaries today — everywhere else, build
from source below.

### From source

Requires Rust (2024 edition).

```bash
./scripts/setup.sh            # checks tools, builds
cp .env.example .env          # fill in RETRACER_DATABASE_URL/RETRACER_BOOTNODES

cargo run -p retracerd
```

Or skip `.env` and pass flags directly:

```bash
cargo run -p retracerd -- \
  --bootnodes /ip4/127.0.0.1/tcp/30334/p2p/<peer-id> \
  --database-url postgres://retracer:retracer@localhost:5433/retracer
```

Migrations run automatically on startup against whatever database you pointed
`--database-url` at. You now have:

| | |
| --- | --- |
| HTTP API | `http://localhost:8080` |
| gRPC API | `localhost:50051` |

```bash
curl localhost:8080/v1/chains
curl localhost:8080/v1/chains/corechain-devnet/status
```

`status` reports how far behind the chain you are, taken from the node itself
rather than guessed:

```json
{ "indexed_height": 1042, "node_tip_height": 1045, "blocks_behind": 3,
  "tip_timestamp": 1766400000 }
```

`node_tip_height` and `blocks_behind` are absent, not zero, until a peer
answers — "not connected" and "caught up" are different states.

---

## Configuration

All flags are optional; the defaults match a local devnet. `--bootnodes`,
`--database-url`, `--node-rpc-url`, `--auth-token`, and `--rate-limit-rps`
can also come from a `.env` file (copy `.env.example`) via
`RETRACER_BOOTNODES`/`RETRACER_DATABASE_URL`/`RETRACER_NODE_RPC_URL`/
`RETRACER_AUTH_TOKEN`/`RETRACER_RATE_LIMIT_RPS` — a flag always overrides
the env value.

| Flag | Default | Description |
| --- | --- | --- |
| `--bootnodes` | none | Comma-separated multiaddrs to dial |
| `--port` | `0` | P2P listen port (`0` picks a free one) |
| `--chain-id` | `corechain-devnet` | Label for this chain's rows. Not read off the wire |
| `--database-url` | `postgres://retracer:retracer@localhost:5433/retracer` | Postgres connection string |
| `--node-rpc-url` | none | This chain's node HTTP RPC base URL, e.g. `http://127.0.0.1:8081`. Only used for the validator-uptime endpoint; leave unset to disable it |
| `--rest-port` | `8080` | HTTP API port; `0` disables it |
| `--grpc-port` | `50051` | gRPC API port |
| `--kind-schema` | `kind_schema.toml` | Payload field configuration |
| `--blocks-topic` | `arxium/blocks/v1` | Must match the node's gossip topic |
| `--sync-protocol` | `/arxium/sync/1` | Must match the node's sync protocol |
| `--finality-depth` | `250` | Fallback rollback limit, used only when the node reports no finality |
| `--max-pending-blocks` | `4096` | Gap-fill buffer cap |
| `--write-pool-size` | `4` | Postgres connections for the writer |
| `--read-pool-size` | `16` | Postgres connections for reads |
| `--auth-token` | none | Shared secret required as `Authorization: Bearer <token>` on both surfaces (`/health` stays open). Unset = both surfaces stay open, same as today |
| `--rate-limit-rps` | none | Per-IP request budget, both surfaces. Unset = no rate limiting |

`--blocks-topic` and `--sync-protocol` are a wire agreement with the node you're
following, so they must match what *it* publishes — they're not derived from
`--chain-id`.

---

## HTTP API

Every path is scoped to a chain. `GET /v1/chains` lists the ones this deployment
serves.

```
GET  /health
GET  /v1/chains

GET  /v1/chains/{chain}/status
GET  /v1/chains/{chain}/stats
GET  /v1/chains/{chain}/proposers

GET  /v1/chains/{chain}/blocks?limit=&before=
GET  /v1/chains/{chain}/blocks/{height|hash}

GET  /v1/chains/{chain}/actions?limit=&before_height=&before_index=
GET  /v1/chains/{chain}/actions/{action_hash}

GET  /v1/chains/{chain}/accounts/{address}/actions?limit=&role=
GET  /v1/chains/{chain}/search?q=
```

Pages are newest-first and cap at 100. Action cursors are a
`(before_height, before_index)` pair and both halves must be sent together — a
block holds many actions, so half a cursor would silently repeat or skip the
rest of one.

```bash
curl "localhost:8080/v1/chains/corechain-devnet/blocks?limit=5"
curl "localhost:8080/v1/chains/corechain-devnet/accounts/arx1.../actions?role=to"
curl "localhost:8080/v1/chains/corechain-devnet/search?q=42"
```

---

## gRPC API

Defined in [`proto/retracer.proto`](proto/retracer.proto). Same reads as
HTTP, plus two server-streaming RPCs HTTP doesn't offer:

- `SubscribeBlocks` — live block tail, with optional `from_height` to replay
  history first.
- `SubscribeAccountActions` — live tail of any action where an address holds a
  role (sender, recipient, or any role your schema defines).

The chain is selected by the `x-chain-id` header; omit it for the default chain.

```bash
grpcurl -plaintext -proto proto/retracer.proto \
  -H 'x-chain-id: corechain-devnet' \
  -d '{"height": 1}' \
  localhost:50051 retracer.Retracer/GetBlock
```

---

## Indexing your payload fields

Actions are stored with their payload as JSONB, so nothing is ever lost. To make
specific fields *searchable*, describe them in `kind_schema.toml`.

**Addresses** get their own index and become queryable per account, with a role:

```toml
[[kind]]
name = "Transfer"
  [[kind.roles]]
  path = "$.to"
  role = "to"        # from | to | validator_subject | delegator | other:<label>
```

`GET /v1/chains/{chain}/accounts/{addr}/actions?role=to` now returns transfers
*received* by that address, not just sent.

**Any other field** can be indexed for filtering:

```toml
  [[kind.index]]
  path = "$.amount"
  type = "numeric"   # text | numeric | bigint
```

That becomes a Postgres expression index at startup. Paths must be plain dotted
field names; anything else is rejected at startup rather than escaped.

Removing an entry doesn't drop its index — do that with `DROP INDEX` when you
mean it.

For roles a dotted path can't express (conditional or computed), implement
`storage::ActionIndexable` in Rust and pass it to `run`. See
[Design notes](../Retracer_Design.md#tier-a--tier-b-address-extraction).

---

## Using it for your own chain

A complete worked example lives in
[`examples/spoke-indexer/`](examples/spoke-indexer/) — copy that directory as
your starting point. It's a workspace member, so it's compiled and tested on
every build and can't silently rot.

If your chain is built on the Arxium stack, you need a binary, not a fork —
supply your payload type and your address format:

```rust
use retracer_core::{ChainHooks, parse_args, run};
use xc_primitives::Block;

#[derive(serde::Serialize, serde::Deserialize)]
enum MyPayload { MintNft { token_id: u64 }, /* ... */ }

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = parse_args()?;
    run::<Block<MyPayload>>(args, ChainHooks {
        address_validator: Some(std::sync::Arc::new(|a| a.starts_with("spoke1"))),
        ..Default::default()
    }).await
}
```

### Many chains in one process

```rust
let mut runner = Runner::new(&db_url, 4, 16, 50051).await?
    .with_rest_port(Some(8080));

runner.add_chain::<Block<HubPayload>>(hub_config, hub_hooks).await?;
runner.add_chain::<Block<SpokePayload>>(spoke_config, spoke_hooks).await?;
runner.run().await
```

Each chain keeps its own payload type, address format and finality depth, and
they share one database and one API endpoint.

If your chain *isn't* on the Arxium stack, implement `storage::IndexableBlock`
and `ingestion::HasHeight` for your own block type — see
[Design notes](../Retracer_Design.md#following-a-different-chain).

---

## What it deliberately doesn't do

- **Account balances and nonces.** Not derivable from indexed actions; ask the
  node directly.
- **Validator set membership.** Live membership comes from the node's
  `/validators`. Retracer reports who has actually *proposed* blocks — and,
  since 2026-08-25, who *should have*: `GET
  /v1/chains/{chain_id}/validators/uptime?from=&to=` backfills turns owed
  (the primary round-robin designee per height, a pure function of the
  node's own `/validators?height=N` — not a replay of chain-specific
  dispatch logic) against turns actually proposed. One node call per height,
  so it's a bounded on-demand backfill (`MAX_UPTIME_RANGE`), not a live
  figure. Needs `--node-rpc-url`/`RETRACER_NODE_RPC_URL` configured per
  chain; without it the route 400s rather than guessing an address.
- **Mempool / pending actions.** Confirmed blocks only.
- **Auth or rate limiting.** Off by default (unchanged trusted-consumer
  behavior), now opt-in via `--auth-token`/`RETRACER_AUTH_TOKEN` (a shared
  `Authorization: Bearer` secret) and `--rate-limit-rps`/
  `RETRACER_RATE_LIMIT_RPS` (per-IP), enforced identically on both the gRPC
  and REST surfaces. `/health` stays open for liveness probes either way.

Reasoning for each is in [Design notes](../Retracer_Design.md#boundary-rules).

---

## Development

```bash
./scripts/setup.sh      # first time: check tools, build
./scripts/test.sh       # full suite including the Postgres integration tests
./scripts/reset-db.sh   # wipe the local database
```

Or directly:

```bash
cargo build --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

Database-backed tests are opt-in behind `RETRACER_TEST_DATABASE_URL`, since
Postgres isn't guaranteed to be reachable wherever `cargo test --workspace`
runs. A plain `cargo test` silently skips 11 of them; `./scripts/test.sh` sets
the variable and warns if Postgres isn't reachable.

Layout:

| Crate | Role |
| --- | --- |
| `ingestion` | libp2p client: gossip + sync backfill |
| `storage` | Postgres schema, writes, reads |
| `grpc-service` | tonic server |
| `rest-service` | axum HTTP server |
| `retracer-core` | Run loop, wiring, CLI parsing |
| `retracerd` | The binary |
| `examples/spoke-indexer` | Worked integration example (workspace member) |

Only `retracer-core` depends on the others; the service crates never depend
on each other.

`.sqlx/` holds cached query metadata so the four `sqlx::query!` macros in
`storage` can be type-checked without a live database — that's what lets
`SQLX_OFFLINE=true cargo build` compile with no Postgres reachable at all. Run
`cargo sqlx prepare --workspace` against a migrated database and commit the
result whenever you add, change or remove one of those macros; the build fails
loudly if the cache is missing an entry, but a *stale* entry for a query that no
longer exists just lingers.

The schema is a single `migrations/0001_init.sql` while nothing has shipped.
Once it has, that file is frozen — add a new numbered migration instead, since
SQLx verifies applied migrations by hashing their exact bytes.

---

## Documentation

| | |
| --- | --- |
| [Design notes](../Retracer_Design.md) | Why it's built this way, boundary rules, internals |
| [Open items](../Retracer_OpenItems.md) | Known gaps and deferred work |
| `../Implementation_log_*.md` | Change history |
