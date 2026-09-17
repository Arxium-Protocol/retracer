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
curl -fsSL https://raw.githubusercontent.com/Arxium-Protocol/retracer/main/scripts/install.sh -o install.sh && less install.sh && bash install.sh
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

See every flag with `cargo run -p retracerd -- --help`.

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
`--database-url`, `--node-rpc-url`, `--node-rpc-token`, `--auth-token`, `--rate-limit-rps`,
`--trusted-proxies`, `--grpc-bind` and `--rest-bind`
can also come from a `.env` file (copy `.env.example`) via
`RETRACER_BOOTNODES`/`RETRACER_DATABASE_URL`/`RETRACER_NODE_RPC_URL`/`RETRACER_NODE_RPC_TOKEN`/
`RETRACER_AUTH_TOKEN`/`RETRACER_RATE_LIMIT_RPS`/`RETRACER_TRUSTED_PROXIES`/`RETRACER_GRPC_BIND`/
`RETRACER_REST_BIND` — a flag always overrides the env value.

| Flag | Default | Description |
| --- | --- | --- |
| `--bootnodes` | none | Comma-separated multiaddrs to dial |
| `--port` | `0` | P2P listen port (`0` picks a free one) |
| `--chain-id` | `corechain-devnet` | Label for this chain's rows. Not read off the wire |
| `--database-url` | `postgres://retracer:retracer@localhost:5433/retracer` | Postgres connection string |
| `--node-rpc-url` | none | This chain's node HTTP RPC base URL, e.g. `http://127.0.0.1:8081`. Only used for the validator-uptime endpoint; leave unset to disable it |
| `--node-rpc-token` | none | Optional bearer token sent on every HTTP request to this chain's node RPC. Prefer `RETRACER_NODE_RPC_TOKEN` so the value is not visible in process arguments |
| `--rest-port` | `8080` | HTTP API port; `0` disables it |
| `--grpc-port` | `50051` | gRPC API port |
| `--kind-schema` | `kind_schema.toml` | Payload field configuration |
| `--reindex-addresses` | off | Re-run the kind schema's role extraction over every stored action at startup, then follow the chain as usual. Run once after adding a role to `kind_schema.toml` |
| `--blocks-topic` | `arxium/blocks/v1` | Must match the node's gossip topic |
| `--sync-protocol` | `/arxium/sync/1` | Must match the node's sync protocol |
| `--finality-depth` | `250` | Fallback rollback limit, used only when the node reports no finality |
| `--max-pending-blocks` | `4096` | Gap-fill buffer cap |
| `--write-pool-size` | `4` | Postgres connections for the writer |
| `--read-pool-size` | `16` | Postgres connections for reads |
| `--auth-token` | none | Shared secret required as `Authorization: Bearer <token>` on both surfaces (`/health` and `/ready` stay open). Unset = both surfaces stay open, same as today |
| `--rate-limit-rps` | none | Per-IP request budget, both surfaces. Unset = no rate limiting |
| `--trusted-proxies` | none | Comma-separated IPs/CIDRs (e.g. `10.0.0.8,10.0.0.0/8`) whose `X-Forwarded-For` the limiter may believe. Unset = the socket peer is always the client; only set addresses you operate |
| `--grpc-bind` | `127.0.0.1` | Interface the gRPC surface listens on |
| `--rest-bind` | `127.0.0.1` | Interface the REST surface listens on |

`--blocks-topic` and `--sync-protocol` are a wire agreement with the node you're
following, so they must match what *it* publishes — they're not derived from
`--chain-id`.

### Exposure

Both surfaces listen on loopback by default. Set `--grpc-bind`/`--rest-bind`
(or the matching env vars) to a private or WireGuard address — never a public
one — and turn on `--auth-token` before doing so: both surfaces are plaintext,
and auth defaults off.

### CoreChain wire compatibility

The bundled CoreChain binary accepts both block generations used by supported
Arxium nodes:

- Arxium `v0.1.1` through `v0.1.5`, whose blocks do not contain `state_root`.
- The current `state_root` block shape pinned in this workspace.

Both gossip blocks and sync block pages are decoded with complete-input checks.
Legacy hashes are computed from the legacy bytes, not from a current block with
an invented empty root. Other chains remain exact-decoding-only unless their
embedder supplies an explicit `WireDecoder`.

Newer Arxium revisions chain-scope their network names. For those nodes, pass
their exact `arxium/blocks/v1/<chain-id>` topic and
`/arxium/sync/1/<chain-id>` protocol with the two flags above. This naming
change is independent of the block encoding and cannot be inferred from
`xc_wire::WIRE_VERSION`.

---

## HTTP API

Every path is scoped to a chain. `GET /v1/chains` lists the ones this deployment
serves. The full contract — every parameter and response schema — is the
OpenAPI 3.1 document at `GET /openapi.json`, browsable at `GET /docs`; it is
generated from the handlers at compile time, and a test fails if a route is
registered without being in it.

```
GET  /health
GET  /ready
GET  /metrics
GET  /openapi.json
GET  /docs
GET  /v1/chains

GET  /v1/chains/{chain}/status
GET  /v1/chains/{chain}/stats
GET  /v1/chains/{chain}/proposers

GET  /v1/chains/{chain}/blocks?limit=&before=
GET  /v1/chains/{chain}/blocks/{height|hash}

GET  /v1/chains/{chain}/actions?limit=&before_height=&before_index=&kind=&field=&value=
GET  /v1/chains/{chain}/actions/{action_hash}

GET  /v1/chains/{chain}/accounts/{address}/actions?limit=&role=
GET  /v1/chains/{chain}/search?q=

GET  /v1/chains/{chain}/validators/uptime?from=&to=
```

`/health` is process liveness only. `/ready` returns 200 when PostgreSQL
answers within a fixed timeout — that is, when reads work — and 503 otherwise.
The body also reports each chain's peer visibility, node tip and whether the
index has caught up, but those don't affect the status: a stalled chain or a
peer restart shouldn't take a serviceable read replica out of rotation. Alert
on lag through `/metrics` (`retracer_blocks_behind`) instead.
Both probes stay open when inbound API authentication is enabled, while
configured rate limiting still applies to `/ready` (but never `/health`).

`/metrics` is Prometheus text exposition, unlike `/health` and `/ready` it
stays behind `--auth-token`/`--rate-limit-rps` like every other route. A
database failure still returns 200 with `retracer_database_up 0`, so a scrape
always has a body to alert on. See [Monitoring](#monitoring) below.

`/v1/chains` includes each chain's `genesis_hash` from the node's own
`/genesis-hash` (when `--node-rpc-url` is set) — `chain_id` is only this
deployment's label; the genesis hash is the network's identity. It also
carries `arxium_node_rev`, the Arxium git rev this build's wire types are
pinned to (also printed by `retracerd --version`). The P2P wire is bincode
with no version tag, so a node on a different rev can produce blocks this
Retracer misdecodes without an error — check the rev before trusting a
deployment against a node you didn't pin it to.

`/actions` filters by `kind`, and by one indexed payload field with
`field=$.path&value=` — the field must be declared as a `[[kind.index]]` for
that kind in `kind_schema.toml` (see [Indexing your payload
fields](#indexing-your-payload-fields)); anything else is a 400 naming the
fields that are indexed. `/stats` is served from a 15-second cache.

Pages are newest-first and cap at 100. Action cursors are a
`(before_height, before_index)` pair and both halves must be sent together — a
block holds many actions, so half a cursor would silently repeat or skip the
rest of one.

```bash
curl "localhost:8080/v1/chains/corechain-devnet/blocks?limit=5"
curl "localhost:8080/v1/chains/corechain-devnet/accounts/<address>/actions?role=to"
curl "localhost:8080/v1/chains/corechain-devnet/search?q=42"
curl "localhost:8080/v1/chains/corechain-devnet/actions?kind=TransferAsset&field=\$.asset&value=arxasset1..."
```

---

## gRPC API

Defined in [`proto/retracer.proto`](proto/retracer.proto). Same reads as
HTTP, plus two server-streaming RPCs HTTP doesn't offer:

- `SubscribeBlocks` — live block tail, with optional `from_height` to replay
  history first.
- `SubscribeAccountActions` — live tail of any action where an address holds a
  role (sender, recipient, or any role your schema defines).

Both streams can fall behind the broadcast buffer if a subscriber reads too
slowly. When that happens the stream ends with a `DataLoss` status rather than
silently skipping the gap. `SubscribeBlocks` clients should resubscribe with
`from_height` set to resume the replay. `SubscribeAccountActions` has no
replay of its own — read the missed history with `GetAccountActions`, then
resubscribe.

The chain is selected by the `x-chain-id` header; omit it for the default chain.

```bash
grpcurl -plaintext -proto proto/retracer.proto \
  -H 'x-chain-id: corechain-devnet' \
  -d '{"height": 1}' \
  localhost:50051 retracer.Retracer/GetBlock
```

---

## Monitoring

`GET /metrics` is Prometheus text: per-chain lag and finality, plus database
and per-table size (see [What it deliberately doesn't do](#what-it-deliberately-doesnt-do)
below — the intent is to measure growth, not prune it). It requires the same
`--auth-token` as every other route once one is set.

```yaml
scrape_configs:
  - job_name: retracer
    # Each scrape re-runs the database and per-table size queries; don't
    # inherit a sub-second global interval meant for cheaper endpoints.
    scrape_interval: 30s
    static_configs:
      - targets: ["127.0.0.1:8080"]
    authorization:
      credentials_file: /etc/retracer/metrics-token
```

Starter alerts:

```yaml
- alert: RetracerDown
  expr: up{job="retracer"} == 0
  for: 2m
- alert: RetracerDatabaseDown   # up==1 alone misses this: a DB outage still
  expr: retracer_database_up == 0   # returns 200 with retracer_database_up 0
  for: 2m
- alert: RetracerLagging
  expr: retracer_blocks_behind > 30
  for: 5m
- alert: RetracerChainStalled   # node stopped producing, lag stays 0
  expr: retracer_tip_age_seconds > 120
  for: 5m
- alert: RetracerDiskGrowth     # set the budget to the host's disk allowance
  expr: predict_linear(retracer_database_size_bytes[6h], 7 * 24 * 3600) > 20e9
  for: 30m
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

That becomes a Postgres expression index at startup, and the field becomes
filterable: `GET .../actions?kind=<kind>&field=$.amount&value=...`. Paths must
be plain dotted field names; anything else is rejected at startup rather than
escaped.

Removing an entry doesn't drop its index — do that with `DROP INDEX` when you
mean it.

Roles are extracted when a block is indexed, so adding a `[[kind.roles]]`
entry only covers new blocks. Start once with `--reindex-addresses` to
backfill it over everything already stored (additive — a removed role's old
rows stay until you `DELETE` them).

For roles a dotted path can't express (conditional or computed), implement
`storage::ActionIndexable` in Rust and pass it to `run`. See the
`ActionIndexable` rustdoc (`cargo doc -p storage --open`).

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
[`crates/storage/src/wire.rs`](crates/storage/src/wire.rs).

---

## What it deliberately doesn't do

- **Account balances and nonces.** Not derivable from indexed actions; ask the
  node directly.
- **Validator set membership.** Live membership comes from the node's
  `/validators`. Retracer reports who has actually *proposed* blocks — and
  who *should have*: `GET /v1/chains/{chain_id}/validators/uptime?from=&to=`
  compares each indexed height's round-0 designee (a pure function of the
  node's own `/validators?height=N`, mirroring the node's own
  `eligible_proposer` formula, not a replay of chain-specific dispatch logic)
  against who actually proposed it. A block produced at round > 0 (a backup
  taking over after the primary missed) is attributed to the primary as a
  missed turn and to the backup as a `backup_proposals` count, never as extra
  uptime for the backup. Only indexed heights with a known proposer are
  counted; `heights_counted` in the response says how many of the requested
  heights that was, so a range reaching past the indexed tip is visible
  rather than silently under-counted. Node calls are cached per
  `(chain, height)` for `UPTIME_CACHE_TTL` (5 minutes) and capped per request
  by `MAX_UPTIME_RANGE`; this is a backfill endpoint, not a live figure.
  Needs `--node-rpc-url`/`RETRACER_NODE_RPC_URL` configured per
    chain; without it the route 400s rather than guessing an address. Protected
    node RPCs also require `--node-rpc-token`/`RETRACER_NODE_RPC_TOKEN`; the
    credential is sent as `Authorization: Bearer` and is redacted from Debug output.
    Send bearer-authenticated requests only over loopback, HTTPS, or the encrypted
    WireGuard network; ordinary remote HTTP exposes the credential in transit.
    Rows indexed before migration `0002` report round 0 regardless of the
    block's real round, since that column did not exist yet.
- **Mempool / pending actions.** Confirmed blocks only.
- **Auth or rate limiting.** Off by default (unchanged trusted-consumer
  behavior), now opt-in via `--auth-token`/`RETRACER_AUTH_TOKEN` (a shared
  `Authorization: Bearer` secret) and `--rate-limit-rps`/
  `RETRACER_RATE_LIMIT_RPS` (per-IP), enforced identically on both the gRPC
  and REST surfaces. `/health` stays open for liveness probes either way.

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

Queries are plain `sqlx::query` / `query_as` (runtime-checked), so the build
needs no database and there is no `.sqlx/` cache to keep in sync; the Postgres
integration tests are what catch a query/schema mismatch.

Applied migrations are frozen — SQLx verifies them by hashing their exact
bytes — so a schema change is always a new numbered file under `migrations/`.

---

## Documentation

| | |
| --- | --- |
| `cargo doc --workspace --open` | Rustdoc for every crate: internals, boundary rules, module docs |
| [`examples/spoke-indexer/README.md`](examples/spoke-indexer/README.md) | Worked multi-chain example |
| [`proto/retracer.proto`](proto/retracer.proto) | The gRPC schema |
