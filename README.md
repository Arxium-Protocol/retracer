# Retracer

A read-only blockchain indexer for Arxium chains. It reads blocks off a node's
public HTTP RPC (`/status`, `/blocks`), writes blocks and actions into Postgres,
and serves them back over HTTP/JSON with server-sent-event tails — so explorers, wallet backends and
dashboards don't have to hit node RPC or replay chain logic themselves.

- **No privileged access.** The same read-only RPC any client can call. No
  Arxium crate is linked: blocks are plain JSON, and a node whose block shape
  this build doesn't understand is refused at startup, not misread.
- **Any Arxium-stack chain.** The payload type is yours; the indexer is generic
  over it. No forking required.
- **Many chains, one endpoint.** Follow a Hub and its Spokes from one process
  and query them all from one API.

---

## Quickstart

A Postgres database is required either way — bring your own; Retracer doesn't
run one for you.

### From a release (no Rust toolchain, no Docker)

Five commands from nothing to a first query, on x86_64 Linux with systemd.
You need a Postgres you can reach and an Arxium node RPC URL (node v0.7.0 or
newer, see [Compatibility](#compatibility-and-versioning)).

```bash
curl -fsSL https://raw.githubusercontent.com/Arxium-Protocol/retracer/main/scripts/install.sh | bash   # 1. prompts for node RPC URL, database URL, auth token; installs the retracerd service
systemctl status retracerd                                        # 2. running? logs: journalctl -u retracerd -f
curl -s localhost:8080/ready                                      # 3. 200 once Postgres answers
curl -s localhost:8080/v1/chains                                  # 4. the chain id you'll use in every path
curl -s localhost:8080/v1/chains/corechain-devnet/status          # 5. indexed_height vs node_tip_height
```

Then read on for the [HTTP API](#http-api), or open `http://localhost:8080/docs`.

Prefer to read the installer before running it, or need `--version`/
`--base-path`/`--dry-run`/`--yes`:

```bash
curl -fsSL https://raw.githubusercontent.com/Arxium-Protocol/retracer/main/scripts/install.sh -o install.sh && less install.sh && bash install.sh --help
```

The installer verifies the release checksum and writes
`~/.retracer/configs/retracerd.env`; edit it and `systemctl restart retracerd`.
Releases only ship `x86_64-linux-gnu` binaries today — everywhere else, build
from source below.

### From source

Requires Rust (2024 edition).

```bash
./scripts/setup.sh            # checks tools, builds
cp .env.example .env          # fill in RETRACER_DATABASE_URL/RETRACER_NODE_RPC_URL

cargo run -p retracerd
```

Or skip `.env` and pass flags directly:

```bash
cargo run -p retracerd -- \
  --node-rpc-url http://127.0.0.1:8081 \
  --database-url postgres://retracer:retracer@localhost:5433/retracer
```

See every flag with `cargo run -p retracerd -- --help`.

Migrations run automatically on startup against whatever database you pointed
`--database-url` at. You now have:

| | |
| --- | --- |
| HTTP API | `http://localhost:8080` |

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

All flags are optional; the defaults match a local devnet. `--node-rpc-url`,
`--database-url`, `--node-rpc-url`, `--node-rpc-token`, `--auth-token`, `--rate-limit-rps`,
`--trusted-proxies` and `--rest-bind`
can also come from a `.env` file (copy `.env.example`) via
`RETRACER_DATABASE_URL`/`RETRACER_NODE_RPC_URL`/`RETRACER_NODE_RPC_TOKEN`/
`RETRACER_AUTH_TOKEN`/`RETRACER_RATE_LIMIT_RPS`/`RETRACER_TRUSTED_PROXIES`/
`RETRACER_REST_BIND` — a flag always overrides the env value.

| Flag | Default | Description |
| --- | --- | --- |
| `--chain-id` | `corechain-devnet` | Label for this chain's rows. Not read off the wire |
| `--database-url` | `postgres://retracer:retracer@localhost:5433/retracer` | Postgres connection string |
| `--node-rpc-url` | `http://127.0.0.1:8081` | This chain's node HTTP RPC base URL. Blocks are read from it (`/status`, `/blocks`); refuses a node whose `/status` reports `xc-rpc` < 0.3.0 |
| `--node-rpc-token` | none | Optional bearer token sent on every HTTP request to this chain's node RPC. Prefer `RETRACER_NODE_RPC_TOKEN` so the value is not visible in process arguments |
| `--rest-port` | `8080` | HTTP API port |
| `--kind-schema` | `kind_schema.toml` | Payload field configuration |
| `--reindex-addresses` | off | Re-run the kind schema's role extraction over every stored action at startup, then follow the chain as usual. Run once after adding a role to `kind_schema.toml` |
| `--blocks-topic` | `arxium/blocks/v1/<chain-id>` | The node's gossip topic, reported per chain (the explorer derives the genesis hash from it) |
| `--sync-protocol` | `/arxium/sync/1/<chain-id>` | The node's sync protocol, reported per chain |
| `--finality-depth` | `250` | Fallback rollback limit, used only when the node reports no finality |
| `--write-pool-size` | `4` | Postgres connections for the writer |
| `--read-pool-size` | `16` | Postgres connections for reads |
| `--auth-token` | none | Shared secret required as `Authorization: Bearer <token>` on every request (`/health` and `/ready` stay open). Unset = the API stays open, same as today, and webhook registration is refused |
| `--rate-limit-rps` | none | Per-IP request budget. Unset = no rate limiting |
| `--trusted-proxies` | none | Comma-separated IPs/CIDRs (e.g. `10.0.0.8,10.0.0.0/8`) whose `X-Forwarded-For` the limiter may believe. Unset = the socket peer is always the client; only set addresses you operate |
| `--rest-bind` | `127.0.0.1` | Interface the API listens on |

`--blocks-topic` and `--sync-protocol` are a wire agreement with the node you're
following, so they must match what *it* publishes — they're not derived from
`--chain-id`.

### Exposure

The API listens on loopback by default. Set `--rest-bind` (or
`RETRACER_REST_BIND`) to a private or WireGuard address — never a public one —
and turn on `--auth-token` before doing so: HTTP is plaintext, and auth
defaults off.

---

## Compatibility and versioning

**`/v1/` is additive.** Within `/v1/`, routes, query parameters and response
fields are only ever added. Nothing is removed or renamed, an existing field
never changes type or meaning, and pagination cursors keep working across
upgrades. Clients must ignore fields they don't know. A change that can't be
made that way ships as `/v2/` next to `/v1/`, and `/v1/` keeps answering for
at least two further minor releases after `/v2/` appears. While `retracerd`
is 0.x this promise has one exception: a minor release may still break `/v1/`,
and when it does the release notes say **Breaking** and the OpenAPI document
changes — diff `GET /openapi.json` between versions to see exactly what.
`/health`, `/ready` and `/metrics` are operational, not part of the promise.

**Node versions.** Retracer reads a node's public RPC and is pinned to the
shape of its block and effects JSON, not to a release number. A build refuses
at startup any node whose `/status.version` (the `xc-rpc` crate) is below its
floor, printed by `retracerd --version` and reported per chain as
`min_node_version` in `GET /v1/chains`. Today:

| Retracer | Node RPC floor (`xc-rpc`) | Node release |
| --- | --- | --- |
| v0.4.x | 0.2.0 | Arxium v0.7.0 and newer |
| v0.5.x | 0.3.0 | Arxium with `GET /effects?from=&to=` (next release) |

Newer nodes keep working until they change the wire shape, at which point a
Retracer release raises the floor and this table gains a row. State (accounts,
holders, validators, dropped actions) is read one page at a time from the
node's `GET /effects?from=&to=`, alongside the matching `/blocks` page; a
height the node has no effects row for indexes with no state rows.

**Schema reference.** The OpenAPI 3.1 document at `GET /openapi.json`
(browsable at `GET /docs`) is the contract: every route, parameter and
response type, generated from the handlers at compile time. Storage tables are
plain SQL under [`migrations/`](migrations/) — `blocks`, `actions`,
`account_actions` and `action_addresses` for history; `account_state`,
`asset_balances`, `asset_holder_states`, `stakes`, `validator_status`,
`validator_sets`, `asset_registrations` and `dropped_actions` for state by
height. The schema is Retracer's own and may change between releases; query
the API, not the database.

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

GET  /v1/chains/{chain}/accounts/{address}?at=
GET  /v1/chains/{chain}/assets/{asset}/holders?at=&jurisdiction=&after=&limit=
GET  /v1/chains/{chain}/assets/{asset}/audit/transfers?format=json|csv
GET  /v1/chains/{chain}/assets/{asset}/audit/holders?at=&format=json|csv
GET  /v1/chains/{chain}/validators/{address}
GET  /v1/chains/{chain}/attestors?at=
GET  /v1/chains/{chain}/actions/dropped?sender=&before_height=&before_signature=&limit=
```

The last five are **state**, kept by height from the node's
`GET /blocks/{h}/effects` (what each block changed): an account's balance,
nonce and identity fields plus its non-zero asset holdings and live stakes;
an asset's non-zero holders with balance, compliance state and jurisdiction; a validator's
status, voting power, status history, authorized operator, BLS key and
equivocation slashes; the registered attestors; and the actions a producer
rejected, with the reason. `at=` answers "as of block H" (default: tip).
Amounts are decimal strings — the chain's u128 doesn't fit a JSON number.
State is only as complete as the effects the node served: blocks indexed
from a node older than the effects record have no state rows, and
`dropped` is only ever populated by the producing node.

The two asset audit exports are complete, deterministic records rather than
paged explorer views. They return JSON by default or CSV with `format=csv`,
set `Content-Disposition` for download, and include the SHA-256 of the exact
response body in `X-Retracer-Audit-SHA256`. Transfer exports cover the baseline
`TransferAsset`, `ForcedTransfer`, and `IssuerForcedTransfer` kinds and resolve
each party's compliance state strictly before the transfer block (H-1).

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
carries `min_node_version`, the oldest node (`/status.version`, its `xc-rpc`
crate version) this build reads blocks from — also printed by
`retracerd --version`. An older node is refused at startup.

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
C=localhost:8080/v1/chains/corechain-devnet
curl "$C/blocks?limit=5"                                          # newest five blocks
curl "$C/blocks/1042"                                             # by height, or by hash
curl "$C/actions?limit=20&before_height=1042&before_index=0"      # next page of actions
curl "$C/actions/<action_hash>"
curl "$C/accounts/<address>/actions?role=to"                      # history where the address received
curl "$C/accounts/<address>/actions?role=from,to"                 # sent and received, one page
curl "$C/accounts/<address>"                                      # balance, nonce, holdings, stakes at tip
curl "$C/accounts/<address>?at=1000"                              # the same as of block 1000
curl "$C/assets/<asset>/holders?limit=100"                        # cap table, keyset-paged with after=
curl "$C/validators/<address>"                                    # status, voting power, history
curl "$C/actions/dropped?sender=<address>"                        # producer-rejected, with reason
curl "$C/search?q=42"                                             # height, hash, address or action hash
curl "$C/actions?kind=TransferAsset&field=\$.asset&value=arxasset1..."
```

---

## Live tails (server-sent events)

Three routes stream instead of paging, Horizon-style — plain `GET`s answering
`text/event-stream`, one JSON row per `data:` line, the same shapes the paged
reads return:

- `GET /v1/chains/{chain}/blocks/stream?from_height=N` — every block, with
  `id:` = height. `from_height` replays from storage through the tip before
  following live; omit it for the live tail only.
- `GET /v1/chains/{chain}/actions/stream?from_height=N&address=A` — one event
  per action (an empty block emits nothing), each carrying `block_timestamp`,
  with `id:` = `height:index`. `address` keeps only actions where that address
  holds a role (sender, recipient, or any role your schema defines).
- `GET /v1/chains/{chain}/actions/dropped/stream?from_height=N&address=A` —
  one event per action the producer rejected, with the `reason`, `id:` =
  `height:signature`; `address` keeps only that sender's. The feed for
  "transfer refused for compliance reason X" — only a Retracer following the
  *producing* node sees rejections at all (see `actions/dropped`). Blocks
  also carry a `dropped` array on `blocks/stream` and `/blocks/{height}`,
  present only when non-empty.

The connection is allowed to drop. A subscriber that reads too slowly for the
broadcast buffer is simply cut off — the stream ends — and reconnects with
`from_height` set to the last height it handled plus one; that cursor, not
gap detection on the wire, is the reliability story. A browser `EventSource`
gets `Last-Event-ID` for free; a backend keeps its own checkpoint.

```bash
curl -N "localhost:8080/v1/chains/corechain-devnet/actions/stream?from_height=100"
```

### Webhooks

The same events, delivered as HTTP `POST`s to a receiver that has a URL
rather than an open socket — an issuer's back office learning that a
transfer was refused for a compliance reason. Registration needs
`--auth-token` (an open API must not be made to POST chain data anywhere).

```bash
curl -s -X POST -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  localhost:8080/v1/chains/corechain-devnet/webhooks \
  -d '{"url":"https://backoffice.example/arxium","secret":"<16+ chars>","address":"arx1issuer...","events":["dropped"]}'
```

- `address` (optional) keeps only actions that address holds a role on and
  rejections it sent; `events` ⊂ `["action","dropped"]`, default both;
  `from_height` starts delivery from history instead of the current tip.
- `GET .../webhooks` lists hooks (secrets are never returned);
  `DELETE .../webhooks/{id}` removes one. `POST` with an existing URL
  replaces its secret and filter and re-enables it.

Each delivery is one JSON body — the SSE event with an added
`"event": "action" | "dropped"` — and these headers:

| Header | Value |
| --- | --- |
| `X-Retracer-Chain` | chain id |
| `X-Retracer-Event` | `action` or `dropped` |
| `X-Retracer-Id` | the SSE `id:` — `height:index` or `height:signature`; dedupe on it |
| `X-Retracer-Timestamp` | unix seconds when sent |
| `X-Retracer-Signature` | `sha256=` + hex HMAC-SHA256 of `"<timestamp>.<body>"` under the secret |

Delivery is at-least-once, in chain order, one hook at a time: a hook's
cursor (`cursor_height`) advances only once every event of a block answered
2xx, so a receiver that is down is replayed from Postgres when it returns.
Failures back off up to a minute; after three days of continuous failure the
hook is disabled (`enabled: false`, `last_error` says why) — re-`POST` it to
re-arm. Rejections (`dropped`) are only known to a Retracer following the
*producing* node.

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
*received* by that address, not just sent. `role` takes a comma-separated
list (up to 8), so `role=from,to` is a wallet's whole Activity in one newest-first
page, with a self-transfer listed once. Every action row, here and on every
other endpoint and stream, carries `block_timestamp` (Unix seconds), so a
dated history needs no per-block lookups.

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
let mut runner = Runner::new(&db_url, 4, 16, 8080).await?;

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

- **A nonce for signing.** `/accounts/{address}` reports the nonce as of the
  newest indexed block, which trails the node by ingestion latency; a wallet
  building the next action reads the node.
- **The asset registry** (symbol, decimals, issuer), BLS keys, min-stake,
  action-fee, finality. Node reads.
- **Validator set membership by height.** `/validators/{address}` gives the
  newest status and set; per-height membership comes from the node's
  `/validators?height=`. Retracer also reports who has actually *proposed*
  blocks — and who *should have*: `GET /v1/chains/{chain_id}/validators/uptime?from=&to=`
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
  `RETRACER_RATE_LIMIT_RPS` (per-IP). `/health` stays open for liveness
  probes either way.

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
| `ingestion` | Node RPC poller: `/status` + `/blocks` pages |
| `storage` | Postgres schema, writes, reads |
| `rest-service` | axum HTTP server, paged reads + SSE tails |
| `retracer-core` | Run loop, wiring, CLI parsing |
| `retracerd` | The binary |

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
