# Example: indexing your own chain

A working indexer for an imaginary Spoke Chain called **MintChain**. Copy this
directory as the starting point for your own.

The thing to notice is how little there is. Blocks are read as the JSON your
node serves on `GET /blocks`, so you write a binary — not a fork, and no
payload enum to mirror.

## What you supply

| | Where | Required? |
| --- | --- | --- |
| Your address format | [`src/lib.rs`](src/lib.rs) → `is_mintchain_address` | Optional |
| Address roles and queryable fields | [`kind_schema.toml`](kind_schema.toml) | Optional |
| Logic a config file can't express | `AirdropRecipients` | Only if needed |

Everything else — node RPC poller, Postgres schema, HTTP API and SSE tails, reorg
handling — is inherited unchanged.

## Run it

```bash
../../scripts/setup.sh          # Postgres up, workspace built

cargo run -p spoke-indexer -- \
  --chain-id mintchain-devnet \
  --kind-schema examples/spoke-indexer/kind_schema.toml \
  --node-rpc-url http://127.0.0.1:8081
```

Then:

```bash
curl localhost:8080/v1/chains/mintchain-devnet/status
curl "localhost:8080/v1/chains/mintchain-devnet/accounts/spoke1.../actions?role=to"
```

## The integration points

**1. Your address format.** Optional. Without it the indexer works but stops
validating addresses and stops recognising accounts in `Search`. That's
deliberate: a validator accepting anything would make `Search` classify every
block hash as an address.

**2. Roles and projections**, declared in `kind_schema.toml` — no rebuild, just
a restart:

```toml
[[kind]]
name = "MintNft"
  [[kind.roles]]      # who else this action concerns, besides the sender
  path = "$.recipient"
  role = "to"
  [[kind.index]]      # a payload field worth filtering on
  path = "$.price"
  type = "bigint"
```

Roles make an address findable via `?role=to`. Projections become partial
Postgres expression indexes at startup.

**When config isn't enough**, implement `ActionIndexable` in Rust. `Airdrop`
here has an *array* of recipients and a dotted path can't index into a list —
that's the motivating case. A Tier B impl owns its kind outright; a
`kind_schema.toml` entry for the same kind would be ignored, not merged, so a
kind always has exactly one place defining its roles.

## Following several chains at once

[`src/bin/multi_chain.rs`](src/bin/multi_chain.rs) runs CoreChain and MintChain
from one process, one database, one API endpoint — each with its own node,
address format and finality depth.

```bash
cargo run -p spoke-indexer --bin multi_chain
curl localhost:8080/v1/chains
```

Chains are added one at a time rather than listed in a config file, because each
carries its own hooks (Tier B extractors are Rust):

```rust
runner.add_chain::<RpcBlock>(hub_config, hub_hooks).await?;
runner.add_chain::<RpcBlock>(spoke_config, spoke_hooks).await?;
```

The chain is always explicit in the path (`/v1/chains/{chain_id}/...`), so
adding a second chain changes nothing for clients of the first.

## If your chain isn't on the Arxium stack

Implement `storage::IndexableBlock` and `ingestion::HasHeight` for your own
block type instead of using `xc_primitives::Block`. You can also switch off
`storage`'s default `xc-primitives` feature and compile with no Arxium
dependency at all. See
[Design notes](../../../Retracer_Design.md#following-a-different-chain).
