# Example: indexing your own chain

A working indexer for an imaginary Spoke Chain called **MintChain**. Copy this
directory as the starting point for your own.

The thing to notice is how little there is. Retracer is generic over your
block type, so you write a binary — not a fork.

## What you supply

| | Where | Required? |
| --- | --- | --- |
| Your payload enum | [`src/lib.rs`](src/lib.rs) → `MintPayload` | Yes |
| Your address format | `is_mintchain_address` | Optional |
| Address roles and queryable fields | [`kind_schema.toml`](kind_schema.toml) | Optional |
| Logic a config file can't express | `AirdropRecipients` | Only if needed |

Everything else — P2P client, Postgres schema, HTTP and gRPC APIs, reorg
handling — is inherited unchanged.

## Run it

```bash
../../scripts/setup.sh          # Postgres up, workspace built

cargo run -p spoke-indexer -- \
  --chain-id mintchain-devnet \
  --kind-schema examples/spoke-indexer/kind_schema.toml \
  --bootnodes /ip4/127.0.0.1/tcp/30334/p2p/<peer-id>
```

Then:

```bash
curl localhost:8080/v1/chains/mintchain-devnet/status
curl "localhost:8080/v1/chains/mintchain-devnet/accounts/spoke1.../actions?role=to"
```

## The three integration points

**1. Your payload enum.** The one hard requirement: it must decode from the
exact bytes your node gossips. Blocks arrive as bincode, which is not
self-describing — there's no decoding "whatever shape is there" — so copy the
enum from your node's source rather than retyping it.

```rust
run::<Block<MintPayload>>(args, hooks).await
```

**2. Your address format.** Optional. Without it the indexer works but stops
validating addresses and stops recognising accounts in `Search`. That's
deliberate: a validator accepting anything would make `Search` classify every
block hash as an address.

**3. Roles and projections**, declared in `kind_schema.toml` — no rebuild, just
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
from one process, one database, one API endpoint — each with its own payload
type, address format, gossip topic and finality depth.

```bash
cargo run -p spoke-indexer --bin multi_chain
curl localhost:8080/v1/chains
```

Chains are added one at a time rather than listed in a config file, because each
carries its own Rust type:

```rust
runner.add_chain::<Block<CorePayload>>(hub_config, hub_hooks).await?;
runner.add_chain::<Block<MintPayload>>(spoke_config, spoke_hooks).await?;
```

The first chain added is the default — it serves gRPC requests arriving with no
`x-chain-id` header, which keeps existing single-chain clients working when you
add a second chain. Over REST the chain is always explicit in the path.

## If your chain isn't on the Arxium stack

Implement `storage::IndexableBlock` and `ingestion::HasHeight` for your own
block type instead of using `xc_primitives::Block`. You can also switch off
`storage`'s default `xc-primitives` feature and compile with no Arxium
dependency at all. See
[Design notes](../../../Retracer_Design.md#following-a-different-chain).
