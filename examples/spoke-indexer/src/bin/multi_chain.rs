//! Following a Hub and a Spoke from one process, one database, one API.
//!
//! The single-chain example (`src/main.rs`) is the common case. This is the one
//! that needs showing, because it's the part you cannot express in a config
//! file: each chain carries its own Rust payload type, so chains are added one
//! at a time and `add_chain::<B>` builds a separate pipeline for each.
//!
//! Here the Hub runs CoreChain's payload and the Spoke runs MintChain's. They
//! share nothing but the block envelope, and they're served from one endpoint.
//!
//! ```text
//! cargo run -p spoke-indexer --bin multi_chain
//! ```
//!
//! Then: `curl localhost:8080/v1/chains` to see both, and
//! `curl localhost:8080/v1/chains/mintchain-devnet/status` for one of them.
//! Over gRPC the same choice is the `x-chain-id` header.

use anyhow::Result;
use ingestion::ActionPayload as CorePayload;
use retracer_core::{ChainConfig, ChainHooks, Runner};
use spoke_indexer::MintPayload;
use std::sync::Arc;
use xc_primitives::Block;

const DATABASE_URL: &str = "postgres://retracer:retracer@localhost:5433/retracer";

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    // Pools, migrations and the API ports are process-level, shared by every
    // chain. Connection counts are a property of the database, not of how many
    // chains you happen to follow, so they aren't multiplied per chain.
    let mut runner = Runner::new(DATABASE_URL, 4, 16, 50051)
        .await?
        .with_rest_port(Some(8080));

    // First chain added is the default — it serves gRPC requests that arrive
    // with no `x-chain-id` header, which is what keeps existing single-chain
    // clients working when you add a second chain.
    runner
        .add_chain::<Block<CorePayload>>(
            ChainConfig {
                chain_id: "corechain-devnet".into(),
                display_name: Some("Arxium CoreChain".into()),
                bootnodes: vec![],
                port: 0,
                blocks_topic: ingestion::DEFAULT_BLOCKS_TOPIC.into(),
                sync_protocol: ingestion::DEFAULT_SYNC_PROTOCOL.into(),
                max_pending_blocks: ingestion::DEFAULT_MAX_PENDING_BLOCKS,
                // CoreChain is single-proposer with no forks, so nothing to
                // un-index. Zero declares that rather than leaving a rollback
                // budget nothing will ever spend.
                finality_depth: 0,
                kind_schema: "kind_schema.toml".into(),
                node_rpc_url: None,
            },
            ChainHooks {
                address_validator: Some(Arc::new(ingestion::is_corechain_address)),
                ..Default::default()
            },
        )
        .await?;

    // Different payload type, different address format, different topic, its
    // own finality depth — and, because it can fork, a real rollback budget.
    runner
        .add_chain::<Block<MintPayload>>(
            ChainConfig {
                chain_id: "mintchain-devnet".into(),
                display_name: Some("MintChain".into()),
                bootnodes: vec![],
                port: 0,
                // Must match what MintChain's node publishes. Not derived from
                // chain_id: it's a wire agreement with that node, and two
                // chains sharing a gossip mesh need distinct topics.
                blocks_topic: "mintchain/blocks/v1".into(),
                sync_protocol: "/mintchain/sync/1".into(),
                max_pending_blocks: ingestion::DEFAULT_MAX_PENDING_BLOCKS,
                finality_depth: 32,
                kind_schema: "examples/spoke-indexer/kind_schema.toml".into(),
                node_rpc_url: None,
            },
            ChainHooks {
                tier_b: vec![Box::new(spoke_indexer::AirdropRecipients)],
                address_validator: Some(Arc::new(spoke_indexer::is_mintchain_address)),
            },
        )
        .await?;

    // Serves both until a chain task ends. One task ending stops the process
    // rather than leaving a half-dead indexer answering queries for a chain
    // that silently stopped advancing.
    runner.run().await
}
