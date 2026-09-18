//! Following a Hub and a Spoke from one process, one database, one API.
//!
//! The single-chain example (`src/main.rs`) is the common case. Chains are
//! added one at a time — each gets its own node, kind schema, address format
//! and Tier B extractors — and served from one endpoint.
//!
//! ```text
//! cargo run -p spoke-indexer --bin multi_chain
//! ```
//!
//! Then: `curl localhost:8080/v1/chains` to see both, and
//! `curl localhost:8080/v1/chains/mintchain-devnet/status` for one of them.
//! Over gRPC the same choice is the `x-chain-id` header.

use anyhow::Result;
use retracer_core::rpc_block::RpcBlock;
use retracer_core::{ChainConfig, ChainHooks, Runner};
use std::sync::Arc;

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
    // `with_grpc_bind`/`with_rest_bind` default to loopback if not called.
    let mut runner = Runner::new(DATABASE_URL, 4, 16, 50051)
        .await?
        .with_rest_port(Some(8080));

    // First chain added is the default — it serves gRPC requests that arrive
    // with no `x-chain-id` header, which is what keeps existing single-chain
    // clients working when you add a second chain.
    runner
        .add_chain::<RpcBlock>(
            ChainConfig {
                chain_id: "corechain-devnet".into(),
                display_name: Some("Arxium CoreChain".into()),
                blocks_topic: retracer_core::default_blocks_topic("corechain-devnet"),
                sync_protocol: retracer_core::default_sync_protocol("corechain-devnet"),
                // CoreChain is single-proposer with no forks, so nothing to
                // un-index. Zero declares that rather than leaving a rollback
                // budget nothing will ever spend.
                finality_depth: 0,
                kind_schema: "kind_schema.toml".into(),
                node_rpc_url: "http://127.0.0.1:8081".into(),
                node_rpc_token: None,
            },
            ChainHooks {
                address_validator: Some(Arc::new(ingestion::is_corechain_address)),
                ..Default::default()
            },
        )
        .await?;

    // Different node, different address format, its own finality depth — and,
    // because it can fork, a real rollback budget.
    runner
        .add_chain::<RpcBlock>(
            ChainConfig {
                chain_id: "mintchain-devnet".into(),
                display_name: Some("MintChain".into()),
                blocks_topic: "mintchain/blocks/v1".into(),
                sync_protocol: "/mintchain/sync/1".into(),
                finality_depth: 32,
                kind_schema: "examples/spoke-indexer/kind_schema.toml".into(),
                node_rpc_url: "http://127.0.0.1:8082".into(),
                node_rpc_token: None,
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
