//! A complete indexer for an imaginary Spoke Chain, "MintChain".
//!
//! This file is the entire integration. The interesting parts — the payload
//! enum, the address format, the Tier B extractor — live in `src/lib.rs`; all
//! that happens here is handing them to `run`.
//!
//! ```text
//! cargo run -p spoke-indexer -- \
//!   --chain-id mintchain-devnet \
//!   --kind-schema examples/spoke-indexer/kind_schema.toml \
//!   --bootnodes /ip4/127.0.0.1/tcp/30334/p2p/<peer-id>
//! ```
//!
//! See `src/bin/multi_chain.rs` for following several chains at once.

use anyhow::Result;
use retracer_core::{ChainHooks, parse_args, run};
use spoke_indexer::{AirdropRecipients, MintPayload, is_mintchain_address};
use std::sync::Arc;
use xc_primitives::Block;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    // Reuses Retracer's own flags — --chain-id, --bootnodes, --database-url,
    // --rest-port and the rest. Parse your own instead if you'd prefer; `Args`
    // is a plain struct you can build by hand.
    let args = parse_args()?;

    // The turbofish is the whole of "which chain is this". `Block` is the
    // shared envelope from xc-primitives; `MintPayload` is yours.
    run::<Block<MintPayload>>(
        args,
        ChainHooks {
            tier_b: vec![Box::new(AirdropRecipients)],
            address_validator: Some(Arc::new(is_mintchain_address)),
        },
    )
    .await
}
