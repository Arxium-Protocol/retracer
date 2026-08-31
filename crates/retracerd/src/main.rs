use anyhow::Result;
use retracer_core::ChainHooks;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    // Missing .env is fine — flags and the hardcoded defaults still work; this
    // only saves builders from retyping --database-url/--bootnodes every run.
    dotenvy::dotenv().ok();

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .init();

    let args = retracer_core::parse_args()?;

    // Every CoreChain-specific choice the indexer makes is made here, and only
    // here — the library crates are generic over all of it:
    //
    // - `CoreChainBlock` normalizes the supported released/current wire shapes
    //   while preserving each sender generation's block hash. A Spoke Chain
    //   binary uses its own exact block/payload type instead.
    // - `address_validator` is the chain's address format. Without one, the
    //   indexer still works but stops validating addresses and stops resolving
    //   `Search` queries to accounts.
    // - No Tier B impls: the CLI only ever runs Arxium's own kinds, which Tier A
    //   (kind_schema.toml) already covers in full. Tier B
    //   (`storage::ActionIndexable`) exists for an embedder linking
    //   retracer-core directly with its own extractors — see that crate's
    //   module docs.
    let hooks = ChainHooks {
        tier_b: Vec::new(),
        address_validator: Some(std::sync::Arc::new(ingestion::is_corechain_address)),
    };

    retracer_core::run_with_decoder::<retracer_core::corechain_wire::CoreChainBlock>(
        args,
        hooks,
        retracer_core::corechain_wire::decoder(),
    )
    .await
}
