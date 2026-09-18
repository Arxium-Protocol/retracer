use anyhow::Result;
use retracer_core::ChainHooks;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    let mut argv = std::env::args().skip(1);
    if let Some(first) = argv.find(|a| matches!(a.as_str(), "-h" | "--help" | "-V" | "--version")) {
        if first == "-h" || first == "--help" {
            print!("{}", retracer_core::USAGE);
        } else {
            // Release builds set RETRACER_VERSION to the tag. A local build has
            // no tag to report — CARGO_PKG_VERSION is not bumped per release —
            // so say so explicitly rather than print a stale-looking "0.1.0".
            let node = retracer_core::MIN_NODE_VERSION;
            match option_env!("RETRACER_VERSION") {
                Some(tag) => println!("retracerd {tag} (needs node xc-rpc >= {node})"),
                None => println!(
                    "retracerd {} (dev build, not a release; needs node xc-rpc >= {node})",
                    env!("CARGO_PKG_VERSION")
                ),
            }
        }
        return Ok(());
    }

    // Missing .env is fine — flags and the hardcoded defaults still work; this
    // only saves builders from retyping --database-url/--node-rpc-url every run.
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
    // - `RpcBlock` is the block as any Arxium-stack node serves it over RPC.
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
    retracer_core::run::<retracer_core::rpc_block::RpcBlock>(args, hooks).await
}
