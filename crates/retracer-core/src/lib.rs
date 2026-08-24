//! The indexer's actual run loop, as a library — `retracerd` (the CLI
//! binary, `crates/retracerd`) is a thin wrapper over `parse_args`/`run`.
//!
//! Splitting this out is what makes Tier B address extraction
//! (`storage::ActionIndexable`, `Retracer_AddressExtraction_Plan.md`
//! §6/§8) reachable at all: a builder with a payload shape `kind_schema.toml`
//! can't express writes their own `ActionIndexable` impls in Rust and calls
//! `run` directly with them — there's no way to hand Rust code to a CLI
//! flag, so this had to become a library `run` could be embedded through.
//!
//! Two entry points:
//!
//! * [`run`] — one process, one chain. What the CLI uses.
//! * [`Runner`] — one process, many chains, one gRPC endpoint answering across
//!   all of them. Each chain is added with its own block type, so a Hub and its
//!   Spokes can be followed together even though their payloads differ.

pub mod tip;

use anyhow::{Context, Result};
use libp2p::Multiaddr;
use sqlx::PgPool;
use std::sync::Arc;
use storage::{ActionIndexable, AddressValidator};
use tip::{Tip, TipAction};
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

/// The chain-specific pieces that can't be expressed as a CLI flag because they
/// are Rust code. Both have working defaults, so an embedder that needs neither
/// passes `ChainHooks::default()`.
#[derive(Default)]
pub struct ChainHooks {
    /// Tier B address extraction (see module docs). Empty behaves exactly like
    /// Tier A (`kind_schema.toml`) alone.
    pub tier_b: Vec<Box<dyn ActionIndexable>>,
    /// The chain's address-format check. `None` means addresses aren't
    /// validated and `Search` never classifies a query as an account — see
    /// `storage::AddressValidator` for why that's the right default rather than
    /// a permissive one.
    pub address_validator: Option<AddressValidator>,
}

const DEFAULT_DATABASE_URL: &str = "postgres://retracer:retracer@localhost:5433/retracer";
const DEFAULT_CHAIN_ID: &str = "corechain-devnet";
const DEFAULT_GRPC_PORT: u16 = 50051;
const DEFAULT_REST_PORT: u16 = 8080;
const DEFAULT_KIND_SCHEMA: &str = "kind_schema.toml";
const DEFAULT_WRITE_POOL_SIZE: u32 = 4;
const DEFAULT_READ_POOL_SIZE: u32 = 16;
/// Matches The Graph's `ETHEREUM_REORG_THRESHOLD` default. Irrelevant on a
/// fork-free chain like CoreChain, where no rollback ever triggers.
const DEFAULT_FINALITY_DEPTH: u64 = 250;
/// Per-subscriber backlog on the live block stream.
///
// ponytail: fixed backlog per subscriber; a subscriber more than this many
// blocks behind just misses the gap (see `subscribe_blocks`'s doc comment)
// rather than the channel growing unbounded.
const BLOCK_BROADCAST_CAPACITY: usize = 256;

/// Everything about following one chain. Separate from [`Args`] because a
/// [`Runner`] holds many of these against one set of pools and one gRPC port.
pub struct ChainConfig {
    pub chain_id: String,
    /// Free-text label for `ListChains`; `chain_id` remains the stable key.
    pub display_name: Option<String>,
    pub bootnodes: Vec<Multiaddr>,
    pub port: u16,
    /// The gossip topic and sync protocol this chain's *node* publishes on.
    /// Deliberately not derived from `chain_id` — see `ingestion::Config`.
    pub blocks_topic: String,
    pub sync_protocol: String,
    pub max_pending_blocks: usize,
    /// Deepest reorg to un-index before refusing and stalling. Zero declares
    /// the chain fork-free.
    pub finality_depth: u64,
    pub kind_schema: String,
}

/// Process-level configuration, plus the single chain the CLI describes.
pub struct Args {
    pub database_url: String,
    pub grpc_port: u16,
    /// HTTP/JSON surface. `None` disables it — gRPC alone is enough between
    /// services we own on both ends; REST exists for external builders.
    pub rest_port: Option<u16>,
    pub write_pool_size: u32,
    pub read_pool_size: u32,
    pub chain: ChainConfig,
}

/// Minimal manual flag parsing — a handful of flags, not worth a clap
/// dependency for. Describes exactly one chain; multi-chain deployments build
/// [`ChainConfig`]s themselves and drive a [`Runner`], because each chain needs
/// its own Rust block type and that can't come from a flag.
pub fn parse_args() -> Result<Args> {
    let mut bootnodes = Vec::new();
    let mut port = 0u16;
    let mut database_url = DEFAULT_DATABASE_URL.to_string();
    let mut chain_id = DEFAULT_CHAIN_ID.to_string();
    let mut grpc_port = DEFAULT_GRPC_PORT;
    let mut rest_port = Some(DEFAULT_REST_PORT);
    let mut kind_schema = DEFAULT_KIND_SCHEMA.to_string();
    let mut blocks_topic = ingestion::DEFAULT_BLOCKS_TOPIC.to_string();
    let mut sync_protocol = ingestion::DEFAULT_SYNC_PROTOCOL.to_string();
    let mut max_pending_blocks = ingestion::DEFAULT_MAX_PENDING_BLOCKS;
    let mut write_pool_size = DEFAULT_WRITE_POOL_SIZE;
    let mut read_pool_size = DEFAULT_READ_POOL_SIZE;
    let mut finality_depth = DEFAULT_FINALITY_DEPTH;
    let mut args = std::env::args().skip(1);
    while let Some(flag) = args.next() {
        match flag.as_str() {
            "--bootnodes" => {
                let value = args.next().context("--bootnodes requires a value")?;
                for addr in value.split(',').filter(|s| !s.is_empty()) {
                    bootnodes.push(addr.parse().with_context(|| format!("invalid multiaddr: {addr}"))?);
                }
            }
            "--port" => {
                let value = args.next().context("--port requires a value")?;
                port = value.parse().context("--port must be a u16")?;
            }
            "--database-url" => {
                database_url = args.next().context("--database-url requires a value")?;
            }
            "--chain-id" => {
                chain_id = args.next().context("--chain-id requires a value")?;
            }
            "--rest-port" => {
                let value = args.next().context("--rest-port requires a value")?;
                // 0 means "don't serve REST" rather than "pick a free port":
                // an indexer that silently exposed HTTP on an arbitrary port
                // would be a surprise, and there is already a way to ask for a
                // specific one.
                rest_port = match value.parse::<u16>().context("--rest-port must be a u16")? {
                    0 => None,
                    port => Some(port),
                };
            }
            "--grpc-port" => {
                let value = args.next().context("--grpc-port requires a value")?;
                grpc_port = value.parse().context("--grpc-port must be a u16")?;
            }
            "--kind-schema" => {
                kind_schema = args.next().context("--kind-schema requires a value")?;
            }
            "--blocks-topic" => {
                blocks_topic = args.next().context("--blocks-topic requires a value")?;
            }
            "--sync-protocol" => {
                sync_protocol = args.next().context("--sync-protocol requires a value")?;
            }
            "--max-pending-blocks" => {
                let value = args.next().context("--max-pending-blocks requires a value")?;
                max_pending_blocks = value.parse().context("--max-pending-blocks must be a usize")?;
                anyhow::ensure!(max_pending_blocks > 0, "--max-pending-blocks must be at least 1");
            }
            "--write-pool-size" => {
                let value = args.next().context("--write-pool-size requires a value")?;
                write_pool_size = value.parse().context("--write-pool-size must be a u32")?;
                anyhow::ensure!(write_pool_size > 0, "--write-pool-size must be at least 1");
            }
            "--read-pool-size" => {
                let value = args.next().context("--read-pool-size requires a value")?;
                read_pool_size = value.parse().context("--read-pool-size must be a u32")?;
                anyhow::ensure!(read_pool_size > 0, "--read-pool-size must be at least 1");
            }
            "--finality-depth" => {
                let value = args.next().context("--finality-depth requires a value")?;
                finality_depth = value.parse().context("--finality-depth must be a u64")?;
            }
            other => anyhow::bail!("unknown flag: {other}"),
        }
    }
    Ok(Args {
        database_url,
        grpc_port,
        rest_port,
        write_pool_size,
        read_pool_size,
        chain: ChainConfig {
            chain_id,
            display_name: None,
            bootnodes,
            port,
            blocks_topic,
            sync_protocol,
            max_pending_blocks,
            finality_depth,
            kind_schema,
        },
    })
}

/// Connects, migrates, and follows one chain until its p2p task exits.
///
/// `B` is the chain's block type. It is a generic parameter rather than a config
/// value because the wire format is bincode, which is not self-describing:
/// decoding needs the sender's exact Rust layout at compile time. A chain built
/// on `xc-primitives` passes `Block<TheirPayload>` and gets the
/// `storage::IndexableBlock` impl for free; one with a different block envelope
/// implements that trait plus `ingestion::HasHeight` for its own type and needs
/// no fork of these crates. `retracerd` passes
/// `Block<ingestion::ActionPayload>`, the CoreChain one.
///
/// A convenience wrapper over [`Runner`] for the one-chain case.
pub async fn run<B>(args: Args, hooks: ChainHooks) -> Result<()>
where
    B: ingestion::HasHeight
        + storage::IndexableBlock
        + serde::de::DeserializeOwned
        + Send
        + Sync
        + 'static,
{
    let mut runner = Runner::new(
        &args.database_url,
        args.write_pool_size,
        args.read_pool_size,
        args.grpc_port,
    )
    .await?
    .with_rest_port(args.rest_port);
    runner.add_chain::<B>(args.chain, hooks).await?;
    runner.run().await
}

/// Follows one or more chains against shared Postgres pools and a single gRPC
/// endpoint, which routes by the `x-chain-id` header (the first chain added is
/// the default).
///
/// Chains are added one at a time rather than passed as a list because each one
/// carries its own block type: `add_chain::<Block<HubPayload>>(..)` followed by
/// `add_chain::<Block<SpokePayload>>(..)` monomorphises a separate pipeline per
/// chain. That's what lets a Hub and its Spokes — which share a block envelope
/// but not a payload enum — be served from one process, and it's why this isn't
/// expressible as a config file.
pub struct Runner {
    write_pool: PgPool,
    read_pool: PgPool,
    grpc_port: u16,
    rest_port: Option<u16>,
    runtimes: Vec<grpc_service::ChainRuntime>,
    rest_chains: Vec<rest_service::RestChain>,
    tasks: Vec<JoinHandle<Result<()>>>,
}

impl Runner {
    pub async fn new(
        database_url: &str,
        write_pool_size: u32,
        read_pool_size: u32,
        grpc_port: u16,
    ) -> Result<Self> {
        // Separate pools so a flood of gRPC read queries can never starve the
        // ingestion writers of a connection (and vice versa). Shared across
        // chains rather than per-chain: connection count is a property of the
        // database, not of how many chains happen to be followed.
        let write_pool = storage::connect(database_url, write_pool_size)
            .await
            .context("failed to connect to Postgres (write pool)")?;
        let read_pool = storage::connect(database_url, read_pool_size)
            .await
            .context("failed to connect to Postgres (read pool)")?;
        storage::migrate(&write_pool).await.context("failed to run migrations")?;

        Ok(Runner {
            write_pool,
            read_pool,
            grpc_port,
            rest_port: None,
            runtimes: Vec::new(),
            rest_chains: Vec::new(),
            tasks: Vec::new(),
        })
    }

    /// Serve the HTTP/JSON surface too. `None` leaves it off.
    pub fn with_rest_port(mut self, port: Option<u16>) -> Self {
        self.rest_port = port;
        self
    }

    /// Registers a chain and spawns its ingestion and indexing tasks.
    pub async fn add_chain<B>(&mut self, config: ChainConfig, hooks: ChainHooks) -> Result<()>
    where
        B: ingestion::HasHeight
            + storage::IndexableBlock
            + serde::de::DeserializeOwned
            + Send
            + Sync
            + 'static,
    {
        anyhow::ensure!(
            !self.runtimes.iter().any(|r| r.chain_id == config.chain_id),
            "chain {:?} added twice; two pipelines writing one chain_id would \
             interleave rollbacks and corrupt the index",
            config.chain_id
        );
        if config.bootnodes.is_empty() {
            info!(chain_id = %config.chain_id, "no bootnodes given; will only see blocks from peers that dial in");
        }

        let cursor = storage::get_cursor(&self.write_pool, &config.chain_id).await?;
        info!(chain_id = %config.chain_id, ?cursor, "resuming ingestion");

        // Tier A address extraction (Retracer_AddressExtraction_Plan.md §2):
        // missing file just means no kind gets role indexing beyond
        // from_address — not a startup error. A malformed file is, though.
        let kind_schema_path = std::path::Path::new(&config.kind_schema);
        let kind_schema = if kind_schema_path.exists() {
            storage::KindSchema::load(kind_schema_path)
                .with_context(|| format!("failed to load {}", config.kind_schema))?
        } else {
            warn!(
                path = %config.kind_schema,
                chain_id = %config.chain_id,
                "no kind schema config found; action_addresses will only cover configured kinds (none)"
            );
            storage::KindSchema::empty()
        };
        // Declared payload projections become Postgres expression indexes.
        // Done before ingestion starts so a builder's queries are fast from the
        // first block rather than after a manual backfill.
        let projections = storage::create_projection_indexes(&self.write_pool, &kind_schema)
            .await
            .context("failed to create projection indexes")?;
        if projections > 0 {
            info!(chain_id = %config.chain_id, projections, "payload projections indexed");
        }

        // Shared with grpc-service so SubscribeAccountActions can resolve roles
        // (e.g. a Transfer's recipient) the same way insert_block does.
        let address_extractor = Arc::new(storage::AddressExtractor::new(kind_schema, hooks.tier_b));

        storage::register_chain(
            &self.write_pool,
            &config.chain_id,
            config.display_name.as_deref(),
            &config.blocks_topic,
            &config.sync_protocol,
            config.finality_depth as i64,
        )
        .await
        .context("failed to register chain")?;

        let (blocks_tx, _) = tokio::sync::broadcast::channel(BLOCK_BROADCAST_CAPACITY);
        // None until a peer answers a status request — "not connected yet",
        // which the API reports as absent rather than as zero lag.
        let (network_tx, network_view) = tokio::sync::watch::channel(ingestion::NetworkView::default());

        // Bounded so a stalled indexing loop applies backpressure to the p2p
        // receive loop in `ingestion::run` instead of buffering blocks in
        // memory without limit.
        let (block_tx, block_rx) = tokio::sync::mpsc::channel::<B>(300);
        // Depth 4 is plenty: rollbacks are rare, and only one can be
        // outstanding at a time because the indexing loop blocks on the
        // rollback before reading again.
        let (rewind_tx, rewind_rx) = tokio::sync::mpsc::channel::<u64>(4);

        let ingestion_config = ingestion::Config {
            bootnodes: config.bootnodes,
            listen_port: config.port,
            resume_from: cursor.map(|height| height as u64 + 1),
            blocks_topic: config.blocks_topic.clone(),
            sync_protocol: config.sync_protocol.clone(),
            max_pending_blocks: config.max_pending_blocks,
        };
        self.tasks.push(tokio::spawn(ingestion::run(
            ingestion_config,
            block_tx,
            rewind_rx,
            network_tx,
        )));

        self.tasks.push(tokio::spawn(index_chain(
            self.write_pool.clone(),
            config.chain_id.clone(),
            config.finality_depth,
            cursor,
            network_view.clone(),
            address_extractor.clone(),
            blocks_tx.clone(),
            block_rx,
            rewind_tx,
        )));

        self.rest_chains.push(rest_service::RestChain {
            chain_id: config.chain_id.clone(),
            display_name: config.display_name.clone(),
            blocks_topic: config.blocks_topic.clone(),
            sync_protocol: config.sync_protocol.clone(),
            finality_depth: config.finality_depth,
            address_validator: hooks.address_validator.clone(),
            network_view: network_view.clone(),
        });
        self.runtimes.push(grpc_service::ChainRuntime {
            chain_id: config.chain_id,
            display_name: config.display_name,
            blocks_topic: config.blocks_topic,
            sync_protocol: config.sync_protocol,
            finality_depth: config.finality_depth,
            address_extractor,
            address_validator: hooks.address_validator,
            blocks_tx,
            network_view,
        });
        Ok(())
    }

    /// Serves gRPC and runs until the first chain task finishes or fails.
    ///
    /// One task ending takes the process down rather than leaving the rest
    /// running: a half-dead multi-chain indexer still answers queries for the
    /// chain that died, with data that silently stops advancing. Failing
    /// visibly is the better outcome — a supervisor restarts it.
    pub async fn run(self) -> Result<()> {
        anyhow::ensure!(!self.runtimes.is_empty(), "no chains registered; nothing to do");

        let chain_ids: Vec<&str> = self.runtimes.iter().map(|r| r.chain_id.as_str()).collect();
        info!(chains = ?chain_ids, default = %chain_ids[0], "serving");

        let grpc_addr = format!("0.0.0.0:{}", self.grpc_port).parse().context("invalid gRPC port")?;
        let grpc_service = grpc_service::server(self.read_pool.clone(), self.runtimes);
        tokio::spawn(async move {
            info!(%grpc_addr, "gRPC listening");
            if let Err(err) = tonic::transport::Server::builder()
                .add_service(grpc_service)
                .serve(grpc_addr)
                .await
            {
                warn!("gRPC server exited: {err}");
            }
        });

        if let Some(rest_port) = self.rest_port {
            let router = rest_service::router(self.read_pool.clone(), self.rest_chains);
            tokio::spawn(async move {
                let addr = format!("0.0.0.0:{rest_port}");
                match tokio::net::TcpListener::bind(&addr).await {
                    Ok(listener) => {
                        info!(%addr, "REST listening");
                        if let Err(err) = axum::serve(listener, router).await {
                            warn!("REST server exited: {err}");
                        }
                    }
                    Err(err) => warn!("REST server could not bind {addr}: {err}"),
                }
            });
        }

        let (result, _, _) = futures::future::select_all(self.tasks).await;
        result.context("a chain task panicked")?
    }
}

/// The indexing loop for one chain: applies each block, and handles forks by
/// un-indexing back to the last agreeing height and telling ingestion to rewind
/// with it.
#[allow(clippy::too_many_arguments)]
async fn index_chain<B>(
    write_pool: PgPool,
    chain_id: String,
    finality_depth: u64,
    cursor: Option<i64>,
    network_view: tokio::sync::watch::Receiver<ingestion::NetworkView>,
    address_extractor: Arc<storage::AddressExtractor>,
    blocks_tx: tokio::sync::broadcast::Sender<storage::BlockRow>,
    mut block_rx: tokio::sync::mpsc::Receiver<B>,
    rewind_tx: tokio::sync::mpsc::Sender<u64>,
) -> Result<()>
where
    B: storage::IndexableBlock + Send + Sync + 'static,
{
    let mut tip = match cursor {
        Some(height) => storage::get_block_hash(&write_pool, &chain_id, height)
            .await?
            .map(|hash| Tip { height, hash }),
        None => None,
    };
    // Height the current rollback episode began at, so `classify` can measure
    // cumulative depth rather than per-step depth. Cleared once a block
    // actually extends the chain again.
    let mut rewound_from: Option<i64> = None;

    while let Some(block) = block_rx.recv().await {
        let height = storage::IndexableBlock::height(&block) as i64;
        let parent_hash = block.parent_hash();

        // Read per block rather than cached: finality advances while we index,
        // and a stale value would refuse rollbacks the chain has since allowed
        // — or, worse, allow one it has since certified against.
        let finalized = network_view.borrow().finalized_height.map(|h| h as i64);

        match tip::classify(
            tip.as_ref(),
            height,
            &parent_hash,
            rewound_from,
            finality_depth,
            finalized,
        ) {
            TipAction::Extend => {}
            TipAction::Stale => continue,
            TipAction::Gap => {
                // Not a fork — `ingestion` backfills gaps over the sync
                // protocol before forwarding past one, so this stays a
                // warn-only safety net. Rolling back here would delete blocks
                // that are perfectly good.
                warn!(
                    %chain_id,
                    height,
                    tip_height = tip.as_ref().map(|t| t.height),
                    "block arrived beyond our tip with heights missing in between"
                );
            }
            TipAction::Fork { rollback_to } => {
                let from = rewound_from.unwrap_or(height - 1);
                warn!(
                    %chain_id,
                    height,
                    rollback_to,
                    expected_parent = tip.as_ref().map(|t| t.hash.as_str()).unwrap_or(""),
                    got_parent = %parent_hash,
                    "fork detected; un-indexing and re-requesting"
                );
                let removed = storage::rollback_to(&write_pool, &chain_id, rollback_to).await?;
                warn!(%chain_id, removed_blocks = removed, rollback_to, "rolled back");

                tip = if rollback_to < 0 {
                    None
                } else {
                    storage::get_block_hash(&write_pool, &chain_id, rollback_to)
                        .await?
                        .map(|hash| Tip { height: rollback_to, hash })
                };
                rewound_from = Some(from);
                // Drop this block: it belongs after the height we just rewound
                // to, and will come back through the sync protocol once
                // ingestion has rewound too.
                let _ = rewind_tx.send((rollback_to + 1).max(0) as u64).await;
                continue;
            }
            TipAction::ForkBelowFinalized { would_rollback_to, finalized_height } => {
                error!(
                    %chain_id,
                    height,
                    would_rollback_to,
                    finalized_height,
                    "refusing to un-index a finalized block; a peer is serving a chain that \
                     contradicts a finality certificate. Ingestion is stalled for this chain."
                );
                continue;
            }
            TipAction::ForkTooDeep { would_rollback_to, depth } => {
                // Deliberately not fatal and deliberately not obeyed. Refusing
                // leaves the index intact and stalled rather than letting a
                // peer with a bogus chain walk us back to genesis — a human
                // should look at this before any more data is deleted.
                error!(
                    %chain_id,
                    height,
                    would_rollback_to,
                    depth,
                    finality_depth,
                    "refusing to roll back past the finality depth; ingestion is stalled \
                     for this chain until restarted or the finality depth is raised"
                );
                continue;
            }
        }

        let hash = block.hash();
        match storage::insert_block(&write_pool, &chain_id, &block, &address_extractor).await {
            Ok(()) => {
                info!(%chain_id, height, hash = %hash, actions = block.actions().len(), "indexed block");
                tip = Some(Tip { height, hash });
                rewound_from = None;
                // Only fails when there are no subscribers connected — fine,
                // nothing is listening.
                if let Ok(row) = storage::block_row_from_wire(&block) {
                    let _ = blocks_tx.send(row);
                }
            }
            Err(err) => warn!(%chain_id, height, "failed to index block: {err}"),
        }
    }
    Ok(())
}
