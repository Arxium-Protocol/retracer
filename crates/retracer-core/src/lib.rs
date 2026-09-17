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

pub mod auth;
pub mod corechain_wire;
pub mod tip;

pub use rest_service::NodeRpcToken;

use anyhow::{Context, Result};
use libp2p::Multiaddr;
use sqlx::PgPool;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use storage::{ActionIndexable, AddressValidator};
use tip::{Tip, TipAction};
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

/// How long API servers get to finish in-flight requests after shutdown starts.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(10);

/// Binds before anything is spawned, so a port conflict fails startup loudly
/// instead of leaving a process that looks healthy and serves nothing.
async fn bind_listener(addr: SocketAddr, surface: &str) -> Result<tokio::net::TcpListener> {
    tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("{surface}: failed to bind {addr}"))
}

/// Resolves on Ctrl+C, or SIGTERM on Unix (what `systemctl stop` sends).
async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(err) = tokio::signal::ctrl_c().await {
            warn!("cannot listen for Ctrl+C: {err}");
            std::future::pending::<()>().await;
        }
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(err) => {
                warn!("cannot listen for SIGTERM: {err}");
                std::future::pending::<()>().await;
            }
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        () = ctrl_c => {}
        () = terminate => {}
    }
}

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
/// Loopback-only until an operator opts in — see `--grpc-bind`/`--rest-bind`.
const DEFAULT_BIND: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);
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
    /// Base URL of this chain's own node HTTP RPC (e.g.
    /// `http://127.0.0.1:8081`), used only for `GET /validators?height=N` —
    /// the same public endpoint any external participant could call, not a
    /// privileged one (see `Retracer_Design.md`'s boundary rules). `None`
    /// disables the validator-uptime endpoint; there's no safe default to
    /// guess since it's a different address than the p2p bootnodes.
    pub node_rpc_url: Option<String>,
    /// Optional bearer credential for this chain's node HTTP RPC. Its Debug
    /// representation is redacted by [`NodeRpcToken`].
    pub node_rpc_token: Option<NodeRpcToken>,
}

/// Process-level configuration, plus the single chain the CLI describes.
pub struct Args {
    pub database_url: String,
    pub grpc_port: u16,
    /// HTTP/JSON surface. `None` disables it — gRPC alone is enough between
    /// services we own on both ends; REST exists for external builders.
    pub rest_port: Option<u16>,
    /// Interface the gRPC surface listens on. Defaults to loopback; set to
    /// `0.0.0.0` (or a specific interface) only behind a firewall, a private
    /// network, or `--auth-token` — gRPC is plaintext.
    pub grpc_bind: IpAddr,
    /// Interface the REST surface listens on. Same default and caveats as
    /// `grpc_bind`.
    pub rest_bind: IpAddr,
    pub write_pool_size: u32,
    pub read_pool_size: u32,
    pub chain: ChainConfig,
    /// Shared secret every request on both surfaces must present as
    /// `Authorization: Bearer <token>`. `None` (the default) leaves both
    /// surfaces open, matching today's trusted-consumer behavior.
    pub auth_token: Option<String>,
    /// Per-IP request budget, in requests/second, on both surfaces. `None`
    /// (the default) disables rate limiting entirely.
    pub rate_limit_rps: Option<u32>,
    /// Proxies whose `X-Forwarded-For` the limiter may believe when
    /// attributing rate-limit buckets. `None` (the default) means the socket
    /// peer is always the client.
    pub trusted_proxies: Option<auth::TrustedProxies>,
    /// Re-run `kind_schema.toml` role extraction over already-indexed actions
    /// before following the chain. See `storage::reindex_action_addresses`.
    pub reindex_addresses: bool,
}

/// Parses a listen address, naming `source` (a flag or env var) in the error
/// so a typo like `--grpc-bind localhost` fails with a message pointing at
/// the right flag instead of a bare "invalid IP address" from the stdlib.
fn parse_bind(raw: &str, source: &str) -> Result<IpAddr> {
    raw.parse().with_context(|| {
        format!("{source} must be an IP address such as 127.0.0.1, 0.0.0.0 or ::1")
    })
}

/// Parses a per-second rate limit, rejecting values the guard cannot honour.
/// The guard enforces a per-60s window (`RateLimiter` stores `rps * 60`), so a
/// value whose minute budget overflows `u32` is an error rather than a silent
/// saturation, and zero — which would deny every request — is rejected rather
/// than arming a deny-all limiter.
fn parse_rate_limit_rps(raw: &str, source: &str) -> Result<u32> {
    let rps: u32 = raw
        .parse()
        .with_context(|| format!("{source} must be a u32"))?;
    validate_rate_limit_rps(rps, source)?;
    Ok(rps)
}

/// Rejects rate limits the guard cannot honour, naming `source` (a flag or
/// env var, or the builder method) in the error. Shared by the CLI boundary
/// and [`Runner::run`] so programmatic embedders get the same refusal as
/// flag parsing: zero would deny every request, and a value whose minute
/// budget overflows `u32` would otherwise saturate silently.
fn validate_rate_limit_rps(rps: u32, source: &str) -> Result<()> {
    anyhow::ensure!(rps > 0, "{source} must be greater than zero");
    anyhow::ensure!(
        rps.checked_mul(60).is_some(),
        "{source} is too large: the per-minute budget would overflow a u32"
    );
    Ok(())
}

/// `retracerd --help` text. Kept in sync with `parse_args`'s match arms by
/// the `usage_lists_every_flag` test below — add a flag to both or neither.
pub const USAGE: &str = "\
retracerd - a Retracer indexer for one chain

USAGE:
    retracerd [OPTIONS]

OPTIONS:
    --bootnodes <multiaddrs>       Comma-separated peer multiaddrs to dial on startup.
                                   [default: none] [env: RETRACER_BOOTNODES]
    --port <u16>                   P2P listen port; 0 picks a free one.
                                   [default: 0]
    --database-url <url>           Postgres connection string.
                                   [default: postgres://retracer:retracer@localhost:5433/retracer]
                                   [env: RETRACER_DATABASE_URL]
    --chain-id <string>            Label for this chain's rows; not read off the wire.
                                   [default: corechain-devnet]
    --rest-port <u16>              HTTP API port; 0 disables it.
                                   [default: 8080]
    --grpc-port <u16>              gRPC API port.
                                   [default: 50051]
    --grpc-bind <ip>               Interface the gRPC surface listens on.
                                   [default: 127.0.0.1] [env: RETRACER_GRPC_BIND]
    --rest-bind <ip>               Interface the REST surface listens on.
                                   [default: 127.0.0.1] [env: RETRACER_REST_BIND]
    --kind-schema <path>           Payload field configuration file.
                                   [default: kind_schema.toml]
    --reindex-addresses            Re-run the kind schema's role extraction over
                                   every stored action at startup (after editing
                                   kind_schema.toml), then follow the chain as usual.
    --blocks-topic <topic>         Must match the node's gossip topic.
                                   [default: derived from --chain-id]
    --sync-protocol <protocol>     Must match the node's sync protocol.
                                   [default: derived from --chain-id]
    --max-pending-blocks <usize>   Gap-fill buffer cap; must be at least 1.
                                   [default: 4096]
    --write-pool-size <u32>        Postgres connections for the writer; must be at least 1.
                                   [default: 4]
    --read-pool-size <u32>        Postgres connections for reads; must be at least 1.
                                   [default: 16]
    --finality-depth <u64>         Fallback rollback limit, used only when the node reports no
                                   finality.
                                   [default: 250]
    --node-rpc-url <url>           This chain's node HTTP RPC base URL. Only used for the
                                   validator-uptime endpoint; unset disables it.
                                   [default: none] [env: RETRACER_NODE_RPC_URL]
    --node-rpc-token <token>       Bearer token sent on every HTTP request to this chain's node
                                   RPC. Prefer RETRACER_NODE_RPC_TOKEN so the value is not
                                   visible in process arguments.
                                   [default: none] [env: RETRACER_NODE_RPC_TOKEN]
    --auth-token <token>           Shared secret required as \"Authorization: Bearer <token>\" on
                                   both API surfaces (/health and /ready stay open). Unset means
                                   both surfaces stay open to anyone who can reach them.
                                   [default: none] [env: RETRACER_AUTH_TOKEN]
    --rate-limit-rps <u32>         Per-IP request budget, both surfaces. Unset disables rate
                                   limiting.
                                   [default: none] [env: RETRACER_RATE_LIMIT_RPS]
    --trusted-proxies <list>       Comma-separated IPs/CIDRs whose X-Forwarded-For the limiter
                                   may believe. Unset means the socket peer is always the client.
                                   [default: none] [env: RETRACER_TRUSTED_PROXIES]
    -h, --help                     Print this help and exit.
    -V, --version                  Print the version and exit.
";

/// Minimal manual flag parsing — a handful of flags, not worth a clap
/// dependency for. Describes exactly one chain; multi-chain deployments build
/// [`ChainConfig`]s themselves and drive a [`Runner`], because each chain needs
/// its own Rust block type and that can't come from a flag.
pub fn parse_args() -> Result<Args> {
    // RETRACER_BOOTNODES/RETRACER_DATABASE_URL seed the same defaults a flag
    // would override, so a builder can put them in .env once instead of
    // retyping --bootnodes/--database-url on every run. Precedence is flag >
    // env > hardcoded default.
    let mut bootnodes = Vec::new();
    if let Ok(value) = std::env::var("RETRACER_BOOTNODES") {
        for addr in value.split(',').filter(|s| !s.is_empty()) {
            bootnodes.push(
                addr.parse()
                    .with_context(|| format!("invalid multiaddr: {addr}"))?,
            );
        }
    }
    let mut port = 0u16;
    let mut database_url =
        std::env::var("RETRACER_DATABASE_URL").unwrap_or_else(|_| DEFAULT_DATABASE_URL.to_string());
    let mut chain_id = DEFAULT_CHAIN_ID.to_string();
    let mut grpc_port = DEFAULT_GRPC_PORT;
    let mut rest_port = Some(DEFAULT_REST_PORT);
    let mut grpc_bind = match std::env::var("RETRACER_GRPC_BIND") {
        Ok(value) if !value.is_empty() => parse_bind(&value, "RETRACER_GRPC_BIND")?,
        _ => DEFAULT_BIND,
    };
    let mut rest_bind = match std::env::var("RETRACER_REST_BIND") {
        Ok(value) if !value.is_empty() => parse_bind(&value, "RETRACER_REST_BIND")?,
        _ => DEFAULT_BIND,
    };
    let mut kind_schema = DEFAULT_KIND_SCHEMA.to_string();
    let mut reindex_addresses = false;
    let mut blocks_topic = None;
    let mut sync_protocol = None;
    let mut max_pending_blocks = ingestion::DEFAULT_MAX_PENDING_BLOCKS;
    let mut write_pool_size = DEFAULT_WRITE_POOL_SIZE;
    let mut read_pool_size = DEFAULT_READ_POOL_SIZE;
    let mut finality_depth = DEFAULT_FINALITY_DEPTH;
    // Empty string (an unfilled `.env.example` var left in place) is treated
    // the same as unset, not as "auth token is the empty string" — the
    // latter would silently lock every caller out, since no real
    // `Authorization` header value equals "Bearer " with nothing after it.
    let mut node_rpc_url = std::env::var("RETRACER_NODE_RPC_URL")
        .ok()
        .filter(|v| !v.is_empty());
    let mut node_rpc_token = std::env::var("RETRACER_NODE_RPC_TOKEN")
        .ok()
        .filter(|v| !v.is_empty())
        .map(NodeRpcToken::new)
        .transpose()?;
    let mut auth_token = std::env::var("RETRACER_AUTH_TOKEN")
        .ok()
        .filter(|v| !v.is_empty());
    let mut rate_limit_rps: Option<u32> = match std::env::var("RETRACER_RATE_LIMIT_RPS") {
        Ok(value) if !value.is_empty() => {
            Some(parse_rate_limit_rps(&value, "RETRACER_RATE_LIMIT_RPS")?)
        }
        _ => None,
    };
    let mut trusted_proxies: Option<auth::TrustedProxies> =
        match std::env::var("RETRACER_TRUSTED_PROXIES") {
            Ok(value) if !value.is_empty() => Some(
                auth::TrustedProxies::parse_list(&value)
                    .map_err(|err| anyhow::anyhow!("RETRACER_TRUSTED_PROXIES: {err}"))?,
            ),
            _ => None,
        };
    let mut args = std::env::args().skip(1);
    while let Some(flag) = args.next() {
        match flag.as_str() {
            "--bootnodes" => {
                let value = args.next().context("--bootnodes requires a value")?;
                bootnodes.clear();
                for addr in value.split(',').filter(|s| !s.is_empty()) {
                    bootnodes.push(
                        addr.parse()
                            .with_context(|| format!("invalid multiaddr: {addr}"))?,
                    );
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
            "--grpc-bind" => {
                let value = args.next().context("--grpc-bind requires a value")?;
                grpc_bind = parse_bind(&value, "--grpc-bind")?;
            }
            "--rest-bind" => {
                let value = args.next().context("--rest-bind requires a value")?;
                rest_bind = parse_bind(&value, "--rest-bind")?;
            }
            "--kind-schema" => {
                kind_schema = args.next().context("--kind-schema requires a value")?;
            }
            "--reindex-addresses" => reindex_addresses = true,
            "--blocks-topic" => {
                blocks_topic = Some(args.next().context("--blocks-topic requires a value")?);
            }
            "--sync-protocol" => {
                sync_protocol = Some(args.next().context("--sync-protocol requires a value")?);
            }
            "--max-pending-blocks" => {
                let value = args
                    .next()
                    .context("--max-pending-blocks requires a value")?;
                max_pending_blocks = value
                    .parse()
                    .context("--max-pending-blocks must be a usize")?;
                anyhow::ensure!(
                    max_pending_blocks > 0,
                    "--max-pending-blocks must be at least 1"
                );
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
            "--node-rpc-url" => {
                node_rpc_url = Some(args.next().context("--node-rpc-url requires a value")?);
            }
            "--node-rpc-token" => {
                let value = args.next().context("--node-rpc-token requires a value")?;
                node_rpc_token = Some(NodeRpcToken::new(value)?);
            }
            "--auth-token" => {
                auth_token = Some(args.next().context("--auth-token requires a value")?);
            }
            "--rate-limit-rps" => {
                let value = args.next().context("--rate-limit-rps requires a value")?;
                rate_limit_rps = Some(parse_rate_limit_rps(&value, "--rate-limit-rps")?);
            }
            "--trusted-proxies" => {
                let value = args.next().context("--trusted-proxies requires a value")?;
                trusted_proxies = Some(
                    auth::TrustedProxies::parse_list(&value)
                        .map_err(|err| anyhow::anyhow!("--trusted-proxies: {err}"))?,
                );
            }
            other => anyhow::bail!("unknown flag: {other} (run with --help to list flags)"),
        }
    }
    let blocks_topic = blocks_topic.unwrap_or_else(|| ingestion::default_blocks_topic(&chain_id));
    let sync_protocol =
        sync_protocol.unwrap_or_else(|| ingestion::default_sync_protocol(&chain_id));
    Ok(Args {
        database_url,
        grpc_port,
        rest_port,
        grpc_bind,
        rest_bind,
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
            node_rpc_url,
            node_rpc_token,
        },
        auth_token,
        rate_limit_rps,
        trusted_proxies,
        reindex_addresses,
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
/// no fork of these crates. `retracerd` uses [`corechain_wire::CoreChainBlock`]
/// so released and current CoreChain blocks retain their generation-specific
/// hashes after decoding.
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
    .with_rest_port(args.rest_port)
    .with_grpc_bind(args.grpc_bind)
    .with_rest_bind(args.rest_bind)
    .with_auth_token(args.auth_token)
    .with_rate_limit_rps(args.rate_limit_rps)
    .with_trusted_proxies(args.trusted_proxies);
    runner.add_chain::<B>(args.chain, hooks).await?;
    runner.run().await
}

/// One-chain convenience wrapper with an explicit historical wire decoder.
pub async fn run_with_decoder<B>(
    args: Args,
    hooks: ChainHooks,
    decoder: ingestion::WireDecoder<B>,
) -> Result<()>
where
    B: ingestion::HasHeight + storage::IndexableBlock + Send + Sync + 'static,
{
    run_with_decoder_and_certificate_verifier(args, hooks, decoder, None).await
}

/// One-chain wrapper that lets a chain supply independently-verifiable
/// certificate finality without changing the generic multi-chain API.
pub async fn run_with_decoder_and_certificate_verifier<B>(
    args: Args,
    hooks: ChainHooks,
    decoder: ingestion::WireDecoder<B>,
    certificate_verifier: Option<ingestion::CertificateVerifier>,
) -> Result<()>
where
    B: ingestion::HasHeight + storage::IndexableBlock + Send + Sync + 'static,
{
    let mut runner = Runner::new(
        &args.database_url,
        args.write_pool_size,
        args.read_pool_size,
        args.grpc_port,
    )
    .await?
    .with_rest_port(args.rest_port)
    .with_grpc_bind(args.grpc_bind)
    .with_rest_bind(args.rest_bind)
    .with_auth_token(args.auth_token)
    .with_rate_limit_rps(args.rate_limit_rps)
    .with_trusted_proxies(args.trusted_proxies);
    let chain_id = args.chain.chain_id.clone();
    runner
        .add_chain_with_decoder_and_certificate_verifier(
            args.chain,
            hooks,
            decoder,
            certificate_verifier,
        )
        .await?;
    if args.reindex_addresses {
        let extractor = runner
            .runtimes
            .iter()
            .find(|r| r.chain_id == chain_id)
            .map(|r| r.address_extractor.clone())
            .expect("chain was just added");
        let added =
            storage::reindex_action_addresses(&runner.write_pool, &chain_id, &extractor).await?;
        info!(chain_id, added, "action_addresses reindex complete");
    }
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
    grpc_bind: IpAddr,
    rest_bind: IpAddr,
    auth_token: Option<String>,
    rate_limit_rps: Option<u32>,
    trusted_proxies: Option<auth::TrustedProxies>,
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
        storage::migrate(&write_pool)
            .await
            .context("failed to run migrations")?;

        Ok(Runner {
            write_pool,
            read_pool,
            grpc_port,
            rest_port: None,
            grpc_bind: DEFAULT_BIND,
            rest_bind: DEFAULT_BIND,
            auth_token: None,
            rate_limit_rps: None,
            trusted_proxies: None,
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

    /// Interface the gRPC surface listens on. Defaults to loopback; set to
    /// `0.0.0.0` (or a specific interface) only behind a firewall, a private
    /// network, or `--auth-token` — gRPC is plaintext.
    pub fn with_grpc_bind(mut self, addr: IpAddr) -> Self {
        self.grpc_bind = addr;
        self
    }

    /// Interface the REST surface listens on. Same default and caveats as
    /// [`Self::with_grpc_bind`].
    pub fn with_rest_bind(mut self, addr: IpAddr) -> Self {
        self.rest_bind = addr;
        self
    }

    /// Require `Authorization: Bearer <token>` on every request, both
    /// surfaces. `None` (the default) leaves both open.
    pub fn with_auth_token(mut self, token: Option<String>) -> Self {
        self.auth_token = token;
        self
    }

    /// Per-IP request budget in requests/second, both surfaces. `None` (the
    /// default) disables rate limiting. Zero or overflowing values fail
    /// [`Runner::run`] rather than arming a deny-all or saturated limiter.
    pub fn with_rate_limit_rps(mut self, rps: Option<u32>) -> Self {
        self.rate_limit_rps = rps;
        self
    }

    /// Proxies whose `X-Forwarded-For` the limiter may believe. `None` (the
    /// default) means the socket peer is always the client. Only configure
    /// addresses you operate: trusting a header from anyone else lets them
    /// mint a fresh rate-limit bucket per request.
    pub fn with_trusted_proxies(mut self, proxies: Option<auth::TrustedProxies>) -> Self {
        self.trusted_proxies = proxies;
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
        self.add_chain_with_decoder::<B>(config, hooks, ingestion::WireDecoder::<B>::exact())
            .await
    }

    /// Registers a chain with a documented historical wire compatibility
    /// policy. Exact decoding remains the default through [`Self::add_chain`].
    pub async fn add_chain_with_decoder<B>(
        &mut self,
        config: ChainConfig,
        hooks: ChainHooks,
        decoder: ingestion::WireDecoder<B>,
    ) -> Result<()>
    where
        B: ingestion::HasHeight + storage::IndexableBlock + Send + Sync + 'static,
    {
        self.add_chain_with_decoder_and_certificate_verifier(config, hooks, decoder, None)
            .await
    }

    pub async fn add_chain_with_decoder_and_certificate_verifier<B>(
        &mut self,
        config: ChainConfig,
        hooks: ChainHooks,
        decoder: ingestion::WireDecoder<B>,
        certificate_verifier: Option<ingestion::CertificateVerifier>,
    ) -> Result<()>
    where
        B: ingestion::HasHeight + storage::IndexableBlock + Send + Sync + 'static,
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
        let projections = kind_schema.projections().to_vec();
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
        let (network_tx, network_view) =
            tokio::sync::watch::channel(ingestion::NetworkView::default());

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
        self.tasks.push(tokio::spawn(
            ingestion::run_with_decoder_and_certificate_verifier(
                ingestion_config,
                block_tx,
                rewind_rx,
                network_tx,
                decoder,
                certificate_verifier,
            ),
        ));

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
            projections,
            network_view: network_view.clone(),
            node_rpc_url: config.node_rpc_url.clone(),
            node_rpc_token: config.node_rpc_token.clone(),
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

    /// Serves gRPC (and REST, if configured) and runs until a chain task or an
    /// API server finishes or fails, or a shutdown signal arrives.
    ///
    /// One chain task ending takes the process down rather than leaving the
    /// rest running: a half-dead multi-chain indexer still answers queries for
    /// the chain that died, with data that silently stops advancing. Failing
    /// visibly is the better outcome — a supervisor restarts it. An API server
    /// dying is fatal for the same reason: if gRPC dies while REST keeps
    /// answering `/health`, a supervisor never restarts the process and
    /// clients relying on gRPC lose service silently.
    pub async fn run(self) -> Result<()> {
        anyhow::ensure!(
            !self.runtimes.is_empty(),
            "no chains registered; nothing to do"
        );

        let chain_ids: Vec<&str> = self.runtimes.iter().map(|r| r.chain_id.as_str()).collect();
        info!(chains = ?chain_ids, default = %chain_ids[0], "serving");

        if let Some(rps) = self.rate_limit_rps {
            validate_rate_limit_rps(rps, "with_rate_limit_rps")?;
        }
        let guard = auth::GuardConfig::new(self.auth_token, self.rate_limit_rps)
            .with_trusted_proxies(self.trusted_proxies);
        if guard.is_active() {
            info!(
                auth = guard.token.is_some(),
                rate_limit = guard.rate_limiter.is_some(),
                "request guard active on both surfaces"
            );
        } else {
            let grpc_bind = self.grpc_bind;
            let rest_bind = self.rest_bind;
            warn!(
                %grpc_bind, %rest_bind,
                "no --auth-token and no --rate-limit-rps: both API surfaces are \
                 open to anyone who can reach {grpc_bind}/{rest_bind}; only bind \
                 a non-loopback address on a closed network"
            );
        }

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let mut servers: tokio::task::JoinSet<Result<()>> = tokio::task::JoinSet::new();

        let grpc_addr = SocketAddr::new(self.grpc_bind, self.grpc_port);
        let grpc_listener = bind_listener(grpc_addr, "gRPC").await?;
        let grpc_service = grpc_service::server(self.read_pool.clone(), self.runtimes);
        let grpc_service = tonic::service::interceptor::InterceptedService::new(
            grpc_service,
            auth::GrpcGuard(guard.clone()),
        );
        info!(%grpc_addr, "gRPC listening");
        let mut grpc_stop = shutdown_rx.clone();
        servers.spawn(async move {
            tonic::transport::Server::builder()
                .add_service(grpc_service)
                .serve_with_incoming_shutdown(
                    tokio_stream::wrappers::TcpListenerStream::new(grpc_listener),
                    async move {
                        let _ = grpc_stop.wait_for(|stop| *stop).await;
                    },
                )
                .await
                .context("gRPC server failed")
        });

        if let Some(rest_port) = self.rest_port {
            let rest_addr = SocketAddr::new(self.rest_bind, rest_port);
            let listener = bind_listener(rest_addr, "REST").await?;
            let router = rest_service::router(self.read_pool.clone(), self.rest_chains).layer(
                axum::middleware::from_fn_with_state(guard, auth::rest_guard),
            );
            info!(%rest_addr, "REST listening");
            let service = router.into_make_service_with_connect_info::<SocketAddr>();
            let mut rest_stop = shutdown_rx.clone();
            servers.spawn(async move {
                axum::serve(listener, service)
                    .with_graceful_shutdown(async move {
                        let _ = rest_stop.wait_for(|stop| *stop).await;
                    })
                    .await
                    .context("REST server failed")
            });
        }

        let chains = futures::future::select_all(self.tasks);
        let outcome = tokio::select! {
            (result, _, _) = chains => result.context("a chain task panicked").and_then(|inner| inner),
            Some(joined) = servers.join_next() => match joined {
                Ok(Ok(())) => Err(anyhow::anyhow!("an API server stopped unexpectedly")),
                Ok(Err(err)) => Err(err),
                Err(err) => Err(anyhow::Error::new(err).context("an API server task panicked")),
            },
            () = shutdown_signal() => {
                info!("shutdown signal received; draining API servers");
                Ok(())
            }
        };
        let _ = shutdown_tx.send(true);
        let _ = tokio::time::timeout(SHUTDOWN_GRACE, async {
            while servers.join_next().await.is_some() {}
        })
        .await;
        outcome
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

    // Blocks that classified as `Extend`, waiting to be committed together.
    // Only backfill fills this: `recv_many` returns as soon as the channel has
    // anything, so a live chain producing a block every couple of seconds
    // still commits one at a time and gains no latency.
    let mut batch: Vec<B> = Vec::with_capacity(storage::INSERT_BATCH_BLOCKS);
    let mut inbox: Vec<B> = Vec::with_capacity(storage::INSERT_BATCH_BLOCKS);

    loop {
        inbox.clear();
        if block_rx
            .recv_many(&mut inbox, storage::INSERT_BATCH_BLOCKS)
            .await
            == 0
        {
            break; // channel closed
        }

        for block in inbox.drain(..) {
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
                    // Commit what's pending before rolling back: `rollback_to`
                    // deletes committed rows, and blocks still in the batch would
                    // survive it and reappear above the rewind point.
                    let _ = flush_batch(
                        &write_pool,
                        &chain_id,
                        &mut batch,
                        &address_extractor,
                        &blocks_tx,
                    )
                    .await;
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
                            .map(|hash| Tip {
                                height: rollback_to,
                                hash,
                            })
                    };
                    rewound_from = Some(from);
                    // Drop this block: it belongs after the height we just rewound
                    // to, and will come back through the sync protocol once
                    // ingestion has rewound too.
                    let _ = rewind_tx.send((rollback_to + 1).max(0) as u64).await;
                    continue;
                }
                TipAction::ForkBelowFinalized {
                    would_rollback_to,
                    finalized_height,
                } => {
                    // Ingestion stalls here; make what was already accepted
                    // durable rather than losing it with the batch.
                    let _ = flush_batch(
                        &write_pool,
                        &chain_id,
                        &mut batch,
                        &address_extractor,
                        &blocks_tx,
                    )
                    .await;
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
                TipAction::ForkTooDeep {
                    would_rollback_to,
                    depth,
                } => {
                    let _ = flush_batch(
                        &write_pool,
                        &chain_id,
                        &mut batch,
                        &address_extractor,
                        &blocks_tx,
                    )
                    .await;
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
            // Tip advances optimistically so the next block in this same batch
            // classifies against it. `flush_batch` restores it from the database
            // if the commit fails, so a failed batch cannot leave the tip claiming
            // heights that were never written.
            tip = Some(Tip { height, hash });
            rewound_from = None;
            batch.push(block);

            if batch.len() >= storage::INSERT_BATCH_BLOCKS {
                flush_batch(
                    &write_pool,
                    &chain_id,
                    &mut batch,
                    &address_extractor,
                    &blocks_tx,
                )
                .await;
            }
        }

        // Channel drained — commit what this chunk accumulated. Once caught
        // up this runs per block, so batching costs no latency when live.
        if !flush_batch(
            &write_pool,
            &chain_id,
            &mut batch,
            &address_extractor,
            &blocks_tx,
        )
        .await
        {
            tip = durable_tip(&write_pool, &chain_id).await;
        }
    }

    let _ = flush_batch(
        &write_pool,
        &chain_id,
        &mut batch,
        &address_extractor,
        &blocks_tx,
    )
    .await;
    Ok(())
}

/// Commits a batch and broadcasts its blocks, leaving `batch` empty.
///
/// Broadcast happens only after the commit succeeds, so a `SubscribeBlocks`
/// subscriber never sees a block that failed to land. A failed batch is logged
/// and dropped: every write is idempotent and the cursor advances inside the
/// same transaction, so the next sync request re-fetches from the last
/// committed height and re-applies.
async fn flush_batch<B>(
    pool: &sqlx::PgPool,
    chain_id: &str,
    batch: &mut Vec<B>,
    address_extractor: &storage::AddressExtractor,
    blocks_tx: &tokio::sync::broadcast::Sender<storage::BlockRow>,
) -> bool
where
    B: storage::IndexableBlock,
{
    if batch.is_empty() {
        return true;
    }
    let first = storage::IndexableBlock::height(&batch[0]);
    let last = storage::IndexableBlock::height(&batch[batch.len() - 1]);
    let actions: usize = batch.iter().map(|b| b.actions().len()).sum();

    match storage::insert_blocks(pool, chain_id, batch, address_extractor).await {
        Ok(()) => {
            // One line per batch, not per block: backfilling 207k heights
            // previously emitted 207k log lines, which is its own cost and
            // buries everything else in the journal.
            info!(
                %chain_id, first_height = first, last_height = last,
                blocks = batch.len(), actions, "indexed blocks"
            );
            for block in batch.iter() {
                if let Ok(row) = storage::block_row_from_wire(block) {
                    let _ = blocks_tx.send(row);
                }
            }
        }
        Err(err) => {
            warn!(
                %chain_id, first_height = first, last_height = last,
                "failed to index blocks: {err}"
            );
            batch.clear();
            return false;
        }
    }
    batch.clear();
    true
}

/// The tip as the database actually has it.
///
/// Called after a failed flush, because the loop advances `tip` optimistically
/// while filling a batch so that consecutive blocks classify against each
/// other. If the commit fails those heights were never written, and leaving
/// `tip` pointing at them would make every subsequent block classify against a
/// tip that does not exist — spurious gaps and forks.
async fn durable_tip(pool: &sqlx::PgPool, chain_id: &str) -> Option<Tip> {
    let height = storage::get_cursor(pool, chain_id).await.ok().flatten()?;
    let hash = storage::get_block_hash(pool, chain_id, height)
        .await
        .ok()
        .flatten()?;
    Some(Tip { height, hash })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every flag `parse_args` matches on, kept in sync with `USAGE` by this
    /// test — add a flag to both `parse_args`'s match arms and here, or
    /// neither.
    const FLAGS: &[&str] = &[
        "--bootnodes",
        "--port",
        "--database-url",
        "--chain-id",
        "--rest-port",
        "--grpc-port",
        "--grpc-bind",
        "--rest-bind",
        "--kind-schema",
        "--blocks-topic",
        "--sync-protocol",
        "--max-pending-blocks",
        "--write-pool-size",
        "--read-pool-size",
        "--finality-depth",
        "--node-rpc-url",
        "--node-rpc-token",
        "--auth-token",
        "--rate-limit-rps",
        "--trusted-proxies",
    ];

    #[test]
    fn usage_lists_every_flag() {
        for flag in FLAGS {
            assert!(USAGE.contains(flag), "USAGE is missing {flag}");
        }
        assert!(USAGE.contains("--help"));
        assert!(USAGE.contains("--version"));
    }

    #[test]
    fn rate_limit_parsing_accepts_sane_values() {
        assert_eq!(parse_rate_limit_rps("1", "test").unwrap(), 1);
        assert_eq!(parse_rate_limit_rps("100", "test").unwrap(), 100);
        // Largest rps whose per-minute budget still fits a u32.
        assert_eq!(
            parse_rate_limit_rps("71582788", "test").unwrap(),
            71_582_788
        );
    }

    #[test]
    fn rate_limit_parsing_rejects_zero_overflow_and_garbage() {
        assert!(parse_rate_limit_rps("0", "test").is_err());
        assert!(parse_rate_limit_rps("71582789", "test").is_err());
        assert!(parse_rate_limit_rps("4294967295", "test").is_err());
        assert!(parse_rate_limit_rps("many", "test").is_err());
        assert!(parse_rate_limit_rps("", "test").is_err());
    }

    #[test]
    fn parse_bind_accepts_ips_and_names_the_source() {
        assert_eq!(
            parse_bind("127.0.0.1", "test").unwrap(),
            IpAddr::V4(Ipv4Addr::LOCALHOST)
        );
        assert_eq!(
            parse_bind("0.0.0.0", "test").unwrap(),
            IpAddr::V4(Ipv4Addr::UNSPECIFIED)
        );
        assert!(parse_bind("::1", "test").is_ok());

        let err = parse_bind("localhost", "--grpc-bind").unwrap_err();
        assert!(format!("{err:#}").contains("--grpc-bind"));
        let err = parse_bind("10.0.0.1:80", "RETRACER_REST_BIND").unwrap_err();
        assert!(format!("{err:#}").contains("RETRACER_REST_BIND"));
    }

    #[tokio::test]
    async fn bind_listener_names_the_surface_when_the_port_is_taken() {
        let occupied = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind an ephemeral port");
        let addr = occupied.local_addr().expect("local addr");

        let err = bind_listener(addr, "REST")
            .await
            .expect_err("the port is already taken");
        assert!(format!("{err:#}").contains("REST"));
    }
}
