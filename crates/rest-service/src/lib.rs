//! A plain HTTP/JSON surface over the same `storage` queries the gRPC service
//! serves, for external builders.
//!
//! gRPC plus a hand-maintained `.proto` works fine between services we own on
//! both ends, but it's an adoption barrier for anyone else: every indexer a
//! builder has used before answers `curl`. This crate is additive — it does not
//! replace or wrap `grpc-service`, it just reads the same rows — so the two
//! surfaces can't drift in behaviour, only in shape.
//!
//! The chain is a path segment here (`/v1/chains/{chain_id}/...`) rather than
//! the `x-chain-id` header gRPC uses. Same routing decision, different idiom:
//! a REST path is the discoverable place for it, and a URL that names its chain
//! can be pasted into a browser or a bug report and still mean one thing.

use anyhow::Context;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use futures::stream::{self, StreamExt};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use storage::AddressValidator;

#[derive(Clone, PartialEq, Eq)]
pub struct NodeRpcToken(String);

impl NodeRpcToken {
    pub fn new(token: String) -> anyhow::Result<Self> {
        anyhow::ensure!(!token.is_empty(), "node RPC token must not be empty");
        Ok(Self(token))
    }

    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for NodeRpcToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("NodeRpcToken([REDACTED])")
    }
}

/// Same cap the gRPC surface and the node's own RPC use. Kept identical on
/// purpose: two surfaces over one dataset disagreeing about page size is a
/// difference a client discovers the hard way, halfway through pagination.
const MAX_PAGE_SIZE: i64 = 100;

/// Caps how many `GET /validators?height=N` calls one uptime request can
/// fan out to the node — each height not already in [`UptimeCache`] costs one
/// node call, so an unbounded range would let one caller hammer the node.
/// Raise if a real caller needs more.
const MAX_UPTIME_RANGE: u64 = 5_000;
const MAX_UPTIME_CONCURRENCY: usize = 16;
const NODE_RPC_TIMEOUT: Duration = Duration::from_secs(5);
const READINESS_DB_TIMEOUT: Duration = Duration::from_secs(2);

/// How long a fetched validator set stays valid in [`UptimeCache`]. Sets are
/// stable once finalised; the TTL bounds staleness where they are not, and
/// repeat uptime requests over the same range stop costing one node call per
/// height.
const UPTIME_CACHE_TTL: Duration = Duration::from_secs(300);
const UPTIME_CACHE_MAX_ENTRIES: usize = 20_000;

struct UptimeCacheEntry {
    fetched: Instant,
    validators: Vec<String>,
}

/// Cache of node validator sets keyed by `(chain_id, height)`, bounding the
/// uptime endpoint's node fan-out. Swept on insert past capacity rather than
/// by a background task, mirroring the rate limiter's sweep-on-grow.
struct UptimeCache {
    ttl: Duration,
    max_entries: usize,
    entries: Mutex<HashMap<(String, u64), UptimeCacheEntry>>,
}

impl UptimeCache {
    fn new() -> Self {
        Self::with_limits(UPTIME_CACHE_TTL, UPTIME_CACHE_MAX_ENTRIES)
    }

    fn with_limits(ttl: Duration, max_entries: usize) -> Self {
        Self {
            ttl,
            max_entries,
            entries: Mutex::new(HashMap::new()),
        }
    }

    fn get(&self, chain_id: &str, height: u64) -> Option<Vec<String>> {
        let entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        entries
            .get(&(chain_id.to_string(), height))
            .filter(|e| e.fetched.elapsed() < self.ttl)
            .map(|e| e.validators.clone())
    }

    fn insert(&self, chain_id: &str, height: u64, validators: Vec<String>) {
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        if entries.len() > self.max_entries {
            entries.retain(|_, e| e.fetched.elapsed() < self.ttl);
        }
        entries.insert(
            (chain_id.to_string(), height),
            UptimeCacheEntry {
                fetched: Instant::now(),
                validators,
            },
        );
    }
}

/// What the REST layer needs to know about a chain. A subset of
/// `grpc_service::ChainRuntime` — no broadcast channel, because this surface
/// has no streaming endpoints.
#[derive(Clone)]
pub struct RestChain {
    pub chain_id: String,
    pub display_name: Option<String>,
    pub blocks_topic: String,
    pub sync_protocol: String,
    pub finality_depth: u64,
    pub address_validator: Option<AddressValidator>,
    pub network_view: tokio::sync::watch::Receiver<ingestion::NetworkView>,
    /// Base URL of this chain's node HTTP RPC, for `GET /validators?height=N`
    /// — used only by [`get_validator_uptime`]. `None` disables that route
    /// with a 400 rather than guessing an address.
    pub node_rpc_url: Option<String>,
    /// Optional bearer credential sent on every HTTP request to this chain's
    /// node RPC. Kept separate per chain because chains can use different nodes.
    pub node_rpc_token: Option<NodeRpcToken>,
}

#[derive(Clone)]
struct AppState {
    pool: PgPool,
    chains: Arc<Vec<RestChain>>,
    known: Arc<HashSet<String>>,
    http: reqwest::Client,
    uptime_cache: Arc<UptimeCache>,
}

impl AppState {
    /// Unknown chain is 404, never a fall back to a default. Unlike the gRPC
    /// surface there isn't even a default to fall back to — the chain is always
    /// explicit in the path, which is the main reason to prefer a path segment
    /// here over a header.
    fn chain(&self, chain_id: &str) -> Result<&RestChain, ApiError> {
        if !self.known.contains(chain_id) {
            return Err(ApiError::NotFound(format!(
                "unknown chain {chain_id:?}; GET /v1/chains lists the ones this indexer serves"
            )));
        }
        Ok(self
            .chains
            .iter()
            .find(|c| c.chain_id == chain_id)
            .expect("membership just checked"))
    }
}

pub fn router(pool: PgPool, chains: Vec<RestChain>) -> Router {
    let known = chains.iter().map(|c| c.chain_id.clone()).collect();
    let http = reqwest::Client::builder()
        .timeout(NODE_RPC_TIMEOUT)
        .build()
        .expect("reqwest client with only a timeout set never fails to build");
    let state = AppState {
        pool,
        chains: Arc::new(chains),
        known: Arc::new(known),
        http,
        uptime_cache: Arc::new(UptimeCache::new()),
    };

    Router::new()
        .route("/v1/chains", get(list_chains))
        .route("/v1/chains/{chain_id}/status", get(get_status))
        .route("/v1/chains/{chain_id}/stats", get(get_stats))
        .route("/v1/chains/{chain_id}/blocks", get(list_blocks))
        .route("/v1/chains/{chain_id}/blocks/{height}", get(get_block))
        .route("/v1/chains/{chain_id}/actions", get(list_actions))
        .route(
            "/v1/chains/{chain_id}/actions/{action_hash}",
            get(get_action),
        )
        .route(
            "/v1/chains/{chain_id}/accounts/{address}/actions",
            get(get_account_actions),
        )
        .route("/v1/chains/{chain_id}/proposers", get(list_proposers))
        .route(
            "/v1/chains/{chain_id}/validators/uptime",
            get(get_validator_uptime),
        )
        .route("/v1/chains/{chain_id}/search", get(search))
        .route("/health", get(health))
        .route("/ready", get(readiness))
        .with_state(state)
}

// ---------------------------------------------------------------- errors

enum ApiError {
    NotFound(String),
    BadRequest(String),
    Internal(anyhow::Error),
}

impl From<anyhow::Error> for ApiError {
    fn from(err: anyhow::Error) -> Self {
        ApiError::Internal(err)
    }
}

#[derive(Serialize)]
struct ErrorBody {
    error: String,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            ApiError::NotFound(m) => (StatusCode::NOT_FOUND, m),
            ApiError::BadRequest(m) => (StatusCode::BAD_REQUEST, m),
            // The underlying error is logged rather than returned: it can carry
            // connection strings and SQL, and a caller can act on "something
            // broke here" but not on our query text.
            ApiError::Internal(err) => {
                tracing::error!("rest request failed: {err:#}");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal error".to_string(),
                )
            }
        };
        (status, Json(ErrorBody { error: message })).into_response()
    }
}

type ApiResult<T> = Result<Json<T>, ApiError>;

// ---------------------------------------------------------------- handlers

async fn health() -> &'static str {
    "ok"
}

#[derive(Debug, Serialize)]
struct ChainReadiness {
    chain_id: String,
    network_visible: bool,
    network_fresh: bool,
    node_tip_height: Option<u64>,
    indexed_height: Option<i64>,
    caught_up: bool,
}

#[derive(Debug, Serialize)]
struct Readiness {
    ready: bool,
    postgres: bool,
    chains: Vec<ChainReadiness>,
}

fn readiness_report(
    postgres: bool,
    chains: &[RestChain],
    statuses: &HashMap<String, storage::IndexStatus>,
) -> Readiness {
    let chains: Vec<_> = chains
        .iter()
        .map(|chain| {
            let network = *chain.network_view.borrow();
            let network_fresh = network.has_fresh_status();
            let fresh_tip = if network_fresh {
                network.tip_height
            } else {
                None
            };
            let status = statuses
                .get(&chain.chain_id)
                .copied()
                .map(|status| status.with_network_tip(fresh_tip));
            ChainReadiness {
                chain_id: chain.chain_id.clone(),
                network_visible: network.tip_height.is_some(),
                network_fresh,
                node_tip_height: network.tip_height,
                indexed_height: status.and_then(|status| status.indexed_height),
                caught_up: network_fresh
                    && status.is_some_and(|status| {
                        status.indexed_height.is_some()
                            && status.indexed_height == status.node_tip_height
                    }),
            }
        })
        .collect();
    let ready = postgres
        && chains
            .iter()
            .all(|chain| chain.network_fresh && chain.caught_up);
    Readiness {
        ready,
        postgres,
        chains,
    }
}

enum DbCheck<T> {
    Ready(T),
    Failed(anyhow::Error),
    TimedOut,
}

async fn bounded_db_check<T>(
    timeout: Duration,
    check: impl Future<Output = anyhow::Result<T>>,
) -> DbCheck<T> {
    match tokio::time::timeout(timeout, check).await {
        Ok(Ok(value)) => DbCheck::Ready(value),
        Ok(Err(error)) => DbCheck::Failed(error),
        Err(_) => DbCheck::TimedOut,
    }
}

async fn load_index_statuses(
    pool: &PgPool,
    chains: &[RestChain],
) -> anyhow::Result<HashMap<String, storage::IndexStatus>> {
    sqlx::query("SELECT 1").execute(pool).await?;
    let mut statuses = HashMap::with_capacity(chains.len());
    for chain in chains {
        statuses.insert(
            chain.chain_id.clone(),
            storage::get_status(pool, &chain.chain_id).await?,
        );
    }
    Ok(statuses)
}

async fn readiness(State(state): State<AppState>) -> Response {
    let report = match bounded_db_check(
        READINESS_DB_TIMEOUT,
        load_index_statuses(&state.pool, &state.chains),
    )
    .await
    {
        DbCheck::Ready(statuses) => readiness_report(true, &state.chains, &statuses),
        DbCheck::Failed(error) => {
            tracing::error!("readiness database check failed: {error:#}");
            readiness_report(false, &state.chains, &HashMap::new())
        }
        DbCheck::TimedOut => {
            tracing::warn!("readiness database check timed out");
            readiness_report(false, &state.chains, &HashMap::new())
        }
    };
    let status = readiness_status(&report);
    (status, Json(report)).into_response()
}

fn readiness_status(report: &Readiness) -> StatusCode {
    if report.ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

#[derive(Serialize)]
struct ChainInfo {
    chain_id: String,
    display_name: Option<String>,
    blocks_topic: String,
    sync_protocol: String,
    finality_depth: u64,
}

async fn list_chains(State(state): State<AppState>) -> ApiResult<Vec<ChainInfo>> {
    Ok(Json(
        state
            .chains
            .iter()
            .map(|c| ChainInfo {
                chain_id: c.chain_id.clone(),
                display_name: c.display_name.clone(),
                blocks_topic: c.blocks_topic.clone(),
                sync_protocol: c.sync_protocol.clone(),
                finality_depth: c.finality_depth,
            })
            .collect(),
    ))
}

async fn get_status(
    State(state): State<AppState>,
    Path(chain_id): Path<String>,
) -> ApiResult<storage::IndexStatus> {
    let chain = state.chain(&chain_id)?;
    Ok(Json(
        storage::get_status(&state.pool, &chain_id)
            .await?
            .with_network_tip(chain.network_view.borrow().tip_height),
    ))
}

async fn get_stats(
    State(state): State<AppState>,
    Path(chain_id): Path<String>,
) -> ApiResult<storage::Stats> {
    state.chain(&chain_id)?;
    Ok(Json(storage::get_stats(&state.pool, &chain_id).await?))
}

#[derive(Deserialize)]
struct BlockPage {
    limit: Option<i64>,
    before: Option<i64>,
}

async fn list_blocks(
    State(state): State<AppState>,
    Path(chain_id): Path<String>,
    Query(page): Query<BlockPage>,
) -> ApiResult<Vec<storage::BlockSummary>> {
    state.chain(&chain_id)?;
    let limit = clamp_limit(page.limit)?;
    Ok(Json(
        storage::list_blocks(&state.pool, &chain_id, limit, page.before).await?,
    ))
}

/// `{height}` accepts a height or a block hash, the same either/or the gRPC
/// `GetBlockRequest` takes — a numeric segment is a height, anything else is a
/// hash. Two routes would be more explicit, but a caller holding an identifier
/// out of a search result shouldn't have to know which kind it is.
async fn get_block(
    State(state): State<AppState>,
    Path((chain_id, height)): Path<(String, String)>,
) -> ApiResult<storage::BlockRow> {
    state.chain(&chain_id)?;
    let row = match height.parse::<i64>() {
        Ok(h) => storage::get_block_by_height(&state.pool, &chain_id, h).await?,
        Err(_) => storage::get_block_by_hash(&state.pool, &chain_id, &height).await?,
    };
    row.map(Json)
        .ok_or_else(|| ApiError::NotFound("block not found".into()))
}

#[derive(Deserialize)]
struct ActionPage {
    limit: Option<i64>,
    before_height: Option<i64>,
    before_index: Option<i32>,
    role: Option<String>,
}

impl ActionPage {
    /// Both cursor halves or neither — a height without an index would have to
    /// guess at the missing half, and either guess silently drops or repeats
    /// the actions in the boundary block. Same rule the gRPC surface enforces.
    fn cursor(&self) -> Result<Option<(i64, i32)>, ApiError> {
        match (self.before_height, self.before_index) {
            (Some(h), Some(i)) => Ok(Some((h, i))),
            (None, None) => Ok(None),
            _ => Err(ApiError::BadRequest(
                "before_height and before_index must be sent together".into(),
            )),
        }
    }
}

async fn list_actions(
    State(state): State<AppState>,
    Path(chain_id): Path<String>,
    Query(page): Query<ActionPage>,
) -> ApiResult<Vec<storage::ActionRow>> {
    state.chain(&chain_id)?;
    let limit = clamp_limit(page.limit)?;
    Ok(Json(
        storage::list_actions(&state.pool, &chain_id, limit, page.cursor()?).await?,
    ))
}

async fn get_action(
    State(state): State<AppState>,
    Path((chain_id, action_hash)): Path<(String, String)>,
) -> ApiResult<storage::ActionRow> {
    state.chain(&chain_id)?;
    storage::get_action_by_hash(&state.pool, &chain_id, &action_hash)
        .await?
        .map(Json)
        .ok_or_else(|| ApiError::NotFound("action not found".into()))
}

async fn get_account_actions(
    State(state): State<AppState>,
    Path((chain_id, address)): Path<(String, String)>,
    Query(page): Query<ActionPage>,
) -> ApiResult<Vec<storage::ActionRow>> {
    let chain = state.chain(&chain_id)?;
    if let Some(valid) = &chain.address_validator
        && !valid(&address)
    {
        return Err(ApiError::BadRequest(
            "not a valid address for this chain".into(),
        ));
    }
    let limit = clamp_limit(page.limit)?;
    Ok(Json(
        storage::get_account_actions(
            &state.pool,
            &chain_id,
            &address,
            limit,
            page.cursor()?,
            page.role.as_deref(),
        )
        .await?,
    ))
}

async fn list_proposers(
    State(state): State<AppState>,
    Path(chain_id): Path<String>,
) -> ApiResult<Vec<storage::ProposerRow>> {
    state.chain(&chain_id)?;
    Ok(Json(storage::list_proposers(&state.pool, &chain_id).await?))
}

#[derive(Deserialize)]
struct UptimeQuery {
    from: u64,
    to: u64,
}

#[derive(Serialize)]
struct UptimeReport {
    from: u64,
    to: u64,
    /// Indexed heights with a known proposer inside the range. Anything
    /// below `to - from + 1` means part of the range is not indexed yet.
    heights_counted: u64,
    validators: Vec<ValidatorUptime>,
}

#[derive(Serialize)]
struct ValidatorUptime {
    address: String,
    /// Heights where this address was the round-0 (primary) designee.
    turns_owed: u64,
    /// Owed turns this address filled itself at round 0.
    turns_proposed: u64,
    /// `turns_owed - turns_proposed`.
    turns_missed: u64,
    /// Blocks this address produced at round > 0, standing in for a
    /// validator that missed. Never counted toward its own uptime.
    backup_proposals: u64,
    /// `turns_proposed / turns_owed`; `None` when nothing was owed.
    uptime: Option<f64>,
}

/// Backfills validator uptime over `[from, to]` by calling the node's own
/// `GET /validators?height=N` (a `UptimeCache` entry per `(chain, height)`
/// keeps a repeat range from re-fetching, see `UPTIME_CACHE_TTL`) and
/// comparing the round-0 designee at each height against who actually
/// proposed it (`storage::list_proposed_heights`, already-local data). Only
/// heights `list_proposed_heights` returns are fetched from the node, so an
/// unindexed height never counts as an owed turn and never costs a node call.
async fn get_validator_uptime(
    State(state): State<AppState>,
    Path(chain_id): Path<String>,
    Query(query): Query<UptimeQuery>,
) -> ApiResult<UptimeReport> {
    let chain = state.chain(&chain_id)?;
    let Some(node_rpc_url) = chain.node_rpc_url.clone() else {
        return Err(ApiError::BadRequest(format!(
            "chain {chain_id:?} has no node RPC URL configured; validator uptime is unavailable"
        )));
    };
    let node_rpc_token = chain.node_rpc_token.clone();
    if query.from > query.to {
        return Err(ApiError::BadRequest("from must be <= to".to_string()));
    }
    if query.to - query.from + 1 > MAX_UPTIME_RANGE {
        return Err(ApiError::BadRequest(format!(
            "range too large; at most {MAX_UPTIME_RANGE} heights per request"
        )));
    }

    let rows = storage::list_proposed_heights(
        &state.pool,
        &chain_id,
        query.from as i64,
        query.to as i64,
    )
    .await?;

    if rows.is_empty() {
        return Ok(Json(UptimeReport {
            from: query.from,
            to: query.to,
            heights_counted: 0,
            validators: Vec::new(),
        }));
    }

    let http = state.http.clone();
    let cache = state.uptime_cache.clone();
    let cache_chain = chain_id.to_string();
    let sets: Vec<anyhow::Result<(u64, Vec<String>)>> = stream::iter(
        rows.iter().map(|row| row.height as u64).collect::<Vec<_>>(),
    )
    .map(|height| {
        let http = http.clone();
        let cache = cache.clone();
        let cache_chain = cache_chain.clone();
        let node_rpc_url = node_rpc_url.clone();
        let node_rpc_token = node_rpc_token.clone();
        async move {
            if let Some(set) = cache.get(&cache_chain, height) {
                return Ok((height, set));
            }
            let mut set =
                fetch_validator_set(&http, &node_rpc_url, node_rpc_token.as_ref(), height).await?;
            set.sort();
            cache.insert(&cache_chain, height, set.clone());
            Ok((height, set))
        }
    })
    .buffer_unordered(MAX_UPTIME_CONCURRENCY)
    .collect()
    .await;

    let mut validator_sets: HashMap<u64, Vec<String>> = HashMap::new();
    for result in sets {
        let (height, set) = result?;
        validator_sets.insert(height, set);
    }

    let heights_counted = rows.len() as u64;
    Ok(Json(UptimeReport {
        from: query.from,
        to: query.to,
        heights_counted,
        validators: compute_uptime(&rows, &validator_sets),
    }))
}

/// Mirrors `core/primitives::consensus::eligible_proposer`: the validator
/// entitled to propose `height` at `round`, from a lexicographically sorted
/// set. `None` for an empty set.
fn eligible_designee(sorted_validators: &[String], height: u64, round: u64) -> Option<&str> {
    if sorted_validators.is_empty() {
        return None;
    }
    let idx = (height as usize).wrapping_add(round as usize) % sorted_validators.len();
    Some(sorted_validators[idx].as_str())
}

fn compute_uptime(
    rows: &[storage::ProposedHeight],
    sets: &HashMap<u64, Vec<String>>,
) -> Vec<ValidatorUptime> {
    let mut owed: HashMap<String, u64> = HashMap::new();
    let mut proposed: HashMap<String, u64> = HashMap::new();
    let mut backup: HashMap<String, u64> = HashMap::new();

    for row in rows {
        let height = row.height as u64;
        let round = row.round as u64;
        let Some(set) = sets.get(&height) else {
            tracing::warn!(height, "no validator set fetched for an owed height; skipping");
            continue;
        };
        let Some(primary) = eligible_designee(set, height, 0) else {
            continue;
        };

        *owed.entry(primary.to_string()).or_insert(0) += 1;

        if round == 0 {
            if row.proposer == primary {
                *proposed.entry(primary.to_string()).or_insert(0) += 1;
            } else {
                tracing::warn!(
                    height,
                    proposer = %row.proposer,
                    primary,
                    "round-0 block was not proposed by the primary designee"
                );
            }
        } else {
            *backup.entry(row.proposer.clone()).or_insert(0) += 1;
            if let Some(expected) = eligible_designee(set, height, round)
                && row.proposer != expected
            {
                tracing::warn!(
                    height,
                    round,
                    proposer = %row.proposer,
                    expected,
                    "backup proposer does not match the round's eligible designee"
                );
            }
        }
    }

    let mut addresses: std::collections::BTreeSet<String> = owed.keys().cloned().collect();
    addresses.extend(backup.keys().cloned());

    addresses
        .into_iter()
        .map(|address| {
            let turns_owed = owed.get(&address).copied().unwrap_or(0);
            let turns_proposed = proposed.get(&address).copied().unwrap_or(0);
            let backup_proposals = backup.get(&address).copied().unwrap_or(0);
            let uptime = (turns_owed > 0).then(|| turns_proposed as f64 / turns_owed as f64);
            ValidatorUptime {
                address,
                turns_owed,
                turns_proposed,
                turns_missed: turns_owed.saturating_sub(turns_proposed),
                backup_proposals,
                uptime,
            }
        })
        .collect()
}

async fn fetch_validator_set(
    http: &reqwest::Client,
    node_rpc_url: &str,
    token: Option<&NodeRpcToken>,
    height: u64,
) -> anyhow::Result<Vec<String>> {
    let url = format!("{node_rpc_url}/validators?height={height}");
    let mut request = http.get(&url);
    if let Some(token) = token {
        request = request.bearer_auth(token.expose());
    }
    let response = request.send().await.with_context(|| format!("GET {url}"))?;
    if !response.status().is_success() {
        anyhow::bail!("GET {url} returned {}", response.status());
    }
    response
        .json()
        .await
        .with_context(|| format!("decoding response body from {url}"))
}

/// Upper bound on a search string, which fans out to a height lookup, an
/// address check and two hash lookups. Hashes and heights are short; anything
/// longer is a caller mistake or a cost probe, not a query.
const MAX_SEARCH_LEN: usize = 256;

#[derive(Deserialize)]
struct SearchQuery {
    q: String,
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum SearchHit {
    BlockHeight { height: i64 },
    AccountAddress { address: String },
    ActionHash { action_hash: String },
}

/// Same "try each kind in turn" order as the gRPC `Search` and the node's own
/// `/search`. The address check sits before the hash lookups, which is why a
/// chain with no configured validator never classifies anything as an account
/// rather than classifying everything as one.
async fn search(
    State(state): State<AppState>,
    Path(chain_id): Path<String>,
    Query(query): Query<SearchQuery>,
) -> ApiResult<SearchHit> {
    let chain = state.chain(&chain_id)?;
    let q = query.q;
    if q.len() > MAX_SEARCH_LEN {
        return Err(ApiError::BadRequest(format!(
            "q must be {MAX_SEARCH_LEN} characters or fewer"
        )));
    }

    if let Ok(height) = q.parse::<i64>()
        && storage::block_exists_at_height(&state.pool, &chain_id, height).await?
    {
        return Ok(Json(SearchHit::BlockHeight { height }));
    }
    if chain
        .address_validator
        .as_ref()
        .is_some_and(|valid| valid(&q))
    {
        return Ok(Json(SearchHit::AccountAddress { address: q }));
    }
    if let Some(height) = storage::block_height_by_hash(&state.pool, &chain_id, &q).await? {
        return Ok(Json(SearchHit::BlockHeight { height }));
    }
    if storage::get_action_by_hash(&state.pool, &chain_id, &q)
        .await?
        .is_some()
    {
        return Ok(Json(SearchHit::ActionHash { action_hash: q }));
    }
    Err(ApiError::NotFound(
        "no block, account, or action matches".into(),
    ))
}

/// Absent means the default page; zero or negative is a caller mistake worth
/// reporting rather than silently reinterpreting, since a client computing a
/// limit and arriving at 0 wants to know.
fn clamp_limit(limit: Option<i64>) -> Result<i64, ApiError> {
    match limit {
        None => Ok(MAX_PAGE_SIZE),
        Some(n) if n <= 0 => Err(ApiError::BadRequest(
            "limit must be greater than zero".into(),
        )),
        Some(n) => Ok(n.min(MAX_PAGE_SIZE)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::Infallible;
    use std::future::pending;
    use std::time::Instant;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn rest_chain(network_view: ingestion::NetworkView) -> RestChain {
        let (_, network_view) = tokio::sync::watch::channel(network_view);
        RestChain {
            chain_id: "test-chain".into(),
            display_name: None,
            blocks_topic: "blocks".into(),
            sync_protocol: "sync".into(),
            finality_depth: 0,
            address_validator: None,
            network_view,
            node_rpc_url: None,
            node_rpc_token: None,
        }
    }

    fn fresh_network(tip_height: u64) -> ingestion::NetworkView {
        ingestion::NetworkView {
            active_peer_count: 1,
            status_peer_count: 1,
            tip_height: Some(tip_height),
            finalized_height: None,
            last_status_at: Some(Instant::now()),
        }
    }

    fn index_status(indexed_height: Option<i64>) -> storage::IndexStatus {
        storage::IndexStatus {
            indexed_height,
            tip_timestamp: indexed_height.map(|_| 1),
            node_tip_height: None,
            blocks_behind: None,
        }
    }

    fn statuses(indexed_height: Option<i64>) -> HashMap<String, storage::IndexStatus> {
        HashMap::from([("test-chain".into(), index_status(indexed_height))])
    }

    async fn mock_validator_server() -> (String, tokio::sync::oneshot::Receiver<String>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (request_tx, request_rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut chunk = [0u8; 1024];
            loop {
                let read = stream.read(&mut chunk).await.unwrap();
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&chunk[..read]);
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }
            let _ = request_tx.send(String::from_utf8(request).unwrap());
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 8\r\nconnection: close\r\n\r\n[\"arx1\"]",
                )
                .await
                .unwrap();
        });
        (format!("http://{address}"), request_rx)
    }

    #[test]
    fn uptime_cache_hits_misses_and_expires() {
        let cache = UptimeCache::new();
        assert!(cache.get("chain", 7).is_none());
        cache.insert("chain", 7, vec!["arx1".to_string()]);
        assert_eq!(cache.get("chain", 7), Some(vec!["arx1".to_string()]));
        assert!(cache.get("chain", 8).is_none());
        assert!(cache.get("other", 7).is_none());

        let stale = UptimeCache::with_limits(Duration::ZERO, 10);
        stale.insert("chain", 7, vec!["arx1".to_string()]);
        assert!(
            stale.get("chain", 7).is_none(),
            "a zero TTL must expire immediately"
        );
    }

    #[test]
    fn uptime_cache_sweep_bounds_memory() {
        // Zero TTL so every entry is already expired: once past capacity each
        // insert must sweep, keeping the map far below the inserted count.
        let cache = UptimeCache::with_limits(Duration::ZERO, 4);
        for height in 0..10 {
            cache.insert("chain", height, vec!["arx1".to_string()]);
        }
        let len = cache.entries.lock().unwrap().len();
        assert!(
            len <= 5,
            "sweep must have reclaimed expired entries, len = {len}"
        );
    }

    #[test]
    fn limit_defaults_and_caps_but_rejects_nonsense() {
        assert_eq!(clamp_limit(None).ok(), Some(MAX_PAGE_SIZE));
        assert_eq!(clamp_limit(Some(10)).ok(), Some(10));
        assert_eq!(
            clamp_limit(Some(10_000)).ok(),
            Some(MAX_PAGE_SIZE),
            "must cap, not trust"
        );
        assert!(clamp_limit(Some(0)).is_err());
        assert!(clamp_limit(Some(-1)).is_err());
    }

    #[test]
    fn action_cursor_requires_both_halves_or_neither() {
        let both = ActionPage {
            limit: None,
            before_height: Some(5),
            before_index: Some(2),
            role: None,
        };
        assert_eq!(both.cursor().ok().flatten(), Some((5, 2)));

        let neither = ActionPage {
            limit: None,
            before_height: None,
            before_index: None,
            role: None,
        };
        assert_eq!(neither.cursor().ok().flatten(), None);

        // Half a cursor must be refused, not completed with a guess — guessing
        // either drops or repeats the boundary block's actions, and both look
        // like ordinary output to the caller.
        let half = ActionPage {
            limit: None,
            before_height: Some(5),
            before_index: None,
            role: None,
        };
        assert!(half.cursor().is_err());
    }

    #[test]
    fn eligible_designee_matches_the_node_formula() {
        let sorted = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        assert_eq!(eligible_designee(&sorted, 0, 0), Some("a"));
        assert_eq!(eligible_designee(&sorted, 1, 0), Some("b"));
        assert_eq!(eligible_designee(&sorted, 3, 0), Some("a"), "wraps around");
        assert_eq!(eligible_designee(&sorted, 1, 1), Some("c"));
        assert_eq!(eligible_designee(&sorted, 2, 2), Some("b"));
        assert_eq!(eligible_designee(&[], 0, 0), None);
    }

    fn proposed_height(height: i64, proposer: &str, round: i64) -> storage::ProposedHeight {
        storage::ProposedHeight {
            height,
            proposer: proposer.to_string(),
            round,
        }
    }

    #[test]
    fn all_turns_filled_gives_full_uptime() {
        let set = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let rows: Vec<storage::ProposedHeight> = (0..=5)
            .map(|height| {
                let primary = eligible_designee(&set, height as u64, 0).unwrap();
                proposed_height(height, primary, 0)
            })
            .collect();
        let sets: HashMap<u64, Vec<String>> = (0..=5u64).map(|h| (h, set.clone())).collect();

        let uptime = compute_uptime(&rows, &sets);
        assert_eq!(uptime.len(), 3);
        for row in &uptime {
            assert_eq!(row.turns_owed, 2);
            assert_eq!(row.turns_proposed, 2);
            assert_eq!(row.turns_missed, 0);
            assert_eq!(row.backup_proposals, 0);
            assert_eq!(row.uptime, Some(1.0));
        }
    }

    #[test]
    fn backup_takeover_charges_the_primary_not_the_backup() {
        let set = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        // height 0: a's turn, a proposes. height 1: b's turn, but c takes
        // over at round 1. height 2: c's turn, c proposes at round 0.
        let rows = vec![
            proposed_height(0, "a", 0),
            proposed_height(1, "c", 1),
            proposed_height(2, "c", 0),
        ];
        let sets: HashMap<u64, Vec<String>> = (0..=2u64).map(|h| (h, set.clone())).collect();

        let uptime = compute_uptime(&rows, &sets);
        let b = uptime.iter().find(|r| r.address == "b").unwrap();
        assert_eq!(b.turns_owed, 1);
        assert_eq!(b.turns_proposed, 0);
        assert_eq!(b.turns_missed, 1);
        assert_eq!(b.uptime, Some(0.0));

        let c = uptime.iter().find(|r| r.address == "c").unwrap();
        assert_eq!(c.turns_owed, 1);
        assert_eq!(c.turns_proposed, 1);
        assert_eq!(c.backup_proposals, 1);
        assert_eq!(c.uptime, Some(1.0), "never counted above 1.0");
    }

    #[test]
    fn unindexed_heights_are_not_owed() {
        let set = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        // Height 1 (b's turn) is missing entirely, e.g. not indexed yet.
        let rows = vec![proposed_height(0, "a", 0), proposed_height(2, "c", 0)];
        let sets: HashMap<u64, Vec<String>> = [0u64, 2u64].into_iter().map(|h| (h, set.clone())).collect();

        let uptime = compute_uptime(&rows, &sets);
        assert!(
            uptime.iter().all(|r| r.address != "b"),
            "an unindexed height must not create an owed turn for its designee"
        );
    }

    #[test]
    fn readiness_rejects_a_chain_that_never_connected() {
        let chain = rest_chain(ingestion::NetworkView::default());
        let report = readiness_report(true, &[chain], &statuses(Some(0)));

        assert!(!report.ready);
        assert!(!report.chains[0].network_visible);
        assert!(!report.chains[0].network_fresh);
    }

    #[test]
    fn readiness_rejects_disconnected_or_stale_status() {
        let disconnected = rest_chain(ingestion::NetworkView::default());
        let disconnected_report = readiness_report(true, &[disconnected], &statuses(Some(4)));
        assert!(!disconnected_report.ready);

        let stale = rest_chain(ingestion::NetworkView {
            active_peer_count: 1,
            status_peer_count: 1,
            tip_height: Some(4),
            finalized_height: Some(3),
            last_status_at: Some(Instant::now() - Duration::from_secs(60)),
        });
        let stale_report = readiness_report(true, &[stale], &statuses(Some(4)));
        assert!(!stale_report.ready);
        assert!(stale_report.chains[0].network_visible);
        assert!(!stale_report.chains[0].network_fresh);
        assert!(!stale_report.chains[0].caught_up);
    }

    #[test]
    fn readiness_rejects_no_indexed_data_and_lag() {
        let empty = rest_chain(fresh_network(4));
        let empty_report = readiness_report(true, &[empty], &statuses(None));
        assert!(!empty_report.ready);
        assert_eq!(empty_report.chains[0].indexed_height, None);

        let lagging = rest_chain(fresh_network(4));
        let lagging_report = readiness_report(true, &[lagging], &statuses(Some(3)));
        assert!(!lagging_report.ready);
        assert!(!lagging_report.chains[0].caught_up);

        let ahead = rest_chain(fresh_network(4));
        let ahead_report = readiness_report(true, &[ahead], &statuses(Some(5)));
        assert!(!ahead_report.ready);
        assert!(!ahead_report.chains[0].caught_up);
    }

    #[test]
    fn readiness_accepts_only_a_caught_up_chain_with_postgres() {
        let chain = rest_chain(fresh_network(4));
        let ready = readiness_report(true, &[chain], &statuses(Some(4)));
        assert!(ready.ready);
        assert!(ready.chains[0].caught_up);
        assert_eq!(readiness_status(&ready), StatusCode::OK);

        let chain = rest_chain(fresh_network(4));
        let database_down = readiness_report(false, &[chain], &statuses(Some(4)));
        assert!(!database_down.ready);
        assert_eq!(
            readiness_status(&database_down),
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[tokio::test]
    async fn bounded_database_check_reports_success_failure_and_timeout() {
        assert!(matches!(
            bounded_db_check(Duration::from_secs(1), async { Ok::<_, anyhow::Error>(7) }).await,
            DbCheck::Ready(7)
        ));
        assert!(matches!(
            bounded_db_check(Duration::from_secs(1), async {
                Err::<(), _>(anyhow::anyhow!("database unavailable"))
            })
            .await,
            DbCheck::Failed(_)
        ));
        assert!(matches!(
            bounded_db_check(
                Duration::from_millis(1),
                pending::<std::result::Result<Infallible, anyhow::Error>>()
            )
            .await,
            DbCheck::TimedOut
        ));
    }

    #[test]
    fn node_rpc_token_debug_is_redacted() {
        let token = NodeRpcToken::new("do-not-print-me".into()).unwrap();
        let debug = format!("{token:?}");
        assert!(debug.contains("REDACTED"));
        assert!(!debug.contains("do-not-print-me"));
    }

    #[tokio::test]
    async fn node_rpc_sends_bearer_header_when_configured() {
        let (base_url, request) = mock_validator_server().await;
        let token = NodeRpcToken::new("node-secret".into()).unwrap();
        let validators = fetch_validator_set(&reqwest::Client::new(), &base_url, Some(&token), 7)
            .await
            .unwrap();

        assert_eq!(validators, vec!["arx1"]);
        let request = request.await.unwrap().to_ascii_lowercase();
        assert!(request.contains("authorization: bearer node-secret\r\n"));
    }

    #[tokio::test]
    async fn node_rpc_omits_bearer_header_when_unset() {
        let (base_url, request) = mock_validator_server().await;
        fetch_validator_set(&reqwest::Client::new(), &base_url, None, 7)
            .await
            .unwrap();

        let request = request.await.unwrap().to_ascii_lowercase();
        assert!(!request.contains("authorization:"));
    }
}
