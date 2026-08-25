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
use std::sync::Arc;
use std::time::Duration;
use storage::AddressValidator;

/// Same cap the gRPC surface and the node's own RPC use. Kept identical on
/// purpose: two surfaces over one dataset disagreeing about page size is a
/// difference a client discovers the hard way, halfway through pagination.
const MAX_PAGE_SIZE: i64 = 100;

/// Caps how many `GET /validators?height=N` calls one uptime request can
/// fan out to the node — this is an on-demand backfill computation (one
/// node call per height), not a cached live figure, so an unbounded range
/// would let one caller hammer the node. Raise if a real caller needs more;
/// add caching before raising it much further.
const MAX_UPTIME_RANGE: u64 = 5_000;
const MAX_UPTIME_CONCURRENCY: usize = 16;
const NODE_RPC_TIMEOUT: Duration = Duration::from_secs(5);

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
}

#[derive(Clone)]
struct AppState {
    pool: PgPool,
    chains: Arc<Vec<RestChain>>,
    known: Arc<HashSet<String>>,
    http: reqwest::Client,
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
        Ok(self.chains.iter().find(|c| c.chain_id == chain_id).expect("membership just checked"))
    }
}

pub fn router(pool: PgPool, chains: Vec<RestChain>) -> Router {
    let known = chains.iter().map(|c| c.chain_id.clone()).collect();
    let http = reqwest::Client::builder()
        .timeout(NODE_RPC_TIMEOUT)
        .build()
        .expect("reqwest client with only a timeout set never fails to build");
    let state = AppState { pool, chains: Arc::new(chains), known: Arc::new(known), http };

    Router::new()
        .route("/v1/chains", get(list_chains))
        .route("/v1/chains/{chain_id}/status", get(get_status))
        .route("/v1/chains/{chain_id}/stats", get(get_stats))
        .route("/v1/chains/{chain_id}/blocks", get(list_blocks))
        .route("/v1/chains/{chain_id}/blocks/{height}", get(get_block))
        .route("/v1/chains/{chain_id}/actions", get(list_actions))
        .route("/v1/chains/{chain_id}/actions/{action_hash}", get(get_action))
        .route("/v1/chains/{chain_id}/accounts/{address}/actions", get(get_account_actions))
        .route("/v1/chains/{chain_id}/proposers", get(list_proposers))
        .route("/v1/chains/{chain_id}/validators/uptime", get(get_validator_uptime))
        .route("/v1/chains/{chain_id}/search", get(search))
        .route("/health", get(health))
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
                (StatusCode::INTERNAL_SERVER_ERROR, "internal error".to_string())
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
    Ok(Json(storage::list_blocks(&state.pool, &chain_id, limit, page.before).await?))
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
    row.map(Json).ok_or_else(|| ApiError::NotFound("block not found".into()))
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
    Ok(Json(storage::list_actions(&state.pool, &chain_id, limit, page.cursor()?).await?))
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
        return Err(ApiError::BadRequest("not a valid address for this chain".into()));
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
struct ValidatorUptime {
    address: String,
    /// Heights where this address was the primary round-robin designee
    /// (`sorted(validator_set_at_height)[height % len]`, the same formula
    /// `core/primitives::consensus::expected_proposer` uses) — a pure
    /// function of on-chain-public data, not a replay of chain-specific
    /// dispatch logic, so this stays on the right side of the boundary
    /// rules even though `GetValidatorSet` itself is deliberately absent.
    /// Backup-proposer takeover (a validator's turn passing to the next
    /// one after a silent slot) is not counted here — that needs the
    /// chain's `SLOT_DURATION` constant, which isn't part of any public
    /// API, so this is "was it your turn," not "were you eligible."
    turns_owed: u64,
    turns_proposed: i64,
    /// `None` when this address was never the primary designee in range —
    /// dividing by zero owed turns isn't a 0% uptime, it's "not this
    /// validator's turn to be measured here."
    uptime: Option<f64>,
}

/// Backfills validator uptime over `[from, to]` by calling the node's own
/// `GET /validators?height=N` once per height (see `Retracer_Design.md`'s
/// boundary rules on why the validator set isn't derived locally) and
/// comparing the primary round-robin designee at each height against who
/// actually proposed it (`storage::count_proposers_in_range`, already-local
/// data). One node call per height is deliberate here — this is an
/// on-demand backfill, not a live figure; a live/continuously-updated
/// version would need caching this doesn't do (see `MAX_UPTIME_RANGE`).
async fn get_validator_uptime(
    State(state): State<AppState>,
    Path(chain_id): Path<String>,
    Query(query): Query<UptimeQuery>,
) -> ApiResult<Vec<ValidatorUptime>> {
    let chain = state.chain(&chain_id)?;
    let Some(node_rpc_url) = chain.node_rpc_url.clone() else {
        return Err(ApiError::BadRequest(format!(
            "chain {chain_id:?} has no node RPC URL configured; validator uptime is unavailable"
        )));
    };
    if query.from > query.to {
        return Err(ApiError::BadRequest("from must be <= to".to_string()));
    }
    if query.to - query.from + 1 > MAX_UPTIME_RANGE {
        return Err(ApiError::BadRequest(format!("range too large; at most {MAX_UPTIME_RANGE} heights per request")));
    }

    let proposed = storage::count_proposers_in_range(&state.pool, &chain_id, query.from as i64, query.to as i64).await?;

    let http = state.http.clone();
    let sets: Vec<anyhow::Result<(u64, Vec<String>)>> = stream::iter(query.from..=query.to)
        .map(|height| {
            let http = http.clone();
            let node_rpc_url = node_rpc_url.clone();
            async move {
                let mut set = fetch_validator_set(&http, &node_rpc_url, height).await?;
                set.sort();
                Ok((height, set))
            }
        })
        .buffer_unordered(MAX_UPTIME_CONCURRENCY)
        .collect()
        .await;

    let mut owed: HashMap<String, u64> = HashMap::new();
    for result in sets {
        let (height, set) = result?;
        if let Some(designee) = primary_designee(&set, height) {
            *owed.entry(designee.to_string()).or_insert(0) += 1;
        }
    }

    Ok(Json(compute_uptime(owed, proposed)))
}

/// The primary round-robin designee for `height`, given the validator set
/// already sorted the way `core/primitives::consensus::expected_proposer`
/// sorts it (lexicographically). `None` for an empty set.
fn primary_designee(sorted_validators: &[String], height: u64) -> Option<&str> {
    if sorted_validators.is_empty() {
        return None;
    }
    Some(sorted_validators[(height as usize) % sorted_validators.len()].as_str())
}

fn compute_uptime(owed: HashMap<String, u64>, proposed: HashMap<String, i64>) -> Vec<ValidatorUptime> {
    let mut rows: Vec<ValidatorUptime> = owed
        .into_iter()
        .map(|(address, turns_owed)| {
            let turns_proposed = proposed.get(&address).copied().unwrap_or(0);
            let uptime = (turns_owed > 0).then(|| turns_proposed as f64 / turns_owed as f64);
            ValidatorUptime { address, turns_owed, turns_proposed, uptime }
        })
        .collect();
    rows.sort_by(|a, b| a.address.cmp(&b.address));
    rows
}

async fn fetch_validator_set(http: &reqwest::Client, node_rpc_url: &str, height: u64) -> anyhow::Result<Vec<String>> {
    let url = format!("{node_rpc_url}/validators?height={height}");
    let response = http.get(&url).send().await.with_context(|| format!("GET {url}"))?;
    if !response.status().is_success() {
        anyhow::bail!("GET {url} returned {}", response.status());
    }
    response.json().await.with_context(|| format!("decoding response body from {url}"))
}

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

    if let Ok(height) = q.parse::<i64>()
        && storage::block_exists_at_height(&state.pool, &chain_id, height).await?
    {
        return Ok(Json(SearchHit::BlockHeight { height }));
    }
    if chain.address_validator.as_ref().is_some_and(|valid| valid(&q)) {
        return Ok(Json(SearchHit::AccountAddress { address: q }));
    }
    if let Some(height) = storage::block_height_by_hash(&state.pool, &chain_id, &q).await? {
        return Ok(Json(SearchHit::BlockHeight { height }));
    }
    if storage::get_action_by_hash(&state.pool, &chain_id, &q).await?.is_some() {
        return Ok(Json(SearchHit::ActionHash { action_hash: q }));
    }
    Err(ApiError::NotFound("no block, account, or action matches".into()))
}

/// Absent means the default page; zero or negative is a caller mistake worth
/// reporting rather than silently reinterpreting, since a client computing a
/// limit and arriving at 0 wants to know.
fn clamp_limit(limit: Option<i64>) -> Result<i64, ApiError> {
    match limit {
        None => Ok(MAX_PAGE_SIZE),
        Some(n) if n <= 0 => {
            Err(ApiError::BadRequest("limit must be greater than zero".into()))
        }
        Some(n) => Ok(n.min(MAX_PAGE_SIZE)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limit_defaults_and_caps_but_rejects_nonsense() {
        assert_eq!(clamp_limit(None).ok(), Some(MAX_PAGE_SIZE));
        assert_eq!(clamp_limit(Some(10)).ok(), Some(10));
        assert_eq!(clamp_limit(Some(10_000)).ok(), Some(MAX_PAGE_SIZE), "must cap, not trust");
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

        let neither =
            ActionPage { limit: None, before_height: None, before_index: None, role: None };
        assert_eq!(neither.cursor().ok().flatten(), None);

        // Half a cursor must be refused, not completed with a guess — guessing
        // either drops or repeats the boundary block's actions, and both look
        // like ordinary output to the caller.
        let half =
            ActionPage { limit: None, before_height: Some(5), before_index: None, role: None };
        assert!(half.cursor().is_err());
    }

    #[test]
    fn primary_designee_rotates_lexicographically_by_height_modulo_set_size() {
        let validators = vec!["arx1b".to_string(), "arx1a".to_string(), "arx1c".to_string()];
        // Sorted order is a, b, c regardless of input order — matches the
        // node's own `sorted.sort()` before indexing by `height % len`.
        let mut sorted = validators.clone();
        sorted.sort();
        assert_eq!(primary_designee(&sorted, 0), Some("arx1a"));
        assert_eq!(primary_designee(&sorted, 1), Some("arx1b"));
        assert_eq!(primary_designee(&sorted, 2), Some("arx1c"));
        assert_eq!(primary_designee(&sorted, 3), Some("arx1a"), "wraps around");
        assert_eq!(primary_designee(&[], 0), None);
    }

    #[test]
    fn uptime_divides_proposed_by_owed_and_treats_never_owed_as_unmeasured() {
        let owed = HashMap::from([("a".to_string(), 4u64), ("b".to_string(), 2u64)]);
        let proposed = HashMap::from([("a".to_string(), 3i64)]);
        let mut rows = compute_uptime(owed, proposed);
        rows.sort_by(|a, b| a.address.cmp(&b.address));

        assert_eq!(rows[0].address, "a");
        assert_eq!(rows[0].turns_owed, 4);
        assert_eq!(rows[0].turns_proposed, 3);
        assert_eq!(rows[0].uptime, Some(0.75));

        assert_eq!(rows[1].address, "b");
        assert_eq!(rows[1].turns_owed, 2);
        assert_eq!(rows[1].turns_proposed, 0, "no proposed-count row means zero, not missing");
        assert_eq!(rows[1].uptime, Some(0.0));
    }
}
