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

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::collections::HashSet;
use std::sync::Arc;
use storage::AddressValidator;

/// Same cap the gRPC surface and the node's own RPC use. Kept identical on
/// purpose: two surfaces over one dataset disagreeing about page size is a
/// difference a client discovers the hard way, halfway through pagination.
const MAX_PAGE_SIZE: i64 = 100;

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
}

#[derive(Clone)]
struct AppState {
    pool: PgPool,
    chains: Arc<Vec<RestChain>>,
    known: Arc<HashSet<String>>,
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
    let state = AppState { pool, chains: Arc::new(chains), known: Arc::new(known) };

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
}
