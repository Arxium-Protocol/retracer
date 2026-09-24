// Copyright (c) 2026 Arxium Protocol AG
// SPDX-License-Identifier: Apache-2.0

//! Webhooks: the live tail delivered as HTTP POSTs to a receiver that has a
//! URL rather than an open socket — an issuer's back office learning that a
//! transfer was refused for a compliance reason, without tailing SSE.
//!
//! The delivery story is the SSE cursor made durable. Each hook keeps
//! `cursor_height` in Postgres; the per-chain [`dispatch`] task wakes on
//! every committed block (and on a timer, so a hook registered between
//! blocks starts promptly), and for each enabled hook pages blocks from
//! `cursor + 1` to the tip out of Postgres and POSTs their events in order.
//! The cursor advances only once every event of a block returned 2xx. A
//! receiver that is down is therefore replayed when it comes back, and one
//! that answered but whose 2xx we lost sees a block again — at-least-once,
//! deduped on `X-Retracer-Id`, the same `height:index` /
//! `height:signature` the SSE routes put in `id:`.
//!
//! The broadcast is a wake-up signal here, not the data path, so a lagged
//! receiver is nothing to recover from: the next wake pages from the cursor.

use super::sse::{DroppedEvent, action_matches_address};
use super::{ApiError, ApiResult, AppState};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::{Json, Router};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use storage::{ActionRow, BlockRow, WebhookRow};
use tokio::sync::broadcast;
use utoipa::ToSchema;

const EVENT_ACTION: &str = "action";
const EVENT_DROPPED: &str = "dropped";
const DELIVERY_TIMEOUT: Duration = Duration::from_secs(10);
/// Hooks registered between blocks, or backing off, are picked up at this
/// cadence even when the chain is idle.
const IDLE_WAKE: Duration = Duration::from_secs(5);
const MAX_BACKOFF: Duration = Duration::from_secs(60);
// ponytail: a constant, not a flag. Make it `--webhook-max-failing` when an
// operator asks for a different window.
const MAX_FAILING: i64 = 3 * 24 * 60 * 60;

#[derive(Deserialize, ToSchema)]
pub(super) struct RegisterWebhook {
    /// Receiver. Every delivery is a `POST` here.
    url: String,
    /// HMAC-SHA256 key for `X-Retracer-Signature`; at least 16 characters.
    secret: String,
    /// Only actions this address holds a role on / rejections it sent.
    address: Option<String>,
    /// Subset of `["action", "dropped"]`; default both.
    events: Option<Vec<String>>,
    /// Start delivering from this height instead of the current tip — to
    /// backfill a receiver with history it has not seen.
    from_height: Option<u64>,
}

/// Register a hook, or re-arm the one already at this URL (new secret and
/// filter, enabled again, cursor reset). Requires `--auth-token`: anyone who
/// can register a hook can make this process POST chain data anywhere, so
/// an open API refuses with 403.
#[utoipa::path(post, path = "/v1/chains/{chain_id}/webhooks", tag = "webhooks", params(("chain_id" = String, Path)), request_body = RegisterWebhook, responses((status = 201, body = WebhookRow), (status = 400, body = super::ErrorBody), (status = 403, body = super::ErrorBody), (status = 404, body = super::ErrorBody)))]
pub(super) async fn register(
    State(state): State<AppState>,
    Path(chain_id): Path<String>,
    Json(body): Json<RegisterWebhook>,
) -> Result<(StatusCode, Json<WebhookRow>), ApiError> {
    let chain = state.chain(&chain_id)?;
    require_writable(&state)?;
    let url =
        reqwest::Url::parse(&body.url).map_err(|e| ApiError::BadRequest(format!("url: {e}")))?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(ApiError::BadRequest("url must be http or https".into()));
    }
    if body.secret.len() < 16 {
        return Err(ApiError::BadRequest(
            "secret must be at least 16 characters".into(),
        ));
    }
    if let (Some(address), Some(valid)) = (&body.address, &chain.address_validator)
        && !valid(address)
    {
        return Err(ApiError::BadRequest(
            "not a valid address for this chain".into(),
        ));
    }
    let events = body
        .events
        .unwrap_or_else(|| vec![EVENT_ACTION.into(), EVENT_DROPPED.into()]);
    if events.is_empty()
        || events
            .iter()
            .any(|e| e != EVENT_ACTION && e != EVENT_DROPPED)
    {
        return Err(ApiError::BadRequest(format!(
            "events must be a non-empty subset of [{EVENT_ACTION:?}, {EVENT_DROPPED:?}]"
        )));
    }
    let tip = storage::get_cursor(&state.pool, &chain_id)
        .await?
        .unwrap_or(-1);
    let cursor = match body.from_height {
        Some(from) => from as i64 - 1,
        None => tip,
    };
    let row = storage::upsert_webhook(
        &state.pool,
        &chain_id,
        url.as_str(),
        &body.secret,
        body.address.as_deref(),
        &events,
        cursor,
    )
    .await?;
    Ok((StatusCode::CREATED, Json(row)))
}

#[utoipa::path(get, path = "/v1/chains/{chain_id}/webhooks", tag = "webhooks", params(("chain_id" = String, Path)), responses((status = 200, body = Vec<WebhookRow>), (status = 403, body = super::ErrorBody), (status = 404, body = super::ErrorBody)))]
pub(super) async fn list(
    State(state): State<AppState>,
    Path(chain_id): Path<String>,
) -> ApiResult<Vec<WebhookRow>> {
    state.chain(&chain_id)?;
    require_writable(&state)?;
    Ok(Json(storage::list_webhooks(&state.pool, &chain_id).await?))
}

#[utoipa::path(delete, path = "/v1/chains/{chain_id}/webhooks/{id}", tag = "webhooks", params(("chain_id" = String, Path), ("id" = i64, Path)), responses((status = 204), (status = 403, body = super::ErrorBody), (status = 404, body = super::ErrorBody)))]
pub(super) async fn remove(
    State(state): State<AppState>,
    Path((chain_id, id)): Path<(String, i64)>,
) -> Result<StatusCode, ApiError> {
    state.chain(&chain_id)?;
    require_writable(&state)?;
    if storage::delete_webhook(&state.pool, &chain_id, id).await? {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(ApiError::NotFound(format!("no webhook {id} on {chain_id}")))
    }
}

fn require_writable(state: &AppState) -> Result<(), ApiError> {
    if state.webhooks_writable {
        Ok(())
    } else {
        Err(ApiError::Forbidden(
            "webhooks need --auth-token: an open API must not be made to POST anywhere".into(),
        ))
    }
}

pub(super) fn routes() -> Router<AppState> {
    use axum::routing::{delete, get};
    Router::new()
        .route("/v1/chains/{chain_id}/webhooks", get(list).post(register))
        .route("/v1/chains/{chain_id}/webhooks/{id}", delete(remove))
}

// ------------------------------------------------------------------ delivery

/// One POST body. `event` says which of the two shapes follows.
#[derive(Serialize)]
#[serde(tag = "event")]
enum Delivery {
    #[serde(rename = "action")]
    Action(ActionRow),
    #[serde(rename = "dropped")]
    Dropped(DroppedEvent),
}

impl Delivery {
    fn id(&self) -> String {
        match self {
            Delivery::Action(a) => format!("{}:{}", a.block_height, a.index_in_block),
            Delivery::Dropped(e) => format!("{}:{}", e.dropped.block_height, e.dropped.signature),
        }
    }

    fn kind(&self) -> &'static str {
        match self {
            Delivery::Action(_) => EVENT_ACTION,
            Delivery::Dropped(_) => EVENT_DROPPED,
        }
    }
}

/// The deliveries a block owes one hook, in the order the SSE routes would
/// have streamed them.
fn deliveries(
    hook: &WebhookRow,
    extractor: &storage::AddressExtractor,
    block: &BlockRow,
) -> Vec<Delivery> {
    let mut out = Vec::new();
    let address = hook.address.as_deref();
    if hook.events.iter().any(|e| e == EVENT_ACTION) {
        out.extend(
            block
                .actions
                .iter()
                .filter(|a| address.is_none_or(|addr| action_matches_address(extractor, a, addr)))
                .map(|a| Delivery::Action(a.clone())),
        );
    }
    if hook.events.iter().any(|e| e == EVENT_DROPPED) {
        out.extend(
            block
                .dropped
                .iter()
                .filter(|d| address.is_none_or(|addr| d.sender == addr))
                .map(|d| {
                    Delivery::Dropped(DroppedEvent {
                        dropped: d.clone(),
                        block_timestamp: block.timestamp,
                    })
                }),
        );
    }
    out
}

/// `sha256=<hex>` over `"{timestamp}.{body}"` — the timestamp is signed so a
/// captured delivery cannot be replayed later with a fresh-looking header.
fn signature(secret: &str, timestamp: i64, body: &[u8]) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes())
        .expect("HMAC accepts a key of any length");
    mac.update(timestamp.to_string().as_bytes());
    mac.update(b".");
    mac.update(body);
    format!("sha256={}", hex::encode(mac.finalize().into_bytes()))
}

fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

async fn post(
    http: &reqwest::Client,
    hook: &WebhookRow,
    chain_id: &str,
    delivery: &Delivery,
) -> anyhow::Result<()> {
    let body = serde_json::to_vec(delivery)?;
    let timestamp = now_secs();
    let resp = http
        .post(&hook.url)
        .header("content-type", "application/json")
        .header("x-retracer-chain", chain_id)
        .header("x-retracer-event", delivery.kind())
        .header("x-retracer-id", delivery.id())
        .header("x-retracer-timestamp", timestamp.to_string())
        .header(
            "x-retracer-signature",
            signature(&hook.secret, timestamp, &body),
        )
        .body(body)
        .send()
        .await?;
    anyhow::ensure!(
        resp.status().is_success(),
        "receiver answered {}",
        resp.status()
    );
    Ok(())
}

/// Everything one hook owes between its cursor and the tip. Stops at the
/// first failed delivery, leaving the cursor on the last fully delivered
/// block.
async fn drain(
    pool: &PgPool,
    http: &reqwest::Client,
    chain_id: &str,
    extractor: &storage::AddressExtractor,
    hook: &WebhookRow,
    tip: i64,
) -> anyhow::Result<()> {
    let mut next = hook.cursor_height + 1;
    while next <= tip {
        let page =
            storage::get_blocks_in_range(pool, chain_id, next, tip, storage::BLOCK_PAGE).await?;
        let Some(last) = page.last().map(|b| b.height) else {
            return Ok(());
        };
        for block in &page {
            for delivery in deliveries(hook, extractor, block) {
                post(http, hook, chain_id, &delivery)
                    .await
                    .map_err(|e| e.context(format!("delivering {}", delivery.id())))?;
            }
            storage::webhook_delivered(pool, hook.id, block.height).await?;
        }
        next = last + 1;
    }
    Ok(())
}

/// Per-chain delivery loop; runs until the block broadcast closes.
///
/// Hooks are delivered one after another on each wake. A receiver that
/// times out costs the others at most `DELIVERY_TIMEOUT` per wake before it
/// is backed off.
// ponytail: sequential across hooks; give each hook its own task if one
// chain ever carries enough hooks for a slow receiver to delay the rest.
pub async fn dispatch(
    pool: PgPool,
    chain_id: String,
    extractor: Arc<storage::AddressExtractor>,
    mut blocks: broadcast::Receiver<BlockRow>,
) {
    let http = reqwest::Client::builder()
        .timeout(DELIVERY_TIMEOUT)
        .build()
        .expect("reqwest client with only a timeout set never fails to build");
    // Consecutive failures per hook → when to try it again. Only while the
    // process runs: a restart retries everything immediately, which is fine.
    let mut backoff: HashMap<i64, (u32, Instant)> = HashMap::new();
    loop {
        // A block, a lag, or the idle tick: all mean "look at the cursors".
        if let Ok(Err(broadcast::error::RecvError::Closed)) =
            tokio::time::timeout(IDLE_WAKE, blocks.recv()).await
        {
            return;
        }
        let hooks = match storage::list_webhooks(&pool, &chain_id).await {
            Ok(hooks) => hooks,
            Err(err) => {
                tracing::warn!(%chain_id, %err, "webhooks: listing hooks failed");
                continue;
            }
        };
        if hooks.iter().all(|h| !h.enabled) {
            continue;
        }
        let tip = match storage::get_cursor(&pool, &chain_id).await {
            Ok(Some(tip)) => tip,
            Ok(None) => continue,
            Err(err) => {
                tracing::warn!(%chain_id, %err, "webhooks: reading tip failed");
                continue;
            }
        };
        let now = Instant::now();
        for hook in hooks.iter().filter(|h| h.enabled && h.cursor_height < tip) {
            if backoff.get(&hook.id).is_some_and(|(_, until)| *until > now) {
                continue;
            }
            match drain(&pool, &http, &chain_id, &extractor, hook, tip).await {
                Ok(()) => {
                    backoff.remove(&hook.id);
                }
                Err(err) => {
                    let fails = backoff.get(&hook.id).map_or(0, |(n, _)| *n) + 1;
                    let wait = (Duration::from_secs(1) * 2u32.saturating_pow(fails.min(6)))
                        .min(MAX_BACKOFF);
                    backoff.insert(hook.id, (fails, now + wait));
                    tracing::warn!(%chain_id, hook = hook.id, url = %hook.url, retry_in = ?wait, "webhook delivery failed: {err:#}");
                    let now_secs = now_secs();
                    if let Err(err) = storage::webhook_failed(
                        &pool,
                        hook.id,
                        &format!("{err:#}"),
                        now_secs,
                        now_secs - MAX_FAILING,
                    )
                    .await
                    {
                        tracing::warn!(%chain_id, hook = hook.id, %err, "webhooks: recording failure failed");
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::{lazy_state, rest_chain};
    use axum::body::Body;
    use axum::http::Request;
    use storage::{ActionRow, DroppedRow};
    use tower::ServiceExt as _;

    fn hook(address: Option<&str>, events: &[&str]) -> WebhookRow {
        WebhookRow {
            id: 1,
            chain_id: "test-chain".into(),
            url: "http://receiver/".into(),
            secret: "0123456789abcdef".into(),
            address: address.map(str::to_string),
            events: events.iter().map(|e| e.to_string()).collect(),
            cursor_height: 0,
            enabled: true,
            failing_since: None,
            last_error: None,
            created_at: 0,
        }
    }

    fn block() -> BlockRow {
        BlockRow {
            height: 9,
            hash: "h9".into(),
            parent_hash: "h8".into(),
            timestamp: 1_700_000_009,
            proposer: None,
            undecoded_action_count: 0,
            actions: vec![
                ActionRow {
                    action_hash: "a0".into(),
                    block_height: 9,
                    index_in_block: 0,
                    kind: "Transfer".into(),
                    from_address: "arx1issuer".into(),
                    payload: serde_json::json!({}),
                    block_timestamp: 1_700_000_009,
                },
                ActionRow {
                    action_hash: "a1".into(),
                    block_height: 9,
                    index_in_block: 1,
                    kind: "Transfer".into(),
                    from_address: "arx1other".into(),
                    payload: serde_json::json!({}),
                    block_timestamp: 1_700_000_009,
                },
            ],
            dropped: vec![DroppedRow {
                block_height: 9,
                signature: "0xbb".into(),
                sender: "arx1issuer".into(),
                reason: "compliance".into(),
            }],
        }
    }

    /// The filter and event selection match the SSE routes, and the ids are
    /// the SSE `id:`s so a receiver can dedupe across both.
    #[test]
    fn deliveries_follow_the_hooks_filter_in_stream_order() {
        let extractor = storage::AddressExtractor::tier_a_only(storage::KindSchema::empty());
        let all = deliveries(&hook(None, &["action", "dropped"]), &extractor, &block());
        assert_eq!(
            all.iter().map(Delivery::id).collect::<Vec<_>>(),
            ["9:0", "9:1", "9:0xbb"]
        );
        let issuer = deliveries(
            &hook(Some("arx1issuer"), &["action", "dropped"]),
            &extractor,
            &block(),
        );
        assert_eq!(
            issuer.iter().map(Delivery::id).collect::<Vec<_>>(),
            ["9:0", "9:0xbb"]
        );
        let dropped_only = deliveries(&hook(None, &["dropped"]), &extractor, &block());
        assert_eq!(dropped_only.len(), 1);
        let body: serde_json::Value = serde_json::to_value(&dropped_only[0]).unwrap();
        assert_eq!(body["event"], "dropped");
        assert_eq!(body["reason"], "compliance");
        assert_eq!(body["block_timestamp"], 1_700_000_009);
    }

    /// Pinned so a receiver written against the README keeps verifying.
    #[test]
    fn signature_is_hmac_sha256_over_timestamp_dot_body() {
        let sig = signature("0123456789abcdef", 1_700_000_000, b"{}");
        let mut mac = Hmac::<Sha256>::new_from_slice(b"0123456789abcdef").unwrap();
        mac.update(b"1700000000.{}");
        assert_eq!(
            sig,
            format!("sha256={}", hex::encode(mac.finalize().into_bytes()))
        );
        assert_ne!(sig, signature("0123456789abcdef", 1_700_000_001, b"{}"));
    }

    /// An open API (no `--auth-token`) never registers a hook.
    #[tokio::test]
    async fn registration_is_refused_without_auth() {
        let state = lazy_state(vec![rest_chain(ingestion::NetworkView::default())]);
        let app = crate::router(state.pool.clone(), state.chains.to_vec(), "0", false);
        let resp = app
            .oneshot(
                Request::post("/v1/chains/test-chain/webhooks")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"url":"http://receiver/","secret":"0123456789abcdef"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), 403);
    }

    /// Validation runs before Postgres is touched, so it is testable on the
    /// lazy pool.
    #[tokio::test]
    async fn registration_rejects_bad_input() {
        let state = lazy_state(vec![rest_chain(ingestion::NetworkView::default())]);
        let app = crate::router(state.pool.clone(), state.chains.to_vec(), "0", true);
        for body in [
            r#"{"url":"ftp://receiver/","secret":"0123456789abcdef"}"#,
            r#"{"url":"http://receiver/","secret":"short"}"#,
            r#"{"url":"http://receiver/","secret":"0123456789abcdef","events":["block"]}"#,
        ] {
            let resp = app
                .clone()
                .oneshot(
                    Request::post("/v1/chains/test-chain/webhooks")
                        .header("content-type", "application/json")
                        .body(Body::from(body))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(resp.status(), 400, "{body}");
        }
    }

    /// A real POST to a local receiver: headers, body shape, and a signature
    /// the receiver can verify from the secret alone. A non-2xx is an error
    /// so the cursor stays put.
    #[tokio::test]
    async fn post_signs_what_the_receiver_can_verify_and_fails_on_non_2xx() {
        use axum::extract::Request;
        use axum::routing::post as post_route;
        use std::sync::Mutex;

        type Seen = Arc<Mutex<Vec<(axum::http::HeaderMap, Vec<u8>)>>>;
        let seen: Seen = Default::default();
        let sink = seen.clone();
        let app = Router::new().route(
            "/hook",
            post_route(move |req: Request| {
                let sink = sink.clone();
                async move {
                    let (parts, body) = req.into_parts();
                    let bytes = axum::body::to_bytes(body, 1 << 20).await.unwrap().to_vec();
                    let status = if bytes.contains(&b'z') { 503 } else { 200 };
                    sink.lock().unwrap().push((parts.headers, bytes));
                    StatusCode::from_u16(status).unwrap()
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(axum::serve(listener, app).into_future());

        let mut h = hook(None, &["dropped"]);
        h.url = format!("http://{addr}/hook");
        let http = reqwest::Client::new();
        let delivery = Delivery::Dropped(DroppedEvent {
            dropped: block().dropped[0].clone(),
            block_timestamp: 1,
        });
        post(&http, &h, "test-chain", &delivery).await.expect("2xx");

        let (headers, body) = seen.lock().unwrap()[0].clone();
        assert_eq!(headers["x-retracer-event"], "dropped");
        assert_eq!(headers["x-retracer-id"], "9:0xbb");
        assert_eq!(headers["x-retracer-chain"], "test-chain");
        let ts: i64 = headers["x-retracer-timestamp"]
            .to_str()
            .unwrap()
            .parse()
            .unwrap();
        assert_eq!(
            headers["x-retracer-signature"],
            signature(&h.secret, ts, &body)
        );
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["event"], "dropped");
        assert_eq!(json["sender"], "arx1issuer");

        let mut bad = block().dropped[0].clone();
        bad.reason = "z".into();
        let delivery = Delivery::Dropped(DroppedEvent {
            dropped: bad,
            block_timestamp: 1,
        });
        let err = post(&http, &h, "test-chain", &delivery).await.unwrap_err();
        assert!(err.to_string().contains("503"), "{err}");
    }
}
