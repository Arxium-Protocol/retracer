// Copyright (c) 2026 Arxium Protocol AG
// SPDX-License-Identifier: Apache-2.0

//! Live tails as server-sent events, Horizon-style: the same JSON rows the
//! paged reads return, one per `data:` line, resumable by `from_height`.
//!
//! The connection is allowed to drop. A client that keeps the height of the
//! last event it handled reconnects with `?from_height=<that + 1>` and gets
//! everything it missed replayed from Postgres before the live tail resumes.
//! That is the whole reliability story — there is no gap detection on the
//! wire. A subscriber that falls behind the broadcast buffer is simply cut
//! off (the stream ends), and its reconnect-with-cursor repairs it.
//!
//! `id:` carries the height (blocks) or `height:index` (actions), so a
//! browser `EventSource` gets `Last-Event-ID` for free; the Go clients use
//! `from_height` explicitly.

use super::{ApiError, AppState};
use axum::extract::{Path, Query, State};
use axum::response::sse::{Event, KeepAlive, Sse};
use futures::stream::{self, Stream, StreamExt};
use serde::{Deserialize, Serialize};
use std::convert::Infallible;
use std::pin::Pin;
use storage::{ActionRow, BlockRow};
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use utoipa::{IntoParams, ToSchema};

#[derive(Deserialize, IntoParams)]
pub(super) struct StreamQuery {
    /// Replay from this height (inclusive) through the indexed tip, then
    /// follow live. Omit for live only.
    from_height: Option<u64>,
}

#[derive(Deserialize, IntoParams)]
pub(super) struct ActionStreamQuery {
    /// Replay from this height (inclusive) through the indexed tip, then
    /// follow live. Omit for live only.
    from_height: Option<u64>,
    /// Only actions where this address holds a role — the sender, or a
    /// kind_schema.toml-resolved role such as a Transfer's recipient.
    address: Option<String>,
}

/// Blocks in `[from_height, tip]` from storage, followed by every block the
/// indexer commits from then on, with the handoff arranged so no height is
/// skipped or sent twice.
///
/// Every block is sent, including empty ones — an explorer's live view wants
/// the block, not just its actions.
#[utoipa::path(get, path = "/v1/chains/{chain_id}/blocks/stream", tag = "blocks", params(("chain_id" = String, Path), StreamQuery), responses((status = 200, description = "`text/event-stream`, one `storage::BlockRow` per event, `id` = height", content_type = "text/event-stream"), (status = 404, body = super::ErrorBody)))]
pub(super) async fn stream_blocks(
    State(state): State<AppState>,
    Path(chain_id): Path<String>,
    Query(query): Query<StreamQuery>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, ApiError> {
    let blocks = block_stream(&state, &chain_id, query.from_height).await?;
    Ok(sse(blocks.map(|block| {
        Event::default()
            .id(block.height.to_string())
            .json_data(block)
    })))
}

/// One `actions/stream` event: the action row plus the timestamp of the block
/// it landed in, which the row itself does not carry and which a consumer
/// turning actions into dated notifications needs — without it, the wallet
/// would be back to tailing blocks just to learn the time.
#[derive(Serialize, ToSchema)]
pub(super) struct ActionEvent {
    #[serde(flatten)]
    pub action: ActionRow,
    pub block_timestamp: i64,
}

/// The same tail as `blocks/stream`, flattened to one event per action. An
/// empty block emits nothing, which is what makes this the right feed for a
/// wallet backend that only reacts to actions: it no longer tails every
/// block on the chain to find the few that carry one.
#[utoipa::path(get, path = "/v1/chains/{chain_id}/actions/stream", tag = "actions", params(("chain_id" = String, Path), ActionStreamQuery), responses((status = 200, description = "`text/event-stream`, one `ActionEvent` per event, `id` = `height:index`", body = ActionEvent, content_type = "text/event-stream"), (status = 404, body = super::ErrorBody)))]
pub(super) async fn stream_actions(
    State(state): State<AppState>,
    Path(chain_id): Path<String>,
    Query(query): Query<ActionStreamQuery>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, ApiError> {
    let chain = state.chain(&chain_id)?;
    if let (Some(address), Some(valid)) = (&query.address, &chain.address_validator)
        && !valid(address)
    {
        return Err(ApiError::BadRequest(
            "not a valid address for this chain".into(),
        ));
    }
    let extractor = chain.address_extractor.clone();
    let address = query.address;
    let blocks = block_stream(&state, &chain_id, query.from_height).await?;
    let actions = blocks.flat_map(move |block| {
        let block_timestamp = block.timestamp;
        let extractor = extractor.clone();
        let address = address.clone();
        stream::iter(
            block
                .actions
                .into_iter()
                .filter(move |action| {
                    address
                        .as_deref()
                        .is_none_or(|a| action_matches_address(&extractor, action, a))
                })
                .map(move |action| ActionEvent {
                    action,
                    block_timestamp,
                }),
        )
    });
    Ok(sse(actions.map(|event| {
        Event::default()
            .id(format!(
                "{}:{}",
                event.action.block_height, event.action.index_in_block
            ))
            .json_data(event)
    })))
}

fn sse<S>(events: S) -> Sse<impl Stream<Item = Result<Event, Infallible>>>
where
    S: Stream<Item = Result<Event, axum::Error>> + Send + 'static,
{
    // `json_data` only fails on a row that doesn't serialize, and every row
    // here already serializes for the paged reads — treat it as a bug and
    // drop the event rather than abort the tail.
    Sse::new(events.filter_map(|event| async {
        match event {
            Ok(event) => Some(Ok(event)),
            Err(err) => {
                tracing::error!(%err, "SSE row failed to serialize");
                None
            }
        }
    }))
    .keep_alive(KeepAlive::default())
}

type BlockStream = Pin<Box<dyn Stream<Item = BlockRow> + Send>>;

/// True if `address` holds any role on `action` — the original sender
/// (`from_address`) or a role resolved via `AddressExtractor` (Tier A's
/// kind_schema.toml, or a Tier B `ActionIndexable` impl for kinds that claim
/// one). Matches what `GET .../accounts/{address}/actions?role=to` already
/// finds historically via `action_addresses`, computed live here instead so
/// a filtered stream notifies recipients, not just senders.
fn action_matches_address(
    extractor: &storage::AddressExtractor,
    action: &ActionRow,
    address: &str,
) -> bool {
    action.from_address == address
        || extractor
            .resolve(&action.kind, &action.payload)
            .into_iter()
            .any(|(addr, _)| addr == address)
}

/// Replay-then-live, shared by both routes. Lifted from the gRPC
/// `SubscribeBlocks` with one change: a lagged live receiver ends the stream
/// instead of surfacing an error status — the client's cursor is its recovery.
async fn block_stream(
    state: &AppState,
    chain_id: &str,
    from_height: Option<u64>,
) -> Result<BlockStream, ApiError> {
    let chain = state.chain(chain_id)?;

    // Subscribe before reading the tip, so a block committed between the tip
    // read and the live stream starting is still seen live rather than
    // falling in the gap between replay and live.
    let live_rx = chain.blocks_tx.subscribe();

    // Replay up to the tip as it stood when the stream opened, paged lazily:
    // a resume from genesis is the whole chain, and collecting it first held
    // every row in memory and emitted nothing until the last one landed.
    let tip = match from_height {
        Some(from) => storage::get_cursor(&state.pool, chain_id)
            .await?
            .unwrap_or(from as i64 - 1),
        None => -1,
    };
    // The ceiling is the tip the replay covers, not the last row of some
    // page — a per-page ceiling would reopen the replay/live gap.
    let replay_ceiling = tip;

    let pool = state.pool.clone();
    let replay_chain = chain_id.to_string();
    let start = from_height.map(|f| f as i64).unwrap_or(tip + 1);
    let replay = stream::unfold(start, move |next| {
        let pool = pool.clone();
        let chain_id = replay_chain.clone();
        async move {
            if next > tip {
                return None;
            }
            let page = match storage::get_blocks_in_range(
                &pool,
                &chain_id,
                next,
                tip,
                storage::BLOCK_PAGE,
            )
            .await
            {
                Ok(page) => page,
                Err(err) => {
                    tracing::warn!(%chain_id, %err, "SSE replay read failed; ending stream");
                    return None;
                }
            };
            // Empty page before the tip means the range has holes rather
            // than more rows — stop instead of looping on the same height.
            let last = page.last().map(|b| b.height)?;
            Some((stream::iter(page), last + 1))
        }
    })
    .flatten();

    let lag_chain = chain_id.to_string();
    let live = BroadcastStream::new(live_rx)
        .take_while(move |item| {
            let keep = match item {
                Ok(_) => true,
                Err(BroadcastStreamRecvError::Lagged(skipped)) => {
                    tracing::warn!(chain_id = %lag_chain, skipped, "SSE subscriber lagged; ending stream");
                    false
                }
            };
            async move { keep }
        })
        .filter_map(move |item| async move {
            match item {
                Ok(row) if row.height > replay_ceiling => Some(row),
                _ => None,
            }
        });

    Ok(Box::pin(replay.chain(live)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::{lazy_state, rest_chain};
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt as _;

    /// Live only (no `from_height`) never touches Postgres, so the lazy
    /// pool in `lazy_state` suffices.
    async fn stream(state: &AppState) -> BlockStream {
        block_stream(state, "test-chain", None)
            .await
            .ok()
            .expect("test-chain is registered")
    }

    fn block(height: i64, actions: usize) -> BlockRow {
        BlockRow {
            height,
            hash: format!("hash-{height}"),
            parent_hash: format!("hash-{}", height - 1),
            timestamp: 1_700_000_000 + height,
            proposer: None,
            undecoded_action_count: 0,
            actions: (0..actions as i32)
                .map(|index_in_block| ActionRow {
                    action_hash: format!("a-{height}-{index_in_block}"),
                    block_height: height,
                    index_in_block,
                    kind: "Transfer".into(),
                    from_address: "arx1from".into(),
                    payload: serde_json::json!({}),
                })
                .collect(),
        }
    }

    #[tokio::test]
    async fn blocks_stream_emits_every_live_block_with_height_as_id() {
        let chain = rest_chain(ingestion::NetworkView::default());
        let blocks_tx = chain.blocks_tx.clone();
        let state = lazy_state(vec![chain]);

        let mut events = stream(&state).await;
        blocks_tx.send(block(5, 0)).unwrap();
        blocks_tx.send(block(6, 2)).unwrap();
        assert_eq!(events.next().await.unwrap().height, 5);
        assert_eq!(events.next().await.unwrap().height, 6);
    }

    #[tokio::test]
    async fn actions_stream_skips_empty_blocks() {
        let chain = rest_chain(ingestion::NetworkView::default());
        let blocks_tx = chain.blocks_tx.clone();
        let state = lazy_state(vec![chain]);

        let mut actions = stream(&state).await.flat_map(|b| stream::iter(b.actions));
        blocks_tx.send(block(1, 0)).unwrap();
        blocks_tx.send(block(2, 0)).unwrap();
        blocks_tx.send(block(3, 2)).unwrap();
        let first = actions.next().await.unwrap();
        let second = actions.next().await.unwrap();
        assert_eq!((first.block_height, first.index_in_block), (3, 0));
        assert_eq!((second.block_height, second.index_in_block), (3, 1));
    }

    /// A subscriber that falls behind the broadcast buffer (capacity 4 in
    /// the test chain) is cut off — the stream ends — rather than handed a
    /// silently truncated history. Its `from_height` reconnect is the repair.
    #[tokio::test]
    async fn lagged_subscriber_ends_the_stream() {
        let chain = rest_chain(ingestion::NetworkView::default());
        let blocks_tx = chain.blocks_tx.clone();
        let state = lazy_state(vec![chain]);

        let mut events = stream(&state).await;
        for height in 0..10 {
            blocks_tx.send(block(height, 0)).unwrap();
        }
        assert!(events.next().await.is_none(), "lag must end the stream");
    }

    #[tokio::test]
    async fn unknown_chain_is_404_and_the_route_answers_event_stream() {
        let chain = rest_chain(ingestion::NetworkView::default());
        let blocks_tx = chain.blocks_tx.clone();
        let state = lazy_state(vec![chain]);
        let app = crate::router(state.pool.clone(), state.chains.to_vec(), "0");

        let resp = app
            .clone()
            .oneshot(
                Request::get("/v1/chains/nope/blocks/stream")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), 404);

        let resp = app
            .oneshot(
                Request::get("/v1/chains/test-chain/actions/stream")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        assert_eq!(resp.headers()["content-type"], "text/event-stream");
        blocks_tx.send(block(9, 1)).unwrap();
        // The tail never ends on its own (the router keeps a sender), so
        // read the first data frame rather than the whole body.
        let mut frames = resp.into_body().into_data_stream();
        let frame = frames.next().await.unwrap().unwrap();
        let text = std::str::from_utf8(&frame).unwrap();
        assert!(text.contains("id: 9:0\n"), "{text}");
        assert!(text.contains("\"action_hash\":\"a-9-0\""), "{text}");
        assert!(text.contains("\"block_timestamp\":1700000009"), "{text}");
    }

    /// `?address=` keeps only actions the address holds a role on — the
    /// sender here, since the test chain declares no Tier A roles.
    #[tokio::test]
    async fn actions_stream_address_filter_keeps_only_that_senders_actions() {
        let chain = rest_chain(ingestion::NetworkView::default());
        let blocks_tx = chain.blocks_tx.clone();
        let state = lazy_state(vec![chain]);
        let app = crate::router(state.pool.clone(), state.chains.to_vec(), "0");

        let resp = app
            .oneshot(
                Request::get("/v1/chains/test-chain/actions/stream?address=arx1other")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let mut b = block(4, 2);
        b.actions[1].from_address = "arx1other".into();
        blocks_tx.send(b).unwrap();
        let mut frames = resp.into_body().into_data_stream();
        let frame = frames.next().await.unwrap().unwrap();
        let text = std::str::from_utf8(&frame).unwrap();
        assert!(text.contains("id: 4:1\n"), "{text}");
        assert!(!text.contains("a-4-0"), "{text}");
    }
}
