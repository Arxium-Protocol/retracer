pub mod proto {
    tonic::include_proto!("retracer");
}

use futures::StreamExt;
use proto::get_block_request::By;
use proto::retracer_server::{Retracer, RetracerServer};
use proto::search_response::Result as SearchResult;
use proto::Chain as ProtoChain;
use proto::{
    Action, Block, GetAccountActionsRequest, GetAccountActionsResponse, GetActionRequest,
    GetBlockRequest, GetStatsRequest, GetStatsResponse, GetStatusRequest, GetStatusResponse,
    ListActionsRequest, ListActionsResponse, ListBlocksRequest, ListBlocksResponse,
    ListChainsRequest, ListChainsResponse, ListProposersRequest, ListProposersResponse,
    Proposer, SearchRequest, SearchResponse,
    SubscribeAccountActionsRequest, SubscribeBlocksRequest,
};
use sqlx::PgPool;
use std::pin::Pin;
use std::collections::HashMap;
use std::sync::Arc;
use storage::{ActionRow, AddressExtractor, AddressValidator, BlockRow, BlockSummary};
use tokio::sync::broadcast;
use tokio_stream::Stream;
use tokio_stream::wrappers::BroadcastStream;
use tonic::{Request, Response, Status};

/// Same page-size cap the node's own RPC uses (`xc_storage::MAX_PAGE_SIZE`) —
/// not imported directly since this crate has no reason to depend on
/// `xc_storage`, just matching its value.
const MAX_PAGE_SIZE: u32 = 100;

/// Header selecting which chain an RPC is about. Absent means the default
/// chain — see the `service` comment in `retracer.proto`.
pub const CHAIN_HEADER: &str = "x-chain-id";

/// Everything the service needs that varies per chain.
///
/// `address_validator` is the chain's address-format check (see
/// `storage::AddressValidator`). `None` disables both the input validation on
/// address-keyed RPCs and `Search`'s "is this an account?" branch, which is the
/// correct fallback for a chain that hasn't supplied one — guessing permissively
/// would make `Search` classify every block hash as an address.
pub struct ChainRuntime {
    pub chain_id: String,
    pub display_name: Option<String>,
    pub blocks_topic: String,
    pub sync_protocol: String,
    pub finality_depth: u64,
    pub address_extractor: Arc<AddressExtractor>,
    pub address_validator: Option<AddressValidator>,
    /// Per-chain, not shared: one broadcast channel across all chains would
    /// deliver chain B's blocks to a chain A subscriber, and filtering after
    /// the fact would be a correctness rule that has to be remembered at every
    /// call site instead of one that can't be broken.
    pub blocks_tx: broadcast::Sender<BlockRow>,
    /// Live view of the chain's tip as reported by the node. A watch receiver
    /// rather than a stored column: it changes on the node's cadence, not on
    /// ours, and writing it to Postgres would just add a stale copy.
    pub network_view: tokio::sync::watch::Receiver<ingestion::NetworkView>,
}

/// `chains` must be non-empty; the first entry is the default, serving requests
/// that arrive without an `x-chain-id` header. That's what keeps a single-chain
/// deployment (and every existing consumer) working unchanged.
pub fn server(pool: PgPool, chains: Vec<ChainRuntime>) -> RetracerServer<Service> {
    RetracerServer::new(Service::new(pool, chains))
}

pub struct Service {
    pool: PgPool,
    chains: HashMap<String, ChainRuntime>,
    /// Registration order, so `ListChains` is stable rather than hash-ordered.
    order: Vec<String>,
    default_chain_id: String,
}

impl Service {
    /// See [`server`]; split out so tests can build a `Service` through the
    /// same default-chain rule production uses rather than by hand.
    fn new(pool: PgPool, chains: Vec<ChainRuntime>) -> Self {
        let default_chain_id = chains
            .first()
            .expect("at least one chain must be registered")
            .chain_id
            .clone();
        let order: Vec<String> = chains.iter().map(|c| c.chain_id.clone()).collect();
        let chains = chains.into_iter().map(|c| (c.chain_id.clone(), c)).collect();
        Service { pool, chains, order, default_chain_id }
    }

    /// Resolves the `x-chain-id` header to a registered chain.
    ///
    /// An unknown chain is `not_found` rather than a silent fall back to the
    /// default: quietly answering about a different chain than the one asked
    /// for is the kind of wrong answer a client cannot detect.
    fn chain<T>(&self, request: &Request<T>) -> Result<&ChainRuntime, Status> {
        let requested = match request.metadata().get(CHAIN_HEADER) {
            Some(value) => value
                .to_str()
                .map_err(|_| Status::invalid_argument("x-chain-id must be valid ASCII"))?,
            None => self.default_chain_id.as_str(),
        };
        self.chains.get(requested).ok_or_else(|| {
            Status::not_found(format!(
                "unknown chain {requested:?}; call ListChains for the ones this indexer serves"
            ))
        })
    }
}

impl ChainRuntime {
    /// Rejects a malformed address before it reaches a query. A chain with no
    /// validator configured accepts anything: the lookup simply returns no rows,
    /// which is the same answer a well-formed unknown address gets.
    fn check_address(&self, address: &str) -> Result<(), Status> {
        match &self.address_validator {
            Some(valid) if !valid(address) => {
                Err(Status::invalid_argument("not a valid address for this chain"))
            }
            _ => Ok(()),
        }
    }
}

type ResponseStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

impl From<ActionRow> for Action {
    fn from(row: ActionRow) -> Self {
        Action {
            action_hash: row.action_hash,
            block_height: row.block_height as u64,
            index_in_block: row.index_in_block as u32,
            kind: row.kind,
            from_address: row.from_address,
            payload_json: row.payload.to_string(),
        }
    }
}

impl From<BlockRow> for Block {
    fn from(row: BlockRow) -> Self {
        Block {
            height: row.height as u64,
            hash: row.hash,
            parent_hash: row.parent_hash,
            timestamp: row.timestamp,
            proposer: row.proposer,
            action_count: row.actions.len() as u32,
            actions: row.actions.into_iter().map(Action::from).collect(),
        }
    }
}

/// A summary fills the same `Block` message with an empty action list and a
/// real count. The count is what makes that safe: a consumer asking "how many"
/// gets the right answer from either shape.
impl From<BlockSummary> for Block {
    fn from(row: BlockSummary) -> Self {
        Block {
            height: row.height as u64,
            hash: row.hash,
            parent_hash: row.parent_hash,
            timestamp: row.timestamp,
            proposer: row.proposer,
            actions: Vec::new(),
            action_count: row.action_count as u32,
        }
    }
}

#[tonic::async_trait]
impl Retracer for Service {
    type SubscribeBlocksStream = ResponseStream<Block>;
    type SubscribeAccountActionsStream = ResponseStream<Action>;

    /// The one RPC that isn't scoped to a chain — it's how a client discovers
    /// which values of `x-chain-id` the other RPCs will accept.
    async fn list_chains(
        &self,
        _request: Request<ListChainsRequest>,
    ) -> Result<Response<ListChainsResponse>, Status> {
        Ok(Response::new(ListChainsResponse {
            chains: self
                .order
                .iter()
                .filter_map(|id| self.chains.get(id))
                .map(|c| ProtoChain {
                    chain_id: c.chain_id.clone(),
                    display_name: c.display_name.clone(),
                    blocks_topic: c.blocks_topic.clone(),
                    sync_protocol: c.sync_protocol.clone(),
                    finality_depth: c.finality_depth,
                    is_default: c.chain_id == self.default_chain_id,
                })
                .collect(),
        }))
    }

    async fn get_block(&self, request: Request<GetBlockRequest>) -> Result<Response<Block>, Status> {
        let chain = self.chain(&request)?;
        let by = request
            .into_inner()
            .by
            .ok_or_else(|| Status::invalid_argument("must set either height or hash"))?;

        let row = match by {
            By::Height(height) => storage::get_block_by_height(&self.pool, &chain.chain_id, height as i64).await,
            By::Hash(hash) => storage::get_block_by_hash(&self.pool, &chain.chain_id, &hash).await,
        }
        .map_err(|err| Status::internal(err.to_string()))?;

        row.map(Block::from)
            .map(Response::new)
            .ok_or_else(|| Status::not_found("block not found"))
    }

    async fn get_action(&self, request: Request<GetActionRequest>) -> Result<Response<Action>, Status> {
        let chain = self.chain(&request)?;
        let action_hash = request.into_inner().action_hash;
        storage::get_action_by_hash(&self.pool, &chain.chain_id, &action_hash)
            .await
            .map_err(|err| Status::internal(err.to_string()))?
            .map(Action::from)
            .map(Response::new)
            .ok_or_else(|| Status::not_found("action not found"))
    }

    async fn get_account_actions(
        &self,
        request: Request<GetAccountActionsRequest>,
    ) -> Result<Response<GetAccountActionsResponse>, Status> {
        let chain = self.chain(&request)?;
        let req = request.into_inner();
        chain.check_address(&req.address)?;
        let limit = if req.limit == 0 { MAX_PAGE_SIZE } else { req.limit.min(MAX_PAGE_SIZE) };

        // Both cursor halves or neither — see list_actions for why.
        let before = match (req.before_height, req.before_index) {
            (Some(h), Some(i)) => Some((h as i64, i as i32)),
            (None, None) => None,
            _ => {
                return Err(Status::invalid_argument(
                    "before_height and before_index must be sent together",
                ));
            }
        };

        let rows = storage::get_account_actions(
            &self.pool,
            &chain.chain_id,
            &req.address,
            limit as i64,
            before,
            req.role.as_deref(),
        )
        .await
        .map_err(|err| Status::internal(err.to_string()))?;

        Ok(Response::new(GetAccountActionsResponse {
            actions: rows.into_iter().map(Action::from).collect(),
        }))
    }

    /// Same "try each kind in turn" approach as the node's own `/search` —
    /// see `core/rpc`'s `search` handler.
    async fn search(&self, request: Request<SearchRequest>) -> Result<Response<SearchResponse>, Status> {
        let chain = self.chain(&request)?;
        let q = request.into_inner().query;

        if let Ok(height) = q.parse::<i64>()
            && storage::block_exists_at_height(&self.pool, &chain.chain_id, height)
                .await
                .map_err(|err| Status::internal(err.to_string()))?
        {
            return Ok(Response::new(SearchResponse {
                result: Some(SearchResult::BlockHeight(height as u64)),
            }));
        }

        if chain.address_validator.as_ref().is_some_and(|valid| valid(&q)) {
            return Ok(Response::new(SearchResponse {
                result: Some(SearchResult::AccountAddress(q)),
            }));
        }

        if let Some(height) = storage::block_height_by_hash(&self.pool, &chain.chain_id, &q)
            .await
            .map_err(|err| Status::internal(err.to_string()))?
        {
            return Ok(Response::new(SearchResponse {
                result: Some(SearchResult::BlockHeight(height as u64)),
            }));
        }

        if storage::get_action_by_hash(&self.pool, &chain.chain_id, &q)
            .await
            .map_err(|err| Status::internal(err.to_string()))?
            .is_some()
        {
            return Ok(Response::new(SearchResponse {
                result: Some(SearchResult::ActionHash(q)),
            }));
        }

        Err(Status::not_found("no block, account, or action matches"))
    }

    async fn get_status(
        &self,
        request: Request<GetStatusRequest>,
    ) -> Result<Response<GetStatusResponse>, Status> {
        let chain = self.chain(&request)?;
        let status = storage::get_status(&self.pool, &chain.chain_id)
            .await
            .map_err(|err| Status::internal(err.to_string()))?
            .with_network_tip(chain.network_view.borrow().tip_height);

        Ok(Response::new(GetStatusResponse {
            chain_id: chain.chain_id.clone(),
            indexed_height: status.indexed_height.map(|h| h as u64),
            node_tip_height: status.node_tip_height.map(|h| h as u64),
            blocks_behind: status.blocks_behind.map(|h| h as u64),
            tip_timestamp: status.tip_timestamp,
        }))
    }

    async fn list_blocks(
        &self,
        request: Request<ListBlocksRequest>,
    ) -> Result<Response<ListBlocksResponse>, Status> {
        let chain = self.chain(&request)?;
        let req = request.into_inner();
        let limit = if req.limit == 0 { MAX_PAGE_SIZE } else { req.limit.min(MAX_PAGE_SIZE) };

        let rows = storage::list_blocks(
            &self.pool,
            &chain.chain_id,
            limit as i64,
            req.before.map(|h| h as i64),
        )
        .await
        .map_err(|err| Status::internal(err.to_string()))?;

        Ok(Response::new(ListBlocksResponse {
            blocks: rows.into_iter().map(Block::from).collect(),
        }))
    }

    async fn get_stats(
        &self,
        request: Request<GetStatsRequest>,
    ) -> Result<Response<GetStatsResponse>, Status> {
        let chain = self.chain(&request)?;
        // The tip comes from the cursor rather than MAX(height), so stats and
        // GetStatus can never disagree about how far this indexer has got.
        let (stats, status) = tokio::try_join!(
            storage::get_stats(&self.pool, &chain.chain_id),
            storage::get_status(&self.pool, &chain.chain_id),
        )
        .map_err(|err| Status::internal(err.to_string()))?;

        Ok(Response::new(GetStatsResponse {
            chain_id: chain.chain_id.clone(),
            total_blocks: stats.total_blocks as u64,
            total_actions: stats.total_actions as u64,
            total_accounts: stats.total_accounts as u64,
            actions_24h: stats.actions_24h as u64,
            avg_block_time_secs: stats.avg_block_time_secs,
            tip_height: status.indexed_height.map(|h| h as u64),
        }))
    }

    async fn list_actions(
        &self,
        request: Request<ListActionsRequest>,
    ) -> Result<Response<ListActionsResponse>, Status> {
        let chain = self.chain(&request)?;
        let req = request.into_inner();
        let limit = if req.limit == 0 { MAX_PAGE_SIZE } else { req.limit.min(MAX_PAGE_SIZE) };

        // Both cursor halves or neither. A height without an index would have
        // to guess at the missing half, and either guess silently drops or
        // repeats the actions in the boundary block.
        let before = match (req.before_height, req.before_index) {
            (Some(h), Some(i)) => Some((h as i64, i as i32)),
            (None, None) => None,
            _ => {
                return Err(Status::invalid_argument(
                    "before_height and before_index must be sent together",
                ));
            }
        };

        let rows = storage::list_actions(&self.pool, &chain.chain_id, limit as i64, before)
            .await
            .map_err(|err| Status::internal(err.to_string()))?;

        Ok(Response::new(ListActionsResponse {
            actions: rows.into_iter().map(Action::from).collect(),
        }))
    }

    async fn list_proposers(
        &self,
        request: Request<ListProposersRequest>,
    ) -> Result<Response<ListProposersResponse>, Status> {
        let chain = self.chain(&request)?;
        let rows = storage::list_proposers(&self.pool, &chain.chain_id)
            .await
            .map_err(|err| Status::internal(err.to_string()))?;

        Ok(Response::new(ListProposersResponse {
            proposers: rows
                .into_iter()
                .map(|row| Proposer {
                    address: row.address,
                    blocks_proposed: row.blocks_proposed as u64,
                    first_proposed_height: row.first_proposed_height as u64,
                    last_proposed_height: row.last_proposed_height as u64,
                })
                .collect(),
        }))
    }

    async fn subscribe_blocks(
        &self,
        request: Request<SubscribeBlocksRequest>,
    ) -> Result<Response<Self::SubscribeBlocksStream>, Status> {
        let chain = self.chain(&request)?;
        let from_height = request.into_inner().from_height;

        // Subscribe before reading the tip, so a block committed between the
        // tip read and the live stream starting is still seen live rather
        // than falling in the gap between replay and live.
        let live_rx = chain.blocks_tx.subscribe();

        let mut replay = Vec::new();
        if let Some(from) = from_height {
            let tip = storage::get_cursor(&self.pool, &chain.chain_id)
                .await
                .map_err(|err| Status::internal(err.to_string()))?
                .unwrap_or(from as i64 - 1);
            replay = storage::get_blocks_in_range(&self.pool, &chain.chain_id, from as i64, tip)
                .await
                .map_err(|err| Status::internal(err.to_string()))?;
        }
        let replay_ceiling = replay_ceiling(from_height, replay.last().map(|b| b.height));

        let replay_stream = futures::stream::iter(replay.into_iter().map(|row| Ok(Block::from(row))));
        let live_stream = BroadcastStream::new(live_rx).filter_map(move |item| async move {
            match item {
                Ok(row) if row.height > replay_ceiling => Some(Ok(Block::from(row))),
                Ok(_) => None,
                // A slow subscriber that falls behind the broadcast channel's
                // capacity misses those blocks rather than blocking ingestion
                // for every other subscriber — logged, not surfaced as a
                // stream error (the stream itself is still healthy).
                Err(err) => {
                    tracing::warn!("SubscribeBlocks subscriber lagged: {err}");
                    None
                }
            }
        });
        Ok(Response::new(Box::pin(replay_stream.chain(live_stream))))
    }

    async fn subscribe_account_actions(
        &self,
        request: Request<SubscribeAccountActionsRequest>,
    ) -> Result<Response<Self::SubscribeAccountActionsStream>, Status> {
        let chain = self.chain(&request)?;
        let address = request.into_inner().address;
        chain.check_address(&address)?;

        let address_extractor = chain.address_extractor.clone();
        let stream = BroadcastStream::new(chain.blocks_tx.subscribe())
            .filter_map(|item| async move {
                match item {
                    Ok(row) => Some(row),
                    Err(err) => {
                        tracing::warn!("SubscribeAccountActions subscriber lagged: {err}");
                        None
                    }
                }
            })
            .flat_map(move |row| {
                let actions: Vec<Result<Action, Status>> = row
                    .actions
                    .into_iter()
                    .filter(|action| action_matches_address(&address_extractor, action, &address))
                    .map(|action| Ok(Action::from(action)))
                    .collect();
                futures::stream::iter(actions)
            });
        Ok(Response::new(Box::pin(stream)))
    }
}

/// True if `address` holds any role on `action` — the original sender
/// (`from_address`) or a role resolved via `AddressExtractor` (Tier A's
/// kind_schema.toml, or a Tier B `ActionIndexable` impl for kinds that claim
/// one). Matches what `GetAccountActions(role: "to")` already finds
/// historically via `action_addresses`, computed live here instead so
/// SubscribeAccountActions notifies recipients, not just senders.
fn action_matches_address(address_extractor: &AddressExtractor, action: &ActionRow, address: &str) -> bool {
    action.from_address == address
        || address_extractor
            .resolve(&action.kind, &action.payload)
            .into_iter()
            .any(|(addr, _)| addr == address)
}

/// Heights at or below the returned value were already sent via replay (or,
/// when `from_height` is unset, nothing was replayed and every live block
/// qualifies) — used to filter the live stream so a block is never sent
/// twice across the replay/live handoff.
fn replay_ceiling(from_height: Option<u64>, last_replayed_height: Option<i64>) -> i64 {
    last_replayed_height.unwrap_or_else(|| from_height.map_or(-1, |f| f as i64 - 1))
}

#[cfg(test)]
mod tests {
    use super::{action_matches_address, replay_ceiling, ChainRuntime, Service, CHAIN_HEADER};
    use std::sync::Arc;
    use storage::{ActionRow, AddressExtractor, KindSchema};
    use tonic::Request;

    fn runtime(chain_id: &str) -> ChainRuntime {
        let (blocks_tx, _) = tokio::sync::broadcast::channel(4);
        ChainRuntime {
            chain_id: chain_id.to_string(),
            display_name: None,
            blocks_topic: "t".into(),
            sync_protocol: "/s/1".into(),
            finality_depth: 0,
            address_extractor: Arc::new(AddressExtractor::tier_a_only(KindSchema::empty())),
            address_validator: None,
            blocks_tx,
            network_view: tokio::sync::watch::channel(ingestion::NetworkView::default()).1,
        }
    }

    /// `connect_lazy` builds a pool without touching the network, so chain
    /// routing — which is pure header handling — is testable without Postgres.
    /// It still needs a tokio runtime for the pool's reaper task, hence the
    /// `#[tokio::test]` on the callers.
    fn service(chain_ids: &[&str]) -> Service {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://unused:unused@127.0.0.1:1/unused")
            .expect("lazy pool never connects");
        let chains = chain_ids.iter().map(|id| runtime(id)).collect();
        Service::new(pool, chains)
    }

    fn request_for(chain: Option<&str>) -> Request<()> {
        let mut req = Request::new(());
        if let Some(chain) = chain {
            req.metadata_mut().insert(CHAIN_HEADER, chain.parse().unwrap());
        }
        req
    }

    #[tokio::test]
    async fn missing_chain_header_uses_the_first_registered_chain() {
        let svc = service(&["hub", "spoke-a"]);
        assert_eq!(svc.chain(&request_for(None)).unwrap().chain_id, "hub");
    }

    #[tokio::test]
    async fn chain_header_selects_a_registered_chain() {
        let svc = service(&["hub", "spoke-a"]);
        assert_eq!(svc.chain(&request_for(Some("spoke-a"))).unwrap().chain_id, "spoke-a");
    }

    /// The important one: an unknown chain must be an error, never a quiet
    /// fallback to the default. Answering about a different chain than the one
    /// asked for is a wrong answer the client has no way to detect.
    #[tokio::test]
    async fn unknown_chain_is_not_found_rather_than_the_default() {
        let svc = service(&["hub"]);
        let err = match svc.chain(&request_for(Some("nope"))) {
            Ok(c) => panic!("unknown chain resolved to {:?}", c.chain_id),
            Err(err) => err,
        };
        assert_eq!(err.code(), tonic::Code::NotFound);
        assert!(err.message().contains("ListChains"), "error should point at discovery");
    }

    #[test]
    fn no_from_height_lets_every_live_block_through() {
        assert_eq!(replay_ceiling(None, None), -1);
    }

    #[test]
    fn from_height_with_empty_replay_range_filters_up_to_the_requested_height() {
        // e.g. from_height is above the current tip — nothing to replay yet.
        assert_eq!(replay_ceiling(Some(5), None), 4);
    }

    #[test]
    fn from_height_with_replayed_blocks_filters_up_to_the_last_one_sent() {
        assert_eq!(replay_ceiling(Some(5), Some(10)), 10);
    }

    fn schema_with_transfer_to_role() -> KindSchema {
        let path = std::env::temp_dir()
            .join(format!("retracer_grpc_test_kind_schema_{}.toml", std::process::id()));
        std::fs::write(
            &path,
            r#"
            [[kind]]
            name = "Transfer"
              [[kind.roles]]
              path = "$.to"
              role = "to"
            "#,
        )
        .unwrap();
        let schema = KindSchema::load(&path).unwrap();
        std::fs::remove_file(&path).unwrap();
        schema
    }

    fn transfer_action(from_address: &str, payload: serde_json::Value) -> ActionRow {
        ActionRow {
            action_hash: "hash".to_string(),
            block_height: 1,
            index_in_block: 0,
            kind: "Transfer".to_string(),
            from_address: from_address.to_string(),
            payload,
        }
    }

    #[test]
    fn matches_sender_regardless_of_kind_schema() {
        let action = transfer_action("sender", serde_json::json!({"to": "recipient"}));
        let extractor = AddressExtractor::tier_a_only(KindSchema::empty());
        assert!(action_matches_address(&extractor, &action, "sender"));
    }

    #[test]
    fn matches_recipient_via_kind_schema_resolved_role() {
        let extractor = AddressExtractor::tier_a_only(schema_with_transfer_to_role());
        let action = transfer_action("sender", serde_json::json!({"to": "recipient"}));
        assert!(action_matches_address(&extractor, &action, "recipient"));
        assert!(!action_matches_address(&extractor, &action, "unrelated"));
    }
}
