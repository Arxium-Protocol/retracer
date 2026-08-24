mod corechain_payload;

pub use corechain_payload::{ActionPayload, is_corechain_address};

use anyhow::{Context, Result};
use libp2p::futures::StreamExt;
use libp2p::request_response::{self, ProtocolSupport, cbor};
use libp2p::swarm::dial_opts::DialOpts;
use libp2p::swarm::{ConnectionId, NetworkBehaviour, SwarmEvent};
use libp2p::{Multiaddr, PeerId, StreamProtocol, gossipsub, noise, tcp, yamux};
use std::collections::{BTreeMap, HashMap};
use std::time::Duration;
use tokio::sync::mpsc::{Sender, UnboundedSender};
use tracing::{info, warn};
use xc_primitives::Block;

/// The only thing this crate needs to know about a block: where it sits in the
/// sequence, so gossip and sync backfill can be merged into one ascending
/// stream. Everything else about a block's shape is `storage`'s business
/// (`storage::IndexableBlock`) — kept as a separate one-method trait so
/// `ingestion` doesn't take a dependency on `storage`, and so a chain can swap
/// its transport and its schema adapter independently.
pub trait HasHeight {
    fn height(&self) -> u64;
}

impl<P> HasHeight for Block<P> {
    fn height(&self) -> u64 {
        self.height
    }
}

/// How often to re-ask peers for their tip height.
///
/// Matches `arxd/network`'s own `STATUS_INTERVAL`. The node polls its peers on
/// this cadence; we poll it on the same one, so the tip we report is never more
/// than one interval stale.
const STATUS_POLL_INTERVAL: Duration = Duration::from_secs(5);

const RECONNECT_INITIAL_BACKOFF: Duration = Duration::from_secs(1);
const RECONNECT_MAX_BACKOFF: Duration = Duration::from_secs(30);

/// Default cap on blocks buffered ahead of `next_expected` while a gap-fill sync
/// request is in flight. Without one, a stalled or lying peer that never
/// answers `Blocks { from }` lets `pending` grow for as long as gossip
/// keeps producing new blocks -- an unbounded memory leak on a live
/// devnet. 4096 is generous headroom over any gap a healthy peer should
/// take more than a few sync round-trips to close.
pub const DEFAULT_MAX_PENDING_BLOCKS: usize = 4096;

/// Must match `arxd/network`'s `BLOCKS_TOPIC` exactly — this is the same
/// gossip stream a validator subscribes to, not a Retracer-specific
/// protocol. A Spoke Chain gossiping on a different topic overrides it via
/// `Config::blocks_topic` (`--blocks-topic`).
pub const DEFAULT_BLOCKS_TOPIC: &str = "arxium/blocks/v1";
/// The node's sync protocol name, taken from `xc-wire` rather than repeated
/// here — the shapes it carries come from the same crate, so protocol name and
/// message layout can no longer drift apart. Overridable via
/// `Config::sync_protocol` (`--sync-protocol`) for a chain that renames it.
pub const DEFAULT_SYNC_PROTOCOL: &str = xc_wire::SYNC_PROTOCOL;

/// Everything `run` needs that a deployment might legitimately vary. These are
/// *not* derived from `--chain-id`: the topic and protocol names are a wire
/// agreement with the node being followed, so they have to match whatever that
/// node publishes, not whatever label this indexer files its rows under.
pub struct Config {
    pub bootnodes: Vec<Multiaddr>,
    pub listen_port: u16,
    /// Height to resume ingestion from; `None` to start from the first block seen.
    pub resume_from: Option<u64>,
    pub blocks_topic: String,
    pub sync_protocol: String,
    pub max_pending_blocks: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            bootnodes: Vec::new(),
            listen_port: 0,
            resume_from: None,
            blocks_topic: DEFAULT_BLOCKS_TOPIC.to_string(),
            sync_protocol: DEFAULT_SYNC_PROTOCOL.to_string(),
            max_pending_blocks: DEFAULT_MAX_PENDING_BLOCKS,
        }
    }
}

use xc_wire::{SyncRequest, SyncResponse};

/// What the network says about itself, refreshed on every status poll.
///
/// Both fields come from the node rather than being inferred here, which is the
/// point: `tip_height` makes sync lag reportable, and `finalized_height` turns
/// reorg safety from a configured guess into the chain's own answer.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct NetworkView {
    /// Highest tip any connected peer reports. `None` until one answers.
    pub tip_height: Option<u64>,
    /// Highest height a peer holds a finality certificate for. `None` on a
    /// chain that doesn't run finality voting, or before any peer answers.
    pub finalized_height: Option<u64>,
}

#[derive(NetworkBehaviour)]
#[behaviour(prelude = "libp2p::swarm::derive_prelude")]
struct Behaviour {
    gossipsub: gossipsub::Behaviour,
    sync: cbor::Behaviour<Vec<u8>, Vec<u8>>,
}

/// Dials `addr` and records its `ConnectionId` so the eventual
/// `ConnectionEstablished`/`OutgoingConnectionError` can be traced back to it.
fn dial_bootnode(
    swarm: &mut libp2p::Swarm<Behaviour>,
    addr: Multiaddr,
    pending_dials: &mut HashMap<ConnectionId, Multiaddr>,
) {
    let opts = DialOpts::from(addr.clone());
    let connection_id = opts.connection_id();
    if let Err(err) = swarm.dial(opts) {
        warn!("failed to dial bootnode {addr}: {err}");
        return;
    }
    pending_dials.insert(connection_id, addr);
}

/// Queues a redial of `addr` after an exponentially increasing delay
/// (matching the retry pattern ArxPlusApi's gRPC indexer client already
/// uses: start at `RECONNECT_INITIAL_BACKOFF`, double each consecutive
/// failure, cap at `RECONNECT_MAX_BACKOFF`; reset on the next successful
/// connect via `backoffs.remove` in the `ConnectionEstablished` handler).
fn schedule_redial(redial_tx: &UnboundedSender<Multiaddr>, addr: Multiaddr, backoffs: &mut HashMap<Multiaddr, Duration>) {
    let delay = backoffs.get(&addr).copied().unwrap_or(RECONNECT_INITIAL_BACKOFF);
    backoffs.insert(addr.clone(), (delay * 2).min(RECONNECT_MAX_BACKOFF));

    let redial_tx = redial_tx.clone();
    tokio::spawn(async move {
        tokio::time::sleep(delay).await;
        let _ = redial_tx.send(addr);
    });
}

fn send_sync_request(swarm: &mut libp2p::Swarm<Behaviour>, peer: &PeerId, request: &SyncRequest) {
    match bincode::serde::encode_to_vec(request, bincode::config::standard()) {
        Ok(bytes) => {
            swarm.behaviour_mut().sync.send_request(peer, bytes);
        }
        Err(err) => warn!("failed to encode sync request: {err}"),
    }
}

/// Buffers `block` in `pending`, then forwards everything from `pending`
/// that's now contiguous starting at `next_expected` (which may be just
/// `block` itself, or `block` plus older blocks a sync response already
/// queued). Only a genuinely stale, already-forwarded block is dropped
/// outright.
///
/// Buffering (rather than forwarding out-of-order blocks immediately) is
/// what makes gap-filling safe: a gossiped block that arrives ahead of
/// `next_expected` must not advance `next_expected` past the gap, or the
/// sync response sent to fill that gap arrives later and finds its own
/// blocks already "in the past" and silently drops them.
async fn insert_and_drain<B: HasHeight>(
    block: B,
    next_expected: &mut Option<u64>,
    pending: &mut BTreeMap<u64, B>,
    block_tx: &Sender<B>,
    max_pending_blocks: usize,
) -> bool {
    if next_expected.is_some_and(|expected| block.height() < expected) {
        return true;
    }
    let mut expected = next_expected.unwrap_or(block.height());

    // Only a block that arrives ahead of `expected` actually accumulates in
    // `pending` -- one that matches `expected` drains immediately below. Cap
    // just that case, so a stalled/lying gap-fill peer can't grow this
    // forever while gossip keeps producing blocks past the gap.
    if block.height() != expected
        && !pending.contains_key(&block.height())
        && pending.len() >= max_pending_blocks
    {
        warn!(
            "gap-fill buffer full ({max_pending_blocks} blocks buffered, stuck at height \
             {expected}); dropping block {}",
            block.height()
        );
        return true;
    }
    pending.insert(block.height(), block);

    while let Some(next_block) = pending.remove(&expected) {
        // Awaits when the bounded channel is full — backpressure onto this
        // p2p receive loop rather than buffering ingested blocks unbounded.
        if block_tx.send(next_block).await.is_err() {
            return false;
        }
        expected += 1;
    }
    *next_expected = Some(expected);
    true
}

/// Connects to the Arxium network as a plain libp2p peer (no privileged
/// access — the same gossipsub subscription any light client could make),
/// dials the given bootnodes, and pushes blocks onto `block_tx` in ascending
/// height order. Two delivery paths feed it, same as a real validator:
/// gossip for new blocks, and the `/arxium/sync/1` request/response protocol
/// to fetch `resume_from..` on startup and to backfill any gap a dropped
/// gossip message leaves behind. Runs until the swarm errors, the process is
/// killed, or the receiver is dropped.
///
/// `B` is the chain's whole block type, not just its payload. bincode is not
/// self-describing, so unlike the post-decode address extraction (which works on
/// untyped `serde_json::Value` and needs no Rust type at all), the wire decode
/// needs the exact sender-side layout at compile time. A chain on `xc-primitives`
/// passes `Block<TheirPayload>`; one with its own block envelope passes that
/// instead, so long as it decodes from the same bincode framing.
/// `Block<ActionPayload>` is just the CoreChain instantiation `retracerd`
/// happens to pass.
/// `rewind_rx` is the return path for reorg handling: the consumer un-indexes
/// back to some height and sends it here, and this loop resets its own notion
/// of the next expected height to match and re-requests from peers. Without it
/// the two sides would disagree after a rollback — this loop would still think
/// the re-delivered blocks were already forwarded and drop them as stale, and
/// ingestion would wedge permanently.
/// `network_tip_tx` publishes the highest tip height any connected peer has
/// reported. That is what makes real sync lag reportable: the sync protocol has
/// always carried the node's tip in `SyncResponse::Status`, but it was only ever
/// asked for once, on a cold start, and used for pagination. Polling it and
/// publishing it turns "how far behind are we?" from unanswerable into a
/// subtraction.
pub async fn run<B>(
    config: Config,
    block_tx: Sender<B>,
    mut rewind_rx: tokio::sync::mpsc::Receiver<u64>,
    network_tx: tokio::sync::watch::Sender<NetworkView>,
) -> Result<()>
where
    B: HasHeight + serde::de::DeserializeOwned + Send + 'static,
{
    let Config { bootnodes, listen_port, resume_from, blocks_topic, sync_protocol, max_pending_blocks } =
        config;
    let keypair = libp2p::identity::Keypair::generate_ed25519();
    let peer_id = PeerId::from(keypair.public());
    info!("retracer p2p identity: {peer_id}");

    let mut swarm = libp2p::SwarmBuilder::with_existing_identity(keypair)
        .with_tokio()
        .with_tcp(tcp::Config::default(), noise::Config::new, yamux::Config::default)?
        .with_quic()
        // Lets a bootnode be given as `/dns4/host/tcp/30334/p2p/...` and not
        // only as a literal `/ip4/`. Without it such an address is not an
        // error at parse time — Multiaddr accepts it happily — it simply has
        // no transport that can dial it, and the indexer sits there with no
        // peers and nothing in the log to say why.
        //
        // What it buys: addressing the node by name. Across a Docker network
        // the container's IP changes on every recreate, and on a host the
        // literal IP is the one thing guaranteed to be wrong after a move.
        // Resolution happens per dial, so a redial after the node restarts
        // finds it wherever it now is.
        .with_dns()?
        .with_behaviour(|keypair| -> Result<Behaviour, _> {
            let gossipsub = gossipsub::Behaviour::new(
                gossipsub::MessageAuthenticity::Signed(keypair.clone()),
                gossipsub::Config::default(),
            )
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { e.into() })?;
            let sync = cbor::Behaviour::new(
                [(
                    StreamProtocol::try_from_owned(sync_protocol)
                        .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { e.into() })?,
                    ProtocolSupport::Outbound,
                )],
                request_response::Config::default(),
            );
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(Behaviour { gossipsub, sync })
        })?
        .build();

    let blocks_topic = gossipsub::IdentTopic::new(&blocks_topic);
    swarm.behaviour_mut().gossipsub.subscribe(&blocks_topic)?;

    swarm
        .listen_on(format!("/ip4/0.0.0.0/tcp/{listen_port}").parse()?)
        .context("failed to start p2p listener")?;

    // Bootnode reconnect state: a dial's `ConnectionId` is known before the
    // peer's identity is, so pending dials are tracked by id and promoted to
    // `bootnode_peers` once `ConnectionEstablished` reports the peer. Losing
    // that connection later (`ConnectionClosed`) redials the same address
    // with exponential backoff, reset on the next successful connection.
    let mut pending_dials: HashMap<ConnectionId, Multiaddr> = HashMap::new();
    let mut bootnode_peers: HashMap<PeerId, Multiaddr> = HashMap::new();
    let mut backoffs: HashMap<Multiaddr, Duration> = HashMap::new();
    let (redial_tx, mut redial_rx) = tokio::sync::mpsc::unbounded_channel::<Multiaddr>();

    for addr in &bootnodes {
        dial_bootnode(&mut swarm, addr.clone(), &mut pending_dials);
    }

    // The next height we expect to forward. `None` until either a resume
    // cursor is known or the first block (gossip or sync) tells us.
    let mut next_expected = resume_from;
    // Each connected peer's last-reported tip, so a `Blocks` response knows
    // whether to ask for another page (mirrors `arxd/network`).
    let mut peer_tips: HashMap<PeerId, u64> = HashMap::new();
    let mut finalized_by_peer: HashMap<PeerId, Option<u64>> = HashMap::new();
    // Blocks that arrived ahead of `next_expected` (a gossiped block while a
    // gap-filling sync request is still in flight) — held here instead of
    // being forwarded immediately, so the *earlier* blocks a sync response
    // brings back can't be mistaken for "already forwarded" and dropped.
    // Bounded by `max_pending_blocks` in insert_and_drain — a peer that never
    // answers the gap request drops new arrivals past the cap instead of
    // growing this forever.
    let mut pending: BTreeMap<u64, B> = BTreeMap::new();

    let mut status_poll = tokio::time::interval(STATUS_POLL_INTERVAL);
    // The first tick fires immediately; skipping the delay-then-burst behaviour
    // keeps a restart from firing a status request before any peer is connected.
    status_poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        let event = tokio::select! {
            event = swarm.select_next_some() => event,
            _ = status_poll.tick() => {
                let peers: Vec<PeerId> = swarm.connected_peers().copied().collect();
                for peer in peers {
                    // Status is the older, universally-supported request and
                    // keeps working against a pre-NodeInfo node; NodeInfo adds
                    // finality on top. A node too old to know NodeInfo simply
                    // fails to decode it and answers nothing, which costs a
                    // warning on its side and leaves finality reported as
                    // absent here — degraded, not broken.
                    send_sync_request(&mut swarm, &peer, &SyncRequest::Status);
                    send_sync_request(&mut swarm, &peer, &SyncRequest::NodeInfo);
                }
                continue;
            }
            Some(addr) = redial_rx.recv() => {
                dial_bootnode(&mut swarm, addr, &mut pending_dials);
                continue;
            }
            Some(from) = rewind_rx.recv() => {
                // The consumer rolled the index back; rewind to match. Buffered
                // blocks are discarded rather than kept: they were built on the
                // branch we just abandoned, and replaying them would hand back
                // exactly the blocks that were just deleted.
                warn!(from, dropped_pending = pending.len(), "rewinding after rollback");
                next_expected = Some(from);
                pending.clear();
                let peers: Vec<PeerId> = swarm.connected_peers().copied().collect();
                for peer in peers {
                    send_sync_request(&mut swarm, &peer, &SyncRequest::Blocks { from });
                }
                continue;
            }
        };
        match event {
            SwarmEvent::NewListenAddr { address, .. } => {
                info!("p2p listening on {address}");
            }
            SwarmEvent::ConnectionEstablished { peer_id, connection_id, .. } => {
                info!("connected to peer {peer_id}");
                if let Some(addr) = pending_dials.remove(&connection_id) {
                    backoffs.remove(&addr);
                    bootnode_peers.insert(peer_id, addr);
                }
                if let Some(from) = next_expected {
                    send_sync_request(&mut swarm, &peer_id, &SyncRequest::Blocks { from });
                } else {
                    send_sync_request(&mut swarm, &peer_id, &SyncRequest::Status);
                }
            }
            SwarmEvent::OutgoingConnectionError { connection_id, error, .. } => {
                if let Some(addr) = pending_dials.remove(&connection_id) {
                    warn!("failed to dial bootnode {addr}: {error}");
                    schedule_redial(&redial_tx, addr, &mut backoffs);
                }
            }
            SwarmEvent::ConnectionClosed { peer_id, cause, .. } => {
                if let Some(addr) = bootnode_peers.remove(&peer_id) {
                    warn!("lost connection to bootnode {addr} (peer {peer_id}): {cause:?}");
                    schedule_redial(&redial_tx, addr, &mut backoffs);
                }
            }
            SwarmEvent::Behaviour(BehaviourEvent::Gossipsub(gossipsub::Event::Message {
                propagation_source,
                message,
                ..
            })) if message.topic == blocks_topic.hash() => {
                match bincode::serde::decode_from_slice::<B, _>(
                    &message.data,
                    bincode::config::standard(),
                ) {
                    Ok((block, _)) => {
                        // A gap here means a gossiped block was dropped
                        // somewhere between it and us — ask the same peer to
                        // fill in what we're missing. The gossiped block
                        // itself is buffered, not forwarded yet: it only
                        // becomes safe to forward once the gap-filling sync
                        // response (below) has been applied, otherwise
                        // `next_expected` would advance past the gap and the
                        // sync response's own blocks would be mistaken for
                        // already-seen and dropped.
                        if let Some(expected) = next_expected
                            && block.height() > expected
                        {
                            send_sync_request(
                                &mut swarm,
                                &propagation_source,
                                &SyncRequest::Blocks { from: expected },
                            );
                        }
                        if !insert_and_drain(block, &mut next_expected, &mut pending, &block_tx, max_pending_blocks).await {
                            return Ok(());
                        }
                    }
                    Err(err) => {
                        warn!("undecodable gossiped block from {propagation_source}: {err}");
                    }
                }
            }
            SwarmEvent::Behaviour(BehaviourEvent::Sync(request_response::Event::Message {
                peer,
                message: request_response::Message::Response { response, .. },
                ..
            })) => {
                let sync_response: SyncResponse<B> = match bincode::serde::decode_from_slice(
                    &response,
                    bincode::config::standard(),
                ) {
                    Ok((resp, _)) => resp,
                    Err(err) => {
                        warn!("failed to decode sync response from {peer}: {err}");
                        continue;
                    }
                };
                match sync_response {
                    SyncResponse::Status { tip_height } => {
                        peer_tips.insert(peer, tip_height);
                        // Highest tip any peer claims. Max rather than the most
                        // recent responder: a peer that is itself behind must
                        // not make the network look like it stopped moving.
                        let network_tip = peer_tips.values().copied().max();
                        network_tx.send_if_modified(|view| {
                            if view.tip_height != network_tip {
                                view.tip_height = network_tip;
                                true
                            } else {
                                false
                            }
                        });
                        let from = next_expected.unwrap_or(1);
                        if tip_height >= from {
                            send_sync_request(&mut swarm, &peer, &SyncRequest::Blocks { from });
                        }
                    }
                    SyncResponse::NodeInfo(info) => {
                        if info.wire_version != xc_wire::WIRE_VERSION {
                            warn!(
                                peer = %peer,
                                theirs = info.wire_version,
                                ours = xc_wire::WIRE_VERSION,
                                "sync wire version mismatch; newer fields may be missing"
                            );
                        }
                        peer_tips.insert(peer, info.tip_height);
                        // Max across peers, same reasoning as the tip: one peer
                        // lagging on finality must not drag the network's
                        // reported finalized height backwards.
                        finalized_by_peer.insert(peer, info.finalized_height);
                        let finalized = finalized_by_peer.values().copied().flatten().max();
                        let tip = peer_tips.values().copied().max();
                        network_tx.send_if_modified(|view| {
                            let next = NetworkView { tip_height: tip, finalized_height: finalized };
                            if *view != next {
                                *view = next;
                                true
                            } else {
                                false
                            }
                        });
                    }
                    SyncResponse::Hashes(hashes) => {
                        // Requested only by fork resolution, which this crate
                        // doesn't drive yet — the consumer peels heights one at
                        // a time. Logged rather than silently dropped so the
                        // capability is visibly unused rather than forgotten.
                        info!(peer = %peer, count = hashes.len(), "received block hashes");
                    }
                    SyncResponse::Blocks(mut blocks) => {
                        blocks.sort_by_key(|b| b.height());
                        for block in blocks {
                            if !insert_and_drain(block, &mut next_expected, &mut pending, &block_tx, max_pending_blocks).await {
                                return Ok(());
                            }
                        }
                        // Server-side responses are capped at a page size
                        // (see `arxd/network`), so keep paging until we've
                        // caught up to this peer's last-known tip.
                        let from = next_expected.unwrap_or(1);
                        if peer_tips.get(&peer).is_some_and(|&tip| tip >= from) {
                            send_sync_request(&mut swarm, &peer, &SyncRequest::Blocks { from });
                        }
                    }
                }
            }
            SwarmEvent::Behaviour(BehaviourEvent::Sync(request_response::Event::OutboundFailure {
                peer,
                error,
                ..
            })) => {
                warn!("sync request to {peer} failed: {error}");
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc::channel;

    const CAP: usize = 8;

    fn block(height: u64) -> Block<ActionPayload> {
        Block { height, parent_hash: String::new(), timestamp: 0, actions: vec![], proposer: None, signature: None }
    }

    /// Reproduces the gap-fill race this fix closes: a gossiped block
    /// arrives ahead of `next_expected` (triggering a sync request for the
    /// gap), and the sync response filling that gap arrives *after*. Before
    /// this fix, the gossiped block advanced `next_expected` immediately, so
    /// the sync response's own blocks looked "already forwarded" and were
    /// silently dropped. All three blocks must now come out, in order.
    #[tokio::test]
    async fn gap_fill_backfill_is_not_dropped_by_a_later_gossip_block() {
        let (tx, mut rx) = channel(300);
        let mut next_expected = Some(1);
        let mut pending = BTreeMap::new();

        // Gossip delivers height 3 while 1 and 2 are still missing.
        assert!(insert_and_drain(block(3), &mut next_expected, &mut pending, &tx, CAP).await);
        assert_eq!(next_expected, Some(1), "must not advance past the gap");

        // The sync response backfilling the gap arrives afterward.
        assert!(insert_and_drain(block(1), &mut next_expected, &mut pending, &tx, CAP).await);
        assert!(insert_and_drain(block(2), &mut next_expected, &mut pending, &tx, CAP).await);
        assert_eq!(next_expected, Some(4), "draining the gap should also release the buffered block 3");

        let mut heights = vec![];
        while let Ok(b) = rx.try_recv() {
            heights.push(b.height);
        }
        assert_eq!(heights, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn stale_block_is_dropped_not_rebuffered() {
        let (tx, mut rx) = channel(300);
        let mut next_expected = Some(5);
        let mut pending = BTreeMap::new();

        assert!(insert_and_drain(block(3), &mut next_expected, &mut pending, &tx, CAP).await);

        assert!(rx.try_recv().is_err(), "a block below next_expected must not be forwarded");
        assert!(pending.is_empty(), "a stale block must not be buffered either");
    }

    /// A stalled/lying gap-fill peer means `expected` never arrives; without
    /// a cap, every later gossiped block would sit in `pending` forever.
    #[tokio::test]
    async fn pending_buffer_is_capped_when_gap_is_never_filled() {
        let (tx, _rx) = channel(300);
        let mut next_expected = Some(1);
        let mut pending = BTreeMap::new();

        for height in 2..2 + CAP as u64 + 10 {
            assert!(insert_and_drain(block(height), &mut next_expected, &mut pending, &tx, CAP).await);
        }

        assert_eq!(pending.len(), CAP, "buffer must not grow past the cap");
        assert_eq!(next_expected, Some(1), "still stuck waiting on the gap");
    }

    /// The point of making `run` generic: a Spoke Chain with a payload enum
    /// that looks nothing like CoreChain's must ingest without forking this
    /// crate. Nothing here is asserted at runtime — instantiating `run` over
    /// `SpokePayload` is the check, and it fails at compile time if the wire
    /// decode ever goes back to hardcoding `ActionPayload`.
    #[test]
    fn run_is_generic_over_the_payload_type() {
        #[derive(serde::Serialize, serde::Deserialize)]
        enum SpokePayload {
            MintNft { token_id: u64 },
        }

        let _ = |cfg: Config, tx: Sender<Block<SpokePayload>>, rw, nv| run(cfg, tx, rw, nv);
        let _ = SpokePayload::MintNft { token_id: 1 };
    }

    /// The block *envelope* is generic too, not just the payload inside it: a
    /// chain that doesn't build on `xc-primitives` at all supplies its own block
    /// type and only has to say where it sits in the sequence. Again a
    /// compile-time check — instantiating `run` over a type with no relation to
    /// `xc_primitives::Block` is the assertion.
    #[test]
    fn run_is_generic_over_the_block_envelope() {
        #[derive(serde::Deserialize)]
        struct ForeignBlock {
            slot: u64,
        }
        impl HasHeight for ForeignBlock {
            fn height(&self) -> u64 {
                self.slot
            }
        }

        let _ = |cfg: Config, tx: Sender<ForeignBlock>, rw, nv| run(cfg, tx, rw, nv);
    }
}
