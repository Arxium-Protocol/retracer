//! Block ingestion: reads a chain's blocks off its node and feeds them, in
//! ascending height order, to the indexer. See [`rpc`] for the transport.
//!
//! This crate used to speak the node's P2P wire (gossipsub + the bincode sync
//! protocol), which tied every build to one exact node revision. That path
//! is gone; the RPC reader needs no Arxium crate at all.

use std::time::{Duration, Instant};

pub mod rpc;

/// The one thing ingestion needs from a block: its height, to order the
/// stream. Everything else about a block's shape is `storage`'s business
/// (`storage::IndexableBlock`) — kept as a separate one-method trait so
/// `ingestion` doesn't take a dependency on `storage`.
pub trait HasHeight {
    fn height(&self) -> u64;
}

/// A `/status` answer from the node is considered current for this long.
const STATUS_FRESHNESS_TIMEOUT: Duration = Duration::from_secs(15);

/// What the node says about itself, refreshed on every status poll.
///
/// The heights come from the node rather than being inferred here. The status
/// timestamp prevents a stalled node's last answer from being mistaken for
/// current visibility.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct NetworkView {
    /// Nodes we currently have a working connection to (0 or 1 over RPC).
    pub active_peer_count: usize,
    /// Of those, the ones that have answered a status request.
    pub status_peer_count: usize,
    /// The node's tip. `None` until it answers.
    pub tip_height: Option<u64>,
    /// Highest height the node holds a finality certificate for. `None` on a
    /// chain that doesn't run finality voting, or before the node answers.
    pub finalized_height: Option<u64>,
    /// Most recent status answer.
    pub last_status_at: Option<Instant>,
}

impl NetworkView {
    pub fn has_fresh_status(&self) -> bool {
        self.active_peer_count > 0
            && self.status_peer_count > 0
            && self.tip_height.is_some()
            && self
                .last_status_at
                .is_some_and(|seen| seen.elapsed() <= STATUS_FRESHNESS_TIMEOUT)
    }
}

/// CoreChain's address format: bech32 with the `arx` human-readable part —
/// the same check `xc_primitives::Address::parse` makes node-side.
pub fn is_corechain_address(candidate: &str) -> bool {
    bech32::decode(candidate).is_ok_and(|(hrp, _)| hrp.as_str() == "arx")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn corechain_address_check() {
        // `arx` + 32 zero bytes, as `Address::from_pubkey_bytes` would emit.
        let hrp = bech32::Hrp::parse("arx").unwrap();
        let ok = bech32::encode::<bech32::Bech32>(hrp, &[0u8; 32]).unwrap();
        assert!(is_corechain_address(&ok));
        assert!(!is_corechain_address("spoke1aaaa"));
        assert!(!is_corechain_address("0xdeadbeef"));
        assert!(!is_corechain_address(&ok.replace("arx1", "btc1")));
    }

    #[test]
    fn network_view_freshness() {
        assert!(!NetworkView::default().has_fresh_status());
        let fresh = NetworkView {
            active_peer_count: 1,
            status_peer_count: 1,
            tip_height: Some(3),
            finalized_height: None,
            last_status_at: Some(Instant::now()),
        };
        assert!(fresh.has_fresh_status());
        let stale = NetworkView {
            last_status_at: Some(Instant::now() - Duration::from_secs(60)),
            ..fresh
        };
        assert!(!stale.has_fresh_status());
    }
}
