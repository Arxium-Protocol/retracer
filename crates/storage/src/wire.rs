//! The adapter between a chain's own block/action types and what indexing
//! actually needs.
//!
//! `P`-genericity (see `ingestion`) only ever covered the action *payload* —
//! the envelope around it was still Arxium's: `xc_primitives::Block` for the
//! block shape, its `sha256(bincode(..))` hashing, and `xc_primitives::Address`
//! for every address. A chain that hashes differently would have had correct
//! rows written under silently wrong hashes, which is worse than a hard error.
//!
//! These two traits are everything `storage` reads off a block. A chain built
//! on `xc-primitives` gets the impls for free (below, behind the default
//! `xc-primitives` feature); one that isn't implements them for its own types
//! and never links Arxium code at all.

use anyhow::Result;

/// What indexing needs from a block. Deliberately all owned `String`s for
/// addresses and hashes: whether an address is bech32, hex, or something else
/// is the chain's business, and by the time a row is written it is text either
/// way.
pub trait IndexableBlock {
    type Action: IndexableAction;

    fn height(&self) -> u64;
    /// The chain's own content hash, in whatever scheme it uses. Retracer
    /// stores this verbatim rather than computing a hash itself — recomputing
    /// would bake one chain's hashing into every chain's rows.
    fn hash(&self) -> String;
    fn parent_hash(&self) -> String;
    fn timestamp(&self) -> u64;
    /// `None` for an unsigned/genesis block, or one from a non-validator node.
    /// A real absence, not a gap — `list_proposers` excludes these rather than
    /// inventing a validator called "unknown".
    fn proposer(&self) -> Option<String>;
    fn actions(&self) -> &[Self::Action];
}

pub trait IndexableAction {
    fn sender(&self) -> String;

    /// Stable, unique identity for this action — the primary key actions are
    /// stored and deduplicated under.
    ///
    /// `None` means the chain has no intrinsic id for this action, and
    /// `insert_block` falls back to its `height:index` position. That fallback
    /// is the whole reason this returns `Option` rather than `String`: the
    /// previous code used an action's *signature* as its identity, so two
    /// unsigned actions in one block both keyed on `""` and the second was
    /// silently dropped by `ON CONFLICT DO NOTHING`. Fine for CoreChain, where
    /// everything admitted is signed, but silent data loss for any chain that
    /// permits unsigned or system-injected actions.
    ///
    /// Whatever this returns must be stable across re-delivery of the same
    /// block, since that is what makes ingestion idempotent.
    fn identity(&self) -> Option<String>;

    /// The payload as JSON. `storage` splits `(kind, payload)` out of the shape
    /// of this value (`split_kind`) and address extraction reads it untyped, so
    /// nothing downstream needs the payload's Rust type.
    fn payload_json(&self) -> Result<serde_json::Value>;
}

/// Blanket impls for chains built on `xc-primitives`, which is every Arxium
/// chain today. Behind a default-on feature so a builder who is *not* on
/// `xc-primitives` can switch it off and compile `storage` without the Arxium
/// path dependency at all.
#[cfg(feature = "xc-primitives")]
mod xc_impls {
    use super::{IndexableAction, IndexableBlock};
    use anyhow::Result;
    use serde::Serialize;
    use xc_primitives::{Action, Block};

    impl<P: Serialize> IndexableBlock for Block<P> {
        type Action = Action<P>;

        fn height(&self) -> u64 {
            self.height
        }
        fn hash(&self) -> String {
            Block::hash(self)
        }
        fn parent_hash(&self) -> String {
            self.parent_hash.clone()
        }
        fn timestamp(&self) -> u64 {
            self.timestamp
        }
        fn proposer(&self) -> Option<String> {
            self.proposer.as_ref().map(|p| p.to_string())
        }
        fn actions(&self) -> &[Self::Action] {
            &self.actions
        }
    }

    impl<P: Serialize> IndexableAction for Action<P> {
        fn sender(&self) -> String {
            self.sender.to_string()
        }

        /// An Arxium action's signature is its identity: it is unique per
        /// action (it covers sender + nonce + payload) and stable across
        /// re-delivery. Empty is treated as absent rather than as an id — an
        /// empty string is not a signature, and letting it through would
        /// recreate the collision `identity`'s docs describe.
        fn identity(&self) -> Option<String> {
            self.signature.clone().filter(|s| !s.is_empty())
        }

        fn payload_json(&self) -> Result<serde_json::Value> {
            Ok(serde_json::to_value(&self.payload)?)
        }
    }
}
