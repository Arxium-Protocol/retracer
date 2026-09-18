//! The adapter between a chain's own block/action types and what indexing
//! actually needs.
//!
//! `P`-genericity (see `ingestion`) only ever covered the action *payload* —
//! the envelope around it was still Arxium's: `xc_primitives::Block` for the
//! block shape, its `sha256(bincode(..))` hashing, and `xc_primitives::Address`
//! for every address. A chain that hashes differently would have had correct
//! rows written under silently wrong hashes, which is worse than a hard error.
//!
//! These two traits are everything `storage` reads off a block. `retracer-core`
//! implements them for the block JSON an Arxium node serves over RPC
//! (`rpc_block::RpcBlock`); a chain with a different envelope implements them
//! for its own types and never links Arxium code at all.

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

    /// Consensus round the block was produced in; `0` for chains without
    /// rounds. See `migrations/0002_block_round.sql`.
    fn round(&self) -> u32 {
        0
    }
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

/// A minimal block/action pair for tests: deterministic hash from its fields,
/// addresses and payloads as plain values. Public so every crate's tests can
/// build chains without a real node's types.
#[doc(hidden)]
pub mod testing {
    use super::{IndexableAction, IndexableBlock};
    use anyhow::Result;
    use std::hash::{DefaultHasher, Hash, Hasher};

    #[derive(Clone, Debug)]
    pub struct TestBlock {
        pub height: u64,
        pub parent_hash: String,
        pub timestamp: u64,
        pub proposer: Option<String>,
        pub round: u32,
        pub actions: Vec<TestAction>,
    }

    #[derive(Clone, Debug)]
    pub struct TestAction {
        pub sender: String,
        pub signature: Option<String>,
        pub payload: serde_json::Value,
    }

    impl TestBlock {
        /// `sha256(bincode(block))` stand-in: differs whenever any indexed
        /// field differs, so two forks at one height get two hashes.
        pub fn hash(&self) -> String {
            let mut h = DefaultHasher::new();
            self.height.hash(&mut h);
            self.parent_hash.hash(&mut h);
            self.timestamp.hash(&mut h);
            self.proposer.hash(&mut h);
            self.round.hash(&mut h);
            for a in &self.actions {
                a.sender.hash(&mut h);
                a.signature.hash(&mut h);
                a.payload.to_string().hash(&mut h);
            }
            format!("0x{:016x}", h.finish())
        }
    }

    impl IndexableBlock for TestBlock {
        type Action = TestAction;
        fn height(&self) -> u64 {
            self.height
        }
        fn hash(&self) -> String {
            TestBlock::hash(self)
        }
        fn parent_hash(&self) -> String {
            self.parent_hash.clone()
        }
        fn timestamp(&self) -> u64 {
            self.timestamp
        }
        fn proposer(&self) -> Option<String> {
            self.proposer.clone()
        }
        fn actions(&self) -> &[TestAction] {
            &self.actions
        }
        fn round(&self) -> u32 {
            self.round
        }
    }

    impl IndexableAction for TestAction {
        fn sender(&self) -> String {
            self.sender.clone()
        }
        /// Empty is absent, same rule as a real signature.
        fn identity(&self) -> Option<String> {
            self.signature.clone().filter(|s| !s.is_empty())
        }
        fn payload_json(&self) -> Result<serde_json::Value> {
            Ok(self.payload.clone())
        }
    }
}
