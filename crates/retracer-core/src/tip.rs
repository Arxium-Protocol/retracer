//! Deciding what an incoming block means relative to what's already stored.
//!
//! Pulled out as a pure function because the wrong answer here *deletes
//! indexed data*. Everything it needs is three values — where our tip is, what
//! its hash is, and what the incoming block claims its parent is — so there is
//! no reason for that decision to be entangled with the async write loop, and
//! every reason for it to be exhaustively testable.

/// What to do with a block that just arrived.
#[derive(Debug, PartialEq, Eq)]
pub enum TipAction {
    /// Extends our tip (or is the first block we've seen). Index it.
    Extend,
    /// Arrived beyond our tip with heights missing in between. Not a fork —
    /// `ingestion` backfills gaps over the sync protocol before forwarding
    /// past one, so this is a warning, not a rollback. Rolling back here would
    /// be actively wrong: the blocks we hold are fine, we're just behind.
    Gap,
    /// Already indexed. `ingestion` normally filters these out; if one reaches
    /// us it's a duplicate delivery, and re-indexing is a no-op anyway.
    Stale,
    /// Claims to extend our tip but doesn't agree with it, so the block we
    /// hold at `height - 1` is on a branch the network abandoned. Roll the
    /// index back to `rollback_to` and re-request from there.
    Fork { rollback_to: i64 },
    /// The fork reaches at or below a height the chain has certified as final
    /// (2/3+ of that height's validator set precommitted). Refused
    /// unconditionally: un-indexing a certified block would contradict a
    /// cryptographic proof, so a peer claiming this is wrong or lying, and the
    /// right move is to stop rather than to believe it.
    ForkBelowFinalized { would_rollback_to: i64, finalized_height: i64 },
    /// A fork that would unwind more than the configured finality depth.
    /// Refused rather than obeyed: past that depth a peer feeding us a bogus
    /// chain could otherwise walk us all the way back to genesis, and a
    /// consensus that genuinely reorgs this deep is not one this indexer
    /// should be silently papering over.
    ForkTooDeep { would_rollback_to: i64, depth: u64 },
}

/// Our current tip: the highest height indexed and its hash.
#[derive(Debug, Clone)]
pub struct Tip {
    pub height: i64,
    pub hash: String,
}

/// `rewound_from` is the height the current rollback episode started at, if one
/// is in progress — cumulative depth is measured against that rather than
/// against each individual step, so peeling one block at a time can't sneak
/// past `finality_depth` an inch at a time.
/// `finalized_height` is the chain's own answer, from the node's finality
/// certificates. When present it takes precedence over `finality_depth`, which
/// is only ever a guess standing in for it — a configured block count cannot
/// know where finality actually reached.
pub fn classify(
    tip: Option<&Tip>,
    incoming_height: i64,
    incoming_parent_hash: &str,
    rewound_from: Option<i64>,
    finality_depth: u64,
    finalized_height: Option<i64>,
) -> TipAction {
    let Some(tip) = tip else {
        // Nothing indexed yet: whatever arrives first defines where we start.
        // We have no hash to check it against and no basis to call it a fork.
        return TipAction::Extend;
    };

    if incoming_height <= tip.height {
        return TipAction::Stale;
    }
    if incoming_height > tip.height + 1 {
        return TipAction::Gap;
    }
    if incoming_parent_hash == tip.hash {
        return TipAction::Extend;
    }

    // Contiguous but disagreeing: our `tip.height` block is the bad one, so the
    // last height we can still trust is the one below it. Peeling a single
    // block per detection (rather than guessing how deep the fork goes) means
    // the re-requested block gets checked against its own parent on the next
    // pass, and the loop converges on the real common ancestor.
    let rollback_to = tip.height - 1;
    let depth = (rewound_from.unwrap_or(tip.height) - rollback_to).max(0) as u64;

    // Checked before the depth heuristic: a certificate is evidence, the depth
    // is a guess, and evidence wins. A shallow fork that still reaches a
    // finalized height must be refused even though the depth looks fine.
    if let Some(finalized) = finalized_height
        && rollback_to < finalized
    {
        return TipAction::ForkBelowFinalized {
            would_rollback_to: rollback_to,
            finalized_height: finalized,
        };
    }

    if depth > finality_depth {
        TipAction::ForkTooDeep { would_rollback_to: rollback_to, depth }
    } else {
        TipAction::Fork { rollback_to }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tip(height: i64, hash: &str) -> Tip {
        Tip { height, hash: hash.to_string() }
    }

    #[test]
    fn first_block_always_extends() {
        assert_eq!(classify(None, 0, "", None, 100, None), TipAction::Extend);
        assert_eq!(classify(None, 9_000, "whatever", None, 100, None), TipAction::Extend);
    }

    #[test]
    fn matching_parent_extends() {
        let t = tip(5, "0xaaa");
        assert_eq!(classify(Some(&t), 6, "0xaaa", None, 100, None), TipAction::Extend);
    }

    #[test]
    fn already_indexed_height_is_stale() {
        let t = tip(5, "0xaaa");
        assert_eq!(classify(Some(&t), 5, "0xaaa", None, 100, None), TipAction::Stale);
        assert_eq!(classify(Some(&t), 2, "0xzzz", None, 100, None), TipAction::Stale);
    }

    /// The distinction that matters most: a block from the future is a gap, and
    /// a gap must never be treated as a fork. Our stored blocks are fine — we
    /// are simply behind — so rolling back would delete good data to fix
    /// nothing.
    #[test]
    fn non_contiguous_height_is_a_gap_not_a_fork() {
        let t = tip(5, "0xaaa");
        assert_eq!(classify(Some(&t), 7, "0xbbb", None, 100, None), TipAction::Gap);
        assert_eq!(classify(Some(&t), 500, "0xbbb", None, 100, None), TipAction::Gap);
    }

    #[test]
    fn contiguous_but_disagreeing_parent_is_a_fork() {
        let t = tip(5, "0xaaa");
        assert_eq!(
            classify(Some(&t), 6, "0xdifferent", None, 100, None),
            TipAction::Fork { rollback_to: 4 }
        );
    }

    /// Depth is cumulative across the episode, not per step — otherwise a peer
    /// feeding a bogus chain could peel one block at a time forever, each
    /// individual step looking like a depth of 1.
    #[test]
    fn rollback_depth_accumulates_across_an_episode() {
        // Episode began at height 20; we've already peeled down to a tip of 18.
        let t = tip(18, "0xaaa");
        assert_eq!(
            classify(Some(&t), 19, "0xdifferent", Some(20), 3, None),
            TipAction::Fork { rollback_to: 17 },
            "20 -> 17 is a depth of 3, still within the limit"
        );

        let t = tip(17, "0xaaa");
        assert_eq!(
            classify(Some(&t), 18, "0xdifferent", Some(20), 3, None),
            TipAction::ForkTooDeep { would_rollback_to: 16, depth: 4 },
            "20 -> 16 is a depth of 4, past the limit"
        );
    }

    /// A certificate outranks the depth heuristic: this fork is only one block
    /// deep, well inside `finality_depth`, but it reaches a height the chain
    /// has certified. Refusing is the whole point — the depth check would have
    /// waved it through.
    #[test]
    fn a_shallow_fork_into_finalized_history_is_still_refused() {
        let t = tip(100, "0xaaa");
        assert_eq!(
            classify(Some(&t), 101, "0xdifferent", None, 250, Some(100)),
            TipAction::ForkBelowFinalized { would_rollback_to: 99, finalized_height: 100 }
        );
    }

    /// Above the finalized height the certificate says nothing, so the ordinary
    /// rollback path applies.
    #[test]
    fn a_fork_above_finalized_history_rolls_back_normally() {
        let t = tip(100, "0xaaa");
        assert_eq!(
            classify(Some(&t), 101, "0xdifferent", None, 250, Some(50)),
            TipAction::Fork { rollback_to: 99 }
        );
    }

    /// With no certificate available — a chain without finality voting, or a
    /// node too old to report it — the configured depth remains the only guard.
    #[test]
    fn depth_guard_still_applies_when_finality_is_unknown() {
        let t = tip(100, "0xaaa");
        assert_eq!(
            classify(Some(&t), 101, "0xdifferent", None, 0, None),
            TipAction::ForkTooDeep { would_rollback_to: 99, depth: 1 }
        );
    }

    #[test]
    fn a_single_deep_fork_is_refused_on_its_own() {
        let t = tip(1_000, "0xaaa");
        assert_eq!(
            classify(Some(&t), 1_001, "0xdifferent", None, 0, None),
            TipAction::ForkTooDeep { would_rollback_to: 999, depth: 1 }
        );
    }

    /// Rolling back off the bottom of the chain is representable — `-1` means
    /// "nothing indexed", which `storage::rollback_to` handles by removing the
    /// cursor row rather than storing a negative height.
    #[test]
    fn fork_at_genesis_rolls_back_below_zero() {
        let t = tip(0, "0xgenesis");
        assert_eq!(
            classify(Some(&t), 1, "0xdifferent", None, 100, None),
            TipAction::Fork { rollback_to: -1 }
        );
    }
}
