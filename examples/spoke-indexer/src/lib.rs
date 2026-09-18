//! The two things a builder supplies to integrate Retracer with their own
//! chain. Everything else — the node RPC poller, Postgres schema, HTTP and
//! gRPC surfaces — is inherited unchanged; there is no fork of Retracer
//! anywhere in this example. Blocks are read as the JSON your node serves on
//! `GET /blocks`, so there is no payload enum to mirror: the payload is
//! whatever JSON the node put in `payload_json`.
//!
//!   1. your address format                       → [`is_mintchain_address`]
//!   2. anything `kind_schema.toml` can't express → [`AirdropRecipients`]
//!
//! They live in a library so both binaries can use them: `src/main.rs` follows
//! one chain, `src/bin/multi_chain.rs` follows a Hub and a Spoke together.

use serde_json::Value;
use storage::{ActionIndexable, Role};

/// MintChain addresses are `spoke1` followed by 32 hex characters.
///
/// Supplying this is optional. Without it the indexer still works, but it stops
/// rejecting malformed addresses and `Search` stops recognising accounts —
/// deliberately, because a validator that accepted anything would make `Search`
/// classify every block hash as an address.
pub fn is_mintchain_address(candidate: &str) -> bool {
    match candidate.strip_prefix("spoke1") {
        Some(body) => body.len() == 32 && body.chars().all(|c| c.is_ascii_hexdigit()),
        None => false,
    }
}

/// Tier B extraction: resolves every recipient of an `Airdrop`.
///
/// `kind_schema.toml` covers the other four variants declaratively, because
/// each keeps its address at a fixed dotted path. `Airdrop` has an *array* of
/// them, and a dotted path cannot index into a list — so this is the point
/// where you drop into Rust. A Tier B impl claims one `kind` and fully owns it:
/// a Tier A entry for `Airdrop` would be ignored, not merged, so a kind always
/// has exactly one place defining its roles.
pub struct AirdropRecipients;

impl ActionIndexable for AirdropRecipients {
    fn kind(&self) -> &str {
        "Airdrop"
    }

    /// `payload` is the action's payload as JSON, already unwrapped from its
    /// enum tag. Return one pair per address you want indexed; returning
    /// nothing is fine and simply means this action writes no address rows.
    fn resolve(&self, payload: &Value) -> Vec<(String, Role)> {
        payload
            .get("recipients")
            .and_then(Value::as_array)
            .map(|recipients| {
                recipients
                    .iter()
                    .filter_map(Value::as_str)
                    .map(|address| (address.to_string(), Role::To))
                    .collect()
            })
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn address_validator_accepts_only_well_formed_addresses() {
        assert!(is_mintchain_address(&format!("spoke1{}", "a".repeat(32))));
        assert!(
            !is_mintchain_address(&format!("spoke1{}", "a".repeat(31))),
            "too short"
        );
        assert!(
            !is_mintchain_address(&format!("spoke1{}", "z".repeat(32))),
            "not hex"
        );
        assert!(
            !is_mintchain_address("arx1qyqszqgpqyqszqgp"),
            "another chain's format"
        );
        assert!(
            !is_mintchain_address("0xdeadbeef"),
            "a hash, not an address"
        );
    }

    /// Worth testing in your own integration too: this runs on every action of
    /// its kind, and a wrong answer silently mis-attributes rows rather than
    /// failing.
    #[test]
    fn airdrop_resolves_one_row_per_recipient() {
        let payload = serde_json::json!({
            "collection": "kittens",
            "recipients": ["spoke1aaa", "spoke1bbb", "spoke1ccc"],
        });

        let resolved = AirdropRecipients.resolve(&payload);

        assert_eq!(resolved.len(), 3);
        assert_eq!(resolved[0], ("spoke1aaa".to_string(), Role::To));
        assert_eq!(resolved[2], ("spoke1ccc".to_string(), Role::To));
    }

    #[test]
    fn airdrop_with_no_recipients_contributes_nothing() {
        // Not an error — an action resolving no addresses simply writes no rows.
        assert!(AirdropRecipients.resolve(&serde_json::json!({})).is_empty());
    }
}
