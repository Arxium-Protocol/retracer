//! A block as the node's HTTP RPC serves it (`GET /blocks?from=&to=`), read
//! straight from JSON. Nothing here comes from an Arxium crate: the hash and
//! each action's decoded payload ride in the JSON itself (`hash`,
//! `payload_json` — node `xc-rpc` ≥ 0.2.0), so this reader never re-encodes
//! bincode or decodes the chain's payload enum. Unknown fields are ignored;
//! a missing one is a decode error, not a silently wrong row.

use anyhow::Result;
use ingestion::HasHeight;
use serde::Deserialize;
use storage::{BlockEffects, IndexableAction, IndexableBlock};

#[derive(Debug, Deserialize)]
pub struct RpcBlock {
    height: u64,
    hash: String,
    parent_hash: String,
    timestamp: u64,
    proposer: Option<String>,
    #[serde(default)]
    round: u32,
    actions: Vec<RpcAction>,
    /// Not part of the block JSON — attached by the reader from
    /// `GET /blocks/{height}/effects` (`HasHeight::set_effects`).
    #[serde(skip)]
    effects: Option<BlockEffects>,
}

#[derive(Debug, Deserialize)]
pub struct RpcAction {
    sender: String,
    signature: Option<String>,
    payload_json: serde_json::Value,
}

impl HasHeight for RpcBlock {
    fn height(&self) -> u64 {
        self.height
    }
    fn set_effects(&mut self, effects: serde_json::Value) -> Result<()> {
        self.effects = Some(serde_json::from_value(effects)?);
        Ok(())
    }
}

impl IndexableBlock for RpcBlock {
    type Action = RpcAction;

    fn height(&self) -> u64 {
        self.height
    }
    fn hash(&self) -> String {
        self.hash.clone()
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
    fn actions(&self) -> &[Self::Action] {
        &self.actions
    }
    fn round(&self) -> u32 {
        self.round
    }
    fn effects(&self) -> Option<&BlockEffects> {
        self.effects.as_ref()
    }
}

impl IndexableAction for RpcAction {
    fn sender(&self) -> String {
        self.sender.clone()
    }
    // Same identity the P2P path uses: the signature, which every admitted
    // CoreChain action carries. `None` falls back to `height:index`.
    fn identity(&self) -> Option<String> {
        self.signature.clone()
    }
    fn payload_json(&self) -> Result<serde_json::Value> {
        Ok(self.payload_json.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_the_node_json_and_ignores_fields_it_does_not_index() {
        let block: RpcBlock = serde_json::from_str(
            r#"{"height":7,"hash":"0xab","parent_hash":"0xaa","timestamp":5,"proposer":"arx1x",
                "round":2,"finalized":true,"weight_used":3,"tx_root":[0],"state_root":"0x",
                "actions":[{"sender":"arx1s","nonce":1,"signature":"sig","payload":[3,1,2,3],
                            "payload_json":{"Transfer":{"to":"arx1t","amount":1}}}]}"#,
        )
        .unwrap();
        assert_eq!(IndexableBlock::height(&block), 7);
        assert_eq!(block.hash(), "0xab");
        assert_eq!(block.round(), 2);
        let action = &block.actions()[0];
        assert_eq!(action.identity().as_deref(), Some("sig"));
        assert_eq!(action.payload_json().unwrap()["Transfer"]["amount"], 1);
        // The node's effects answer decodes into the state rows; a u128
        // balance survives, and unknown top-level fields are ignored.
        let mut block = block;
        block
            .set_effects(serde_json::json!({
                "height": 7,
                "accounts": {"arx1s": {"balance": 340282366920938463463374607431768211455u128, "nonce": 2, "identity_hash": null}},
                "asset_balances": [{"asset": "gold", "owner": "arx1t", "balance": 5}],
                "holder_states": [], "stakes": [],
                "validator_statuses": {"arx1v": {"Jailed": {"until_epoch": 3}}},
                "validator_set": null, "asset_registrations": [],
                "dropped": [{"signature": "bad", "reason": "nonce"}],
                // Arxium b360a9c: a BLS pubkey is `serialize_bytes` → a JSON
                // array of 48 ints; a deregistration is the bare address;
                // `operator_index` is the node's reverse index, ignored.
                "evidence": [{"height": 3, "proposer": "arx1v"}],
                "bls_keys": [{"address": "arx1v", "pubkey": vec![9u8; 48], "effective_height": 8, "previous_pubkey": null}],
                "operators": {"authorization": {"arx1v": "arx1o"}, "operator_index": {"arx1o": ["arx1v"]}},
                "attestor_registrations": [{"attestor": "arx1a", "record": {"name": "att", "registered_at": 7}}],
                "attestor_deregistrations": ["arx1b"],
                "future_field": 1
            }))
            .unwrap();
        let effects = block.effects().unwrap();
        assert_eq!(effects.accounts["arx1s"].balance, u128::MAX);
        assert_eq!(effects.asset_balances[0].balance, 5);
        assert_eq!(
            effects.validator_statuses["arx1v"].as_ref().unwrap()["Jailed"]["until_epoch"],
            3
        );
        assert_eq!(effects.dropped[0].reason, "nonce");
        assert_eq!(effects.evidence[0].height, 3);
        assert_eq!(effects.bls_keys[0].pubkey.as_array().unwrap().len(), 48);
        assert_eq!(
            effects.operators.authorization["arx1v"].as_deref(),
            Some("arx1o")
        );
        assert_eq!(effects.attestor_registrations[0].record["name"], "att");
        assert_eq!(effects.attestor_deregistrations, vec!["arx1b"]);
        // A node older than b360a9c sends none of the five: all default.
        block.set_effects(serde_json::json!({"height": 7})).unwrap();
        assert!(block.effects().unwrap().attestor_registrations.is_empty());
        // A node too old to send `payload_json` is a hard decode error.
        assert!(serde_json::from_str::<RpcBlock>(r#"{"height":0,"hash":"","parent_hash":"","timestamp":0,"proposer":null,"actions":[{"sender":"a","signature":null}]}"#).is_err());
    }
}
