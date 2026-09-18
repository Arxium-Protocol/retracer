//! A block as the node's HTTP RPC serves it (`GET /blocks?from=&to=`), read
//! straight from JSON. Nothing here comes from an Arxium crate: the hash and
//! each action's decoded payload ride in the JSON itself (`hash`,
//! `payload_json` — node `xc-rpc` ≥ 0.2.0), so this reader never re-encodes
//! bincode or decodes the chain's payload enum. Unknown fields are ignored;
//! a missing one is a decode error, not a silently wrong row.

use anyhow::Result;
use ingestion::HasHeight;
use serde::Deserialize;
use storage::{IndexableAction, IndexableBlock};

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
        // A node too old to send `payload_json` is a hard decode error.
        assert!(serde_json::from_str::<RpcBlock>(r#"{"height":0,"hash":"","parent_hash":"","timestamp":0,"proposer":null,"actions":[{"sender":"a","signature":null}]}"#).is_err());
    }
}
