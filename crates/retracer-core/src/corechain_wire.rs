use anyhow::{Result, anyhow};
use ingestion::{ActionPayload, HasHeight, WireDecoder};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use storage::{IndexableAction, IndexableBlock};
use xc_primitives::{Action, Address, Block};
use xc_wire::SyncResponse;

/// CoreChain block normalized for indexing while retaining the hash from its
/// sender's wire generation.
pub struct CoreChainBlock {
    height: u64,
    hash: String,
    parent_hash: String,
    timestamp: u64,
    proposer: Option<String>,
    actions: Vec<CoreChainAction>,
}

pub struct CoreChainAction {
    sender: String,
    identity: Option<String>,
    payload: serde_json::Value,
}

impl HasHeight for CoreChainBlock {
    fn height(&self) -> u64 {
        self.height
    }
}

impl IndexableBlock for CoreChainBlock {
    type Action = CoreChainAction;

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
}

impl IndexableAction for CoreChainAction {
    fn sender(&self) -> String {
        self.sender.clone()
    }

    fn identity(&self) -> Option<String> {
        self.identity.clone()
    }

    fn payload_json(&self) -> Result<serde_json::Value> {
        Ok(self.payload.clone())
    }
}

/// Decodes both the released pre-state-root CoreChain wire and the current
/// state-root wire. Current is attempted first and both paths require complete
/// input consumption.
pub fn decoder() -> WireDecoder<CoreChainBlock> {
    WireDecoder::new(decode_block, decode_sync_response)
}

fn decode_exact<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    let (value, consumed) = bincode::serde::decode_from_slice(bytes, bincode::config::standard())?;
    anyhow::ensure!(
        consumed == bytes.len(),
        "trailing bytes after bincode value"
    );
    Ok(value)
}

fn decode_block(bytes: &[u8]) -> Result<CoreChainBlock> {
    let current = decode_exact::<Block<ActionPayload>>(bytes);
    let legacy = decode_exact::<LegacyBlock>(bytes);
    match (current, legacy) {
        (Ok(_), Ok(_)) => Err(anyhow!("ambiguous CoreChain block wire generation")),
        (Ok(block), Err(_)) => normalize_current_block(block),
        (Err(_), Ok(block)) => normalize_legacy_block(block),
        (Err(current_err), Err(legacy_err)) => Err(anyhow!(
            "unsupported CoreChain block wire: current decode failed ({current_err}); legacy decode failed ({legacy_err})"
        )),
    }
}

fn decode_sync_response(bytes: &[u8]) -> Result<SyncResponse<CoreChainBlock>> {
    let current = decode_exact::<SyncResponse<Block<ActionPayload>>>(bytes);
    let legacy = decode_exact::<SyncResponse<LegacyBlock>>(bytes);
    match (current, legacy) {
        (Ok(current), Ok(legacy)) => {
            if matches!((&current, &legacy), (SyncResponse::Blocks(a), SyncResponse::Blocks(b)) if !a.is_empty() || !b.is_empty())
            {
                return Err(anyhow!("ambiguous CoreChain sync block wire generation"));
            }
            normalize_current_response(current)
        }
        (Ok(response), Err(_)) => normalize_current_response(response),
        (Err(_), Ok(response)) => normalize_legacy_response(response),
        (Err(current_err), Err(legacy_err)) => Err(anyhow!(
            "unsupported CoreChain sync wire: current decode failed ({current_err}); legacy decode failed ({legacy_err})"
        )),
    }
}

fn normalize_current_response(
    response: SyncResponse<Block<ActionPayload>>,
) -> Result<SyncResponse<CoreChainBlock>> {
    Ok(match response {
        SyncResponse::Status { tip_height } => SyncResponse::Status { tip_height },
        SyncResponse::Blocks(blocks) => SyncResponse::Blocks(
            blocks
                .into_iter()
                .map(normalize_current_block)
                .collect::<Result<_>>()?,
        ),
        SyncResponse::NodeInfo(info) => SyncResponse::NodeInfo(info),
        SyncResponse::Hashes(hashes) => SyncResponse::Hashes(hashes),
    })
}

fn normalize_legacy_response(
    response: SyncResponse<LegacyBlock>,
) -> Result<SyncResponse<CoreChainBlock>> {
    Ok(match response {
        SyncResponse::Status { tip_height } => SyncResponse::Status { tip_height },
        SyncResponse::Blocks(blocks) => SyncResponse::Blocks(
            blocks
                .into_iter()
                .map(normalize_legacy_block)
                .collect::<Result<_>>()?,
        ),
        SyncResponse::NodeInfo(info) => SyncResponse::NodeInfo(info),
        SyncResponse::Hashes(hashes) => SyncResponse::Hashes(hashes),
    })
}

fn normalize_current_block(block: Block<ActionPayload>) -> Result<CoreChainBlock> {
    let hash = block.hash();
    let Block {
        height,
        parent_hash,
        timestamp,
        actions,
        proposer,
        ..
    } = block;
    Ok(CoreChainBlock {
        height,
        hash,
        parent_hash,
        timestamp,
        proposer: proposer.map(|address| address.to_string()),
        actions: actions
            .into_iter()
            .map(normalize_action)
            .collect::<Result<_>>()?,
    })
}

fn normalize_legacy_block(block: LegacyBlock) -> Result<CoreChainBlock> {
    let hash = legacy_hash(&block)?;
    let LegacyBlock {
        height,
        parent_hash,
        timestamp,
        actions,
        proposer,
        ..
    } = block;
    Ok(CoreChainBlock {
        height,
        hash,
        parent_hash,
        timestamp,
        proposer: proposer.map(|address| address.to_string()),
        actions: actions
            .into_iter()
            .map(normalize_action)
            .collect::<Result<_>>()?,
    })
}

fn normalize_action<P: Serialize>(action: Action<P>) -> Result<CoreChainAction> {
    Ok(CoreChainAction {
        sender: action.sender.to_string(),
        identity: action.signature.filter(|signature| !signature.is_empty()),
        payload: serde_json::to_value(action.payload)?,
    })
}

fn legacy_hash(block: &LegacyBlock) -> Result<String> {
    let bytes = bincode::serde::encode_to_vec(block, bincode::config::standard())?;
    Ok(format!("0x{}", hex::encode(Sha256::digest(bytes))))
}

/// Exact CoreChain block shape used by Arxium v0.1.1 through v0.1.5, before
/// `state_root` was appended. The recursive evidence blocks use this shape too.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct LegacyBlock {
    height: u64,
    parent_hash: String,
    timestamp: u64,
    actions: Vec<Action<LegacyActionPayload>>,
    proposer: Option<Address>,
    signature: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum LegacyActionPayload {
    Transfer {
        to: Address,
        amount: u128,
    },
    JoinValidator {
        validator: Address,
        stake: u128,
        bls_pubkey: Vec<u8>,
    },
    LeaveValidator {
        validator: Address,
    },
    Stake {
        validator: Address,
        amount: u128,
    },
    Unstake {
        validator: Address,
        amount: u128,
    },
    SubmitEquivocationEvidence {
        block_a: Box<LegacyBlock>,
        block_b: Box<LegacyBlock>,
    },
    RegisterBlsKey {
        validator: Address,
        pubkey: Vec<u8>,
    },
    VerifyIdentityCredential {
        proof: Vec<u8>,
    },
    AuthorizeOperator {
        operator: Address,
    },
    RevokeOperator,
}

#[cfg(test)]
mod tests {
    use super::*;

    // Generated from the exact v0.1.3 definitions. It includes both a join and
    // recursive equivocation evidence so every missing state_root is exercised.
    const LEGACY_FIXTURE: &[u8] = include_bytes!("fixtures/arxium_v0_1_3_block.bin");
    const CURRENT_FIXTURE: &[u8] =
        include_bytes!("../../ingestion/src/arxium_join_validator_block.bin");

    #[test]
    fn decodes_legacy_and_current_corechain_blocks() {
        let decoder = decoder();

        let legacy = decoder
            .decode_block(LEGACY_FIXTURE)
            .expect("v0.1.3 fixture must decode");
        assert_eq!(legacy.height, 42);
        assert_eq!(
            legacy.hash,
            "0x46bb3b6c274535295da406229b9add9973c22af10a7f4434ee7c94b7658317f5"
        );
        assert_eq!(legacy.actions.len(), 2);
        assert_eq!(
            legacy.actions[0].payload["JoinValidator"]["stake"],
            serde_json::json!(123_456_789_012_345_678_901u128)
        );
        assert_eq!(
            legacy.actions[0].payload["JoinValidator"]["bls_pubkey"]
                .as_array()
                .unwrap()
                .len(),
            48
        );
        let evidence = &legacy.actions[1].payload["SubmitEquivocationEvidence"];
        assert!(evidence["block_a"].get("state_root").is_none());
        assert!(evidence["block_b"].get("state_root").is_none());

        let current_wire: Block<ActionPayload> = decode_exact(CURRENT_FIXTURE).unwrap();
        let expected_hash = current_wire.hash();
        let current = decoder
            .decode_block(CURRENT_FIXTURE)
            .expect("current fixture must decode");
        assert_eq!(current.height, 42);
        assert_eq!(current.hash, expected_hash);
        assert_eq!(current.actions.len(), 1);
    }

    #[test]
    fn current_decoder_alone_rejects_the_legacy_fixture() {
        assert!(decode_exact::<Block<ActionPayload>>(LEGACY_FIXTURE).is_err());
    }

    #[test]
    fn decodes_legacy_and_current_sync_block_pages() {
        let legacy_block: LegacyBlock = decode_exact(LEGACY_FIXTURE).unwrap();
        let legacy_response = bincode::serde::encode_to_vec(
            SyncResponse::Blocks(vec![legacy_block.clone(), legacy_block]),
            bincode::config::standard(),
        )
        .unwrap();
        let current_block: Block<ActionPayload> = decode_exact(CURRENT_FIXTURE).unwrap();
        let current_response = bincode::serde::encode_to_vec(
            SyncResponse::Blocks(vec![current_block.clone(), current_block]),
            bincode::config::standard(),
        )
        .unwrap();

        for response in [&legacy_response, &current_response] {
            let decoded = decoder().decode_sync_response(response).unwrap();
            let SyncResponse::Blocks(blocks) = decoded else {
                panic!("expected a block page");
            };
            assert_eq!(blocks.len(), 2);
            assert_eq!(blocks[0].height, 42);
        }
    }

    #[test]
    fn dual_sync_decoder_rejects_trailing_and_incomplete_input() {
        let legacy_block: LegacyBlock = decode_exact(LEGACY_FIXTURE).unwrap();
        let encoded = bincode::serde::encode_to_vec(
            SyncResponse::Blocks(vec![legacy_block]),
            bincode::config::standard(),
        )
        .unwrap();

        let mut trailing = encoded.clone();
        trailing.push(0);
        assert!(decoder().decode_sync_response(&trailing).is_err());
        assert!(
            decoder()
                .decode_sync_response(&encoded[..encoded.len() - 1])
                .is_err()
        );
    }

    #[test]
    fn dual_decoder_still_rejects_trailing_and_incomplete_input() {
        let mut trailing = LEGACY_FIXTURE.to_vec();
        trailing.push(0);
        assert!(decoder().decode_block(&trailing).is_err());
        assert!(
            decoder()
                .decode_block(&LEGACY_FIXTURE[..LEGACY_FIXTURE.len() - 1])
                .is_err()
        );
    }
}
