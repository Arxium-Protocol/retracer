use anyhow::{Context, Result, anyhow};
use ingestion::{ActionPayload, CertificateVerifier, HasHeight, WireDecoder};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use storage::{IndexableAction, IndexableBlock};
use xc_primitives::{Action, Address, Block, RawAction, RawBlock, VotingPower, quorum_reached};
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

/// Like [`decoder`], but a current-generation block whose action payload
/// doesn't decode as `ActionPayload` (an unrecognized variant, or a
/// non-canonically-padded one) is kept, not dropped: it becomes a
/// `CoreChainAction` with a synthetic multi-key payload — see
/// [`unknown_action`] — which `storage::split_kind` classifies as
/// `kind = "unknown"` rather than silently shrinking the block's action list.
/// This is what CoreChain's explorer uses (never `ingestion::WireDecoder::tolerant`,
/// which drops unrecognized actions outright — honest for a chain that
/// accepts that loss, not for a compliance explorer that must stay a
/// complete, externally-verifiable view of the chain). The legacy arm is
/// unaffected: v0.1.x is a frozen historical format that never gains new
/// variants, so it stays exact-decode-only, exactly like [`decoder`].
pub fn tolerant_decoder() -> WireDecoder<CoreChainBlock> {
    WireDecoder::new(decode_block_tolerant, decode_sync_response_tolerant)
}

/// Strict CoreChain ingestion decoder. It rejects a block before storage unless
/// its signed header is intact, every known action signature verifies, and the
/// action list reproduces the signed transaction root. Full nonce, state-root,
/// and execution validation remains node-only because it requires pre-state.
pub fn validated_decoder() -> WireDecoder<CoreChainBlock> {
    WireDecoder::new(decode_block_validated, decode_sync_response_validated)
}

/// Verifies wire-v3 certificates using only public, height-scoped node RPC
/// data. The genesis root is fetched once per certificate and is part of the
/// signed message, preventing a certificate from another chain being accepted.
pub fn http_certificate_verifier(
    node_rpc_url: String,
    node_rpc_token: Option<String>,
) -> CertificateVerifier {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .expect("a reqwest client with a fixed timeout must build");
    Arc::new(move |requested_height, bytes| {
        let client = client.clone();
        let base = node_rpc_url.trim_end_matches('/').to_owned();
        let token = node_rpc_token.clone();
        Box::pin(async move {
            let record: xc_storage::FinalityRecord =
                decode_exact(&bytes).context("invalid finality certificate encoding")?;
            anyhow::ensure!(
                record.height == requested_height,
                "certificate height does not match request"
            );
            anyhow::ensure!(!record.signers.is_empty(), "certificate has no signers");
            let unique: HashSet<_> = record.signers.iter().collect();
            anyhow::ensure!(
                unique.len() == record.signers.len(),
                "certificate repeats a signer"
            );

            let get_json = |url: String| {
                let client = client.clone();
                let token = token.clone();
                async move {
                    let mut request = client.get(url);
                    if let Some(token) = token {
                        request = request.bearer_auth(token);
                    }
                    request
                        .send()
                        .await?
                        .error_for_status()?
                        .json::<serde_json::Value>()
                        .await
                }
            };
            let genesis: String = get_json(format!("{base}/genesis-hash"))
                .await?
                .get("genesis_hash")
                .and_then(serde_json::Value::as_str)
                .context("node RPC genesis-hash response is malformed")?
                .to_owned();
            let genesis = genesis.strip_prefix("0x").unwrap_or(&genesis);
            let genesis: [u8; 32] = hex::decode(genesis)
                .context("node RPC returned an invalid genesis hash")?
                .try_into()
                .map_err(|_| anyhow!("node RPC genesis hash must be 32 bytes"))?;
            let powers: BTreeMap<String, u32> = serde_json::from_value(
                get_json(format!("{base}/validators/power?height={requested_height}")).await?,
            )?;
            let validators: BTreeMap<Address, VotingPower> = powers
                .into_iter()
                .map(|(address, power)| Ok((Address::parse(&address)?, VotingPower(power))))
                .collect::<Result<_>>()?;
            anyhow::ensure!(
                record
                    .signers
                    .iter()
                    .all(|signer| validators.contains_key(signer)),
                "certificate signer is not a validator at its height"
            );
            anyhow::ensure!(
                quorum_reached(&validators, record.signers.iter()),
                "certificate lacks quorum"
            );

            let mut public_keys = Vec::with_capacity(record.signers.len());
            for signer in &record.signers {
                let response = get_json(format!(
                    "{base}/accounts/{signer}/bls-key?height={requested_height}"
                ))
                .await?;
                let key = response
                    .get("pubkey")
                    .context("node RPC BLS-key response is malformed")?;
                public_keys.push(
                    serde_json::from_value(key.clone())
                        .context("node RPC returned an invalid BLS key")?,
                );
            }
            let message = arxd_finality::precommit_signing_bytes(
                &genesis,
                record.height,
                &record.block_hash,
                &record.ep,
            );
            xc_bls::verify_aggregate(&message, &public_keys, &record.aggregate_signature)
                .map_err(|err| anyhow!("invalid aggregate certificate signature: {err}"))
        })
    })
}

fn decode_exact<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    let (value, consumed) = bincode::serde::decode_from_slice(bytes, xc_primitives::wire_config())?;
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

fn decode_block_validated(bytes: &[u8]) -> Result<CoreChainBlock> {
    let block: Block<ActionPayload> = decode_exact(bytes)?;
    validate_block(&block)?;
    normalize_current_block(block)
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

fn decode_sync_response_validated(bytes: &[u8]) -> Result<SyncResponse<CoreChainBlock>> {
    let response: SyncResponse<Block<ActionPayload>> = decode_exact(bytes)?;
    Ok(match response {
        SyncResponse::Blocks(blocks) => SyncResponse::Blocks(
            blocks
                .into_iter()
                .map(|block| {
                    validate_block(&block)?;
                    normalize_current_block(block)
                })
                .collect::<Result<_>>()?,
        ),
        SyncResponse::Status { tip_height } => SyncResponse::Status { tip_height },
        SyncResponse::NodeInfo(info) => SyncResponse::NodeInfo(info),
        SyncResponse::Hashes(hashes) => SyncResponse::Hashes(hashes),
        SyncResponse::Certificate { height, record } => {
            SyncResponse::Certificate { height, record }
        }
    })
}

fn validate_block(block: &Block<ActionPayload>) -> Result<()> {
    // Genesis has no proposer signature or user actions. Every later block
    // must prove both its header and each action's author.
    if block.height > 0 {
        block.verify_proposer_signature().map_err(|err| {
            anyhow!(
                "invalid proposer signature at height {}: {err}",
                block.height
            )
        })?;
        for (index, action) in block.actions.iter().enumerate() {
            action.verify_signature().map_err(|err| {
                anyhow!(
                    "invalid action signature at height {} index {index}: {err}",
                    block.height
                )
            })?;
        }
    }

    let expected_tx_root = xc_poe::tx_root(&block.actions).map_err(|err| {
        anyhow!(
            "failed to calculate tx_root at height {}: {err}",
            block.height
        )
    })?;
    anyhow::ensure!(
        block.tx_root == expected_tx_root,
        "tx_root mismatch at height {}",
        block.height
    );
    Ok(())
}

fn decode_block_tolerant(bytes: &[u8]) -> Result<CoreChainBlock> {
    let current = decode_exact::<RawBlock>(bytes);
    let legacy = decode_exact::<LegacyBlock>(bytes);
    match (current, legacy) {
        (Ok(_), Ok(_)) => Err(anyhow!("ambiguous CoreChain block wire generation")),
        (Ok(raw), Err(_)) => Ok(normalize_current_block_tolerant(raw)),
        (Err(_), Ok(block)) => normalize_legacy_block(block),
        (Err(current_err), Err(legacy_err)) => Err(anyhow!(
            "unsupported CoreChain block wire: current decode failed ({current_err}); legacy decode failed ({legacy_err})"
        )),
    }
}

fn decode_sync_response_tolerant(bytes: &[u8]) -> Result<SyncResponse<CoreChainBlock>> {
    // `SyncResponse<RawBlock>` shares `SyncResponse<Block<ActionPayload>>`'s
    // exact wire layout (see `ingestion::decode_sync_response_tolerant`), so
    // this always decodes structurally when the generation is "current".
    let current = decode_exact::<SyncResponse<RawBlock>>(bytes);
    let legacy = decode_exact::<SyncResponse<LegacyBlock>>(bytes);
    match (current, legacy) {
        (Ok(current), Ok(legacy)) => {
            if matches!((&current, &legacy), (SyncResponse::Blocks(a), SyncResponse::Blocks(b)) if !a.is_empty() || !b.is_empty())
            {
                return Err(anyhow!("ambiguous CoreChain sync block wire generation"));
            }
            Ok(normalize_current_response_tolerant(current))
        }
        (Ok(response), Err(_)) => Ok(normalize_current_response_tolerant(response)),
        (Err(_), Ok(response)) => normalize_legacy_response(response),
        (Err(current_err), Err(legacy_err)) => Err(anyhow!(
            "unsupported CoreChain sync wire: current decode failed ({current_err}); legacy decode failed ({legacy_err})"
        )),
    }
}

fn normalize_current_response_tolerant(
    response: SyncResponse<RawBlock>,
) -> SyncResponse<CoreChainBlock> {
    match response {
        SyncResponse::Status { tip_height } => SyncResponse::Status { tip_height },
        SyncResponse::Blocks(blocks) => SyncResponse::Blocks(
            blocks
                .into_iter()
                .map(normalize_current_block_tolerant)
                .collect(),
        ),
        SyncResponse::NodeInfo(info) => SyncResponse::NodeInfo(info),
        SyncResponse::Hashes(hashes) => SyncResponse::Hashes(hashes),
        SyncResponse::Certificate { height, record } => {
            SyncResponse::Certificate { height, record }
        }
    }
}

/// Unlike [`normalize_current_block`], hashes the [`RawBlock`] itself (never
/// a `Block<ActionPayload>` built from only the actions that happened to
/// decode) — `RawBlock::hash()` reproduces the real on-chain hash regardless
/// of how many actions this reader could interpret, so a block with an
/// unknown action still gets the hash it actually has on-chain, not one
/// silently computed over a truncated action list.
fn normalize_current_block_tolerant(raw: RawBlock) -> CoreChainBlock {
    let hash = raw.hash();
    CoreChainBlock {
        height: raw.height,
        hash,
        parent_hash: raw.parent_hash,
        timestamp: raw.timestamp,
        proposer: raw.proposer.map(|address| address.to_string()),
        actions: raw
            .actions
            .into_iter()
            .map(normalize_raw_action_tolerant)
            .collect(),
    }
}

/// Decodes one action's raw payload as `ActionPayload`, falling back to
/// [`unknown_action`] for a variant this reader's copy of `ActionPayload`
/// doesn't recognize, or for a non-canonically-padded payload (same
/// treatment as a genuine unknown — see `Action<P>`'s `Deserialize` impl and
/// `Implementation_log_2026-09-05.md`). Never drops the action outright: an
/// explorer that silently shrank a block's action list would misreport what
/// actually happened on-chain.
fn normalize_raw_action_tolerant(ra: RawAction) -> CoreChainAction {
    let decoded = bincode::serde::decode_from_slice::<ActionPayload, _>(
        &ra.payload,
        xc_primitives::wire_config(),
    );
    match decoded {
        Ok((payload, consumed)) if consumed == ra.payload.len() => CoreChainAction {
            sender: ra.sender.to_string(),
            identity: ra.signature.filter(|signature| !signature.is_empty()),
            payload: serde_json::to_value(payload)
                .unwrap_or_else(|err| unknown_payload(&ra.payload, ra.nonce, &err.to_string())),
        },
        Ok(_) => unknown_action(ra, "non-canonically-encoded payload (trailing bytes)"),
        Err(err) => unknown_action(ra, &format!("unrecognized payload variant: {err}")),
    }
}

/// Builds the `CoreChainAction` for an action whose payload this reader
/// couldn't interpret. The payload JSON deliberately has more than one key,
/// so `storage::split_kind` falls through to `kind = "unknown"` instead of
/// treating it as a recognized single-variant action — see
/// `storage::wire::split_kind`. `discriminant` is a best-effort read of the
/// bincode variant index prefix (not validated against any known enum), kept
/// even when payload interpretation otherwise fails, since it's often enough
/// on its own to tell which future variant this was.
fn unknown_action(ra: RawAction, reason: &str) -> CoreChainAction {
    let sender = ra.sender.to_string();
    let identity = ra.signature.filter(|signature| !signature.is_empty());
    CoreChainAction {
        sender,
        identity,
        payload: unknown_payload(&ra.payload, ra.nonce, reason),
    }
}

fn unknown_payload(raw_payload: &[u8], nonce: u64, reason: &str) -> serde_json::Value {
    let discriminant =
        bincode::serde::decode_from_slice::<u32, _>(raw_payload, bincode::config::standard())
            .ok()
            .map(|(discriminant, _)| discriminant);
    serde_json::json!({
        "discriminant": discriminant,
        "raw_payload_hex": hex::encode(raw_payload),
        "nonce": nonce,
        "reason": reason,
    })
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
        SyncResponse::Certificate { height, record } => {
            SyncResponse::Certificate { height, record }
        }
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
        SyncResponse::Certificate { height, record } => {
            SyncResponse::Certificate { height, record }
        }
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
            .map(normalize_legacy_action)
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

fn normalize_legacy_action<P: Serialize>(action: LegacyAction<P>) -> Result<CoreChainAction> {
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
    actions: Vec<LegacyAction<LegacyActionPayload>>,
    proposer: Option<Address>,
    signature: Option<String>,
}

/// `xc_primitives::Action<P>`'s wire shape as it was before Track B's
/// length-prefixed payload change — kept here, decoupled from the live type,
/// because this fixture is a frozen historical wire format (v0.1.1-v0.1.5)
/// that must keep decoding exactly as it always did, regardless of any later
/// change to the current `Action<P>` encoding.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct LegacyAction<P> {
    sender: Address,
    nonce: u64,
    signature: Option<String>,
    payload: P,
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
    // Captured v0.2.0 standard-bincode block. It includes the fields added
    // after the released v0.1.3 wire shape.
    const CURRENT_FIXTURE: &[u8] = &[
        0x2a, 0x08, 0x30, 0x78, 0x70, 0x61, 0x72, 0x65, 0x6e, 0x74, 0x00, 0x00, 0x07, 0x07, 0x07,
        0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07,
        0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x00,
        0x00, 0x3d, 0x30, 0x78, 0x66, 0x69, 0x78, 0x74, 0x75, 0x72, 0x65, 0x2d, 0x73, 0x74, 0x61,
        0x74, 0x65, 0x2d, 0x72, 0x6f, 0x6f, 0x74, 0x2d, 0x62, 0x38, 0x31, 0x31, 0x62, 0x62, 0x64,
        0x31, 0x39, 0x64, 0x38, 0x35, 0x38, 0x36, 0x61, 0x32, 0x36, 0x30, 0x33, 0x38, 0x33, 0x33,
        0x66, 0x39, 0x38, 0x39, 0x63, 0x65, 0x61, 0x33, 0x38, 0x38, 0x32, 0x36, 0x38, 0x34, 0x63,
        0x39, 0x34, 0x66, 0x00, 0x00,
    ];

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
        assert!(current.actions.is_empty());
    }

    #[test]
    fn current_decoder_alone_rejects_the_legacy_fixture() {
        assert!(decode_exact::<Block<ActionPayload>>(LEGACY_FIXTURE).is_err());
    }

    #[test]
    fn validated_decoder_rejects_unsigned_and_legacy_blocks() {
        // The captured current fixture is structurally valid, but intentionally
        // has no proposer signature. It must never enter the indexer path.
        assert!(validated_decoder().decode_block(CURRENT_FIXTURE).is_err());
        assert!(validated_decoder().decode_block(LEGACY_FIXTURE).is_err());
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

    /// `tolerant_decoder`'s legacy arm must stay byte-for-byte the same as
    /// `decoder`'s — v0.1.x is frozen and never gains new variants, so there's
    /// nothing for the tolerant path to be tolerant *of* here.
    #[test]
    fn tolerant_decoder_still_decodes_the_legacy_fixture() {
        let legacy = tolerant_decoder()
            .decode_block(LEGACY_FIXTURE)
            .expect("v0.1.3 fixture must decode under tolerant_decoder");
        assert_eq!(legacy.height, 42);
        assert_eq!(legacy.actions.len(), 2);
    }

    fn raw_block_with_one_action(
        action: RawAction,
        expected_action_count: usize,
    ) -> (RawBlock, usize) {
        let sender = action.sender.clone();
        let block = RawBlock {
            height: 5,
            parent_hash: "0xparent".to_string(),
            timestamp: 1000,
            actions: vec![action],
            tx_root: [1u8; 32],
            proposer: Some(sender),
            signature: None,
            state_root: "0xstate".to_string(),
            round: 0,
            round_certificate: None,
        };
        (block, expected_action_count)
    }

    /// An action whose payload doesn't decode as `ActionPayload` (here: an
    /// out-of-range enum variant index, standing in for a variant this
    /// reader's copy of `ActionPayload` predates) must not disappear from the
    /// block. It becomes a `kind = "unknown"` row carrying the raw bytes, and
    /// the block's hash must still match what `RawBlock::hash()` (i.e. the
    /// real on-chain hash) says — never a hash computed as if the action had
    /// been dropped.
    #[test]
    fn tolerant_decoder_keeps_an_unrecognized_action_as_an_unknown_row() {
        let sender = Address::from_pubkey_bytes(&[3u8; 32]).unwrap();
        let bogus_payload =
            bincode::serde::encode_to_vec(99u32, bincode::config::standard()).unwrap();
        let (raw_block, expected_count) = raw_block_with_one_action(
            RawAction {
                sender,
                nonce: 7,
                signature: Some("deadbeef".to_string()),
                payload: bogus_payload,
            },
            1,
        );
        let expected_hash = raw_block.hash();
        let bytes =
            bincode::serde::encode_to_vec(&raw_block, xc_primitives::wire_config()).unwrap();

        let decoded = tolerant_decoder()
            .decode_block(&bytes)
            .expect("structurally valid RawBlock must always decode");
        assert_eq!(decoded.hash, expected_hash);
        assert_eq!(decoded.actions.len(), expected_count);

        let payload = &decoded.actions[0].payload;
        assert!(
            payload.get("raw_payload_hex").is_some(),
            "unknown action must carry its raw payload bytes: {payload:?}"
        );
        assert_eq!(payload["nonce"], serde_json::json!(7));

        // `storage::split_kind` only treats a single-key object (or a bare
        // string, for a unit variant) as a real payload kind, falling back to
        // "unknown" for anything else — so a multi-key object here is what
        // makes this row classify as unknown rather than some real kind.
        assert!(
            payload.as_object().is_some_and(|o| o.len() > 1),
            "unknown-row payload must not look like a single-variant payload: {payload:?}"
        );
    }

    /// A non-canonically-padded payload (one trailing byte after an
    /// otherwise-valid `ActionPayload` encoding) gets the same unknown-row
    /// treatment as a genuinely unrecognized variant — accepting it would
    /// mean the decoded action re-encodes to different bytes than arrived on
    /// the wire. See `ingestion::raw_block_into_tolerant` for the same rule
    /// applied to the general-purpose tolerant decoder.
    #[test]
    fn tolerant_decoder_treats_padded_payload_as_unknown() {
        let sender = Address::from_pubkey_bytes(&[4u8; 32]).unwrap();
        let mut padded_payload = bincode::serde::encode_to_vec(
            &ActionPayload::LeaveValidator {
                validator: sender.clone(),
            },
            xc_primitives::wire_config(),
        )
        .unwrap();
        padded_payload.push(0xff);
        let (raw_block, expected_count) = raw_block_with_one_action(
            RawAction {
                sender,
                nonce: 3,
                signature: Some("cafef00d".to_string()),
                payload: padded_payload,
            },
            1,
        );
        let expected_hash = raw_block.hash();
        let bytes =
            bincode::serde::encode_to_vec(&raw_block, xc_primitives::wire_config()).unwrap();

        let decoded = tolerant_decoder().decode_block(&bytes).unwrap();
        assert_eq!(decoded.hash, expected_hash);
        assert_eq!(decoded.actions.len(), expected_count);
        assert!(
            decoded.actions[0]
                .payload
                .as_object()
                .is_some_and(|o| o.len() > 1)
        );
    }
}
