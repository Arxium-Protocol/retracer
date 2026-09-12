use serde::{Deserialize, Serialize};
use xc_primitives::{Address, Block};

/// CoreChain's address format: `arx1` bech32 over an ed25519 pubkey. Lives here
/// with `ActionPayload` because it's the same kind of thing — the CoreChain
/// instantiation of something the indexer itself treats as chain-specific
/// (`storage::AddressValidator`).
pub fn is_corechain_address(candidate: &str) -> bool {
    Address::parse(candidate).is_ok()
}

/// The asset-class taxonomy from `xc_primitives::AssetClass`, mirrored here
/// for the same reason `ActionPayload` is: the pinned `xc-primitives` rev
/// predates it. Variant **order** is the wire format (bincode encodes the
/// discriminant index, never the name), so `Other` stays first.
///
/// Delete all three of these mirrors and import them from `xc_primitives`
/// once the Arxium rev pinned in Cargo.toml carries them.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub enum AssetClass {
    #[default]
    Other,
    RealEstate,
    Equity,
    Bond,
    Stablecoin,
    Commodity,
}

/// Mirror of `xc_primitives::ClaimTopic` — the compliance claims an account
/// can hold and an asset can require.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClaimTopic {
    Kyc,
    Aml,
    Accredited,
    Jurisdiction,
}

/// Mirror of `xc_primitives::AssetMetadata`, the grouped registration fields
/// of `RegisterAsset`. Field order is the wire format, same as variant order.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct AssetMetadata {
    pub asset_class: AssetClass,
    pub decimals: u8,
    pub required_claims: Vec<ClaimTopic>,
    pub allowed_jurisdictions: Option<Vec<String>>,
    pub max_supply: Option<u128>,
    pub metadata_uri: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ActionPayload {
    Transfer {
        to: Address,
        amount: u128,
    },
    JoinValidator {
        validator: Address,
        stake: u128,
        bls_pubkey: Vec<u8>,
        /// Proof of possession of `bls_pubkey`. Added to the node's variant
        /// after this mirror was written — a gap this file's whole purpose is
        /// to catch, found by regenerating the fixture.
        bls_pop: Vec<u8>,
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
        block_a: Box<Block<ActionPayload>>,
        block_b: Box<Block<ActionPayload>>,
    },
    RegisterBlsKey {
        validator: Address,
        pubkey: Vec<u8>,
        /// See `JoinValidator::bls_pop` — same node-side addition.
        pop: Vec<u8>,
    },
    VerifyIdentityCredential {
        proof: Vec<u8>,
    },
    AuthorizeOperator {
        operator: Address,
    },
    RevokeOperator,
    GrantAttestation {
        subject: Address,
        hash: String,
        topics: Vec<ClaimTopic>,
        jurisdiction: Option<String>,
    },
    RevokeAttestation {
        subject: Address,
    },
    RegisterAsset {
        asset_id: String,
        compliance_required: bool,
        metadata: AssetMetadata,
    },
    IssueAsset {
        asset_id: String,
        amount: u128,
    },
    TransferAsset {
        asset_id: String,
        to: Address,
        amount: u128,
    },
    RegisterAttestor {
        attestor: Address,
        name: String,
    },
    DeregisterAttestor {
        attestor: Address,
    },
    SubmitExecutionFault {
        artifact_json: String,
    },
    FreezeAsset {
        asset_id: String,
    },
    UnfreezeAsset {
        asset_id: String,
    },
    ForcedTransfer {
        asset_id: String,
        from: Address,
        to: Address,
        amount: u128,
        reason: String,
    },
    /// Issuer holder controls, Arxium `c90a6f4` — variants 21–26 in this
    /// order. Positional: do not reorder.
    BurnAsset {
        asset_id: String,
        amount: u128,
    },
    SetHolderFrozen {
        asset_id: String,
        holder: Address,
        frozen: bool,
    },
    LockHolderAmount {
        asset_id: String,
        holder: Address,
        amount: u128,
    },
    UnlockHolderAmount {
        asset_id: String,
        holder: Address,
        amount: u128,
    },
    IssuerForcedTransfer {
        asset_id: String,
        from: Address,
        to: Address,
        amount: u128,
        reason: String,
    },
    RecoverHolder {
        asset_id: String,
        lost: Address,
        replacement: Address,
    },
}
