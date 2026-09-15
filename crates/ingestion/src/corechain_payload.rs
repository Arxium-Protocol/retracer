use serde::{Deserialize, Serialize};
use xc_primitives::{Address, Block};

pub fn is_corechain_address(candidate: &str) -> bool {
    Address::parse(candidate).is_ok()
}

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
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClaimTopic {
    Kyc,
    Aml,
    Accredited,
    Jurisdiction,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct AssetMetadata {
    pub asset_class: AssetClass,
    pub decimals: u8,
    pub required_claims: Vec<ClaimTopic>,
    pub allowed_jurisdictions: Option<Vec<String>>,
    pub max_supply: Option<u128>,
    pub metadata_uri: Option<String>,
    pub symbol: String,
    pub name: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AssetRef(pub String);
impl std::fmt::Display for AssetRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Wire mirror of the node's CoreChain payload. Bincode encodes variant and
/// field position, so this is guarded by the exhaustive node-produced fixture.
/// The historic two-field RegisterAsset format belonged to a reset devnet and
/// has no common chain history with this current schema.
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
        asset: AssetRef,
        amount: u128,
    },
    TransferAsset {
        asset: AssetRef,
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
        asset: AssetRef,
    },
    UnfreezeAsset {
        asset: AssetRef,
    },
    ForcedTransfer {
        asset: AssetRef,
        from: Address,
        to: Address,
        amount: u128,
        reason: String,
    },
    BurnAsset {
        asset: AssetRef,
        amount: u128,
    },
    SetHolderFrozen {
        asset: AssetRef,
        holder: Address,
        frozen: bool,
    },
    LockHolderAmount {
        asset: AssetRef,
        holder: Address,
        amount: u128,
    },
    UnlockHolderAmount {
        asset: AssetRef,
        holder: Address,
        amount: u128,
    },
    IssuerForcedTransfer {
        asset: AssetRef,
        from: Address,
        to: Address,
        amount: u128,
        reason: String,
    },
    RecoverHolder {
        asset: AssetRef,
        lost: Address,
        replacement: Address,
    },
    IssueAssetTo {
        asset: AssetRef,
        to: Address,
        amount: u128,
    },
}
