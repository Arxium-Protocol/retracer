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
/// `symbol`/`name` (V6 reset) come last, after `metadata_uri`.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct AssetMetadata {
    pub asset_class: AssetClass,
    pub decimals: u8,
    pub required_claims: Vec<ClaimTopic>,
    pub allowed_jurisdictions: Option<Vec<String>>,
    pub max_supply: Option<u128>,
    pub metadata_uri: Option<String>,
    /// Display ticker, `[A-Z0-9]{1,12}`. Never unique, never an identifier —
    /// show it beside the truncated ref and the issuer's attestation status.
    pub symbol: String,
    /// Display name, 1–64 bytes.
    pub name: String,
}

/// Mirror of `xc_primitives::AssetRef` — the chain-wide asset identity since
/// the V6 reset: `SHA-256("arxium/asset/v1" || issuer_pubkey || 0x00 ||
/// asset_id)`, rendered bech32 with HRP `arxasset`. On the wire it is one
/// length-prefixed string, exactly like `Address`, so a transparent newtype
/// over `String` is byte-identical to the node's type. Every asset variant
/// after `RegisterAsset` names its asset by this; `RegisterAsset` alone still
/// carries the issuer-scoped slug it is derived from.
///
/// Pinned check value: `(arx132yw8ht5p8cetl2jmvknewjawt9xwzdlrk2pyxlnwjyqrdq0dawqaq6lsz, "gold")`
/// → `arxasset1z8d4jt8yt0xtjm6lvk8umc9relegrwq4xu928eqxyjfcsnjuex6qe873qa`.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AssetRef(pub String);

impl std::fmt::Display for AssetRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
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
    /// The slug being claimed — the one asset variant that still carries
    /// `asset_id`; the chain derives the `AssetRef` from `(sender, asset_id)`.
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
    /// Issuer holder controls, Arxium `c90a6f4` — variants 21–26 in this
    /// order. Positional: do not reorder.
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
    /// Arxium `a7d81ed` — variant 27.
    IssueAssetTo {
        asset: AssetRef,
        to: Address,
        amount: u128,
    },
}
