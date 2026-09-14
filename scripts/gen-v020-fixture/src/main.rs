//! Regenerates the frozen fixture in
//! `crates/ingestion/src/lib.rs::decodes_arxium_v020_block_fixture`.
//!
//! Builds one `Action` of every `ActionPayload` variant Arxium's real
//! `arxd-runtime` crate defines (source of truth, not Retracer's copy),
//! bincode-encodes the block, and prints it as a Rust byte-array literal to
//! paste into that test. Run whenever Arxium adds/changes a variant:
//!
//!   cargo run --manifest-path scripts/gen-v020-fixture/Cargo.toml
//!
//! Depends on a sibling Arxium checkout at `../../../Arxium` (i.e.
//! `Retracer` and `Arxium` under the same parent directory) — repoint the
//! path dependencies in this crate's Cargo.toml if yours lives elsewhere.

use arxd_runtime::ActionPayload;
use xc_primitives::{Action, Address, AssetClass, AssetMetadata, AssetRef, ClaimTopic};

fn addr(byte: u8) -> Address {
    Address::from_pubkey_bytes(&[byte; 32]).expect("32 bytes is a valid pubkey")
}

/// The ref `RegisterAsset { asset_id: "a" }` from the fixture sender derives.
fn asset() -> AssetRef {
    AssetRef::derive(&addr(0x07), "a").expect("valid issuer")
}

fn action(payload: ActionPayload) -> Action<ActionPayload> {
    Action {
        sender: addr(0x07),
        nonce: 0,
        signature: None,
        payload,
    }
}

fn main() {
    let actions = vec![
        action(ActionPayload::Transfer {
            to: addr(0x01),
            amount: 1,
        }),
        action(ActionPayload::JoinValidator {
            validator: addr(0x02),
            stake: 1,
            bls_pubkey: vec![0xAA; 4],
            bls_pop: vec![0xAB; 4],
        }),
        action(ActionPayload::LeaveValidator {
            validator: addr(0x02),
        }),
        action(ActionPayload::Stake {
            validator: addr(0x02),
            amount: 1,
        }),
        action(ActionPayload::Unstake {
            validator: addr(0x02),
            amount: 1,
        }),
        action(ActionPayload::SubmitEquivocationEvidence {
            block_a: Box::new(nested_block()),
            block_b: Box::new(nested_block()),
        }),
        action(ActionPayload::RegisterBlsKey {
            validator: addr(0x02),
            pubkey: vec![0xBB; 4],
            pop: vec![0xBC; 4],
        }),
        action(ActionPayload::VerifyIdentityCredential {
            proof: vec![0xCC; 4],
        }),
        action(ActionPayload::AuthorizeOperator {
            operator: addr(0x03),
        }),
        action(ActionPayload::RevokeOperator),
        action(ActionPayload::GrantAttestation {
            subject: addr(0x04),
            hash: "h".to_string(),
            topics: vec![ClaimTopic::Kyc, ClaimTopic::Accredited],
            jurisdiction: Some("CH".to_string()),
        }),
        action(ActionPayload::RevokeAttestation {
            subject: addr(0x04),
        }),
        action(ActionPayload::RegisterAsset {
            asset_id: "a".to_string(),
            compliance_required: true,
            // Every optional field populated on purpose: a `None`/empty
            // metadata would encode as a handful of zero bytes and would not
            // catch a mirror that got the field order wrong.
            metadata: AssetMetadata {
                asset_class: AssetClass::Bond,
                decimals: 6,
                required_claims: vec![ClaimTopic::Kyc, ClaimTopic::Accredited],
                allowed_jurisdictions: Some(vec!["CH".to_string(), "DE".to_string()]),
                max_supply: Some(1_000),
                metadata_uri: Some("ipfs://a".to_string()),
                symbol: "AAA".to_string(),
                name: "Asset A".to_string(),
            },
        }),
        action(ActionPayload::IssueAsset {
            asset: asset(),
            amount: 1,
        }),
        action(ActionPayload::TransferAsset {
            asset: asset(),
            to: addr(0x05),
            amount: 1,
        }),
        action(ActionPayload::RegisterAttestor {
            attestor: addr(0x06),
            name: "n".to_string(),
        }),
        action(ActionPayload::DeregisterAttestor {
            attestor: addr(0x06),
        }),
        action(ActionPayload::SubmitExecutionFault {
            artifact_json: "{}".to_string(),
        }),
        action(ActionPayload::FreezeAsset { asset: asset() }),
        action(ActionPayload::UnfreezeAsset { asset: asset() }),
        action(ActionPayload::ForcedTransfer {
            asset: asset(),
            from: addr(0x04),
            to: addr(0x05),
            amount: 1,
            reason: "court order".to_string(),
        }),
        action(ActionPayload::BurnAsset { asset: asset(), amount: 1 }),
        action(ActionPayload::SetHolderFrozen { asset: asset(), holder: addr(0x04), frozen: true }),
        action(ActionPayload::LockHolderAmount { asset: asset(), holder: addr(0x04), amount: 1 }),
        action(ActionPayload::UnlockHolderAmount { asset: asset(), holder: addr(0x04), amount: 1 }),
        action(ActionPayload::IssuerForcedTransfer {
            asset: asset(),
            from: addr(0x04),
            to: addr(0x05),
            amount: 1,
            reason: "court order".to_string(),
        }),
        action(ActionPayload::RecoverHolder { asset: asset(), lost: addr(0x04), replacement: addr(0x05) }),
        action(ActionPayload::IssueAssetTo { asset: asset(), to: addr(0x05), amount: 1 }),
    ];

    let block = xc_primitives::Block {
        height: 42,
        parent_hash: "0xparent".to_string(),
        timestamp: 7,
        actions,
        tx_root: [0x07; 32],
        proposer: None,
        signature: None,
        state_root: "0xfixture-state-root-5d433074f6447601de4adf45989061ae5551133f".to_string(),
        round: 0,
        round_certificate: None,
    };

    let bytes = bincode::serde::encode_to_vec(&block, bincode::config::standard())
        .expect("block encoding should never fail");

    print!("const V020_BLOCK_FIXTURE: &[u8] = &[");
    for (i, b) in bytes.iter().enumerate() {
        if i % 12 == 0 {
            print!("\n    ");
        }
        print!("{b:#04x}, ");
    }
    println!("\n];");
    println!("// {} bytes, {} actions", bytes.len(), block.actions.len());
    println!("// asset ref for (sender 0x07, \"a\"): {}", asset());
}

fn nested_block() -> xc_primitives::Block<ActionPayload> {
    xc_primitives::Block {
        height: 1,
        parent_hash: "0xgenesis".to_string(),
        timestamp: 0,
        actions: vec![],
        tx_root: [0u8; 32],
        proposer: None,
        signature: None,
        state_root: "0xgenesis-state-root".to_string(),
        round: 0,
        round_certificate: None,
    }
}
