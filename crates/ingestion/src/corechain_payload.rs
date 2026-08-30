use serde::{Deserialize, Serialize};
use xc_primitives::{Address, Block};

/// CoreChain's address format: `arx1` bech32 over an ed25519 pubkey. Lives here
/// with `ActionPayload` because it's the same kind of thing — the CoreChain
/// instantiation of something the indexer itself treats as chain-specific
/// (`storage::AddressValidator`).
pub fn is_corechain_address(candidate: &str) -> bool {
    Address::parse(candidate).is_ok()
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
    },
    VerifyIdentityCredential {
        proof: Vec<u8>,
    },
    AuthorizeOperator {
        operator: Address,
    },
    RevokeOperator,
}
