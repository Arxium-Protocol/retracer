//! CoreChain's payload comes from the node's own `arxd-payload` crate — the
//! fixture test in `lib.rs` is now a decode smoke test across every variant,
//! not the only thing standing between an appended variant and misdecoded
//! blocks.
pub use arxd_payload::ActionPayload;
pub use xc_primitives::AssetRef;

pub fn is_corechain_address(candidate: &str) -> bool {
    xc_primitives::Address::parse(candidate).is_ok()
}
