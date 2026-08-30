//! Multi-chain `Runner` against a real Postgres.
//!
//! Opt-in via `RETRACER_TEST_DATABASE_URL`, same as `storage`'s integration
//! tests — CI runs `cargo test --workspace` with no database.
//!
//! `run()` blocks until a chain task ends, so these stop at `add_chain`: what
//! they establish is that two chains with *different payload types* can be
//! registered against one Runner, which is the claim the type signature makes
//! and the reason multi-chain isn't expressible as a config file.

use retracer_core::{ChainConfig, ChainHooks, NodeRpcToken, Runner};
use serde::{Deserialize, Serialize};
use sqlx::Row;
use xc_primitives::Block;

#[derive(Serialize, Deserialize)]
enum HubPayload {
    Transfer { amount: u64 },
}

/// Shares no variants with `HubPayload` — the point being that one Runner
/// holds pipelines for both.
#[derive(Serialize, Deserialize)]
enum SpokePayload {
    MintNft { token_id: u64 },
}

fn config(chain_id: &str, name: &str) -> ChainConfig {
    ChainConfig {
        chain_id: chain_id.to_string(),
        display_name: Some(name.to_string()),
        bootnodes: Vec::new(),
        // 0 = pick a free port; these listeners just idle, nothing dials them.
        port: 0,
        blocks_topic: format!("{chain_id}/blocks/v1"),
        sync_protocol: format!("/{chain_id}/sync/1"),
        max_pending_blocks: 128,
        finality_depth: 12,
        kind_schema: "does-not-exist.toml".to_string(),
        node_rpc_url: None,
        node_rpc_token: None,
    }
}

#[test]
fn node_rpc_token_is_available_through_the_multichain_api() {
    let mut hub = config("secured-hub", "Secured Hub");
    let mut spoke = config("secured-spoke", "Secured Spoke");
    hub.node_rpc_token = Some(NodeRpcToken::new("hub-secret".into()).unwrap());
    spoke.node_rpc_token = Some(NodeRpcToken::new("spoke-secret".into()).unwrap());

    assert_ne!(hub.node_rpc_token, spoke.node_rpc_token);
    let hub_debug = format!("{:?}", hub.node_rpc_token.as_ref().unwrap());
    let spoke_debug = format!("{:?}", spoke.node_rpc_token.as_ref().unwrap());
    assert_eq!(hub_debug, "NodeRpcToken([REDACTED])");
    assert_eq!(spoke_debug, "NodeRpcToken([REDACTED])");
    assert!(!hub_debug.contains("hub-secret"));
    assert!(!spoke_debug.contains("spoke-secret"));
}

macro_rules! runner_or_skip {
    ($url:ident) => {
        match std::env::var("RETRACER_TEST_DATABASE_URL") {
            Ok(url) => {
                let $url = url;
                Runner::new(&$url, 2, 2, 0).await.expect("runner")
            }
            Err(_) => {
                eprintln!("skipping: RETRACER_TEST_DATABASE_URL not set");
                return;
            }
        }
    };
}

#[tokio::test]
async fn one_runner_follows_two_chains_with_different_payload_types() {
    let mut runner = runner_or_skip!(url);
    let pid = std::process::id();
    let hub = format!("mc-hub-{pid}");
    let spoke = format!("mc-spoke-{pid}");

    runner
        .add_chain::<Block<HubPayload>>(config(&hub, "Hub"), ChainHooks::default())
        .await
        .expect("hub registers");
    // Different `B` on the same Runner — this is the whole feature.
    runner
        .add_chain::<Block<SpokePayload>>(config(&spoke, "Spoke A"), ChainHooks::default())
        .await
        .expect("spoke registers");

    let pool = storage::connect(&std::env::var("RETRACER_TEST_DATABASE_URL").unwrap(), 2)
        .await
        .expect("verify pool");
    let rows = sqlx::query(
        "SELECT chain_id, display_name, blocks_topic, finality_depth
         FROM chains WHERE chain_id = ANY($1) ORDER BY chain_id",
    )
    .bind(vec![hub.clone(), spoke.clone()])
    .fetch_all(&pool)
    .await
    .expect("chains rows");

    assert_eq!(rows.len(), 2, "both chains must be discoverable");
    assert_eq!(rows[0].get::<String, _>(0), hub);
    assert_eq!(rows[0].get::<Option<String>, _>(1).as_deref(), Some("Hub"));
    assert_eq!(rows[0].get::<String, _>(2), format!("{hub}/blocks/v1"));
    assert_eq!(rows[0].get::<i64, _>(3), 12);
    assert_eq!(rows[1].get::<String, _>(0), spoke);
}

/// Two pipelines writing one `chain_id` would interleave rollbacks and corrupt
/// the index, so the second registration is refused rather than allowed to
/// race.
#[tokio::test]
async fn adding_the_same_chain_twice_is_refused() {
    let mut runner = runner_or_skip!(url);
    let chain = format!("mc-dupe-{}", std::process::id());

    runner
        .add_chain::<Block<HubPayload>>(config(&chain, "First"), ChainHooks::default())
        .await
        .expect("first registration");

    let err = runner
        .add_chain::<Block<HubPayload>>(config(&chain, "Second"), ChainHooks::default())
        .await
        .expect_err("duplicate must be refused");
    assert!(format!("{err:#}").contains("added twice"), "got: {err:#}");
}
