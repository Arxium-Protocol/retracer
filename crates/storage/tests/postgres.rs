//! Integration tests against a real Postgres.
//!
//! Opt-in: set `RETRACER_TEST_DATABASE_URL` (the docker-compose default is
//! `postgres://retracer:retracer@localhost:5433/retracer`). Without it
//! every test here no-ops, because CI runs `cargo test --workspace` inside an
//! image builder with no database.
//!
//! These cover what the unit tests structurally cannot: that the SQL is valid,
//! that `UNNEST` unpacks the batched column vectors in the order the bind list
//! implies, that sqlx encodes `&[serde_json::Value]` as `JSONB[]`, and that a
//! rollback actually removes rows from all four tables.
//!
//! Every test uses a unique `chain_id`, so they are isolated from each other
//! and from leftover data without needing a fresh database per run.

use serde::Serialize;
use sqlx::{PgPool, Row};
use storage::{ActionIndexable, AddressExtractor, KindSchema, Role};
use xc_primitives::{Action, Address, Block};

#[derive(Serialize)]
enum TestPayload {
    Transfer { to: String, amount: u64 },
    Noop,
}

/// `None` when the opt-in env var is unset, which every test treats as "skip".
async fn pool() -> Option<PgPool> {
    let url = std::env::var("RETRACER_TEST_DATABASE_URL").ok()?;
    let pool = storage::connect(&url, 4).await.expect("connect to test database");
    storage::migrate(&pool).await.expect("migrations apply");
    Some(pool)
}

/// Unique per test so tests are isolated without a database reset between them.
fn chain_id(name: &str) -> String {
    format!("test-{name}-{}", std::process::id())
}

fn addr(byte: u8) -> Address {
    Address::from_pubkey_bytes(&[byte; 32]).expect("32 bytes is a valid pubkey")
}

fn action(sender: u8, signature: Option<&str>, payload: TestPayload) -> Action<TestPayload> {
    Action {
        sender: addr(sender),
        nonce: 0,
        signature: signature.map(str::to_string),
        payload,
    }
}

fn block(height: u64, parent: &str, actions: Vec<Action<TestPayload>>) -> Block<TestPayload> {
    Block {
        height,
        parent_hash: parent.to_string(),
        timestamp: 1_700_000_000 + height as i64 as u64,
        actions,
        proposer: Some(addr(9)),
        signature: None,
    }
}

async fn count(pool: &PgPool, table: &str, chain: &str) -> i64 {
    sqlx::query(&format!("SELECT COUNT(*) FROM {table} WHERE chain_id = $1"))
        .bind(chain)
        .fetch_one(pool)
        .await
        .expect("count query")
        .get::<i64, _>(0)
}

macro_rules! skip_without_db {
    () => {
        match pool().await {
            Some(pool) => pool,
            None => {
                eprintln!("skipping: RETRACER_TEST_DATABASE_URL not set");
                return;
            }
        }
    };
}

#[tokio::test]
async fn migrations_are_idempotent() {
    let pool = skip_without_db!();
    // `pool()` already migrated once; a second run must be a no-op rather than
    // an error, since every process start calls it.
    storage::migrate(&pool).await.expect("second migrate is a no-op");

    for table in ["chains", "blocks", "actions", "account_actions", "action_addresses", "ingestion_cursor"] {
        let exists: bool = sqlx::query(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = $1)",
        )
        .bind(table)
        .fetch_one(&pool)
        .await
        .expect("table lookup")
        .get(0);
        assert!(exists, "{table} missing from the merged schema");
    }
}

#[tokio::test]
async fn register_chain_inserts_then_updates() {
    let pool = skip_without_db!();
    let chain = chain_id("register");

    storage::register_chain(&pool, &chain, Some("First"), "topic/v1", "/sync/1", 0)
        .await
        .expect("insert");
    storage::register_chain(&pool, &chain, Some("Renamed"), "topic/v2", "/sync/2", 7)
        .await
        .expect("upsert");

    let row = sqlx::query(
        "SELECT display_name, blocks_topic, sync_protocol, finality_depth
         FROM chains WHERE chain_id = $1",
    )
    .bind(&chain)
    .fetch_one(&pool)
    .await
    .expect("row");

    // Config is the source of truth, so every field is overwritten on restart.
    assert_eq!(row.get::<Option<String>, _>(0).as_deref(), Some("Renamed"));
    assert_eq!(row.get::<String, _>(1), "topic/v2");
    assert_eq!(row.get::<String, _>(2), "/sync/2");
    assert_eq!(row.get::<i64, _>(3), 7);
}

/// The batched-write path end to end. This is what the unit tests could not
/// reach: valid SQL, `JSONB[]` encoding, and `UNNEST` column ordering.
#[tokio::test]
async fn insert_block_writes_every_table_with_aligned_rows() {
    let pool = skip_without_db!();
    let chain = chain_id("insert");

    /// Resolves the recipient of a Transfer, so `action_addresses` gets rows
    /// with a fan-out that differs from the action count.
    struct Recipients;
    impl ActionIndexable for Recipients {
        fn kind(&self) -> &str {
            "Transfer"
        }
        fn resolve(&self, payload: &serde_json::Value) -> Vec<(String, Role)> {
            payload
                .get("to")
                .and_then(|v| v.as_str())
                .map(|to| vec![(to.to_string(), Role::To)])
                .unwrap_or_default()
        }
    }
    let extractor = AddressExtractor::new(KindSchema::empty(), vec![Box::new(Recipients)]);

    let b = block(
        0,
        "0x0",
        vec![
            action(1, Some("sig-a"), TestPayload::Transfer { to: "arx1dest".into(), amount: 500 }),
            // Unsigned: must key on its position, not collide with the next one.
            action(2, None, TestPayload::Noop),
            action(3, None, TestPayload::Noop),
        ],
    );
    storage::insert_block(&pool, &chain, &b, &extractor).await.expect("insert_block");

    assert_eq!(count(&pool, "blocks", &chain).await, 1);
    assert_eq!(count(&pool, "actions", &chain).await, 3, "no action may be lost to a key collision");
    assert_eq!(count(&pool, "account_actions", &chain).await, 3);
    assert_eq!(count(&pool, "action_addresses", &chain).await, 1, "only the Transfer resolves a recipient");

    // Identity: signature where present, position where not.
    let hashes: Vec<String> = sqlx::query(
        "SELECT action_hash FROM actions WHERE chain_id = $1 ORDER BY index_in_block",
    )
    .bind(&chain)
    .fetch_all(&pool)
    .await
    .expect("hashes")
    .into_iter()
    .map(|r| r.get(0))
    .collect();
    assert_eq!(hashes, vec!["sig-a", "0:1", "0:2"]);

    // Alignment: each row's payload, kind and sender must belong to the same
    // action as its hash. A mis-ordered UNNEST would still produce valid rows.
    let row = sqlx::query(
        "SELECT kind, from_address, payload FROM actions WHERE chain_id = $1 AND action_hash = 'sig-a'",
    )
    .bind(&chain)
    .fetch_one(&pool)
    .await
    .expect("transfer row");
    assert_eq!(row.get::<String, _>(0), "Transfer");
    assert_eq!(row.get::<String, _>(1), addr(1).to_string());
    let payload: serde_json::Value = row.get(2);
    assert_eq!(payload["amount"], 500, "payload must land on its own action");
    assert_eq!(payload["to"], "arx1dest");

    // The resolved recipient is attributed to the Transfer, not another action.
    let addr_row = sqlx::query(
        "SELECT action_hash, address, role FROM action_addresses WHERE chain_id = $1",
    )
    .bind(&chain)
    .fetch_one(&pool)
    .await
    .expect("address row");
    assert_eq!(addr_row.get::<String, _>(0), "sig-a");
    assert_eq!(addr_row.get::<String, _>(1), "arx1dest");
    assert_eq!(addr_row.get::<String, _>(2), "to");

    assert_eq!(storage::get_cursor(&pool, &chain).await.expect("cursor"), Some(0));
}

#[tokio::test]
async fn insert_block_is_idempotent_on_redelivery() {
    let pool = skip_without_db!();
    let chain = chain_id("idempotent");
    let extractor = AddressExtractor::tier_a_only(KindSchema::empty());

    let b = block(0, "0x0", vec![action(1, None, TestPayload::Noop), action(2, None, TestPayload::Noop)]);
    storage::insert_block(&pool, &chain, &b, &extractor).await.expect("first");
    storage::insert_block(&pool, &chain, &b, &extractor).await.expect("redelivery must not error");

    assert_eq!(count(&pool, "actions", &chain).await, 2, "redelivery must not duplicate");
    assert_eq!(count(&pool, "account_actions", &chain).await, 2);
}

/// A malformed block carrying the same action twice must skip the duplicate,
/// not abort the statement. This is the case that would raise SQLSTATE 21000
/// if the batched insert used DO UPDATE.
#[tokio::test]
async fn duplicate_action_within_one_block_is_skipped_not_fatal() {
    let pool = skip_without_db!();
    let chain = chain_id("dupe");
    let extractor = AddressExtractor::tier_a_only(KindSchema::empty());

    let b = block(
        0,
        "0x0",
        vec![action(1, Some("same"), TestPayload::Noop), action(2, Some("same"), TestPayload::Noop)],
    );
    storage::insert_block(&pool, &chain, &b, &extractor).await.expect("must not error");
    assert_eq!(count(&pool, "actions", &chain).await, 1, "second occurrence is skipped");
}

#[tokio::test]
async fn rollback_removes_rows_above_the_height_and_rewinds_the_cursor() {
    let pool = skip_without_db!();
    let chain = chain_id("rollback");
    let extractor = AddressExtractor::tier_a_only(KindSchema::empty());

    let mut parent = "0x0".to_string();
    for height in 0..5u64 {
        let b = block(height, &parent, vec![action(1, Some(&format!("sig-{height}")), TestPayload::Noop)]);
        parent = b.hash();
        storage::insert_block(&pool, &chain, &b, &extractor).await.expect("insert");
    }
    assert_eq!(count(&pool, "blocks", &chain).await, 5);

    let removed = storage::rollback_to(&pool, &chain, 2).await.expect("rollback");

    assert_eq!(removed, 2, "heights 3 and 4");
    assert_eq!(count(&pool, "blocks", &chain).await, 3);
    assert_eq!(count(&pool, "actions", &chain).await, 3);
    assert_eq!(count(&pool, "account_actions", &chain).await, 3);
    assert_eq!(storage::get_cursor(&pool, &chain).await.expect("cursor"), Some(2));

    // Re-indexing the rolled-back heights must work — this is the path a real
    // reorg takes after ingestion rewinds.
    let tip_hash = storage::get_block_hash(&pool, &chain, 2).await.expect("hash").expect("present");
    let replacement = block(3, &tip_hash, vec![action(1, Some("sig-3-alt"), TestPayload::Noop)]);
    storage::insert_block(&pool, &chain, &replacement, &extractor).await.expect("re-index");
    assert_eq!(count(&pool, "blocks", &chain).await, 4);
}

/// `rollback_to(-1)` means nothing is indexed, which is the absence of a cursor
/// rather than a cursor holding a negative height.
#[tokio::test]
async fn rollback_below_genesis_clears_the_cursor_entirely() {
    let pool = skip_without_db!();
    let chain = chain_id("rollback-genesis");
    let extractor = AddressExtractor::tier_a_only(KindSchema::empty());

    let b = block(0, "0x0", vec![action(1, Some("sig"), TestPayload::Noop)]);
    storage::insert_block(&pool, &chain, &b, &extractor).await.expect("insert");

    storage::rollback_to(&pool, &chain, -1).await.expect("rollback");

    assert_eq!(count(&pool, "blocks", &chain).await, 0);
    assert_eq!(storage::get_cursor(&pool, &chain).await.expect("cursor"), None);
}

#[tokio::test]
async fn projection_indexes_are_created_and_recreating_them_is_safe() {
    let pool = skip_without_db!();

    let path = std::env::temp_dir().join(format!("retracer_proj_{}.toml", std::process::id()));
    std::fs::write(
        &path,
        r#"
        [[kind]]
        name = "Transfer"
          [[kind.index]]
          path = "$.amount"
          type = "numeric"
        "#,
    )
    .expect("write schema");
    let schema = KindSchema::load(&path).expect("load schema");
    let expected = schema.projections()[0].index_name();

    let created = storage::create_projection_indexes(&pool, &schema).await.expect("create");
    assert_eq!(created, 1);

    let exists: bool = sqlx::query("SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = $1)")
        .bind(&expected)
        .fetch_one(&pool)
        .await
        .expect("index lookup")
        .get(0);
    assert!(exists, "expected index {expected} to exist");

    // Every process start runs this, so it has to be safe to repeat.
    storage::create_projection_indexes(&pool, &schema).await.expect("idempotent");

    sqlx::query(&format!("DROP INDEX IF EXISTS {expected}")).execute(&pool).await.ok();
    std::fs::remove_file(&path).ok();
}

/// The read path that migration 0001's `actions_chain_height_idx` exists for,
/// and the one place a correlated subquery could silently return the wrong
/// count.
#[tokio::test]
async fn read_queries_return_what_was_written() {
    let pool = skip_without_db!();
    let chain = chain_id("reads");
    let extractor = AddressExtractor::tier_a_only(KindSchema::empty());

    let b0 = block(0, "0x0", vec![]);
    let parent = b0.hash();
    storage::insert_block(&pool, &chain, &b0, &extractor).await.expect("genesis");
    let b1 = block(
        1,
        &parent,
        vec![action(1, Some("x"), TestPayload::Noop), action(2, Some("y"), TestPayload::Noop)],
    );
    storage::insert_block(&pool, &chain, &b1, &extractor).await.expect("block 1");

    let summaries = storage::list_blocks(&pool, &chain, 10, None).await.expect("list_blocks");
    assert_eq!(summaries.len(), 2);
    assert_eq!(summaries[0].height, 1, "newest first");
    assert_eq!(summaries[0].action_count, 2);
    assert_eq!(summaries[1].action_count, 0, "an empty block counts zero, not NULL");

    let full = storage::get_block_by_height(&pool, &chain, 1)
        .await
        .expect("query")
        .expect("block 1 present");
    assert_eq!(full.actions.len(), 2);

    let sender_actions = storage::get_account_actions(
        &pool,
        &chain,
        &addr(1).to_string(),
        10,
        None,
        None,
    )
    .await
    .expect("account actions");
    assert_eq!(sender_actions.len(), 1);
    assert_eq!(sender_actions[0].action_hash, "x");
}
