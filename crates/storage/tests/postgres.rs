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
use storage::testing::{TestAction, TestBlock};
use storage::{ActionIndexable, AddressExtractor, KindSchema, Role};

#[derive(Serialize)]
enum TestPayload {
    Transfer { to: String, amount: u64 },
    Noop,
}

/// `None` when the opt-in env var is unset, which every test treats as "skip".
async fn pool() -> Option<PgPool> {
    let url = std::env::var("RETRACER_TEST_DATABASE_URL").ok()?;
    let pool = storage::connect(&url, 4)
        .await
        .expect("connect to test database");
    storage::migrate(&pool).await.expect("migrations apply");
    Some(pool)
}

/// Unique per test so tests are isolated without a database reset between them.
fn chain_id(name: &str) -> String {
    format!("test-{name}-{}", std::process::id())
}

fn addr(byte: u8) -> String {
    format!("arx1test{byte:02x}")
}

fn action(sender: u8, signature: Option<&str>, payload: TestPayload) -> TestAction {
    TestAction {
        sender: addr(sender),
        signature: signature.map(str::to_string),
        payload: serde_json::to_value(payload).expect("payload serializes"),
    }
}

fn block(height: u64, parent: &str, actions: Vec<TestAction>) -> TestBlock {
    TestBlock {
        height,
        parent_hash: parent.to_string(),
        timestamp: 1_700_000_000 + height,
        proposer: Some(addr(9)),
        round: 0,
        actions,
        effects: None,
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
    storage::migrate(&pool)
        .await
        .expect("second migrate is a no-op");

    for table in [
        "chains",
        "blocks",
        "actions",
        "account_actions",
        "action_addresses",
        "ingestion_cursor",
    ] {
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
            action(
                1,
                Some("sig-a"),
                TestPayload::Transfer {
                    to: "arx1dest".into(),
                    amount: 500,
                },
            ),
            // Unsigned: must key on its position, not collide with the next one.
            action(2, None, TestPayload::Noop),
            action(3, None, TestPayload::Noop),
        ],
    );
    storage::insert_block(&pool, &chain, &b, &extractor)
        .await
        .expect("insert_block");

    assert_eq!(count(&pool, "blocks", &chain).await, 1);
    assert_eq!(
        count(&pool, "actions", &chain).await,
        3,
        "no action may be lost to a key collision"
    );
    assert_eq!(count(&pool, "account_actions", &chain).await, 3);
    assert_eq!(
        count(&pool, "action_addresses", &chain).await,
        1,
        "only the Transfer resolves a recipient"
    );

    // Identity: signature where present, position where not.
    let hashes: Vec<String> =
        sqlx::query("SELECT action_hash FROM actions WHERE chain_id = $1 ORDER BY index_in_block")
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
    assert_eq!(
        payload["amount"], 500,
        "payload must land on its own action"
    );
    assert_eq!(payload["to"], "arx1dest");

    // The resolved recipient is attributed to the Transfer, not another action.
    let addr_row =
        sqlx::query("SELECT action_hash, address, role FROM action_addresses WHERE chain_id = $1")
            .bind(&chain)
            .fetch_one(&pool)
            .await
            .expect("address row");
    assert_eq!(addr_row.get::<String, _>(0), "sig-a");
    assert_eq!(addr_row.get::<String, _>(1), "arx1dest");
    assert_eq!(addr_row.get::<String, _>(2), "to");

    assert_eq!(
        storage::get_cursor(&pool, &chain).await.expect("cursor"),
        Some(0)
    );
}

#[tokio::test]
async fn insert_block_is_idempotent_on_redelivery() {
    let pool = skip_without_db!();
    let chain = chain_id("idempotent");
    let extractor = AddressExtractor::tier_a_only(KindSchema::empty());

    let b = block(
        0,
        "0x0",
        vec![
            action(1, None, TestPayload::Noop),
            action(2, None, TestPayload::Noop),
        ],
    );
    storage::insert_block(&pool, &chain, &b, &extractor)
        .await
        .expect("first");
    storage::insert_block(&pool, &chain, &b, &extractor)
        .await
        .expect("redelivery must not error");

    assert_eq!(
        count(&pool, "actions", &chain).await,
        2,
        "redelivery must not duplicate"
    );
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
        vec![
            action(1, Some("same"), TestPayload::Noop),
            action(2, Some("same"), TestPayload::Noop),
        ],
    );
    storage::insert_block(&pool, &chain, &b, &extractor)
        .await
        .expect("must not error");
    assert_eq!(
        count(&pool, "actions", &chain).await,
        1,
        "second occurrence is skipped"
    );
}

#[tokio::test]
async fn rollback_removes_rows_above_the_height_and_rewinds_the_cursor() {
    let pool = skip_without_db!();
    let chain = chain_id("rollback");
    let extractor = AddressExtractor::tier_a_only(KindSchema::empty());

    let mut parent = "0x0".to_string();
    for height in 0..5u64 {
        let b = block(
            height,
            &parent,
            vec![action(1, Some(&format!("sig-{height}")), TestPayload::Noop)],
        );
        parent = b.hash();
        storage::insert_block(&pool, &chain, &b, &extractor)
            .await
            .expect("insert");
    }
    assert_eq!(count(&pool, "blocks", &chain).await, 5);

    let removed = storage::rollback_to(&pool, &chain, 2)
        .await
        .expect("rollback");

    assert_eq!(removed, 2, "heights 3 and 4");
    assert_eq!(count(&pool, "blocks", &chain).await, 3);
    assert_eq!(count(&pool, "actions", &chain).await, 3);
    assert_eq!(count(&pool, "account_actions", &chain).await, 3);
    assert_eq!(
        storage::get_cursor(&pool, &chain).await.expect("cursor"),
        Some(2)
    );

    // Re-indexing the rolled-back heights must work — this is the path a real
    // reorg takes after ingestion rewinds.
    let tip_hash = storage::get_block_hash(&pool, &chain, 2)
        .await
        .expect("hash")
        .expect("present");
    let replacement = block(
        3,
        &tip_hash,
        vec![action(1, Some("sig-3-alt"), TestPayload::Noop)],
    );
    storage::insert_block(&pool, &chain, &replacement, &extractor)
        .await
        .expect("re-index");
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
    storage::insert_block(&pool, &chain, &b, &extractor)
        .await
        .expect("insert");

    storage::rollback_to(&pool, &chain, -1)
        .await
        .expect("rollback");

    assert_eq!(count(&pool, "blocks", &chain).await, 0);
    assert_eq!(
        storage::get_cursor(&pool, &chain).await.expect("cursor"),
        None
    );
}

#[tokio::test]
async fn projection_indexes_are_created_and_recreating_them_is_safe() {
    let pool = skip_without_db!();

    // Exercises the create/verify/drop mechanics only, not filtering, so the
    // kind and field are synthetic and unique to this test. `index_name()`
    // hashes kind+path+type into a name on the shared `actions` table, not
    // scoped by chain_id like the rows are — reusing `Transfer`/`$.amount`
    // (as `kind_field_filter_and_reindex` does) raced against that test's
    // own create/reindex/drop under the default parallel test runner.
    let path = std::env::temp_dir().join(format!(
        "retracer_proj_{}_{:?}.toml",
        std::process::id(),
        std::thread::current().id()
    ));
    std::fs::write(
        &path,
        r#"
        [[kind]]
        name = "ProjectionLifecycleTestKind"
          [[kind.index]]
          path = "$.marker"
          type = "text"
        "#,
    )
    .expect("write schema");
    let schema = KindSchema::load(&path).expect("load schema");
    let expected = schema.projections()[0].index_name();

    let created = storage::create_projection_indexes(&pool, &schema)
        .await
        .expect("create");
    assert_eq!(created, 1);

    let exists: bool = sqlx::query("SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = $1)")
        .bind(&expected)
        .fetch_one(&pool)
        .await
        .expect("index lookup")
        .get(0);
    assert!(exists, "expected index {expected} to exist");

    // Every process start runs this, so it has to be safe to repeat.
    storage::create_projection_indexes(&pool, &schema)
        .await
        .expect("idempotent");

    sqlx::query(&format!("DROP INDEX IF EXISTS {expected}"))
        .execute(&pool)
        .await
        .ok();
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
    storage::insert_block(&pool, &chain, &b0, &extractor)
        .await
        .expect("genesis");
    let b1 = block(
        1,
        &parent,
        vec![
            action(1, Some("x"), TestPayload::Noop),
            action(2, Some("y"), TestPayload::Noop),
        ],
    );
    storage::insert_block(&pool, &chain, &b1, &extractor)
        .await
        .expect("block 1");

    let summaries = storage::list_blocks(&pool, &chain, 10, None)
        .await
        .expect("list_blocks");
    assert_eq!(summaries.len(), 2);
    assert_eq!(summaries[0].height, 1, "newest first");
    assert_eq!(summaries[0].action_count, 2);
    assert_eq!(
        summaries[1].action_count, 0,
        "an empty block counts zero, not NULL"
    );

    let full = storage::get_block_by_height(&pool, &chain, 1)
        .await
        .expect("query")
        .expect("block 1 present");
    assert_eq!(full.actions.len(), 2);

    let sender_actions =
        storage::get_account_actions(&pool, &chain, &addr(1).to_string(), 10, None, None)
            .await
            .expect("account actions");
    assert_eq!(sender_actions.len(), 1);
    assert_eq!(sender_actions[0].action_hash, "x");
}

#[tokio::test]
async fn list_proposed_heights_respects_bounds_and_skips_null_proposers() {
    let pool = skip_without_db!();
    let chain = chain_id("proposed-heights");
    let extractor = AddressExtractor::tier_a_only(KindSchema::empty());

    let block_at = |height: u64, parent: &str, proposer: Option<u8>, round: u32| -> TestBlock {
        TestBlock {
            height,
            parent_hash: parent.to_string(),
            timestamp: 1_700_000_000 + height,
            proposer: proposer.map(addr),
            round,
            actions: vec![],
            effects: None,
        }
    };

    // Height 0 has no proposer (e.g. genesis) and must never appear as owed.
    let b0 = block_at(0, "0x0", None, 0);
    let parent = b0.hash();
    storage::insert_block(&pool, &chain, &b0, &extractor)
        .await
        .expect("genesis");
    let b1 = block_at(1, &parent, Some(1), 0);
    let parent = b1.hash();
    storage::insert_block(&pool, &chain, &b1, &extractor)
        .await
        .expect("block 1");
    let b2 = block_at(2, &parent, Some(2), 1);
    storage::insert_block(&pool, &chain, &b2, &extractor)
        .await
        .expect("block 2");

    let full = storage::list_proposed_heights(&pool, &chain, 0, 2)
        .await
        .expect("full range");
    assert_eq!(
        full,
        vec![
            storage::ProposedHeight {
                height: 1,
                proposer: addr(1).to_string(),
                round: 0,
            },
            storage::ProposedHeight {
                height: 2,
                proposer: addr(2).to_string(),
                round: 1,
            },
        ]
    );

    let narrowed = storage::list_proposed_heights(&pool, &chain, 0, 1)
        .await
        .expect("narrowed range");
    assert_eq!(
        narrowed,
        vec![storage::ProposedHeight {
            height: 1,
            proposer: addr(1).to_string(),
            round: 0,
        }]
    );
}

#[tokio::test]
async fn insert_block_persists_round() {
    let pool = skip_without_db!();
    let chain = chain_id("block-round");
    let extractor = AddressExtractor::tier_a_only(KindSchema::empty());

    let block = TestBlock {
        height: 0,
        parent_hash: "0x0".to_string(),
        timestamp: 1_700_000_000,
        proposer: Some(addr(1)),
        round: 2,
        actions: vec![],
        effects: None,
    };
    storage::insert_block(&pool, &chain, &block, &extractor)
        .await
        .expect("insert block with round 2");

    let (round,): (i64,) =
        sqlx::query_as("SELECT round FROM blocks WHERE chain_id = $1 AND height = $2")
            .bind(&chain)
            .bind(0i64)
            .fetch_one(&pool)
            .await
            .expect("select round");
    assert_eq!(round, 2);
}

#[tokio::test]
async fn table_sizes_and_database_size_report_every_table() {
    let pool = skip_without_db!();

    let sizes = storage::table_sizes(&pool)
        .await
        .expect("table sizes query");
    assert_eq!(sizes.len(), 6);

    let db_size = storage::database_size_bytes(&pool)
        .await
        .expect("database size query");
    assert!(db_size > 0);
}

/// The `?kind=&field=&value=` path: the filter must use the same expression
/// the projection index was declared with, and `reindex_action_addresses`
/// must add the rows a newly declared role implies without touching the
/// ones already there.
#[tokio::test]
async fn kind_field_filter_and_reindex() {
    let pool = skip_without_db!();
    let chain = chain_id("filter");

    // Ingest with a schema that indexes `$.amount` but declares no roles.
    let path = std::env::temp_dir().join(format!("retracer_filter_{}.toml", std::process::id()));
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
    let projection = schema.projections()[0].clone();
    storage::create_projection_indexes(&pool, &schema)
        .await
        .expect("indexes");
    let extractor = AddressExtractor::new(schema, Vec::new());

    let to = addr(7).to_string();
    let b1 = block(
        1,
        "0x00",
        vec![
            action(
                1,
                Some("s1"),
                TestPayload::Transfer {
                    to: to.clone(),
                    amount: 5,
                },
            ),
            action(
                1,
                Some("s2"),
                TestPayload::Transfer {
                    to: to.clone(),
                    amount: 9,
                },
            ),
            action(2, Some("s3"), TestPayload::Noop),
        ],
    );
    storage::insert_block(&pool, &chain, &b1, &extractor)
        .await
        .expect("insert");

    let all = storage::list_actions(&pool, &chain, 10, None, None)
        .await
        .expect("all");
    assert_eq!(all.len(), 3);

    let by_kind = storage::ActionFilter {
        kind: "Transfer",
        projection: None,
        value: None,
    };
    let transfers = storage::list_actions(&pool, &chain, 10, None, Some(&by_kind))
        .await
        .expect("kind");
    assert_eq!(transfers.len(), 2);

    let by_field = storage::ActionFilter {
        kind: "Transfer",
        projection: Some(&projection),
        value: Some("9"),
    };
    let nine = storage::list_actions(&pool, &chain, 10, None, Some(&by_field))
        .await
        .expect("field");
    assert_eq!(nine.len(), 1);
    assert_eq!(nine[0].action_hash, "s2");

    // No roles declared, so nothing in action_addresses yet.
    assert_eq!(count(&pool, "action_addresses", &chain).await, 0);

    // Operator adds a `to` role and reindexes: the two transfers gain rows,
    // the Noop does not, and running it again adds nothing.
    std::fs::write(
        &path,
        r#"
        [[kind]]
        name = "Transfer"
          [[kind.roles]]
          path = "$.to"
          role = "to"
        "#,
    )
    .expect("write schema");
    let extractor = AddressExtractor::new(KindSchema::load(&path).expect("reload"), Vec::new());
    let added = storage::reindex_action_addresses(&pool, &chain, &extractor)
        .await
        .expect("reindex");
    assert_eq!(added, 2);
    assert_eq!(count(&pool, "action_addresses", &chain).await, 2);
    let again = storage::reindex_action_addresses(&pool, &chain, &extractor)
        .await
        .expect("reindex again");
    assert_eq!(again, 0);

    let received = storage::get_account_actions(&pool, &chain, &to, 10, None, Some("to"))
        .await
        .expect("received");
    assert_eq!(received.len(), 2);

    sqlx::query(&format!("DROP INDEX IF EXISTS {}", projection.index_name()))
        .execute(&pool)
        .await
        .ok();
    std::fs::remove_file(&path).ok();
}

/// `get_block_by_hash`/`get_action_by_hash` normalize the lookup argument
/// before querying (see `canonicalize_hash`), so a differently cased or
/// `0x`-less query still finds a value stored canonically — true for a
/// block hash (always `block.hash()`'s output) and for a signed action's
/// hash (its signature, always `hex::encode`'s lowercase output). An
/// unsigned action's positional `"{height}:{index}"` identity is not a hash
/// and must still match itself byte-for-byte — normalization must not touch it.
#[tokio::test]
async fn by_hash_lookups_ignore_case_and_prefix_but_leave_non_hex_identities_alone() {
    let pool = skip_without_db!();
    let chain = chain_id("by-hash");
    let extractor = AddressExtractor::tier_a_only(KindSchema::empty());

    let b0 = block(0, "0x0", vec![]);
    storage::insert_block(&pool, &chain, &b0, &extractor)
        .await
        .expect("genesis");
    let parent = b0.hash();
    let b1 = block(
        1,
        &parent,
        vec![
            action(1, Some("0xabcdef"), TestPayload::Noop),
            action(2, None, TestPayload::Noop),
        ],
    );
    storage::insert_block(&pool, &chain, &b1, &extractor)
        .await
        .expect("block 1");

    let by_exact_case = storage::get_block_by_hash(&pool, &chain, &parent)
        .await
        .expect("query")
        .expect("genesis present by its own hash");
    assert_eq!(by_exact_case.height, 0);

    let mixed = if parent.chars().any(|c| c.is_ascii_lowercase()) {
        parent.to_ascii_uppercase()
    } else {
        parent.to_ascii_lowercase()
    };
    let by_different_case = storage::get_block_by_hash(&pool, &chain, &mixed)
        .await
        .expect("query")
        .expect("genesis present regardless of query case");
    assert_eq!(by_different_case.height, 0);

    let without_prefix = parent.trim_start_matches("0x");
    let by_no_prefix = storage::get_block_by_hash(&pool, &chain, without_prefix)
        .await
        .expect("query")
        .expect("genesis present without an explicit 0x prefix");
    assert_eq!(by_no_prefix.height, 0);

    // The signed action's hash (its signature) is real hex, stored canonically
    // (as `hex::encode` always produces it) — a differently-cased query must
    // still find it, and so must one without the `0x` prefix.
    let signed = storage::get_action_by_hash(&pool, &chain, "0xABCDEF")
        .await
        .expect("query")
        .expect("signed action present regardless of query case");
    assert_eq!(signed.action_hash, "0xabcdef");
    let signed_no_prefix = storage::get_action_by_hash(&pool, &chain, "ABCDEF")
        .await
        .expect("query")
        .expect("signed action present without an explicit 0x prefix");
    assert_eq!(signed_no_prefix.action_hash, "0xabcdef");

    // The unsigned action's positional identity is "1:1" — not hex, so it
    // must be looked up byte-for-byte, and a case/prefix change to it is a
    // different (missing) identity, not the same one.
    let unsigned = storage::get_action_by_hash(&pool, &chain, "1:1")
        .await
        .expect("query")
        .expect("unsigned action present under its exact positional identity");
    assert_eq!(unsigned.action_hash, "1:1");
    assert!(
        storage::get_action_by_hash(&pool, &chain, "0x1:1")
            .await
            .expect("query")
            .is_none(),
        "a positional identity is not a hash and must not gain a 0x prefix"
    );
}

/// The state tables (`0003_state.sql`): every effects list lands in its table
/// with a u128 balance intact, `total_accounts` counts state rows rather than
/// addresses seen in actions, redelivery is a no-op, and a rollback removes
/// the state rows above the height along with the history rows.
#[tokio::test]
async fn effects_write_state_tables_and_roll_back_with_the_block() {
    let pool = skip_without_db!();
    let chain = chain_id("effects");
    let extractor = AddressExtractor::tier_a_only(KindSchema::empty());

    let effects = |height: u64| -> storage::BlockEffects {
        serde_json::from_value(serde_json::json!({
            "accounts": {
                addr(1): {"balance": u128::MAX, "nonce": height},
                addr(2): {"balance": 400, "nonce": 0},
            },
            "asset_balances": [{"asset": "gold", "owner": addr(2), "balance": 5}],
            "holder_states": [{"asset": "gold", "holder": addr(2), "state": {"frozen": false}}],
            "stakes": [{"master": addr(1), "validator": addr(9), "allocation": null}],
            "validator_statuses": {addr(9): "Active"},
            "validator_set": {addr(9): 10000},
            "asset_registrations": [{"id": "gold"}],
            "dropped": [{"signature": format!("bad-{height}"), "reason": "nonce"}],
        }))
        .expect("effects decode")
    };

    let mut parent = "0x0".to_string();
    for height in 0..3u64 {
        let mut b = block(height, &parent, vec![]);
        b.effects = Some(effects(height));
        parent = b.hash();
        storage::insert_block(&pool, &chain, &b, &extractor)
            .await
            .expect("insert");
        // Redelivery: same rows, no conflict error, no duplicates.
        storage::insert_block(&pool, &chain, &b, &extractor)
            .await
            .expect("redeliver");
    }

    for (table, rows) in [
        ("account_state", 6),
        ("asset_balances", 3),
        ("asset_holder_states", 3),
        ("stakes", 3),
        ("validator_status", 3),
        ("validator_sets", 3),
        ("asset_registrations", 3),
        ("dropped_actions", 3),
    ] {
        assert_eq!(count(&pool, table, &chain).await, rows, "{table}");
    }
    let balance: String = sqlx::query_scalar(
        "SELECT balance::TEXT FROM account_state WHERE chain_id = $1 AND address = $2 AND height = 2",
    )
    .bind(&chain)
    .bind(addr(1))
    .fetch_one(&pool)
    .await
    .expect("balance");
    assert_eq!(balance, u128::MAX.to_string(), "u128 survives NUMERIC");
    assert_eq!(
        storage::get_stats(&pool, &chain)
            .await
            .expect("stats")
            .total_accounts,
        2,
        "two distinct accounts in state, no actions at all"
    );

    storage::rollback_to(&pool, &chain, 0)
        .await
        .expect("rollback");
    assert_eq!(count(&pool, "account_state", &chain).await, 2);
    assert_eq!(count(&pool, "dropped_actions", &chain).await, 1);
    assert_eq!(count(&pool, "validator_sets", &chain).await, 1);
}

/// The state reads resolve "as of height": the newest row at or below `at`
/// per key, zero balances drop out of holder lists, removed stakes drop out
/// of an account, a validator answers from either table, and the dropped
/// list filters by sender and pages by `(height, signature)`.
#[tokio::test]
async fn state_reads_resolve_as_of_height() {
    let pool = skip_without_db!();
    let chain = chain_id("state-reads");
    let extractor = AddressExtractor::tier_a_only(KindSchema::empty());

    let effects = |v: serde_json::Value| -> storage::BlockEffects {
        serde_json::from_value(v).expect("effects decode")
    };
    let heights = [
        // h0: alice funded, holds gold, stakes with v9, v9 active + in set.
        effects(serde_json::json!({
            "accounts": {addr(1): {"balance": 1000, "nonce": 0, "identity_hash": "h", "claims": ["kyc"]}},
            "asset_balances": [{"asset": "gold", "owner": addr(1), "balance": 5},
                               {"asset": "gold", "owner": addr(2), "balance": 7}],
            "stakes": [{"master": addr(1), "validator": addr(9), "allocation": {"amount": 100}}],
            "validator_statuses": {addr(9): "Active"},
            "validator_set": {addr(9): 10000},
            "dropped": [{"signature": "d0", "sender": addr(1), "reason": "nonce"}],
        })),
        // h1: alice spends, bob's gold goes to 0, alice unstakes, v9 jailed.
        effects(serde_json::json!({
            "accounts": {addr(1): {"balance": 900, "nonce": 1}, addr(2): {"balance": 50, "nonce": 0}},
            "asset_balances": [{"asset": "gold", "owner": addr(2), "balance": 0}],
            "holder_states": [{"asset": "gold", "holder": addr(1), "state": {"frozen": true}}],
            "stakes": [{"master": addr(1), "validator": addr(9), "allocation": null}],
            "validator_statuses": {addr(9): {"Jailed": {"until_epoch": 3}}},
            "dropped": [{"signature": "d1", "sender": addr(2), "reason": "balance"},
                        {"signature": "d2", "sender": addr(1), "reason": "sig"}],
        })),
    ];
    let mut parent = "0x0".to_string();
    for (height, fx) in heights.into_iter().enumerate() {
        let mut b = block(height as u64, &parent, vec![]);
        b.effects = Some(fx);
        parent = b.hash();
        storage::insert_block(&pool, &chain, &b, &extractor)
            .await
            .expect("insert");
    }

    // Account at tip vs as of height 0.
    let alice = storage::get_account_state(&pool, &chain, &addr(1), i64::MAX)
        .await
        .expect("query")
        .expect("alice exists");
    assert_eq!(
        (alice.height, alice.balance.as_str(), alice.nonce),
        (1, "900", 1)
    );
    assert_eq!(alice.assets.len(), 1, "gold unchanged since h0 still shows");
    assert!(alice.stakes.is_empty(), "removed allocation drops out");
    let alice0 = storage::get_account_state(&pool, &chain, &addr(1), 0)
        .await
        .expect("query")
        .expect("alice at 0");
    assert_eq!((alice0.balance.as_str(), alice0.stakes.len()), ("1000", 1));
    assert_eq!(
        alice0.entry["claims"][0], "kyc",
        "identity fields ride along"
    );
    assert!(
        alice0.entry.get("balance").is_none(),
        "typed fields aren't duplicated"
    );
    assert!(
        storage::get_account_state(&pool, &chain, &addr(2), 0)
            .await
            .expect("query")
            .is_none(),
        "bob has no state row at h0"
    );

    // Holders: bob at 0 disappears at tip, present at h0; alice carries state.
    let tip = storage::get_asset_holders(&pool, &chain, "gold", i64::MAX, None, 10)
        .await
        .expect("holders");
    assert_eq!(tip.len(), 1);
    assert_eq!(tip[0].holder, addr(1));
    assert_eq!(tip[0].state.as_ref().unwrap()["frozen"], true);
    let at0 = storage::get_asset_holders(&pool, &chain, "gold", 0, None, 10)
        .await
        .expect("holders");
    assert_eq!(at0.len(), 2);
    assert!(at0[0].state.is_none());
    let page2 = storage::get_asset_holders(&pool, &chain, "gold", 0, Some(&at0[0].holder), 10)
        .await
        .expect("holders page 2");
    assert_eq!(page2.len(), 1);
    assert_eq!(page2[0].holder, addr(2));

    // Validator: newest status, power from the set, full history.
    let v = storage::get_validator(&pool, &chain, &addr(9))
        .await
        .expect("query")
        .expect("v9");
    assert_eq!(v.status.unwrap()["Jailed"]["until_epoch"], 3);
    assert_eq!(
        (v.voting_power, v.set_effective_height),
        (Some(10000), Some(1))
    );
    assert_eq!(v.history.len(), 2);
    assert!(
        storage::get_validator(&pool, &chain, &addr(8))
            .await
            .expect("query")
            .is_none()
    );

    // Dropped: newest first, sender filter, keyset paging.
    let all = storage::list_dropped_actions(&pool, &chain, None, None, 10)
        .await
        .expect("dropped");
    assert_eq!(
        all.iter().map(|d| d.signature.as_str()).collect::<Vec<_>>(),
        ["d2", "d1", "d0"]
    );
    let alice_only = storage::list_dropped_actions(&pool, &chain, Some(&addr(1)), None, 10)
        .await
        .expect("dropped");
    assert_eq!(
        alice_only
            .iter()
            .map(|d| d.signature.as_str())
            .collect::<Vec<_>>(),
        ["d2", "d0"]
    );
    let after_d2 = storage::list_dropped_actions(&pool, &chain, None, Some((1, "d2")), 10)
        .await
        .expect("dropped");
    assert_eq!(
        after_d2
            .iter()
            .map(|d| d.signature.as_str())
            .collect::<Vec<_>>(),
        ["d1", "d0"]
    );
}
