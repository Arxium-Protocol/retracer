use anyhow::{Context, Result};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

mod schema;
mod wire;
pub use schema::{ActionIndexable, AddressExtractor, KindSchema, Projection, ProjectionType, Role};
pub use wire::{IndexableAction, IndexableBlock};

/// Decides whether a string is a well-formed address on this chain. Injected
/// rather than hardcoded because address format is chain-specific — Arxium's
/// `arx1` bech32 is one choice, not the only one.
///
/// Used for two different questions, which is why callers hold an `Option`:
/// rejecting malformed input on address-keyed RPCs, and deciding whether a
/// `Search` query *is* an address before falling through to hash lookups.
/// Absent means "don't validate, and don't classify anything as an address" —
/// the safe default in both cases, since a permissive validator would make
/// `Search` mistake every block hash for an account.
pub type AddressValidator = std::sync::Arc<dyn Fn(&str) -> bool + Send + Sync>;

/// `max_connections` is caller-set rather than a shared default because the
/// ingestion writer and the gRPC read path get separate pools (see
/// `retracerd/src/main.rs`) — a flood of read queries must never starve
/// the single-writer ingestion loop of a connection.
pub async fn connect(database_url: &str, max_connections: u32) -> Result<PgPool> {
    Ok(PgPoolOptions::new()
        .max_connections(max_connections)
        .acquire_timeout(std::time::Duration::from_secs(10))
        .connect(database_url)
        .await?)
}

pub async fn migrate(pool: &PgPool) -> Result<()> {
    sqlx::migrate!("../../migrations").run(pool).await?;
    Ok(())
}

/// Last height this `chain_id` has fully written, if any — used to resume
/// after a restart and, for now, just to log where ingestion picked back up
/// (gap backfill against this cursor is milestone 3, see plan.md §9).
pub async fn get_cursor(pool: &PgPool, chain_id: &str) -> Result<Option<i64>> {
    let row: Option<(i64,)> =
        sqlx::query_as("SELECT last_height FROM ingestion_cursor WHERE chain_id = $1")
            .bind(chain_id)
            .fetch_optional(pool)
            .await?;
    Ok(row.map(|(h,)| h))
}

/// How far this indexer has got: the cursor, and the timestamp of the block
/// sitting at it. One query rather than two, because a cursor read and a
/// separate tip read can straddle a commit and report a height whose timestamp
/// belongs to the previous block.
///
/// The join is a LEFT JOIN so a cursor pointing at a height with no row still
/// returns the height rather than nothing at all — that combination means
/// something is wrong, and reporting "no status" would hide it.
pub async fn get_status(pool: &PgPool, chain_id: &str) -> Result<IndexStatus> {
    let row: Option<(i64, Option<i64>)> = sqlx::query_as(
        "SELECT c.last_height, b.timestamp
         FROM ingestion_cursor c
         LEFT JOIN blocks b ON b.chain_id = c.chain_id AND b.height = c.last_height
         WHERE c.chain_id = $1",
    )
    .bind(chain_id)
    .fetch_optional(pool)
    .await?;

    Ok(match row {
        Some((height, timestamp)) => IndexStatus {
            indexed_height: Some(height),
            tip_timestamp: timestamp,
            node_tip_height: None,
            blocks_behind: None,
        },
        // No cursor row at all: nothing has ever been ingested for this
        // chain_id. Distinct from height 0, which means genesis is indexed.
        None => IndexStatus {
            indexed_height: None,
            tip_timestamp: None,
            node_tip_height: None,
            blocks_behind: None,
        },
    })
}

#[derive(Debug, Clone, Copy, serde::Serialize)]
pub struct IndexStatus {
    pub indexed_height: Option<i64>,
    pub tip_timestamp: Option<i64>,
    /// The chain's tip as reported by the node, and the gap to it. Filled in by
    /// the serving layer from the live sync-protocol view rather than read from
    /// Postgres — the database only knows what we've written, never how much is
    /// still out there.
    pub node_tip_height: Option<i64>,
    pub blocks_behind: Option<i64>,
}

impl IndexStatus {
    /// Attaches the network's tip and derives the gap.
    ///
    /// Saturating, because a node that has just rolled back can legitimately
    /// report a tip below ours for a moment, and "behind by -3 blocks" is not a
    /// thing a caller can act on.
    pub fn with_network_tip(mut self, node_tip: Option<u64>) -> Self {
        self.node_tip_height = node_tip.map(|h| h as i64);
        self.blocks_behind = match (node_tip, self.indexed_height) {
            (Some(tip), Some(indexed)) => Some((tip as i64 - indexed).max(0)),
            // Nothing indexed yet but a known tip: every block is outstanding.
            (Some(tip), None) => Some(tip as i64 + 1),
            (None, _) => None,
        };
        self
    }
}

/// Newest-first page of blocks, without their actions.
///
/// `action_count` is a correlated subquery rather than a `LEFT JOIN ... GROUP
/// BY`, which keeps a block with no actions in the result at a count of zero
/// without needing the join to be outer and the count to be over a specific
/// column. Both are indexed lookups — the block scan on `blocks`' primary key,
/// the count on `actions_chain_height_idx` (migration 0004; before that index
/// existed this subquery was a sequential scan of `actions` per block row).
pub async fn list_blocks(
    pool: &PgPool,
    chain_id: &str,
    limit: i64,
    before: Option<i64>,
) -> Result<Vec<BlockSummary>> {
    let rows: Vec<(i64, String, String, i64, Option<String>, i64)> = sqlx::query_as(
        "SELECT b.height, b.hash, b.parent_hash, b.timestamp, b.proposer,
                (SELECT COUNT(*) FROM actions a
                  WHERE a.chain_id = b.chain_id AND a.block_height = b.height) AS action_count
         FROM blocks b
         WHERE b.chain_id = $1 AND ($2::BIGINT IS NULL OR b.height < $2)
         ORDER BY b.height DESC
         LIMIT $3",
    )
    .bind(chain_id)
    .bind(before)
    .bind(limit)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|(height, hash, parent_hash, timestamp, proposer, action_count)| BlockSummary {
            height,
            hash,
            parent_hash,
            timestamp,
            proposer,
            action_count,
        })
        .collect())
}

/// Newest-first page of actions across every sender.
///
/// The cursor is the `(height, index_in_block)` pair, compared as a row value.
/// Height alone would be wrong in both directions: a block holding more than
/// one action would either repeat its remaining actions on the next page (with
/// `<=`) or skip them (with `<`), and both look like ordinary output.
pub async fn list_actions(
    pool: &PgPool,
    chain_id: &str,
    limit: i64,
    before: Option<(i64, i32)>,
) -> Result<Vec<ActionRow>> {
    let (before_height, before_index) = match before {
        Some((h, i)) => (Some(h), Some(i)),
        None => (None, None),
    };

    Ok(sqlx::query_as(
        "SELECT action_hash, block_height, index_in_block, kind, from_address, payload
         FROM actions
         WHERE chain_id = $1
           AND ($2::BIGINT IS NULL
                OR (block_height, index_in_block) < ($2::BIGINT, $3::INT))
         ORDER BY block_height DESC, index_in_block DESC
         LIMIT $4",
    )
    .bind(chain_id)
    .bind(before_height)
    .bind(before_index)
    .bind(limit)
    .fetch_all(pool)
    .await?)
}

/// Aggregates over the whole index.
///
/// Five separate queries rather than one, because they touch different tables
/// and combining them would mean a cross join or a pile of scalar subqueries
/// that read worse and plan no better. They are not wrapped in a transaction:
/// a stats page that counts blocks a microsecond before an action lands is not
/// wrong in any way a reader could perceive, and holding a snapshot open for a
/// dashboard would be a real cost for an imaginary gain.
pub async fn get_stats(pool: &PgPool, chain_id: &str) -> Result<Stats> {
    let (total_blocks,): (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM blocks WHERE chain_id = $1")
            .bind(chain_id)
            .fetch_one(pool)
            .await?;

    let (total_actions,): (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM actions WHERE chain_id = $1")
            .bind(chain_id)
            .fetch_one(pool)
            .await?;

    // Senders live in account_actions, everyone else in action_addresses.
    // UNION rather than UNION ALL: an address that both sent and received is
    // one address, and the whole point of the number is how many there are.
    let (total_accounts,): (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM (
             SELECT address FROM account_actions WHERE chain_id = $1
             UNION
             SELECT address FROM action_addresses WHERE chain_id = $1
         ) AS seen",
    )
    .bind(chain_id)
    .fetch_one(pool)
    .await?;

    // Against the block's timestamp, which is the chain's clock, not this
    // host's. `indexed_at` would measure when the indexer happened to be
    // running, so a service restarted an hour ago would report the whole chain
    // as fresh.
    let (actions_24h,): (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM actions a
         JOIN blocks b ON b.chain_id = a.chain_id AND b.height = a.block_height
         WHERE a.chain_id = $1
           AND b.timestamp >= EXTRACT(EPOCH FROM NOW())::BIGINT - 86400",
    )
    .bind(chain_id)
    .fetch_one(pool)
    .await?;

    // Genesis is pinned to timestamp 0 so every node hashes it identically, so
    // including it would put a 56-year gap in the average. Excluded by height,
    // not by timestamp, because only height 0 carries that meaning.
    let window: Option<(i64, Option<i64>, Option<i64>)> = sqlx::query_as(
        "SELECT COUNT(*), MIN(timestamp), MAX(timestamp)
         FROM (SELECT timestamp FROM blocks
               WHERE chain_id = $1 AND height > 0
               ORDER BY height DESC LIMIT 100) AS recent",
    )
    .bind(chain_id)
    .fetch_optional(pool)
    .await?;

    let avg_block_time_secs = match window {
        // n blocks have n-1 gaps between them. One block has no gap to measure,
        // and dividing by zero to report a block time would be worse than
        // saying nothing.
        Some((count, Some(min), Some(max))) if count > 1 => (max - min) as f64 / (count - 1) as f64,
        _ => 0.0,
    };

    Ok(Stats { total_blocks, total_actions, total_accounts, actions_24h, avg_block_time_secs })
}

#[derive(Debug, Clone, Copy, serde::Serialize)]
pub struct Stats {
    pub total_blocks: i64,
    pub total_actions: i64,
    pub total_accounts: i64,
    pub actions_24h: i64,
    pub avg_block_time_secs: f64,
}

/// Every address that has proposed at least one indexed block, with how many
/// and when it last did.
///
/// Chain-agnostic despite what an explorer calls the result. This asks "who
/// signed the blocks I hold", which every chain answers the same way, and it
/// needs no knowledge of what a payload means. Deriving *membership* — who is
/// currently entitled to propose — would be a different question entirely, and
/// that one does need chain-specific replay this service deliberately avoids.
///
/// Blocks with a NULL proposer are excluded rather than grouped: genesis has no
/// proposer by construction, and blocks indexed before migration 0003 lost
/// theirs. Grouping them would invent a validator called "unknown".
pub async fn list_proposers(pool: &PgPool, chain_id: &str) -> Result<Vec<ProposerRow>> {
    let rows: Vec<(String, i64, i64, i64)> = sqlx::query_as(
        "SELECT proposer, COUNT(*), MIN(height), MAX(height)
         FROM blocks
         WHERE chain_id = $1 AND proposer IS NOT NULL
         GROUP BY proposer
         ORDER BY COUNT(*) DESC, proposer",
    )
    .bind(chain_id)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|(address, blocks_proposed, first_height, last_height)| ProposerRow {
            address,
            blocks_proposed,
            first_proposed_height: first_height,
            last_proposed_height: last_height,
        })
        .collect())
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct ProposerRow {
    pub address: String,
    pub blocks_proposed: i64,
    pub first_proposed_height: i64,
    pub last_proposed_height: i64,
}

/// Blocks actually proposed per address within `[from_height, to_height]`,
/// keyed by address. Companion to [`list_proposers`], scoped to a height
/// range instead of the whole chain — the numerator side of validator
/// uptime, where the denominator (turns *owed*) comes from the node's own
/// `GET /validators?height=N` (see `rest-service::get_validator_uptime`;
/// see also `Retracer_Design.md`'s boundary rules on why that computation
/// doesn't live here).
pub async fn count_proposers_in_range(
    pool: &PgPool,
    chain_id: &str,
    from_height: i64,
    to_height: i64,
) -> Result<std::collections::HashMap<String, i64>> {
    let rows: Vec<(String, i64)> = sqlx::query_as(
        "SELECT proposer, COUNT(*)
         FROM blocks
         WHERE chain_id = $1 AND proposer IS NOT NULL AND height BETWEEN $2 AND $3
         GROUP BY proposer",
    )
    .bind(chain_id)
    .bind(from_height)
    .bind(to_height)
    .fetch_all(pool)
    .await?;

    Ok(rows.into_iter().collect())
}

/// A block without its actions. Separate from `BlockRow` on purpose: the two
/// carry different things, and letting `BlockRow` hold an empty action list
/// would mean a caller could not tell "this block has no actions" from "this
/// query did not fetch them".
#[derive(Debug, Clone, serde::Serialize)]
pub struct BlockSummary {
    pub height: i64,
    pub hash: String,
    pub parent_hash: String,
    pub timestamp: i64,
    /// None for genesis, for a solo non-validator node's block, and for any
    /// block indexed before migration 0003 — see that file.
    pub proposer: Option<String>,
    pub action_count: i64,
}

/// The hash Retracer has stored for `height`, if any — used to validate
/// an incoming block's `parent_hash` extends the chain we already have.
pub async fn get_block_hash(pool: &PgPool, chain_id: &str, height: i64) -> Result<Option<String>> {
    let row: Option<(String,)> =
        sqlx::query_as("SELECT hash FROM blocks WHERE chain_id = $1 AND height = $2")
            .bind(chain_id)
            .bind(height)
            .fetch_optional(pool)
            .await?;
    Ok(row.map(|(h,)| h))
}

/// Writes `block` and its Actions in a single transaction, then advances
/// `ingestion_cursor`. Idempotent (`ON CONFLICT DO NOTHING`/guarded update)
/// so re-delivery of an already-seen block is a no-op rather than an error.
/// Confirmed on write, no provisional/finality state — see plan.md §6.
/// Creates a Postgres expression index for each projection the chain's
/// `kind_schema.toml` declares, so a builder can filter on a payload field
/// without that field ever becoming a column.
///
/// This is deliberately the *whole* of the feature. Every other indexer in the
/// field answers "let me query my own fields" with a mapping runtime — WASM,
/// TypeScript, Rust modules — because they index arbitrary contract semantics
/// and genuinely need user code. Spoke Chains share one entity graph and vary
/// only in payload contents, so naming a path and letting Postgres index the
/// expression covers the same ground with no sandbox, no determinism story, and
/// no user code in-process.
///
/// Each index is partial, scoped to its `kind`: the expression is meaningless
/// for payloads of other kinds, and a partial index stays small on a table
/// where most rows aren't that kind.
///
/// Returns the number of projections applied. Removing a projection from the
/// config does **not** drop its index — reconciling that automatically would
/// mean this function could delete an index someone added by hand. Dropping a
/// retired projection is a deliberate manual `DROP INDEX`.
pub async fn create_projection_indexes(pool: &PgPool, schema: &KindSchema) -> Result<usize> {
    let projections = schema.projections();
    for projection in projections {
        // Every interpolated fragment here is either a fixed literal
        // (`sql_cast`) or was validated as a plain identifier at parse time
        // (`is_safe_segment`); `kind` is the one value that reaches SQL as
        // data, and it's bound through a literal-quoting escape below rather
        // than trusted. Index DDL cannot take bind parameters, which is why
        // this is built as text at all.
        let kind_literal = projection.kind.replace('\'', "''");
        let sql = format!(
            "CREATE INDEX IF NOT EXISTS {name} ON actions ((({accessor})::{cast})) \
             WHERE kind = '{kind_literal}'",
            name = projection.index_name(),
            accessor = projection.json_accessor(),
            cast = projection.ty.sql_cast(),
        );
        sqlx::query(&sql).execute(pool).await.with_context(|| {
            format!("creating projection index for kind {:?}", projection.kind)
        })?;
        tracing::info!(
            kind = %projection.kind,
            path = %projection.segments.join("."),
            index = %projection.index_name(),
            "projection index ready"
        );
    }
    Ok(projections.len())
}

/// Records (or refreshes) a chain in the `chains` registry.
///
/// Called at startup from each chain's config, so the config stays the source
/// of truth and this table is a projection of it — that's why every field is
/// overwritten on conflict rather than left alone. Its purpose is discovery:
/// without it a client has no way to learn which `x-chain-id` values the gRPC
/// surface will accept.
pub async fn register_chain(
    pool: &PgPool,
    chain_id: &str,
    display_name: Option<&str>,
    blocks_topic: &str,
    sync_protocol: &str,
    finality_depth: i64,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO chains (chain_id, display_name, blocks_topic, sync_protocol, finality_depth)
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (chain_id) DO UPDATE SET
             display_name = EXCLUDED.display_name,
             blocks_topic = EXCLUDED.blocks_topic,
             sync_protocol = EXCLUDED.sync_protocol,
             finality_depth = EXCLUDED.finality_depth",
    )
    .bind(chain_id)
    .bind(display_name)
    .bind(blocks_topic)
    .bind(sync_protocol)
    .bind(finality_depth)
    .execute(pool)
    .await?;
    Ok(())
}

/// Un-indexes everything strictly above `height` for this chain and rewinds the
/// cursor to it. Returns how many blocks were removed.
///
/// This is the whole of reorg support, and it is this short for a structural
/// reason worth stating: our rows are immutable and keyed by height, so a
/// revert has nothing to restore. Indexers whose entities mutate can't do this
/// — The Graph range-stamps every entity version so a revert can reopen the
/// prior one, and Ponder keeps shadow tables for the same purpose. Both are
/// paying for update-in-place, which we never do.
///
/// Ordering follows the `actions → blocks` foreign key; the other two tables
/// have no FK but are deleted first anyway so a failure part-way through can
/// never leave an address row pointing at an action that's already gone.
/// Everything is one transaction, so in practice it's all or nothing.
pub async fn rollback_to(pool: &PgPool, chain_id: &str, height: i64) -> Result<u64> {
    let mut tx = pool.begin().await?;

    for sql in [
        "DELETE FROM action_addresses WHERE chain_id = $1 AND block_height > $2",
        "DELETE FROM account_actions WHERE chain_id = $1 AND block_height > $2",
        "DELETE FROM actions WHERE chain_id = $1 AND block_height > $2",
    ] {
        sqlx::query(sql).bind(chain_id).bind(height).execute(&mut *tx).await?;
    }

    let blocks_removed = sqlx::query("DELETE FROM blocks WHERE chain_id = $1 AND height > $2")
        .bind(chain_id)
        .bind(height)
        .execute(&mut *tx)
        .await?
        .rows_affected();

    // A rollback past genesis means nothing is indexed any more, which is the
    // absence of a cursor rather than a cursor of -1 — `get_cursor`'s callers
    // distinguish "never ingested" from "at height 0", and writing a negative
    // last_height would make that lie.
    if height < 0 {
        sqlx::query("DELETE FROM ingestion_cursor WHERE chain_id = $1")
            .bind(chain_id)
            .execute(&mut *tx)
            .await?;
    } else {
        // Unconditional, unlike the monotonic guard in `insert_block`'s upsert:
        // rewinding the cursor backwards is the entire point here.
        sqlx::query(
            "INSERT INTO ingestion_cursor (chain_id, last_height) VALUES ($1, $2)
             ON CONFLICT (chain_id) DO UPDATE SET last_height = EXCLUDED.last_height",
        )
        .bind(chain_id)
        .bind(height)
        .execute(&mut *tx)
        .await?;
    }

    tx.commit().await?;
    Ok(blocks_removed)
}

/// One block's rows, transposed into column vectors ready for `UNNEST`.
///
/// Split out from `insert_block` as a pure function so the part that can
/// actually be wrong — which rows get built, and whether the parallel vectors
/// stay aligned — is testable without a database. The SQL that consumes it is
/// a straight mapping with no logic of its own.
///
/// `action_hash`/`from_address` serve both the `actions` and `account_actions`
/// inserts; the `addr_*` vectors are longer or shorter than the others, since
/// one action can resolve to any number of `(address, role)` pairs, or none.
#[derive(Default)]
struct BlockWrites {
    action_hash: Vec<String>,
    index_in_block: Vec<i32>,
    kind: Vec<String>,
    from_address: Vec<String>,
    payload: Vec<serde_json::Value>,

    addr_action_hash: Vec<String>,
    addr_address: Vec<String>,
    addr_role: Vec<String>,
}

impl BlockWrites {
    fn build<B: IndexableBlock>(
        block: &B,
        height: i64,
        address_extractor: &AddressExtractor,
    ) -> Result<Self> {
        let actions = block.actions();
        let mut w = BlockWrites {
            action_hash: Vec::with_capacity(actions.len()),
            index_in_block: Vec::with_capacity(actions.len()),
            kind: Vec::with_capacity(actions.len()),
            from_address: Vec::with_capacity(actions.len()),
            payload: Vec::with_capacity(actions.len()),
            ..Default::default()
        };

        for (index, action) in actions.iter().enumerate() {
            // For CoreChain this is the action's signature: every action
            // embedded in a block was admitted through `validate_action` (RPC
            // or gossip), which requires one, and genesis's action list is
            // always empty.
            //
            // A chain that permits unsigned or system-injected actions has no
            // such id, and `identity()` returns None for those. Falling back to
            // the action's position is what keeps them distinct: keying them all
            // on a single sentinel (which is what `signature.unwrap_or_default()`
            // used to do) made every unsigned action after the first collide on
            // the `(chain_id, action_hash)` primary key and get silently
            // discarded by `ON CONFLICT DO NOTHING`. Position is stable across
            // re-delivery of the same block, so idempotency is unaffected.
            let action_hash = action
                .identity()
                .unwrap_or_else(|| format!("{height}:{index}"));
            let (kind, payload) = split_kind(action.payload_json()?);

            for (address, role) in address_extractor.resolve(&kind, &payload) {
                w.addr_action_hash.push(action_hash.clone());
                w.addr_address.push(address);
                w.addr_role.push(role.as_str().to_string());
            }

            w.action_hash.push(action_hash);
            w.index_in_block.push(index as i32);
            w.kind.push(kind);
            w.from_address.push(action.sender());
            w.payload.push(payload);
        }

        Ok(w)
    }
}

/// How many blocks one catch-up transaction covers.
///
/// Backfilling a chain committed one transaction per block, so indexing
/// 207,594 heights meant 207,594 commits — and a Postgres commit is an fsync.
/// The statements inside were already batched; the commits were the cost.
/// Mirrors what the node itself does for its own catch-up (batching WAL
/// fsyncs per sync page rather than per block).
///
/// Only affects backfill: a live chain producing one block every couple of
/// seconds flushes each block as it arrives, because the batch closes as soon
/// as the channel is empty.
pub const INSERT_BATCH_BLOCKS: usize = 200;

/// One block, in its own transaction. Live-path entry point.
pub async fn insert_block<B: IndexableBlock>(
    pool: &PgPool,
    chain_id: &str,
    block: &B,
    address_extractor: &AddressExtractor,
) -> Result<()> {
    insert_blocks(pool, chain_id, std::slice::from_ref(block), address_extractor).await
}

/// Several blocks in a single transaction — the catch-up entry point.
///
/// All-or-nothing: on failure nothing in the batch lands, including the
/// cursor advance, so a restart re-requests from the last committed height and
/// re-applies. Every write is idempotent (`ON CONFLICT DO NOTHING`, and a
/// cursor upsert guarded by `last_height <` ), so replaying a partially
/// re-delivered range is safe.
///
/// Callers must pass blocks in ascending height order and must have already
/// classified them as extending the tip — fork and gap handling stays in
/// `retracer_core`, one block at a time, and never batches.
pub async fn insert_blocks<B: IndexableBlock>(
    pool: &PgPool,
    chain_id: &str,
    blocks: &[B],
    address_extractor: &AddressExtractor,
) -> Result<()> {
    if blocks.is_empty() {
        return Ok(());
    }
    let mut tx = pool.begin().await?;
    for block in blocks {
        insert_block_in_tx(&mut tx, chain_id, block, address_extractor).await?;
    }
    tx.commit().await?;
    Ok(())
}

async fn insert_block_in_tx<B: IndexableBlock>(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    chain_id: &str,
    block: &B,
    address_extractor: &AddressExtractor,
) -> Result<()> {
    let height = block.height() as i64;
    let hash = block.hash();

    let result = sqlx::query(
        "INSERT INTO blocks (chain_id, height, hash, parent_hash, timestamp, proposer)
         VALUES ($1, $2, $3, $4, $5, $6)
         ON CONFLICT (chain_id, height) DO NOTHING",
    )
    .bind(chain_id)
    .bind(height)
    .bind(&hash)
    .bind(block.parent_hash())
    .bind(block.timestamp() as i64)
    // None for genesis, which is unsigned, and for a block from a
    // non-validator solo node. Both are real absences rather than gaps.
    .bind(block.proposer())
    .execute(&mut **tx)
    .await?;

    // A height already occupied by a *different* hash should be impossible:
    // forks are detected before the write (`retracer_core::tip::classify`)
    // and resolved by `rollback_to`, so by the time a block reaches here its
    // parent has already been checked against our tip. Reaching this branch
    // means either a second writer is on the same chain_id or the fork
    // detection was bypassed. Silently keeping the stale row would hide real
    // corruption — surface it loudly instead.
    if result.rows_affected() == 0 {
        let existing: Option<(String,)> =
            sqlx::query_as("SELECT hash FROM blocks WHERE chain_id = $1 AND height = $2")
                .bind(chain_id)
                .bind(height)
                .fetch_optional(&mut **tx)
                .await?;
        if existing.map(|(h,)| h).as_deref() != Some(hash.as_str()) {
            tracing::error!(
                chain_id, height, incoming_hash = %hash,
                "height collision: a different block already occupies this height — possible reorg, not handled in v1"
            );
        }
    }

    let writes = BlockWrites::build(block, height, address_extractor)?;

    // Three statements per block regardless of how many actions it holds. The
    // row-at-a-time version this replaced issued `2 + addresses` awaited round
    // trips *per action* — a 100-action block cost roughly 300 sequential
    // round trips inside one transaction, so throughput was bounded by network
    // latency to Postgres rather than by Postgres. `UNNEST` unpacks the column
    // vectors server-side; `chain_id` and `block_height` stay scalar because
    // they're constant for the whole block.
    //
    // `ON CONFLICT DO NOTHING` (rather than DO UPDATE) is also what makes the
    // batch safe against a malformed block carrying the same action twice:
    // DO UPDATE would abort the statement with "cannot affect row a second
    // time", DO NOTHING skips the duplicate exactly as the per-row loop did.
    if !writes.action_hash.is_empty() {
        sqlx::query(
            "INSERT INTO actions (chain_id, action_hash, block_height, index_in_block, kind, from_address, payload)
             SELECT $1, u.action_hash, $2, u.index_in_block, u.kind, u.from_address, u.payload
             FROM UNNEST($3::TEXT[], $4::INT[], $5::TEXT[], $6::TEXT[], $7::JSONB[])
                  AS u(action_hash, index_in_block, kind, from_address, payload)
             ON CONFLICT (chain_id, action_hash) DO NOTHING",
        )
        .bind(chain_id)
        .bind(height)
        .bind(&writes.action_hash[..])
        .bind(&writes.index_in_block[..])
        .bind(&writes.kind[..])
        .bind(&writes.from_address[..])
        .bind(&writes.payload[..])
        .execute(&mut **tx)
        .await?;

        // Sender rows reuse the `actions` vectors verbatim — `account_actions`
        // is one row per action keyed on its sender, so building a second pair
        // of identical vectors would only cost allocations.
        sqlx::query(
            "INSERT INTO account_actions (chain_id, address, block_height, action_hash)
             SELECT $1, u.address, $2, u.action_hash
             FROM UNNEST($3::TEXT[], $4::TEXT[]) AS u(address, action_hash)
             ON CONFLICT DO NOTHING",
        )
        .bind(chain_id)
        .bind(height)
        .bind(&writes.from_address[..])
        .bind(&writes.action_hash[..])
        .execute(&mut **tx)
        .await?;
    }

    if !writes.addr_action_hash.is_empty() {
        sqlx::query(
            "INSERT INTO action_addresses (chain_id, action_hash, address, role, block_height)
             SELECT $1, u.action_hash, u.address, u.role, $2
             FROM UNNEST($3::TEXT[], $4::TEXT[], $5::TEXT[]) AS u(action_hash, address, role)
             ON CONFLICT DO NOTHING",
        )
        .bind(chain_id)
        .bind(height)
        .bind(&writes.addr_action_hash[..])
        .bind(&writes.addr_address[..])
        .bind(&writes.addr_role[..])
        .execute(&mut **tx)
        .await?;
    }

    sqlx::query(
        "INSERT INTO ingestion_cursor (chain_id, last_height)
         VALUES ($1, $2)
         ON CONFLICT (chain_id) DO UPDATE
         SET last_height = EXCLUDED.last_height
         WHERE ingestion_cursor.last_height < EXCLUDED.last_height",
    )
    .bind(chain_id)
    .bind(height)
    .execute(&mut **tx)
    .await?;

    Ok(())
}

/// Built from a freshly-ingested wire block and broadcast to gRPC streaming
/// subscribers (`SubscribeBlocks`/`SubscribeAccountActions`) — same shape
/// `get_block_by_height`/`get_action_by_hash` return, so one `From<BlockRow>`
/// conversion in `grpc-service` covers both the read RPCs and the streams.
///
/// Action identity is resolved exactly as `insert_block` does it, position
/// fallback included — the streamed row and the stored row have to agree on an
/// action's `action_hash`, or a client that follows a stream and then fetches
/// by hash gets a miss.
pub fn block_row_from_wire<B: IndexableBlock>(block: &B) -> Result<BlockRow> {
    let height = block.height() as i64;
    let actions = block
        .actions()
        .iter()
        .enumerate()
        .map(|(index, action)| -> Result<ActionRow> {
            let (kind, payload) = split_kind(action.payload_json()?);
            Ok(ActionRow {
                action_hash: action.identity().unwrap_or_else(|| format!("{height}:{index}")),
                block_height: height,
                index_in_block: index as i32,
                kind,
                from_address: action.sender(),
                payload,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(BlockRow {
        height,
        hash: block.hash(),
        parent_hash: block.parent_hash(),
        timestamp: block.timestamp() as i64,
        proposer: block.proposer(),
        actions,
    })
}

#[derive(Debug, Clone, serde::Serialize, sqlx::FromRow)]
pub struct ActionRow {
    pub action_hash: String,
    pub block_height: i64,
    pub index_in_block: i32,
    pub kind: String,
    pub from_address: String,
    pub payload: serde_json::Value,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct BlockRow {
    pub height: i64,
    pub hash: String,
    pub parent_hash: String,
    pub timestamp: i64,
    pub proposer: Option<String>,
    pub actions: Vec<ActionRow>,
}

async fn actions_for_block(pool: &PgPool, chain_id: &str, height: i64) -> Result<Vec<ActionRow>> {
    Ok(sqlx::query_as!(
        ActionRow,
        r#"SELECT action_hash, block_height, index_in_block, kind, from_address, payload as "payload!"
           FROM actions WHERE chain_id = $1 AND block_height = $2 ORDER BY index_in_block"#,
        chain_id,
        height,
    )
    .fetch_all(pool)
    .await?)
}

pub async fn get_block_by_height(
    pool: &PgPool,
    chain_id: &str,
    height: i64,
) -> Result<Option<BlockRow>> {
    let Some(row) = sqlx::query!(
        "SELECT hash, parent_hash, timestamp, proposer FROM blocks WHERE chain_id = $1 AND height = $2",
        chain_id,
        height,
    )
    .fetch_optional(pool)
    .await?
    else {
        return Ok(None);
    };
    let actions = actions_for_block(pool, chain_id, height).await?;
    Ok(Some(BlockRow {
        height,
        hash: row.hash,
        parent_hash: row.parent_hash,
        timestamp: row.timestamp,
        proposer: row.proposer,
        actions,
    }))
}

pub async fn get_block_by_hash(
    pool: &PgPool,
    chain_id: &str,
    hash: &str,
) -> Result<Option<BlockRow>> {
    let Some(row) = sqlx::query!(
        "SELECT height, parent_hash, timestamp, proposer FROM blocks WHERE chain_id = $1 AND hash = $2",
        chain_id,
        hash,
    )
    .fetch_optional(pool)
    .await?
    else {
        return Ok(None);
    };
    let actions = actions_for_block(pool, chain_id, row.height).await?;
    Ok(Some(BlockRow {
        height: row.height,
        hash: hash.to_string(),
        parent_hash: row.parent_hash,
        timestamp: row.timestamp,
        proposer: row.proposer,
        actions,
    }))
}

pub async fn get_action_by_hash(
    pool: &PgPool,
    chain_id: &str,
    action_hash: &str,
) -> Result<Option<ActionRow>> {
    Ok(sqlx::query_as!(
        ActionRow,
        r#"SELECT action_hash, block_height, index_in_block, kind, from_address, payload as "payload!"
           FROM actions WHERE chain_id = $1 AND action_hash = $2"#,
        chain_id,
        action_hash,
    )
    .fetch_optional(pool)
    .await?)
}

/// Newest-first page of `address`'s action history — mirrors the node's own
/// `GET /accounts/:address/actions` cursor shape (plan.md §5).
///
/// `role` is `None`/`Some("from")` for the original sender-only history
/// (unchanged, sourced from `account_actions`); any other role queries the
/// Tier A `action_addresses` index instead (address-extraction plan §6) —
/// e.g. `role = "to"` for "received".
///
/// Ordering carries an `index_in_block` tiebreak so two calls with the same
/// arguments return the same order, and the cursor is a `(height, index)`
/// pair — like `list_actions` — so an address with several actions in the
/// boundary block doesn't skip or repeat any of them across pages.
pub async fn get_account_actions(
    pool: &PgPool,
    chain_id: &str,
    address: &str,
    limit: i64,
    before: Option<(i64, i32)>,
    role: Option<&str>,
) -> Result<Vec<ActionRow>> {
    let (before_height, before_index) = match before {
        Some((h, i)) => (Some(h), Some(i)),
        None => (None, None),
    };
    match role {
        None | Some("from") => Ok(sqlx::query_as(
            "SELECT a.action_hash, a.block_height, a.index_in_block, a.kind, a.from_address, a.payload
               FROM account_actions aa
               JOIN actions a ON a.chain_id = aa.chain_id AND a.action_hash = aa.action_hash
               WHERE aa.chain_id = $1 AND aa.address = $2
                 AND ($3::BIGINT IS NULL
                      OR (aa.block_height, a.index_in_block) < ($3::BIGINT, $4::INT))
               ORDER BY aa.block_height DESC, a.index_in_block DESC
               LIMIT $5",
        )
        .bind(chain_id)
        .bind(address)
        .bind(before_height)
        .bind(before_index)
        .bind(limit)
        .fetch_all(pool)
        .await?),
        Some(role) => Ok(sqlx::query_as(
            "SELECT a.action_hash, a.block_height, a.index_in_block, a.kind, a.from_address, a.payload
               FROM action_addresses ad
               JOIN actions a ON a.chain_id = ad.chain_id AND a.action_hash = ad.action_hash
               WHERE ad.chain_id = $1 AND ad.address = $2 AND ad.role = $3
                 AND ($4::BIGINT IS NULL
                      OR (ad.block_height, a.index_in_block) < ($4::BIGINT, $5::INT))
               ORDER BY ad.block_height DESC, a.index_in_block DESC
               LIMIT $6",
        )
        .bind(chain_id)
        .bind(address)
        .bind(role)
        .bind(before_height)
        .bind(before_index)
        .bind(limit)
        .fetch_all(pool)
        .await?),
    }
}

/// Blocks for `chain_id` in `[from, to]`, ascending — used to replay
/// persisted history to a resuming `SubscribeBlocks` client before handing
/// it off to the live broadcast. No dedicated range query: this chain's
/// block count per request is expected to stay small (a resume gap, not a
/// full-history export), so N point lookups reusing `get_block_by_height`
/// is simpler than a second JOIN query to maintain.
/// How many blocks one `get_blocks_in_range` page returns.
///
/// Bounds both the memory a single call holds and how long a `SubscribeBlocks`
/// resume waits before its first message. 500 blocks is ~17 minutes of chain at
/// a 2s interval, and two queries' worth of rows.
pub const BLOCK_PAGE: i64 = 500;

/// Blocks in `[from, to]`, ordered by height, at most `limit` of them.
///
/// Two queries total — one for the block rows, one for every action across the
/// page — rather than per block. The previous implementation looped
/// `from..=to` calling `get_block_by_height`, which is itself two queries, so a
/// `SubscribeBlocks` resume from genesis against a 207k-height chain issued
/// **~415,000 round trips serially** and accumulated every row in memory before
/// emitting its first message. That is the wallet's slow reconnect: not the
/// number of blocks, but an N+1 on the resume path.
///
/// Returning fewer than `limit` rows means the range is exhausted; callers
/// page by advancing `from` past the last height returned.
pub async fn get_blocks_in_range(
    pool: &PgPool,
    chain_id: &str,
    from: i64,
    to: i64,
    limit: i64,
) -> Result<Vec<BlockRow>> {
    let rows: Vec<(i64, String, String, i64, Option<String>)> = sqlx::query_as(
        "SELECT height, hash, parent_hash, timestamp, proposer
           FROM blocks
          WHERE chain_id = $1 AND height >= $2 AND height <= $3
          ORDER BY height
          LIMIT $4",
    )
    .bind(chain_id)
    .bind(from)
    .bind(to)
    .bind(limit)
    .fetch_all(pool)
    .await?;

    let (Some(lo), Some(hi)) = (rows.first().map(|r| r.0), rows.last().map(|r| r.0)) else {
        return Ok(Vec::new());
    };

    // One query for the whole page's actions. On this chain that is usually
    // zero rows — 8 actions across 207k heights — so the page cost is
    // dominated by the block rows, not the actions.
    let actions: Vec<ActionRow> = sqlx::query_as(
        "SELECT action_hash, block_height, index_in_block, kind, from_address, payload
           FROM actions
          WHERE chain_id = $1 AND block_height >= $2 AND block_height <= $3
          ORDER BY block_height, index_in_block",
    )
    .bind(chain_id)
    .bind(lo)
    .bind(hi)
    .fetch_all(pool)
    .await?;

    let mut by_height: std::collections::HashMap<i64, Vec<ActionRow>> =
        std::collections::HashMap::new();
    for action in actions {
        by_height.entry(action.block_height).or_default().push(action);
    }

    Ok(rows
        .into_iter()
        .map(|(height, hash, parent_hash, timestamp, proposer)| BlockRow {
            actions: by_height.remove(&height).unwrap_or_default(),
            height,
            hash,
            parent_hash,
            timestamp,
            proposer,
        })
        .collect())
}

pub async fn block_height_by_hash(
    pool: &PgPool,
    chain_id: &str,
    hash: &str,
) -> Result<Option<i64>> {
    let row: Option<(i64,)> =
        sqlx::query_as("SELECT height FROM blocks WHERE chain_id = $1 AND hash = $2")
            .bind(chain_id)
            .bind(hash)
            .fetch_optional(pool)
            .await?;
    Ok(row.map(|(h,)| h))
}

pub async fn block_exists_at_height(pool: &PgPool, chain_id: &str, height: i64) -> Result<bool> {
    Ok(get_block_hash(pool, chain_id, height).await?.is_some())
}

/// Splits a serde-externally-tagged enum's JSON into (variant name, inner
/// value) — e.g. `{"Transfer":{"to":...}}` -> `("Transfer", {"to":...})`,
/// `"LeaveValidator"` (a unit variant) -> `("LeaveValidator", null)`. Works
/// for any `P: Serialize` enum, so `storage` never needs to know a chain's
/// actual payload type (plan.md §7).
fn split_kind(payload_json: serde_json::Value) -> (String, serde_json::Value) {
    match payload_json {
        serde_json::Value::Object(mut map) if map.len() == 1 => {
            let kind = map.keys().next().expect("checked len == 1").clone();
            let value = map.remove(&kind).expect("checked len == 1");
            (kind, value)
        }
        serde_json::Value::String(kind) => (kind, serde_json::Value::Null),
        other => ("unknown".to_string(), other),
    }
}

#[cfg(all(test, feature = "xc-primitives"))]
mod tests {
    use super::*;
    use xc_primitives::{Action, Address, Block};

    #[derive(serde::Serialize)]
    enum TestPayload {
        Noop,
    }

    fn addr() -> Address {
        Address::from_pubkey_bytes(&[7u8; 32]).expect("32 bytes is a valid pubkey")
    }

    fn action(signature: Option<&str>) -> Action<TestPayload> {
        Action {
            sender: addr(),
            nonce: 0,
            signature: signature.map(str::to_string),
            payload: TestPayload::Noop,
        }
    }

    fn block_with(actions: Vec<Action<TestPayload>>) -> Block<TestPayload> {
        Block {
            height: 9,
            parent_hash: String::new(),
            timestamp: 0,
            actions,
            proposer: None,
            signature: None,
        }
    }

    /// The bug this closes: identity used to be `signature.unwrap_or_default()`,
    /// so every unsigned action in a block collapsed onto `""`. Written through
    /// `insert_block`'s `ON CONFLICT (chain_id, action_hash) DO NOTHING`, that
    /// meant the second one onwards was silently discarded — no error, no log,
    /// just a missing row. Distinct identities are what make that impossible.
    #[test]
    fn unsigned_actions_in_one_block_get_distinct_identities() {
        let row = block_row_from_wire(&block_with(vec![action(None), action(None), action(None)]))
            .expect("payload serializes");

        let hashes: Vec<&str> = row.actions.iter().map(|a| a.action_hash.as_str()).collect();
        assert_eq!(hashes, vec!["9:0", "9:1", "9:2"]);
        assert!(!hashes.iter().any(|h| h.is_empty()), "no action may key on an empty identity");
    }

    /// An empty-string signature is an absent one, not an identity — otherwise
    /// it recreates the same collision through a different door.
    #[test]
    fn empty_signature_is_treated_as_absent() {
        let row = block_row_from_wire(&block_with(vec![action(Some("")), action(Some(""))]))
            .expect("payload serializes");

        assert_eq!(row.actions[0].action_hash, "9:0");
        assert_eq!(row.actions[1].action_hash, "9:1");
    }

    #[test]
    fn lag_is_absent_until_a_peer_reports_a_tip() {
        let status = IndexStatus {
            indexed_height: Some(10),
            tip_timestamp: None,
            node_tip_height: None,
            blocks_behind: None,
        }
        .with_network_tip(None);

        // Absent, not zero. "No peer has answered" and "caught up" are
        // different states and a monitor must be able to tell them apart.
        assert_eq!(status.node_tip_height, None);
        assert_eq!(status.blocks_behind, None);
    }

    #[test]
    fn lag_is_the_gap_to_the_network_tip() {
        let status = IndexStatus {
            indexed_height: Some(10),
            tip_timestamp: None,
            node_tip_height: None,
            blocks_behind: None,
        };

        assert_eq!(status.with_network_tip(Some(42)).blocks_behind, Some(32));
        assert_eq!(status.with_network_tip(Some(10)).blocks_behind, Some(0));
    }

    /// A node that has just rolled back can report a tip below ours for a
    /// moment. "Behind by -3 blocks" is not something a caller can act on.
    #[test]
    fn lag_never_goes_negative() {
        let status = IndexStatus {
            indexed_height: Some(10),
            tip_timestamp: None,
            node_tip_height: None,
            blocks_behind: None,
        }
        .with_network_tip(Some(7));

        assert_eq!(status.blocks_behind, Some(0));
        assert_eq!(status.node_tip_height, Some(7), "the tip itself is still reported honestly");
    }

    /// Nothing indexed but a known tip means every block including genesis is
    /// still outstanding — height 0 is a real block, so the count is tip + 1.
    #[test]
    fn lag_from_a_cold_start_counts_genesis() {
        let status = IndexStatus {
            indexed_height: None,
            tip_timestamp: None,
            node_tip_height: None,
            blocks_behind: None,
        }
        .with_network_tip(Some(5));

        assert_eq!(status.blocks_behind, Some(6));
    }

    /// The normal CoreChain path is unchanged: a signed action is still keyed by
    /// its signature, so existing rows keep resolving.
    #[test]
    fn signed_actions_still_key_on_their_signature() {
        let row = block_row_from_wire(&block_with(vec![action(Some("deadbeef"))]))
            .expect("payload serializes");

        assert_eq!(row.actions[0].action_hash, "deadbeef");
    }

    /// Batched writes are only correct if the parallel column vectors stay
    /// aligned — `UNNEST` zips them positionally, so a single push on one path
    /// but not another would silently file an action's payload under a
    /// different action's hash. Nothing about that failure is visible in the
    /// SQL, which is why the transpose is a pure function with this test on it.
    #[test]
    fn column_vectors_stay_aligned_and_ordered() {
        let extractor = AddressExtractor::tier_a_only(KindSchema::empty());
        let block = block_with(vec![action(Some("aa")), action(None), action(Some("cc"))]);

        let w = BlockWrites::build(&block, 9, &extractor).expect("payload serializes");

        assert_eq!(w.action_hash, vec!["aa", "9:1", "cc"]);
        assert_eq!(w.index_in_block, vec![0, 1, 2], "position must follow source order");
        let n = w.action_hash.len();
        for (label, len) in [
            ("index_in_block", w.index_in_block.len()),
            ("kind", w.kind.len()),
            ("from_address", w.from_address.len()),
            ("payload", w.payload.len()),
        ] {
            assert_eq!(len, n, "{label} vector is out of step with action_hash");
        }
    }

    /// The address vectors are sized independently of the action vectors — one
    /// action can resolve to several `(address, role)` pairs, or none — so
    /// their alignment is a separate claim from the one above. Each pair has to
    /// carry the hash of the action it actually came from, which is the part a
    /// batched insert can get wrong in a way no constraint would catch: the
    /// rows would still be valid, just attributed to the wrong action.
    #[test]
    fn address_rows_carry_the_hash_of_their_own_action() {
        /// Resolves two addresses per action, so a per-action fan-out greater
        /// than one is actually exercised.
        struct TwoRecipients;
        impl ActionIndexable for TwoRecipients {
            fn kind(&self) -> &str {
                "Noop"
            }
            fn resolve(&self, _payload: &serde_json::Value) -> Vec<(String, Role)> {
                vec![("alice".into(), Role::To), ("bob".into(), Role::Delegator)]
            }
        }

        let extractor =
            AddressExtractor::new(KindSchema::empty(), vec![Box::new(TwoRecipients)]);
        let block = block_with(vec![action(Some("aa")), action(None)]);

        let w = BlockWrites::build(&block, 9, &extractor).expect("payload serializes");

        // Both actions fan out to two pairs each, in action order.
        assert_eq!(w.addr_action_hash, vec!["aa", "aa", "9:1", "9:1"]);
        assert_eq!(w.addr_address, vec!["alice", "bob", "alice", "bob"]);
        assert_eq!(w.addr_role, vec!["to", "delegator", "to", "delegator"]);
    }

    /// An action resolving no addresses must contribute no address rows rather
    /// than a placeholder — a placeholder would shift every later pair by one
    /// and misattribute the rest of the block.
    #[test]
    fn unresolved_actions_contribute_no_address_rows() {
        let extractor = AddressExtractor::tier_a_only(KindSchema::empty());
        let block = block_with(vec![action(None), action(Some("bb"))]);

        let w = BlockWrites::build(&block, 9, &extractor).expect("payload serializes");

        assert!(w.addr_action_hash.is_empty());
        assert_eq!(w.action_hash, vec!["9:0", "bb"], "action rows are unaffected");
    }
}
