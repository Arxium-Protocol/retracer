-- Token actions, normalised. A token's actions are stored in `actions` as kind
-- 'Token' with the variant nested ({"Transfer":{"token","to","amount"}}), and
-- kind_schema.toml only indexes the recipient roles, so "everything that
-- happened to token X" was not queryable. This is one row per Token action
-- that names a token, keyed by the action, written at ingest by
-- `storage::insert_block_in_tx` from the same `actions` rows (see
-- TOKEN_EVENTS_SQL there; keep the two statements in step).
--
-- `Create` is not here: its payload carries no token ref (the node derives it
-- from sender + symbol), and Retracer links no Arxium crate to re-derive it.
-- Creation block and initial supply already come from asset_registrations and
-- asset_balances.
--
-- amount is NUMERIC(39,0): a u128 is at most 39 digits. NULL for RenounceMint.
-- to_address is NULL for Burn/RenounceMint.
CREATE TABLE token_events (
    chain_id TEXT NOT NULL,
    action_hash TEXT NOT NULL,
    token TEXT NOT NULL,
    -- Lower-case variant name: mint | transfer | burn | renouncemint
    event TEXT NOT NULL,
    from_address TEXT NOT NULL,
    to_address TEXT,
    amount NUMERIC(39, 0),
    block_height BIGINT NOT NULL,
    -- Block timestamp (unix seconds), copied so daily/24h reads need no join.
    block_time BIGINT NOT NULL,
    PRIMARY KEY (chain_id, action_hash)
);

-- Per-token recent activity, newest first.
CREATE INDEX token_events_token_idx ON token_events (chain_id, token, block_height DESC);
-- Batch reads across many tokens ("transfers in the last 24h").
CREATE INDEX token_events_time_idx ON token_events (chain_id, block_time DESC);

-- Daily rollup. A view, not a table: it is always consistent with
-- token_events, including after `rollback_to` deletes rows, and the
-- (chain_id, token, ...) index bounds every read to one token or one window.
-- Materialise it if a chain's volume ever makes that scan slow.
CREATE VIEW token_daily AS
SELECT chain_id,
       token,
       (to_timestamp(block_time) AT TIME ZONE 'UTC')::date AS day,
       COUNT(*) FILTER (WHERE event = 'transfer') AS transfers,
       COALESCE(SUM(amount) FILTER (WHERE event = 'transfer'), 0) AS transfer_volume,
       COUNT(*) FILTER (WHERE event = 'mint') AS mints,
       COUNT(*) FILTER (WHERE event = 'burn') AS burns
FROM token_events
GROUP BY chain_id, token, day;

-- Backfill from what is already indexed. Same logic as TOKEN_EVENTS_SQL.
INSERT INTO token_events
    (chain_id, action_hash, token, event, from_address, to_address, amount, block_height, block_time)
SELECT a.chain_id, a.action_hash, v.body ->> 'token', lower(v.event), a.from_address,
       v.body ->> 'to', (v.body ->> 'amount')::numeric, a.block_height, b.timestamp
FROM actions a
JOIN blocks b ON b.chain_id = a.chain_id AND b.height = a.block_height
CROSS JOIN LATERAL jsonb_each(
    CASE WHEN jsonb_typeof(a.payload) = 'object' THEN a.payload ELSE '{}'::jsonb END
) AS v(event, body)
WHERE a.kind = 'Token' AND v.body ->> 'token' IS NOT NULL
ON CONFLICT DO NOTHING;
