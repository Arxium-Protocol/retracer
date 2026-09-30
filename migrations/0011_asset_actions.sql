-- Asset references are chain-wide identifiers. Project reference fields in
-- decoded payloads, including nested Token actions and payout assets.
-- One row per (asset, action) deduplicates actions mentioning an asset twice.
CREATE TABLE asset_actions (
    chain_id TEXT NOT NULL,
    asset_ref TEXT NOT NULL,
    action_hash TEXT NOT NULL,
    block_height BIGINT NOT NULL,
    index_in_block INT NOT NULL,
    PRIMARY KEY (chain_id, asset_ref, action_hash),
    FOREIGN KEY (chain_id, action_hash)
        REFERENCES actions (chain_id, action_hash) ON DELETE CASCADE
);

CREATE INDEX asset_actions_position_idx
    ON asset_actions (chain_id, asset_ref, block_height DESC, index_in_block DESC);

CREATE FUNCTION project_asset_action() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO asset_actions (chain_id, asset_ref, action_hash, block_height, index_in_block)
    SELECT NEW.chain_id, refs.ref, NEW.action_hash, NEW.block_height, NEW.index_in_block
    FROM (
        SELECT DISTINCT value #>> '{}' AS ref
        FROM (
            SELECT jsonb_path_query(NEW.payload, '$.**.asset') AS value
            UNION ALL SELECT jsonb_path_query(NEW.payload, '$.**.payout_asset')
            UNION ALL SELECT jsonb_path_query(NEW.payload, '$.**.token')
        ) values_by_field
        WHERE jsonb_typeof(value) = 'string'
    ) refs
    WHERE refs.ref ~ '^arxasset1[02-9ac-hj-np-z]{58}$'
    ON CONFLICT DO NOTHING;
    RETURN NEW;
END;
$$;

-- Transactional with the existing ingestion path. The FK cascade also makes
-- rollback delete index rows with their actions, without a second rewind path.
CREATE TRIGGER actions_project_asset_refs
    AFTER INSERT ON actions FOR EACH ROW EXECUTE FUNCTION project_asset_action();

-- Existing chains gain the same projection on migration; no node replay is
-- required. Creation actions without a reference cannot be inferred here.
INSERT INTO asset_actions (chain_id, asset_ref, action_hash, block_height, index_in_block)
SELECT a.chain_id, refs.ref, a.action_hash, a.block_height, a.index_in_block
FROM actions a
CROSS JOIN LATERAL (
    SELECT DISTINCT value #>> '{}' AS ref
    FROM (
        SELECT jsonb_path_query(a.payload, '$.**.asset') AS value
        UNION ALL SELECT jsonb_path_query(a.payload, '$.**.payout_asset')
        UNION ALL SELECT jsonb_path_query(a.payload, '$.**.token')
    ) values_by_field
    WHERE jsonb_typeof(value) = 'string'
) refs
WHERE refs.ref ~ '^arxasset1[02-9ac-hj-np-z]{58}$'
ON CONFLICT DO NOTHING;
