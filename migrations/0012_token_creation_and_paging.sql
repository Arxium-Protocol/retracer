-- Resolve Create from authoritative registration effects, not a guessed ref.
-- Registration effect position is not action position: match creator + slug
-- in the same block. Initial supply comes from the action, not mutable state.
CREATE FUNCTION project_token_creations(p_chain TEXT, p_height BIGINT)
RETURNS void LANGUAGE SQL AS $$
    INSERT INTO token_events
        (chain_id, action_hash, token, event, from_address, to_address, amount, block_height, block_time)
    SELECT a.chain_id, a.action_hash, r.record ->> 'asset_ref', 'create',
           a.from_address, a.from_address,
           (a.payload -> 'Create' ->> 'initial_supply')::numeric,
           a.block_height, b.timestamp
    FROM actions a
    JOIN blocks b ON b.chain_id = a.chain_id AND b.height = a.block_height
    JOIN asset_registrations r ON r.chain_id = a.chain_id AND r.height = a.block_height
        AND r.record ->> 'issuer' = a.from_address
        AND r.record ->> 'asset_id' = lower(a.payload -> 'Create' ->> 'symbol')
    WHERE a.chain_id = p_chain AND a.block_height = p_height
        AND a.kind = 'Token' AND jsonb_typeof(a.payload -> 'Create') = 'object'
        AND r.record ->> 'asset_ref' IS NOT NULL
    ON CONFLICT DO NOTHING;
$$;

CREATE FUNCTION project_registered_token_creation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM project_token_creations(NEW.chain_id, NEW.height);
    RETURN NEW;
END;
$$;

-- Ingestion writes actions before their registration effects in one transaction.
CREATE TRIGGER registrations_project_token_create
    AFTER INSERT ON asset_registrations
    FOR EACH ROW EXECUTE FUNCTION project_registered_token_creation();

-- Backfill Create events for blocks already indexed before this migration.
DO $$
DECLARE registration RECORD;
BEGIN
    FOR registration IN SELECT DISTINCT chain_id, height FROM asset_registrations LOOP
        PERFORM project_token_creations(registration.chain_id, registration.height);
    END LOOP;
END;
$$;

-- Preserve existing order: height descending, hash ascending within a block.
CREATE INDEX token_events_page_idx
    ON token_events (chain_id, token, block_height DESC, action_hash ASC);
