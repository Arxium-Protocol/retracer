-- Retracer schema.
--
-- Collapsed from what were migrations 0001-0005 into one file: nothing has
-- shipped yet, so there is no deployed database whose history needs
-- preserving, and a single readable schema beats replaying an archaeology of
-- ALTERs. Once this is published that stops being true — from then on, never
-- edit this file; add a new numbered migration instead. SQLx verifies applied
-- migrations by hashing their exact bytes, so an edit to an applied file makes
-- every existing database refuse to start.
--
-- Design notes and the reasoning behind these shapes live in
-- ../Retracer_Design.md.
--
-- Everything is scoped by `chain_id`, an indexer-assigned label that does not
-- exist on the wire. One process can follow several chains at once, which is
-- why it is the leading column of every primary key and every index.

-- Registry of the chains this deployment follows. Upserted at startup from
-- config, so the config stays the source of truth and this is a projection of
-- it. Its purpose is discovery: without it a client has no way to learn which
-- chains the API will answer for.
--
-- Deliberately not referenced by a foreign key from blocks or actions. Those
-- are written on the hot ingestion path, and an FK check per block buys
-- nothing when the writer is the same process that registered the chain.
CREATE TABLE chains (
    chain_id TEXT PRIMARY KEY,
    display_name TEXT,
    -- The wire agreement with this chain's node. Recorded rather than derived,
    -- because they must match what the node publishes and cannot be guessed
    -- from the chain_id.
    blocks_topic TEXT NOT NULL,
    sync_protocol TEXT NOT NULL,
    -- Deepest reorg the indexer will un-index for this chain. 0 declares the
    -- chain fork-free, which is CoreChain's case.
    finality_depth BIGINT NOT NULL DEFAULT 0,
    registered_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE blocks (
    chain_id TEXT NOT NULL,
    height BIGINT NOT NULL,
    hash TEXT NOT NULL,
    parent_hash TEXT NOT NULL,
    timestamp BIGINT NOT NULL,
    -- Validator that produced this block. NULL for genesis (unsigned) and for
    -- a block from a non-validator solo node — both real absences, not gaps.
    proposer TEXT,
    PRIMARY KEY (chain_id, height)
);

CREATE UNIQUE INDEX blocks_chain_hash_idx ON blocks (chain_id, hash);

-- Supports the proposer rollup (COUNT and MIN/MAX(height) grouped by
-- proposer). Partial, because NULL rows are exactly the ones that roll up to
-- nothing and there is no reason to carry them in the index.
CREATE INDEX blocks_proposer_idx
    ON blocks (chain_id, proposer, height DESC)
    WHERE proposer IS NOT NULL;

-- `action_hash` is the action's identity, which is not always a hash: it is
-- the chain's own id for the action where one exists (for Arxium, its
-- signature), and its `height:index` position where none does. Keying every
-- unsigned action on one sentinel instead would make all but the first collide
-- on this primary key and vanish silently through ON CONFLICT DO NOTHING.
--
-- `payload` is JSONB and `kind` is split out of the serialized shape at write
-- time, so this schema never needs to know a chain's payload variants.
CREATE TABLE actions (
    chain_id TEXT NOT NULL,
    action_hash TEXT NOT NULL,
    block_height BIGINT NOT NULL,
    index_in_block INT NOT NULL,
    kind TEXT NOT NULL,
    from_address TEXT NOT NULL,
    payload JSONB NOT NULL,
    PRIMARY KEY (chain_id, action_hash),
    FOREIGN KEY (chain_id, block_height) REFERENCES blocks (chain_id, height)
);

-- Postgres does not index foreign-key columns automatically, so the FK above
-- provides no lookup path on its own. Without this index every read that
-- reaches actions by position is a sequential scan: the per-block action count
-- in the block list (once per row), the action list for a single block, and
-- the newest-first action feed's ORDER BY.
--
-- Column order follows those access patterns — equality on chain_id, then
-- block_height (equality for per-block reads, range for the cursor), then
-- index_in_block so the row-value cursor and the ordering need no sort step.
-- No descending variant: Postgres scans a btree backwards at the same cost.
CREATE INDEX actions_chain_height_idx
    ON actions (chain_id, block_height, index_in_block);

-- Denormalized sender index: one row per action, keyed on who sent it.
CREATE TABLE account_actions (
    chain_id TEXT NOT NULL,
    address TEXT NOT NULL,
    block_height BIGINT NOT NULL,
    action_hash TEXT NOT NULL,
    PRIMARY KEY (chain_id, address, block_height, action_hash)
);

CREATE INDEX account_actions_addr_height_idx
    ON account_actions (chain_id, address, block_height DESC);

-- Non-sender addresses and the role they play, resolved at ingestion time from
-- kind_schema.toml (Tier A) or an ActionIndexable impl (Tier B). Senders are
-- deliberately not duplicated here — they are already covered by
-- actions.from_address and account_actions.
CREATE TABLE action_addresses (
    chain_id TEXT NOT NULL,
    action_hash TEXT NOT NULL,
    address TEXT NOT NULL,
    role TEXT NOT NULL,
    block_height BIGINT NOT NULL,
    PRIMARY KEY (chain_id, action_hash, address, role)
);

CREATE INDEX action_addresses_addr_role_idx
    ON action_addresses (chain_id, address, role, block_height DESC);

-- Highest height fully written per chain. Rewound by a reorg rollback, which
-- is why it is a separate row rather than derived from MAX(blocks.height):
-- the two would disagree mid-rollback.
CREATE TABLE ingestion_cursor (
    chain_id TEXT PRIMARY KEY,
    last_height BIGINT NOT NULL
);
