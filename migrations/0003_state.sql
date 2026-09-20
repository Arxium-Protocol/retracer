-- State by height, from the node's `GET /blocks/{height}/effects`.
--
-- Every table here is sparse and append-only: one row per (key, height) at
-- which the key changed, never an update in place. "State at H" is the row
-- with the greatest height <= H, and a rollback is `DELETE ... WHERE height >
-- $1` exactly like the history tables — same reason `rollback_to` is short.
--
-- Balances are the node's u128, so NUMERIC rather than BIGINT. Anything whose
-- shape is the chain's business (a stake allocation, a holder's freeze state,
-- a validator's status enum) is JSONB verbatim, same policy as `payload`.

CREATE TABLE account_state (
    chain_id TEXT NOT NULL,
    address TEXT NOT NULL,
    height BIGINT NOT NULL,
    balance NUMERIC NOT NULL,
    nonce BIGINT NOT NULL,
    PRIMARY KEY (chain_id, address, height)
);

CREATE TABLE asset_balances (
    chain_id TEXT NOT NULL,
    asset TEXT NOT NULL,
    holder TEXT NOT NULL,
    height BIGINT NOT NULL,
    balance NUMERIC NOT NULL,
    PRIMARY KEY (chain_id, asset, holder, height)
);

-- Per-holder compliance state (frozen / locked amount) — a separate row from
-- the balance because the node changes them independently.
CREATE TABLE asset_holder_states (
    chain_id TEXT NOT NULL,
    asset TEXT NOT NULL,
    holder TEXT NOT NULL,
    height BIGINT NOT NULL,
    state JSONB NOT NULL,
    PRIMARY KEY (chain_id, asset, holder, height)
);

-- `allocation` NULL = the allocation was removed (fully resolved or slashed).
CREATE TABLE stakes (
    chain_id TEXT NOT NULL,
    master TEXT NOT NULL,
    validator TEXT NOT NULL,
    height BIGINT NOT NULL,
    allocation JSONB,
    PRIMARY KEY (chain_id, master, validator, height)
);

-- `status` NULL = the validator's status row was cleared.
CREATE TABLE validator_status (
    chain_id TEXT NOT NULL,
    address TEXT NOT NULL,
    height BIGINT NOT NULL,
    status JSONB,
    PRIMARY KEY (chain_id, address, height)
);

-- One row per epoch boundary: the stake-weighted set effective from
-- `height + 1`, as `{address: voting_power}`.
CREATE TABLE validator_sets (
    chain_id TEXT NOT NULL,
    height BIGINT NOT NULL,
    validators JSONB NOT NULL,
    PRIMARY KEY (chain_id, height)
);

CREATE TABLE asset_registrations (
    chain_id TEXT NOT NULL,
    height BIGINT NOT NULL,
    index_in_block INT NOT NULL,
    record JSONB NOT NULL,
    PRIMARY KEY (chain_id, height, index_in_block)
);

-- Actions the producer tried and rejected while building `block_height`.
-- Only the producing node knows these, so a Retracer following a non-producer
-- has none. Keyed with the height: a rejected action can be resubmitted and
-- rejected again later.
CREATE TABLE dropped_actions (
    chain_id TEXT NOT NULL,
    block_height BIGINT NOT NULL,
    signature TEXT NOT NULL,
    reason TEXT NOT NULL,
    PRIMARY KEY (chain_id, block_height, signature)
);

CREATE INDEX dropped_actions_signature_idx ON dropped_actions (chain_id, signature);
