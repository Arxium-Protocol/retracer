-- The four effect kinds the node's effects record gained in Arxium b360a9c:
-- equivocation evidence, BLS key registrations, operator authorizations and
-- attestor (de)registrations. Same shape as 0003: sparse, append-only, one
-- row per (key, height) at which it changed, NULL = cleared, rolled back by
-- `DELETE ... WHERE height > $1`. Attestors in particular exist nowhere else
-- Retracer can see — they're a merkleized node record, not an account field.

-- `record` NULL = deregistered at this height.
CREATE TABLE attestors (
    chain_id TEXT NOT NULL,
    attestor TEXT NOT NULL,
    height BIGINT NOT NULL,
    record JSONB,
    PRIMARY KEY (chain_id, attestor, height)
);

-- `pubkey` JSONB verbatim (the node's `BlsPublicKey`, 48 bytes). Registered
-- in block `height`, valid from `effective_height` (= height + 1).
CREATE TABLE bls_keys (
    chain_id TEXT NOT NULL,
    address TEXT NOT NULL,
    height BIGINT NOT NULL,
    effective_height BIGINT NOT NULL,
    pubkey JSONB NOT NULL,
    PRIMARY KEY (chain_id, address, height)
);

-- `operator` NULL = authorization revoked at this height.
CREATE TABLE operators (
    chain_id TEXT NOT NULL,
    validator TEXT NOT NULL,
    height BIGINT NOT NULL,
    operator TEXT,
    PRIMARY KEY (chain_id, validator, height)
);

-- Equivocation evidence the block at `height` processed (one slash each):
-- `proposer` double-signed at `evidence_height`.
CREATE TABLE evidence (
    chain_id TEXT NOT NULL,
    height BIGINT NOT NULL,
    proposer TEXT NOT NULL,
    evidence_height BIGINT NOT NULL,
    PRIMARY KEY (chain_id, height, proposer, evidence_height)
);
