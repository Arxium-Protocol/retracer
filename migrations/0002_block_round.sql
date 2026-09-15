-- Consensus round the block was produced in. 0 is the primary designee's
-- turn; N > 0 means a quorum certified N timeouts and a backup proposed.
-- Rows written before this migration default to 0, which is wrong only for
-- historical backup-proposed blocks; re-index to repair them.
-- A constant default makes this a metadata-only change (no table rewrite).
ALTER TABLE blocks ADD COLUMN round BIGINT NOT NULL DEFAULT 0;
