-- Who sent a dropped action. The node started reporting it with the effects
-- record (Arxium `f837d44`); rows written before that have an empty sender.
-- A separate migration rather than an edit of 0003: sqlx checksums applied
-- migrations, and 0003 has already run on test databases.
ALTER TABLE dropped_actions ADD COLUMN sender TEXT NOT NULL DEFAULT '';
CREATE INDEX dropped_actions_sender_idx ON dropped_actions (chain_id, sender, block_height DESC);
