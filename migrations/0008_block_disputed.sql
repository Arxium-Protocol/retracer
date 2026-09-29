-- A block that lost an execution dispute (Arxium PoE v5 §3.3): it stays
-- certified but never becomes FINAL. Set when a later block's effects carry
-- the upheld dispute and the node confirms this block's `settlement` is
-- `disputed`. The other settlement states (pending/attested/final) are pure
-- functions of the node's watermarks, so they're derived at read time, not
-- stored.
ALTER TABLE blocks ADD COLUMN disputed BOOLEAN NOT NULL DEFAULT false;
