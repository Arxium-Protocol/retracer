-- Daily and batch reads are windowed by time, but `token_daily` could not
-- push its day filter below the GROUP BY, so every read scanned a token's
-- whole history. Reads now query `token_events` directly with a block_time
-- bound; this index makes that bound a range scan per token. The view is
-- dropped so there is one definition of "a day" (in storage), not two.
DROP VIEW token_daily;
CREATE INDEX token_events_token_time_idx ON token_events (chain_id, token, block_time DESC);
