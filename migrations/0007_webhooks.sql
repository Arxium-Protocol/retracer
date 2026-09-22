-- Webhook subscriptions: an HTTP receiver that gets one POST per action /
-- rejection instead of holding an SSE tail open. `cursor_height` is the last
-- block whose events were all delivered — advanced only on 2xx, so a
-- receiver that is down is replayed from Postgres when it returns, never
-- skipped. Redelivery of a partially delivered block is possible (at least
-- once); receivers dedupe on `X-Retracer-Id`.
CREATE TABLE webhooks (
    id BIGSERIAL PRIMARY KEY,
    chain_id TEXT NOT NULL REFERENCES chains (chain_id),
    url TEXT NOT NULL,
    -- HMAC-SHA256 key for `X-Retracer-Signature`. Never returned by the API.
    secret TEXT NOT NULL,
    -- NULL = every action / rejection on the chain.
    address TEXT,
    -- Subset of {'action', 'dropped'}.
    events TEXT[] NOT NULL,
    cursor_height BIGINT NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    -- Unix seconds. Set on the first failed delivery after a success,
    -- cleared on the next success; the dispatcher disables the hook once it
    -- has been set for too long. Plain integers like `blocks.timestamp`, so
    -- no chrono/time feature is needed on the sqlx side.
    failing_since BIGINT,
    last_error TEXT,
    created_at BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM now())::BIGINT,
    UNIQUE (chain_id, url)
);
