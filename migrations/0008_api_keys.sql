-- Per-caller API keys, so more than one issuer can use a Retracer that has
-- webhooks on it. A key is a bearer credential like `--auth-token`, but
-- scoped: it acts only for `address` (its webhooks, nothing else's) and,
-- when `rps` is set, has its own request budget instead of the per-IP one.
-- The raw key is shown once at creation and only its SHA-256 is kept.
-- Keys are created by the operator token, never self-served.
CREATE TABLE api_keys (
    id BIGSERIAL PRIMARY KEY,
    chain_id TEXT NOT NULL REFERENCES chains (chain_id),
    key_hash TEXT NOT NULL UNIQUE,
    label TEXT NOT NULL,
    address TEXT NOT NULL,
    -- NULL = the global --rate-limit-rps applies, per IP.
    rps INT,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM now())::BIGINT
);
