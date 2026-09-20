-- The account record as the node holds it, beyond balance/nonce: identity
-- hash, attestation, claims, jurisdiction. JSONB verbatim (chain-shaped,
-- same policy as `payload`), so an explorer can render an account page from
-- Retracer alone. '{}' for rows written before this column existed.
ALTER TABLE account_state ADD COLUMN entry JSONB NOT NULL DEFAULT '{}';
