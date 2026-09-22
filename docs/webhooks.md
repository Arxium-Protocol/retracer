# Webhooks on top of the live tail

**Goal:** an issuer's back office gets an HTTP POST when something happens to
its asset or its holders on chain — the headline case being *"transfer
rejected for compliance reason X"* — without holding an SSE socket open, and
without losing events while it is down.

Retracer already has every piece but the delivery: a per-chain broadcast of
committed blocks (`blocks_tx`), `actions/stream` with an `address` filter,
and `dropped_actions` with the producer's rejection `reason`. A webhook is a
subscriber to that same broadcast that POSTs instead of streaming, and keeps
a cursor in Postgres so a dead receiver is replayed, not skipped.

## Phase 1 — rejections on the live tail (no new infra)

Today `actions/stream` emits committed actions only; rejections are written
to `dropped_actions` but never broadcast. Without this phase the headline use
case has nothing to hook.

- `BlockRow.dropped: Vec<DroppedRow>` — filled from the wire block's
  `effects.dropped` on ingest, and from `dropped_actions` on the paged reads,
  so stream and replay agree. Serialised only when non-empty: existing
  `blocks/stream` and `/blocks/{h}` consumers see no change on a chain with
  no rejections.
- `GET /v1/chains/{chain}/actions/dropped/stream?from_height=N&address=A` —
  one event per rejection, `id:` = `height:signature`, `address` = sender.
  A separate route rather than a named event on `actions/stream`, so no
  existing consumer has to learn a second event shape.

Caveat that stays true in every phase: only the *producing* node knows what
it rejected, so a Retracer following a non-producer has no rejections to
emit. Point the issuer's Retracer at a producer.

## Phase 2 — webhook subscriptions and delivery

- Migration `0007_webhooks.sql`:
  `webhooks(id, chain_id, url, secret, address, events TEXT[], cursor_height,
  cursor_index, enabled, failing_since, last_error, created_at)`.
  `events` ⊂ {`action`, `dropped`}. `address` optional, same semantics as the
  stream filter.
- CRUD under `/v1/chains/{chain}/webhooks` (`POST`, `GET`, `DELETE
  /{id}`), gated on the existing `--auth-token`; refused when no token is
  configured, since anyone who can register a hook can point it anywhere.
- One delivery task per hook. It subscribes to `blocks_tx`, replays from
  `cursor` through the tip using the same replay-then-live handoff as the
  SSE routes, and POSTs one JSON body per event with
  `X-Retracer-Event`, `X-Retracer-Id` (the SSE `id`), `X-Retracer-Timestamp`
  and `X-Retracer-Signature: sha256=<hmac(secret, timestamp.body)>`.
  Cursor advances only on 2xx; on failure, exponential backoff capped at a
  minute, cursor stays, so the receiver gets at-least-once and dedupes on
  `X-Retracer-Id`. Lagging behind the broadcast buffer is not a failure — the
  task just reopens its replay from the cursor.
- After 3 days of continuous failure (a constant for now — a flag when an
  operator needs a different window) the hook is disabled with `last_error`
  set; `GET` shows it, `POST` with the same URL re-enables it.

Status: both phases implemented. Delivery is sequential across a chain's
hooks on each wake (one slow receiver costs the others at most the 10s
request timeout per wake before it is backed off); per-hook tasks if that
ever matters.

What is deliberately not here: a management UI, per-hook payload templates,
a delivery log beyond `last_error`/`failing_since`, fan-out through a broker.
Add each when someone asks for it.

## Phase 3 — per-caller API keys (tenancy)

Implemented. One shared `--auth-token` meant every holder could list and
delete every issuer's hooks. `api_keys` (`0008_api_keys.sql`): a key is a
bearer confined to one address on one chain, SHA-256 stored, raw key shown
once, minted by the operator token only. The guard
(`retracer-core::auth::rest_guard`) resolves a bearer to a `Caller`
(`Operator` | `Key`) in the request extensions; webhook handlers scope
register/list/delete to the key's address; a key with `rps` is budgeted by
key id, others by IP as before. Not asset-scoped on purpose: identity and
budgets belong to a caller, and address scope already implies its assets.
No cache on the key lookup (one indexed read per keyed request) — add a
short TTL map if it ever shows in Postgres load.

