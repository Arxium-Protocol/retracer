// Copyright (c) 2026 Arxium Protocol AG
// SPDX-License-Identifier: Apache-2.0

//! Per-caller API keys: a second kind of bearer besides `--auth-token`, so
//! more than one issuer can use a Retracer that has webhooks on it without
//! seeing each other's. A key acts for exactly one address — its webhooks,
//! nothing else's — and may carry its own request budget.
//!
//! The guard in `retracer-core::auth` turns the bearer into a [`Caller`]
//! and puts it in the request extensions; handlers that care read it back
//! with [`caller`]. Keys are minted only by the operator token: no
//! self-serve signup, that is a product surface, not an indexer feature.

use super::{ApiError, ApiResult, AppState};
use axum::extract::{Path, State};
use axum::http::{Extensions, StatusCode};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use storage::ApiKeyRow;
use utoipa::ToSchema;

/// Who a request is from, once the guard has checked its bearer.
#[derive(Clone, Debug)]
pub enum Caller {
    /// `--auth-token`: sees and manages everything.
    Operator,
    /// An API key: acts for `key.address` on `key.chain_id` only.
    Key(ApiKeyRow),
}

impl Caller {
    /// The address this caller is confined to on `chain_id`, or `None` for
    /// the operator. A key used on another chain is treated as no caller at
    /// all — a key is minted for one chain.
    pub(super) fn scope(&self, chain_id: &str) -> Result<Option<&str>, ApiError> {
        match self {
            Caller::Operator => Ok(None),
            Caller::Key(key) if key.chain_id == chain_id => Ok(Some(&key.address)),
            Caller::Key(_) => Err(ApiError::Forbidden(
                "this API key was issued for another chain".into(),
            )),
        }
    }
}

/// The caller the guard attached, or 403 on an open API (no `--auth-token`
/// means nobody is authenticated, and an open API must not be made to POST
/// chain data anywhere).
pub(super) fn caller(extensions: &Extensions) -> Result<Caller, ApiError> {
    extensions
        .get::<Caller>()
        .cloned()
        .ok_or_else(|| ApiError::Forbidden("this route needs --auth-token or an API key".into()))
}

fn operator(extensions: &Extensions) -> Result<(), ApiError> {
    match caller(extensions)? {
        Caller::Operator => Ok(()),
        Caller::Key(_) => Err(ApiError::Forbidden(
            "API keys are managed with the operator token".into(),
        )),
    }
}

/// Only the hash is stored, so a leaked database does not leak keys.
pub fn hash(raw: &str) -> String {
    hex::encode(Sha256::digest(raw.as_bytes()))
}

/// Resolves a bearer that is not the operator token. `None` = unknown or
/// disabled key.
// ponytail: one indexed lookup per keyed request, no cache. Add a short
// TTL map here if keyed traffic ever shows up in the Postgres load.
pub async fn authenticate(pool: &PgPool, bearer: &str) -> anyhow::Result<Option<Caller>> {
    Ok(storage::get_api_key_by_hash(pool, &hash(bearer))
        .await?
        .map(Caller::Key))
}

#[derive(Deserialize, ToSchema)]
pub(super) struct CreateApiKey {
    /// Who it is for — shown in listings, nothing else.
    label: String,
    /// The one address the key acts for.
    address: String,
    /// Own request budget per second; omit for the per-IP default.
    rps: Option<u32>,
}

/// The raw key, returned once.
#[derive(Serialize, ToSchema)]
pub(super) struct CreatedApiKey {
    #[serde(flatten)]
    row: ApiKeyRow,
    /// Send as `Authorization: Bearer <key>`. Not stored; not shown again.
    key: String,
}

#[utoipa::path(post, path = "/v1/chains/{chain_id}/api-keys", tag = "api-keys", params(("chain_id" = String, Path)), request_body = CreateApiKey, responses((status = 201, body = CreatedApiKey), (status = 400, body = super::ErrorBody), (status = 403, body = super::ErrorBody), (status = 404, body = super::ErrorBody)))]
pub(super) async fn create(
    State(state): State<AppState>,
    Path(chain_id): Path<String>,
    extensions: Extensions,
    Json(body): Json<CreateApiKey>,
) -> Result<(StatusCode, Json<CreatedApiKey>), ApiError> {
    let chain = state.chain(&chain_id)?;
    operator(&extensions)?;
    if body.label.trim().is_empty() {
        return Err(ApiError::BadRequest("label must not be empty".into()));
    }
    if let Some(valid) = &chain.address_validator
        && !valid(&body.address)
    {
        return Err(ApiError::BadRequest(
            "not a valid address for this chain".into(),
        ));
    }
    if body.rps == Some(0) {
        return Err(ApiError::BadRequest("rps must be positive".into()));
    }
    let key = format!("rk_{}", hex::encode(rand::random::<[u8; 32]>()));
    let row = storage::insert_api_key(
        &state.pool,
        &chain_id,
        &hash(&key),
        body.label.trim(),
        &body.address,
        body.rps.map(|r| r.min(i32::MAX as u32) as i32),
    )
    .await?;
    Ok((StatusCode::CREATED, Json(CreatedApiKey { row, key })))
}

#[utoipa::path(get, path = "/v1/chains/{chain_id}/api-keys", tag = "api-keys", params(("chain_id" = String, Path)), responses((status = 200, body = Vec<ApiKeyRow>), (status = 403, body = super::ErrorBody), (status = 404, body = super::ErrorBody)))]
pub(super) async fn list(
    State(state): State<AppState>,
    Path(chain_id): Path<String>,
    extensions: Extensions,
) -> ApiResult<Vec<ApiKeyRow>> {
    state.chain(&chain_id)?;
    operator(&extensions)?;
    Ok(Json(storage::list_api_keys(&state.pool, &chain_id).await?))
}

#[utoipa::path(delete, path = "/v1/chains/{chain_id}/api-keys/{id}", tag = "api-keys", params(("chain_id" = String, Path), ("id" = i64, Path)), responses((status = 204), (status = 403, body = super::ErrorBody), (status = 404, body = super::ErrorBody)))]
pub(super) async fn remove(
    State(state): State<AppState>,
    Path((chain_id, id)): Path<(String, i64)>,
    extensions: Extensions,
) -> Result<StatusCode, ApiError> {
    state.chain(&chain_id)?;
    operator(&extensions)?;
    if storage::delete_api_key(&state.pool, &chain_id, id).await? {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(ApiError::NotFound(format!("no API key {id} on {chain_id}")))
    }
}

pub(super) fn routes() -> Router<AppState> {
    use axum::routing::{delete, get};
    Router::new()
        .route("/v1/chains/{chain_id}/api-keys", get(list).post(create))
        .route("/v1/chains/{chain_id}/api-keys/{id}", delete(remove))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::{lazy_state, rest_chain};
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt as _;

    fn key(chain_id: &str, address: &str) -> ApiKeyRow {
        ApiKeyRow {
            id: 7,
            chain_id: chain_id.into(),
            key_hash: hash("rk_test"),
            label: "issuer".into(),
            address: address.into(),
            rps: None,
            enabled: true,
            created_at: 0,
        }
    }

    #[test]
    fn scope_is_none_for_operator_and_the_address_for_a_key_on_its_chain() {
        assert!(matches!(Caller::Operator.scope("test-chain"), Ok(None)));
        let k = Caller::Key(key("test-chain", "arx1issuer"));
        assert!(matches!(k.scope("test-chain"), Ok(Some("arx1issuer"))));
        assert!(matches!(
            k.scope("other-chain"),
            Err(ApiError::Forbidden(_))
        ));
    }

    /// Key management is operator-only, and needs a caller at all.
    #[tokio::test]
    async fn key_management_needs_the_operator_token() {
        let state = lazy_state(vec![rest_chain(ingestion::NetworkView::default())]);
        let app = crate::router(state.pool.clone(), state.chains.to_vec(), "0");
        for caller in [None, Some(Caller::Key(key("test-chain", "arx1issuer")))] {
            let mut req = Request::get("/v1/chains/test-chain/api-keys");
            if let Some(c) = caller {
                req = req.extension(c);
            }
            let resp = app
                .clone()
                .oneshot(req.body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(resp.status(), 403);
        }
        // The operator gets past the gate and into validation.
        let resp = app
            .oneshot(
                Request::post("/v1/chains/test-chain/api-keys")
                    .extension(Caller::Operator)
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"label":"  ","address":"arx1x"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), 400);
    }
}
