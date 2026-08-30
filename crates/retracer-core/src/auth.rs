//! Optional bearer-token auth and per-IP rate limiting for both the gRPC and
//! REST surfaces. Off by default (`None` token / `None` rate limit), which
//! preserves today's trusted-consumer behavior with zero config — this only
//! changes anything when an operator opts in via `--auth-token`/
//! `--rate-limit-rps` (or the matching env vars).
//!
//! Mirrors the pattern Arxium's own `core/rpc` already uses for its
//! `Authorization: Bearer` guard (`subtle::ConstantTimeEq`, a fixed-window
//! per-IP hit counter swept once it grows large) rather than inventing a
//! second one — same shape, ported to guard two servers (axum + tonic)
//! instead of one.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use axum::extract::{ConnectInfo, Request, State};
use axum::http::{StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use std::net::SocketAddr;
use std::sync::Arc;
use subtle::ConstantTimeEq;

const RATE_LIMIT_WINDOW: Duration = Duration::from_secs(60);
// ponytail: sweep-on-grow rather than a background task; bounds worst-case
// memory without a timer. Add a periodic sweep if the hit rate is high
// enough that this map crosses the threshold on nearly every request.
const RATE_LIMIT_SWEEP_THRESHOLD: usize = 10_000;

/// Fixed-window per-IP request counter, shared between the REST middleware
/// and the gRPC interceptor.
pub struct RateLimiter {
    max_per_window: u32,
    hits: Mutex<HashMap<IpAddr, (Instant, u32)>>,
}

impl RateLimiter {
    pub fn new(max_per_window: u32) -> Self {
        RateLimiter {
            max_per_window,
            hits: Mutex::new(HashMap::new()),
        }
    }

    pub fn allow(&self, ip: IpAddr) -> bool {
        let mut hits = self.hits.lock().unwrap_or_else(|e| e.into_inner());
        let now = Instant::now();

        if hits.len() > RATE_LIMIT_SWEEP_THRESHOLD {
            hits.retain(|_, (seen, _)| now.duration_since(*seen) <= RATE_LIMIT_WINDOW);
        }

        let entry = hits.entry(ip).or_insert((now, 0));
        if now.duration_since(entry.0) > RATE_LIMIT_WINDOW {
            *entry = (now, 0);
        }
        entry.1 += 1;
        entry.1 <= self.max_per_window
    }
}

/// Constant-time `Authorization: Bearer <token>` check, so a wrong guess
/// can't be distinguished from a right one by response timing.
fn token_matches(expected: &str, header_value: Option<&str>) -> bool {
    let expected = format!("Bearer {expected}");
    header_value.is_some_and(|value| {
        value.len() == expected.len() && value.as_bytes().ct_eq(expected.as_bytes()).into()
    })
}

#[derive(Clone)]
pub struct GuardConfig {
    pub token: Option<Arc<String>>,
    pub rate_limiter: Option<Arc<RateLimiter>>,
}

impl GuardConfig {
    pub fn new(token: Option<String>, rate_limit_rps: Option<u32>) -> Self {
        GuardConfig {
            token: token.map(Arc::new),
            rate_limiter: rate_limit_rps
                .map(|rps| Arc::new(RateLimiter::new(rps.saturating_mul(60)))),
        }
    }

    pub fn is_active(&self) -> bool {
        self.token.is_some() || self.rate_limiter.is_some()
    }
}

fn auth_exempt(path: &str) -> bool {
    matches!(path, "/health" | "/ready")
}

fn rate_limit_exempt(path: &str) -> bool {
    path == "/health"
}

/// axum middleware. Both probes bypass authentication, but only `/health`
/// bypasses rate limiting; repeated readiness checks still consume capacity.
pub async fn rest_guard(
    State(guard): State<GuardConfig>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    req: Request,
    next: Next,
) -> Response {
    let path = req.uri().path();
    if rate_limit_exempt(path) {
        return next.run(req).await;
    }
    if !auth_exempt(path)
        && let Some(token) = &guard.token
    {
        let header_value = req
            .headers()
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok());
        if !token_matches(token, header_value) {
            return StatusCode::UNAUTHORIZED.into_response();
        }
    }
    if let Some(limiter) = &guard.rate_limiter
        && !limiter.allow(addr.ip())
    {
        return StatusCode::TOO_MANY_REQUESTS.into_response();
    }
    next.run(req).await
}

/// tonic interceptor, same checks as [`rest_guard`] for the gRPC surface.
#[derive(Clone)]
pub struct GrpcGuard(pub GuardConfig);

impl tonic::service::Interceptor for GrpcGuard {
    fn call(&mut self, req: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
        if let Some(token) = &self.0.token {
            let header_value = req
                .metadata()
                .get("authorization")
                .and_then(|v| v.to_str().ok());
            if !token_matches(token, header_value) {
                return Err(tonic::Status::unauthenticated(
                    "invalid or missing bearer token",
                ));
            }
        }
        if let Some(limiter) = &self.0.rate_limiter
            && let Some(ip) = req.remote_addr().map(|a| a.ip())
            && !limiter.allow(ip)
        {
            return Err(tonic::Status::resource_exhausted("rate limit exceeded"));
        }
        Ok(req)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_matches_exact_bearer_value_only() {
        assert!(token_matches("secret", Some("Bearer secret")));
        assert!(!token_matches("secret", Some("Bearer wrong")));
        assert!(!token_matches("secret", Some("secret")));
        assert!(!token_matches("secret", None));
    }

    #[test]
    fn rate_limiter_allows_up_to_max_then_blocks() {
        let limiter = RateLimiter::new(2);
        let ip: IpAddr = "127.0.0.1".parse().unwrap();
        assert!(limiter.allow(ip));
        assert!(limiter.allow(ip));
        assert!(!limiter.allow(ip));
    }

    #[test]
    fn health_is_unconditional_but_ready_is_still_rate_limited() {
        assert!(auth_exempt("/health"));
        assert!(rate_limit_exempt("/health"));
        assert!(auth_exempt("/ready"));
        assert!(!rate_limit_exempt("/ready"));
        assert!(!auth_exempt("/v1/chains"));
        assert!(!rate_limit_exempt("/v1/chains"));
    }
}
