//! Optional bearer-token auth and per-IP rate limiting for the REST surface,
//! plus per-caller API keys (`rest_service::api_keys`): a bearer that is not
//! the operator token is looked up as a key, and a key with its own `rps`
//! is limited by key id rather than by IP. Off by default (`None` token / `None` rate limit), which
//! preserves today's trusted-consumer behavior with zero config — this only
//! changes anything when an operator opts in via `--auth-token`/
//! `--rate-limit-rps` (or the matching env vars).
//!
//! Mirrors the pattern Arxium's own `core/rpc` already uses for its
//! `Authorization: Bearer` guard (`subtle::ConstantTimeEq`, a fixed-window
//! per-IP hit counter swept once it grows large) rather than inventing a
//! second one — same shape, ported to axum middleware.

use std::collections::HashMap;
use std::hash::Hash;
use std::net::IpAddr;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use axum::extract::{ConnectInfo, Request, State};
use axum::http::{StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use rest_service::api_keys::Caller;
use std::net::SocketAddr;
use std::sync::Arc;
use subtle::ConstantTimeEq;

const RATE_LIMIT_WINDOW: Duration = Duration::from_secs(60);
// ponytail: sweep-on-grow rather than a background task; bounds worst-case
// memory without a timer. Add a periodic sweep if the hit rate is high
// enough that this map crosses the threshold on nearly every request.
const RATE_LIMIT_SWEEP_THRESHOLD: usize = 10_000;

/// Fixed-window request counter per key — an IP for anonymous callers, an
/// API key id for keyed ones (each with its own budget).
pub struct RateLimiter<K = IpAddr> {
    max_per_window: u32,
    hits: Mutex<HashMap<K, (Instant, u32)>>,
}

impl<K: Hash + Eq + Copy> RateLimiter<K> {
    pub fn new(max_per_window: u32) -> Self {
        RateLimiter {
            max_per_window,
            hits: Mutex::new(HashMap::new()),
        }
    }

    pub fn allow(&self, key: K) -> bool {
        self.allow_up_to(key, self.max_per_window)
    }

    /// `allow` with a budget chosen per call, for keys that carry their own.
    pub fn allow_up_to(&self, ip: K, max_per_window: u32) -> bool {
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
        entry.1 <= max_per_window
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

/// One IP or CIDR range, e.g. `10.0.0.8` or `10.0.0.0/8`. A bare address
/// means the single host (`/32` / `/128`). Implemented by hand rather than a
/// new dependency: matching is a mask comparison, and the type never leaves
/// this module except inside [`TrustedProxies`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Cidr {
    network: IpAddr,
    prefix_len: u8,
}

impl Cidr {
    pub fn parse(raw: &str) -> Result<Self, String> {
        let raw = raw.trim();
        let (addr_str, prefix_len) = match raw.split_once('/') {
            Some((addr, prefix)) => (
                addr.trim(),
                prefix
                    .trim()
                    .parse::<u8>()
                    .map_err(|_| format!("invalid prefix in {raw:?}"))?,
            ),
            None if raw.contains(':') => (raw, 128),
            None => (raw, 32),
        };
        let network: IpAddr = addr_str
            .parse()
            .map_err(|_| format!("invalid IP address in {raw:?}"))?;
        let max = match network {
            IpAddr::V4(_) => 32,
            IpAddr::V6(_) => 128,
        };
        if prefix_len > max {
            return Err(format!(
                "prefix /{prefix_len} exceeds address length in {raw:?}"
            ));
        }
        Ok(Self {
            network,
            prefix_len,
        })
    }

    pub fn contains(&self, ip: IpAddr) -> bool {
        // A zero prefix matches the whole family; anything else shifts by
        // 1..=bits, which is always a valid shift amount. (`overflowing_shl`
        // would mask a full-width shift back to a no-op, so it cannot express
        // the /0 case.)
        match (self.network, ip) {
            (IpAddr::V4(net), IpAddr::V4(ip)) => {
                let mask = if self.prefix_len == 0 {
                    0
                } else {
                    u32::MAX << (32 - self.prefix_len)
                };
                u32::from(net) & mask == u32::from(ip) & mask
            }
            (IpAddr::V6(net), IpAddr::V6(ip)) => {
                let mask = if self.prefix_len == 0 {
                    0
                } else {
                    u128::MAX << (128 - self.prefix_len)
                };
                u128::from(net) & mask == u128::from(ip) & mask
            }
            _ => false,
        }
    }
}

/// The proxies whose `X-Forwarded-For` the limiter may believe. Empty (the
/// default) means none: the socket peer is always the client, and a spoofed
/// header can never mint a fresh rate-limit bucket.
#[derive(Clone, Debug, Default)]
pub struct TrustedProxies {
    entries: Vec<Cidr>,
}

impl TrustedProxies {
    /// Parses a comma-separated list of IPs/CIDRs. An empty string yields an
    /// empty (trust-nothing) set rather than an error, so an unfilled config
    /// var behaves like an unset one.
    pub fn parse_list(raw: &str) -> Result<Self, String> {
        raw.split(',')
            .filter(|s| !s.trim().is_empty())
            .map(Cidr::parse)
            .collect::<Result<Vec<_>, _>>()
            .map(|entries| Self { entries })
    }

    pub fn contains(&self, ip: IpAddr) -> bool {
        self.entries.iter().any(|c| c.contains(ip))
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// Effective client IP for rate limiting: the left-most `X-Forwarded-For`
/// entry, but only when the direct peer is a configured trusted proxy.
/// Anything else — untrusted peer, missing header, unparseable entry — falls
/// back to the peer address.
pub fn client_ip(
    peer: IpAddr,
    forwarded_for: Option<&str>,
    trusted: Option<&TrustedProxies>,
) -> IpAddr {
    let Some(trusted) = trusted else {
        return peer;
    };
    if !trusted.contains(peer) {
        return peer;
    }
    forwarded_for
        .and_then(|h| h.split(',').next())
        .map(str::trim)
        .and_then(|s| s.parse().ok())
        .unwrap_or(peer)
}

#[derive(Clone)]
pub struct GuardConfig {
    pub token: Option<Arc<String>>,
    pub rate_limiter: Option<Arc<RateLimiter>>,
    pub trusted_proxies: Option<Arc<TrustedProxies>>,
    /// Where API keys are looked up. `None` = only the operator token is a
    /// valid bearer. Only meaningful with `token` set: keys are minted by
    /// the operator, so an open API has none.
    pub keys: Option<sqlx::PgPool>,
    /// Budgets for keys that carry their own `rps`; the ceiling is per call.
    pub key_limiter: Arc<RateLimiter<i64>>,
}

impl GuardConfig {
    pub fn new(token: Option<String>, rate_limit_rps: Option<u32>) -> Self {
        GuardConfig {
            token: token.map(Arc::new),
            rate_limiter: rate_limit_rps
                .map(|rps| Arc::new(RateLimiter::new(rps.saturating_mul(60)))),
            trusted_proxies: None,
            keys: None,
            key_limiter: Arc::new(RateLimiter::new(0)),
        }
    }

    /// Accept API keys from this database besides the operator token.
    pub fn with_api_keys(mut self, pool: sqlx::PgPool) -> Self {
        self.keys = Some(pool);
        self
    }

    /// Trust `X-Forwarded-For` from these proxies when attributing rate-limit
    /// buckets. `None` or an empty set keeps the previous behaviour: the
    /// socket peer is always the client.
    pub fn with_trusted_proxies(mut self, proxies: Option<TrustedProxies>) -> Self {
        self.trusted_proxies = proxies.filter(|p| !p.is_empty()).map(Arc::new);
        self
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
///
/// With a token configured, every non-exempt request is either the operator
/// (the token itself) or an API key; the resolved [`Caller`] rides in the
/// request extensions for the handlers that are scoped by it. A key with
/// its own `rps` is budgeted by key id; any other caller by IP as before.
pub async fn rest_guard(
    State(guard): State<GuardConfig>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    mut req: Request,
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
        let caller = if token_matches(token, header_value) {
            Caller::Operator
        } else {
            let bearer = header_value.and_then(|v| v.strip_prefix("Bearer "));
            match (bearer, &guard.keys) {
                (Some(bearer), Some(pool)) => {
                    match rest_service::api_keys::authenticate(pool, bearer).await {
                        Ok(Some(caller)) => caller,
                        Ok(None) => return StatusCode::UNAUTHORIZED.into_response(),
                        Err(err) => {
                            tracing::error!("API key lookup failed: {err:#}");
                            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
                        }
                    }
                }
                _ => return StatusCode::UNAUTHORIZED.into_response(),
            }
        };
        if let Caller::Key(key) = &caller
            && let Some(rps) = key.rps
            && !guard
                .key_limiter
                .allow_up_to(key.id, (rps.max(0) as u32).saturating_mul(60))
        {
            return StatusCode::TOO_MANY_REQUESTS.into_response();
        }
        let keyed_budget = matches!(&caller, Caller::Key(k) if k.rps.is_some());
        req.extensions_mut().insert(caller);
        if keyed_budget {
            return next.run(req).await;
        }
    }
    if let Some(limiter) = &guard.rate_limiter {
        let forwarded = req
            .headers()
            .get("x-forwarded-for")
            .and_then(|v| v.to_str().ok());
        let ip = client_ip(addr.ip(), forwarded, guard.trusted_proxies.as_deref());
        if !limiter.allow(ip) {
            return StatusCode::TOO_MANY_REQUESTS.into_response();
        }
    }
    next.run(req).await
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

    /// Keyed budgets: each key id counts alone, against its own ceiling.
    #[test]
    fn key_limiter_budgets_each_key_separately() {
        let limiter: RateLimiter<i64> = RateLimiter::new(0);
        assert!(limiter.allow_up_to(1, 1));
        assert!(!limiter.allow_up_to(1, 1));
        assert!(limiter.allow_up_to(2, 3));
        assert!(limiter.allow_up_to(2, 3));
    }

    #[test]
    fn cidr_matching_covers_hosts_ranges_and_families() {
        let host: IpAddr = "10.0.0.8".parse().unwrap();
        assert!(Cidr::parse("10.0.0.8").unwrap().contains(host));
        assert!(Cidr::parse("10.0.0.0/8").unwrap().contains(host));
        assert!(
            !Cidr::parse("10.0.0.0/8")
                .unwrap()
                .contains("11.0.0.8".parse().unwrap())
        );
        // Non-canonical network addresses still match: both sides are masked.
        assert!(Cidr::parse("10.0.0.1/8").unwrap().contains(host));
        assert!(Cidr::parse("::1").unwrap().contains("::1".parse().unwrap()));
        assert!(
            Cidr::parse("2001:db8::/32")
                .unwrap()
                .contains("2001:db8::1".parse().unwrap())
        );
        // Families never mix.
        assert!(!Cidr::parse("::ffff:10.0.0.0/104").unwrap().contains(host));
        assert!(
            !Cidr::parse("10.0.0.0/8")
                .unwrap()
                .contains("::1".parse().unwrap())
        );
        // Zero-length prefix matches everything in its family.
        assert!(Cidr::parse("0.0.0.0/0").unwrap().contains(host));
        assert!(Cidr::parse("10.0.0.1/33").is_err());
        assert!(Cidr::parse("not-an-ip").is_err());
        assert!(Cidr::parse("10.0.0.0/abc").is_err());
        assert!(TrustedProxies::parse_list("").unwrap().is_empty());
        assert!(
            TrustedProxies::parse_list("10.0.0.0/8, 192.168.1.7")
                .unwrap()
                .contains(host)
        );
        assert!(TrustedProxies::parse_list("10.0.0.0/8, garbage").is_err());
    }

    #[test]
    fn forwarded_headers_only_count_from_trusted_peers() {
        let proxy: IpAddr = "10.0.0.8".parse().unwrap();
        let client: IpAddr = "203.0.113.7".parse().unwrap();
        let trusted = TrustedProxies::parse_list("10.0.0.0/8").unwrap();
        // Trusted peer: left-most entry wins.
        assert_eq!(
            client_ip(proxy, Some("203.0.113.7, 70.0.0.1"), Some(&trusted)),
            client
        );
        // Untrusted peer: header ignored.
        let stranger: IpAddr = "198.51.100.9".parse().unwrap();
        assert_eq!(
            client_ip(stranger, Some("203.0.113.7"), Some(&trusted)),
            stranger
        );
        // No trust configured, or garbage header: peer address.
        assert_eq!(client_ip(proxy, Some("203.0.113.7"), None), proxy);
        assert_eq!(client_ip(proxy, Some("not-an-ip"), Some(&trusted)), proxy);
        assert_eq!(client_ip(proxy, None, Some(&trusted)), proxy);
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
