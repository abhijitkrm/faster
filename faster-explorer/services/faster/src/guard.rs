//! HTTP middleware: per-client rate limiting, CORS allow-list and request
//! metrics.

use std::collections::HashMap;
use std::hash::{BuildHasher, RandomState};
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use axum::extract::{ConnectInfo, MatchedPath, Request, State};
use axum::http::{header, HeaderValue, Method, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;

use crate::metrics;

#[derive(Clone)]
pub struct GuardConfig {
    /// Sustained requests per second per client; 0 disables limiting.
    pub rate: f64,
    pub burst: f64,
    /// Use the first X-Forwarded-For address as the client (only behind a
    /// trusted reverse proxy).
    pub trust_proxy: bool,
    /// Allowed browser origins; ["*"] allows any.
    pub cors_origins: Vec<String>,
}

const SHARDS: usize = 16;

/// Token buckets per client IP, sharded to keep lock hold times short.
pub struct Limiter {
    cfg: GuardConfig,
    shards: Vec<Mutex<HashMap<IpAddr, (f64, Instant)>>>,
    hasher: RandomState,
}

impl Limiter {
    pub fn new(cfg: GuardConfig) -> Arc<Limiter> {
        Arc::new(Limiter { cfg, shards: (0..SHARDS).map(|_| Mutex::default()).collect(), hasher: RandomState::new() })
    }

    fn allow(&self, ip: IpAddr) -> bool {
        if self.cfg.rate <= 0.0 {
            return true;
        }
        let shard = &self.shards[self.hasher.hash_one(ip) as usize % SHARDS];
        let mut m = shard.lock().unwrap();
        let now = Instant::now();
        let e = m.entry(ip).or_insert((self.cfg.burst, now));
        e.0 = (e.0 + now.duration_since(e.1).as_secs_f64() * self.cfg.rate).min(self.cfg.burst);
        e.1 = now;
        if e.0 >= 1.0 {
            e.0 -= 1.0;
            true
        } else {
            false
        }
    }

    /// Drops buckets that have refilled completely (idle clients).
    pub async fn sweep(self: Arc<Self>) {
        let mut t = tokio::time::interval(std::time::Duration::from_secs(60));
        loop {
            t.tick().await;
            let full_after = self.cfg.burst / self.cfg.rate.max(1e-9);
            for s in &self.shards {
                s.lock().unwrap().retain(|_, (_, last)| last.elapsed().as_secs_f64() < full_after);
            }
        }
    }

    fn client_ip(&self, req: &Request) -> Option<IpAddr> {
        if self.cfg.trust_proxy {
            if let Some(ip) = req
                .headers()
                .get("x-forwarded-for")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.split(',').next())
                .and_then(|v| v.trim().parse().ok())
            {
                return Some(ip);
            }
        }
        req.extensions().get::<ConnectInfo<SocketAddr>>().map(|c| c.0.ip())
    }
}

pub async fn guard(State(l): State<Arc<Limiter>>, req: Request, next: Next) -> Response {
    let start = Instant::now();
    let route = req.extensions().get::<MatchedPath>().map(|p| p.as_str().to_string());
    let origin = req.headers().get(header::ORIGIN).cloned();
    let exempt = matches!(req.uri().path(), "/health" | "/ready");
    let mut resp = if req.method() == Method::OPTIONS {
        StatusCode::NO_CONTENT.into_response()
    } else if !exempt && !l.client_ip(&req).is_none_or(|ip| l.allow(ip)) {
        metrics::get().rate_limited.fetch_add(1, Relaxed);
        let mut r = (StatusCode::TOO_MANY_REQUESTS, Json(json!({"error": "rate limit exceeded"}))).into_response();
        r.headers_mut().insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
        r
    } else {
        next.run(req).await
    };
    cors(&l.cfg.cors_origins, origin, &mut resp);
    metrics::get().http(route.as_deref().unwrap_or("unmatched"), resp.status().as_u16(), start.elapsed());
    resp
}

fn cors(allowed: &[String], origin: Option<HeaderValue>, r: &mut Response) {
    let h = r.headers_mut();
    if allowed.iter().any(|o| o == "*") {
        h.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, HeaderValue::from_static("*"));
    } else if let Some(o) = origin.filter(|o| o.to_str().is_ok_and(|s| allowed.iter().any(|a| a == s))) {
        h.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, o);
        h.insert(header::VARY, HeaderValue::from_static("Origin"));
    } else {
        return;
    }
    h.insert(header::ACCESS_CONTROL_ALLOW_METHODS, HeaderValue::from_static("GET, OPTIONS"));
    h.insert(header::ACCESS_CONTROL_ALLOW_HEADERS, HeaderValue::from_static("Content-Type"));
    h.insert(header::ACCESS_CONTROL_MAX_AGE, HeaderValue::from_static("86400"));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_bucket() {
        let l = Limiter::new(GuardConfig { rate: 1.0, burst: 3.0, trust_proxy: false, cors_origins: vec![] });
        let ip: IpAddr = "10.0.0.1".parse().unwrap();
        assert!(l.allow(ip) && l.allow(ip) && l.allow(ip));
        assert!(!l.allow(ip), "burst exhausted");
        assert!(l.allow("10.0.0.2".parse().unwrap()), "other clients unaffected");
        let off = Limiter::new(GuardConfig { rate: 0.0, burst: 0.0, trust_proxy: false, cors_origins: vec![] });
        assert!((0..1000).all(|_| off.allow(ip)));
    }

    #[test]
    fn cors_allow_list() {
        let mut r = StatusCode::OK.into_response();
        cors(&["https://a.example".into()], Some(HeaderValue::from_static("https://b.example")), &mut r);
        assert!(r.headers().get(header::ACCESS_CONTROL_ALLOW_ORIGIN).is_none());
        let mut r = StatusCode::OK.into_response();
        cors(&["https://a.example".into()], Some(HeaderValue::from_static("https://a.example")), &mut r);
        assert_eq!(r.headers()[header::ACCESS_CONTROL_ALLOW_ORIGIN], "https://a.example");
    }
}
