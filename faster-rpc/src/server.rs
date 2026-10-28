//! HTTP server: JSON-RPC over POST (single calls and batches), WebSocket on
//! the same path, probes, metrics, API keys and rate limits.

use std::collections::HashMap;
use std::hash::{BuildHasher, RandomState};
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::body::Bytes;
use axum::extract::ws::WebSocketUpgrade;
use axum::extract::{ConnectInfo, Request, State};
use axum::http::{header, HeaderValue, Method, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde_json::{json, Value};

use crate::methods::Ctx;

#[derive(Default)]
pub struct Metrics {
    pub requests: AtomicU64,
    pub rate_limited: AtomicU64,
    /// method → (calls, errors, total µs)
    pub methods: Mutex<HashMap<String, (u64, u64, u64)>>,
}

impl Metrics {
    fn record(&self, method: &str, ok: bool, d: Duration) {
        let mut m = self.methods.lock().unwrap();
        let e = m.entry(method.chars().take(64).collect()).or_default();
        e.0 += 1;
        e.1 += u64::from(!ok);
        e.2 += d.as_micros() as u64;
    }

    pub fn render(&self) -> String {
        let mut s = String::new();
        s.push_str("# TYPE frpc_http_requests_total counter\n");
        s.push_str(&format!("frpc_http_requests_total {}\n", self.requests.load(Relaxed)));
        s.push_str(&format!("frpc_rate_limited_total {}\n", self.rate_limited.load(Relaxed)));
        s.push_str("# TYPE frpc_calls_total counter\n# TYPE frpc_call_errors_total counter\n# TYPE frpc_call_seconds_sum counter\n");
        for (m, (n, e, us)) in self.methods.lock().unwrap().iter() {
            s.push_str(&format!("frpc_calls_total{{method=\"{m}\"}} {n}\n"));
            s.push_str(&format!("frpc_call_errors_total{{method=\"{m}\"}} {e}\n"));
            s.push_str(&format!("frpc_call_seconds_sum{{method=\"{m}\"}} {}\n", *us as f64 / 1e6));
        }
        s
    }
}

pub struct App {
    pub ctx: Arc<Ctx>,
    pub metrics: Arc<Metrics>,
    pub max_batch: usize,
    pub ready_max_lag: u64,
}

pub type Shared = Arc<App>;

pub fn router(app: Shared, guard: Arc<Guard>) -> Router {
    Router::new()
        .route("/", get(ws).post(rpc))
        .route("/health", get(|| async { "ok" }))
        .route("/ready", get(ready))
        .with_state(app)
        .layer(axum::middleware::from_fn_with_state(guard, guard_mw))
        .layer(tower_http::compression::CompressionLayer::new())
        .layer(axum::extract::DefaultBodyLimit::max(2 << 20))
}

pub fn admin_router(app: Shared) -> Router {
    Router::new().route("/metrics", get(|State(a): State<Shared>| async move { a.metrics.render() })).with_state(app)
}

async fn ws(State(a): State<Shared>, up: WebSocketUpgrade) -> Response {
    let ctx = a.ctx.clone();
    up.on_upgrade(move |s| crate::ws::serve(s, ctx))
}

async fn ready(State(a): State<Shared>) -> Response {
    let h = a.ctx.sync.heights();
    match h.height {
        Some(x) if h.chain_head.saturating_sub(x) <= a.ready_max_lag => (StatusCode::OK, "ready").into_response(),
        _ => (StatusCode::SERVICE_UNAVAILABLE, format!("archive at {:?}, node at {}", h.height, h.chain_head))
            .into_response(),
    }
}

fn error(id: Value, code: i64, msg: &str) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "error": {"code": code, "message": msg}})
}

async fn one(a: &App, req: &Value) -> Option<Value> {
    let Some(o) = req.as_object() else { return Some(error(Value::Null, -32600, "invalid request")) };
    let id = o.get("id").cloned();
    let Some(method) = o.get("method").and_then(|m| m.as_str()) else {
        return Some(error(id.unwrap_or(Value::Null), -32600, "invalid request"));
    };
    let params = o.get("params").cloned().unwrap_or(json!([]));
    if !(params.is_array() || params.is_object()) {
        return Some(error(id.unwrap_or(Value::Null), -32602, "params must be an array or object"));
    }
    let t = Instant::now();
    let r = a.ctx.call(method, &params).await;
    a.metrics.record(method, r.is_ok(), t.elapsed());
    // Notifications (no id) get no response.
    let id = id?;
    Some(match r {
        Ok(v) => json!({"jsonrpc": "2.0", "id": id, "result": v}),
        Err(e) => json!({"jsonrpc": "2.0", "id": id, "error": e}),
    })
}

async fn rpc(State(a): State<Shared>, body: Bytes) -> Response {
    a.metrics.requests.fetch_add(1, Relaxed);
    let v: Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(_) => return Json(error(Value::Null, -32700, "parse error")).into_response(),
    };
    match v {
        Value::Array(calls) => {
            if calls.is_empty() {
                return Json(error(Value::Null, -32600, "empty batch")).into_response();
            }
            if calls.len() > a.max_batch {
                return Json(error(Value::Null, -32600, &format!("batch larger than {}", a.max_batch))).into_response();
            }
            let out: Vec<Value> =
                futures::future::join_all(calls.iter().map(|c| one(&a, c))).await.into_iter().flatten().collect();
            if out.is_empty() {
                return StatusCode::NO_CONTENT.into_response();
            }
            Json(Value::Array(out)).into_response()
        }
        v => match one(&a, &v).await {
            Some(r) => Json(r).into_response(),
            None => StatusCode::NO_CONTENT.into_response(),
        },
    }
}

// ── access control ──────────────────────────────────────────────────────

#[derive(Clone)]
pub struct GuardConfig {
    pub rate: f64,
    pub burst: f64,
    pub trust_proxy: bool,
    pub cors_origins: Vec<String>,
    pub api_keys: Vec<String>,
}

const SHARDS: usize = 16;

pub struct Guard {
    cfg: GuardConfig,
    shards: Vec<Mutex<HashMap<String, (f64, Instant)>>>,
    hasher: RandomState,
    metrics: Arc<Metrics>,
}

impl Guard {
    pub fn new(cfg: GuardConfig, metrics: Arc<Metrics>) -> Arc<Guard> {
        Arc::new(Guard {
            cfg,
            shards: (0..SHARDS).map(|_| Mutex::default()).collect(),
            hasher: RandomState::new(),
            metrics,
        })
    }

    fn allow(&self, client: &str) -> bool {
        if self.cfg.rate <= 0.0 {
            return true;
        }
        let shard = &self.shards[self.hasher.hash_one(client) as usize % SHARDS];
        let mut m = shard.lock().unwrap();
        let now = Instant::now();
        let e = m.entry(client.to_string()).or_insert((self.cfg.burst, now));
        e.0 = (e.0 + now.duration_since(e.1).as_secs_f64() * self.cfg.rate).min(self.cfg.burst);
        e.1 = now;
        if e.0 >= 1.0 {
            e.0 -= 1.0;
            true
        } else {
            false
        }
    }

    pub async fn sweep(self: Arc<Self>) {
        let mut t = tokio::time::interval(Duration::from_secs(60));
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

/// API key from `Authorization: Bearer` or a `/?key=` query parameter (for
/// WebSocket clients that cannot set headers).
fn api_key(req: &Request) -> Option<String> {
    if let Some(k) =
        req.headers().get(header::AUTHORIZATION).and_then(|v| v.to_str().ok()).and_then(|v| v.strip_prefix("Bearer "))
    {
        return Some(k.trim().to_string());
    }
    req.uri().query()?.split('&').find_map(|kv| kv.strip_prefix("key=")).map(String::from)
}

async fn guard_mw(State(g): State<Arc<Guard>>, req: Request, next: Next) -> Response {
    let origin = req.headers().get(header::ORIGIN).cloned();
    let exempt = matches!(req.uri().path(), "/health" | "/ready");
    let mut resp = if req.method() == Method::OPTIONS {
        StatusCode::NO_CONTENT.into_response()
    } else if exempt {
        next.run(req).await
    } else {
        let key = api_key(&req);
        if !g.cfg.api_keys.is_empty() && !key.as_ref().is_some_and(|k| g.cfg.api_keys.iter().any(|a| a == k)) {
            (StatusCode::UNAUTHORIZED, Json(error(Value::Null, -32001, "missing or unknown API key"))).into_response()
        } else {
            let client = match key {
                Some(k) if !g.cfg.api_keys.is_empty() => format!("k:{k}"),
                _ => g.client_ip(&req).map(|ip| ip.to_string()).unwrap_or_default(),
            };
            if g.allow(&client) {
                next.run(req).await
            } else {
                g.metrics.rate_limited.fetch_add(1, Relaxed);
                let mut r = (StatusCode::TOO_MANY_REQUESTS, Json(error(Value::Null, -32005, "rate limit exceeded")))
                    .into_response();
                r.headers_mut().insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
                r
            }
        }
    };
    let h = resp.headers_mut();
    let allowed = &g.cfg.cors_origins;
    if allowed.iter().any(|o| o == "*") {
        h.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, HeaderValue::from_static("*"));
    } else if let Some(o) = origin.filter(|o| o.to_str().is_ok_and(|s| allowed.iter().any(|a| a == s))) {
        h.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, o);
        h.insert(header::VARY, HeaderValue::from_static("Origin"));
    } else {
        return resp;
    }
    h.insert(header::ACCESS_CONTROL_ALLOW_METHODS, HeaderValue::from_static("GET, POST, OPTIONS"));
    h.insert(header::ACCESS_CONTROL_ALLOW_HEADERS, HeaderValue::from_static("Content-Type, Authorization"));
    resp
}
