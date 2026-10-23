//! HTTP API: `POST /query` (JSON or Arrow), `GET /height` (with long-poll),
//! `GET /status`, health probes; plus access control and rate limiting.

use std::collections::HashMap;
use std::hash::{BuildHasher, RandomState};
use std::io::Write as _;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
use arrow::record_batch::RecordBatch;
use axum::body::{Body, Bytes};
use axum::extract::{ConnectInfo, Query as UrlQuery, Request, State};
use axum::http::{header, HeaderMap, HeaderValue, Method, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::metrics::Metrics;
use crate::model::{cell_json, rpc_object, RpcPart, Table, TABLES};
use crate::query::{self, Limits, Plan, Query};
use crate::store::Store;

pub struct AppState {
    pub store: Arc<Store>,
    pub metrics: Arc<Metrics>,
    pub limits: Limits,
    pub ready_max_lag: u64,
    /// Concurrent queries; extra requests wait.
    pub query_slots: tokio::sync::Semaphore,
}

pub type Shared = Arc<AppState>;

pub fn router(state: Shared, guard: Arc<Guard>) -> Router {
    Router::new()
        .route("/query", post(query_handler))
        .route("/height", get(height))
        .route("/status", get(status))
        .route("/health", get(|| async { "ok" }))
        .route("/ready", get(ready))
        .with_state(state)
        .layer(axum::middleware::from_fn_with_state(guard, guard_mw))
        .layer(tower_http::compression::CompressionLayer::new())
        .layer(axum::extract::DefaultBodyLimit::max(4 << 20))
}

pub fn admin_router(state: Shared) -> Router {
    Router::new().route("/metrics", get(metrics)).with_state(state)
}

fn err(code: StatusCode, msg: impl ToString) -> Response {
    (code, Json(json!({"error": msg.to_string()}))).into_response()
}

#[derive(Deserialize)]
struct QueryParams {
    /// "json" (default), "arrow"
    format: Option<String>,
    /// "rows" (default) or "rpc": JSON-RPC shaped objects (all fields).
    shape: Option<String>,
    /// Arrow IPC buffer compression: "lz4" (default) or "none".
    compression: Option<String>,
}

async fn query_handler(
    State(s): State<Shared>,
    UrlQuery(p): UrlQuery<QueryParams>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let t0 = Instant::now();
    let mut q: Query = match serde_json::from_slice(&body) {
        Ok(q) => q,
        Err(e) => {
            s.metrics.query_errors.fetch_add(1, Relaxed);
            return err(StatusCode::BAD_REQUEST, format!("bad query: {e}"));
        }
    };
    let rpc_shape = p.shape.as_deref() == Some("rpc");
    if rpc_shape {
        let all = Some(vec!["*".to_string()]);
        q.fields.block = all.clone();
        q.fields.transaction = all.clone();
        q.fields.log = all.clone();
        q.fields.trace = all;
    }
    let arrow = p.format.as_deref() == Some("arrow")
        || headers.get(header::ACCEPT).and_then(|v| v.to_str().ok()).is_some_and(|a| a.contains("arrow"));
    let plan = match Plan::new(&q, &s.store, &s.limits) {
        Ok(p) => p,
        Err(e) => {
            s.metrics.query_errors.fetch_add(1, Relaxed);
            if let Some(r) = e.downcast_ref::<query::Reorg>() {
                return (StatusCode::CONFLICT, Json(json!({"error": r.to_string(), "reorg": true, "block": r.block})))
                    .into_response();
            }
            return err(StatusCode::BAD_REQUEST, e);
        }
    };
    let lz4 = p.compression.as_deref() != Some("none");
    if arrow {
        return stream_arrow(s, plan, lz4, t0).await;
    }
    let _slot = s.query_slots.acquire().await;
    let r = match query::execute(s.store.clone(), plan, &s.metrics).await {
        Ok(r) => r,
        Err(e) => {
            s.metrics.query_errors.fetch_add(1, Relaxed);
            tracing::error!(error = %e, "query failed");
            return err(StatusCode::INTERNAL_SERVER_ERROR, e);
        }
    };
    s.metrics.queries.fetch_add(1, Relaxed);
    let res = tokio::task::spawn_blocking(move || encode_json(&r, rpc_shape).map(|b| (b, "application/json"))).await;
    s.metrics.query.observe(t0.elapsed());
    match res {
        Ok(Ok((bytes, ct))) => {
            let mut resp = Response::new(Body::from(bytes));
            resp.headers_mut().insert(header::CONTENT_TYPE, HeaderValue::from_static(ct));
            resp
        }
        Ok(Err(e)) => err(StatusCode::INTERNAL_SERVER_ERROR, e),
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, e),
    }
}

fn meta_json(r: &query::Response) -> Value {
    json!({
        "next_block": r.next_block,
        "archive_height": r.archive_height,
        "chain_head": r.chain_head,
        "finalized_block": r.finalized,
        "last_block": r.last_block,
        "rows": r.rows,
        "elapsed_ms": r.elapsed.as_secs_f64() * 1000.0,
    })
}

/// `{"blocks":[…],"transactions":[…],"logs":[…],"traces":[…], …meta}`
fn encode_json(r: &query::Response, rpc: bool) -> anyhow::Result<Vec<u8>> {
    let mut out = Vec::with_capacity(1 << 16);
    out.push(b'{');
    for t in TABLES {
        write!(out, "\"{}\":[", t.name())?;
        let mut first = true;
        for b in &r.tables[t.idx()] {
            for row in 0..b.num_rows() {
                if !first {
                    out.push(b',');
                }
                first = false;
                if rpc {
                    write_rpc_row(&mut out, t, b, row)?;
                } else {
                    write_row(&mut out, t, b, row)?;
                }
            }
        }
        out.extend_from_slice(b"],");
    }
    let meta = meta_json(r);
    let m = serde_json::to_vec(&meta)?;
    out.extend_from_slice(&m[1..]);
    Ok(out)
}

fn write_row(out: &mut Vec<u8>, t: Table, b: &RecordBatch, row: usize) -> anyhow::Result<()> {
    out.push(b'{');
    let schema = b.schema();
    for (i, f) in schema.fields().iter().enumerate() {
        if i > 0 {
            out.push(b',');
        }
        serde_json::to_writer(&mut *out, f.name())?;
        out.push(b':');
        let pos = t.col_index(f.name()).expect("known column");
        let kind = t.cols()[pos].kind;
        let v = crate::model::cell_json_kind(kind, b.column(i).as_ref(), row, false);
        serde_json::to_writer(&mut *out, &v)?;
    }
    out.push(b'}');
    Ok(())
}

fn write_rpc_row(out: &mut Vec<u8>, t: Table, b: &RecordBatch, row: usize) -> anyhow::Result<()> {
    let v = match t {
        Table::Transactions => json!({
            "tx": rpc_object(t, b, row, RpcPart::Tx),
            "receipt": rpc_object(t, b, row, RpcPart::Receipt),
        }),
        Table::Traces => {
            let mut o = serde_json::Map::new();
            for (i, c) in t.cols().iter().enumerate() {
                o.insert(c.name.to_string(), cell_json(t, b, i, row, true));
            }
            Value::Object(o)
        }
        _ => Value::Object(rpc_object(t, b, row, RpcPart::Whole)),
    };
    serde_json::to_writer(&mut *out, &v)?;
    Ok(())
}

/// Arrow responses are a sequence of frames `[tag u8][len u32 LE][bytes]`:
/// tags 0..3 hold an Arrow IPC stream of rows of blocks / transactions /
/// logs / traces (a table can span several frames, in block order), 254 a
/// JSON error, and 255 the JSON metadata, which is always last.
pub fn encode_frames(tables: &[Vec<RecordBatch>; 4], lz4: bool) -> anyhow::Result<Vec<u8>> {
    let opts = if lz4 {
        IpcWriteOptions::default().try_with_compression(Some(arrow::ipc::CompressionType::LZ4_FRAME))?
    } else {
        IpcWriteOptions::default()
    };
    let mut out = Vec::new();
    for t in TABLES {
        let bs = &tables[t.idx()];
        let Some(first) = bs.iter().find(|b| b.num_rows() > 0) else { continue };
        let mut buf = Vec::new();
        {
            let mut w = StreamWriter::try_new_with_options(&mut buf, &first.schema(), opts.clone())?;
            for b in bs {
                w.write(b)?;
            }
            w.finish()?;
        }
        frame(&mut out, t.idx() as u8, &buf);
    }
    Ok(out)
}

fn frame(out: &mut Vec<u8>, tag: u8, body: &[u8]) {
    out.push(tag);
    out.extend_from_slice(&(body.len() as u32).to_le_bytes());
    out.extend_from_slice(body);
}

async fn stream_arrow(s: Shared, plan: Plan, lz4: bool, t0: Instant) -> Response {
    let (tx, rx) = tokio::sync::mpsc::channel::<Bytes>(8);
    let enc: query::Encoder = Arc::new(move |t| Ok(Bytes::from(encode_frames(t, lz4)?)));
    tokio::spawn(async move {
        let _slot = s.query_slots.acquire().await;
        let out = query::Output::Stream { enc, tx: tx.clone() };
        let mut tail = Vec::new();
        match query::execute_to(s.store.clone(), plan, &s.metrics, out).await {
            Ok(r) => {
                s.metrics.queries.fetch_add(1, Relaxed);
                frame(&mut tail, 255, &serde_json::to_vec(&meta_json(&r)).unwrap_or_default());
            }
            Err(e) => {
                s.metrics.query_errors.fetch_add(1, Relaxed);
                tracing::error!(error = %e, "query failed");
                frame(&mut tail, 254, &serde_json::to_vec(&json!({"error": e.to_string()})).unwrap_or_default());
            }
        }
        let _ = tx.send(Bytes::from(tail)).await;
        s.metrics.query.observe(t0.elapsed());
    });
    let stream =
        futures::stream::unfold(rx, |mut rx| async move { rx.recv().await.map(|b| (Ok::<_, std::io::Error>(b), rx)) });
    let mut resp = Response::new(Body::from_stream(stream));
    resp.headers_mut().insert(header::CONTENT_TYPE, HeaderValue::from_static("application/vnd.faster-sync.arrow"));
    resp
}

#[derive(Deserialize)]
struct HeightParams {
    /// Wait until the height is above this block.
    above: Option<u64>,
    timeout_ms: Option<u64>,
}

async fn height(State(s): State<Shared>, UrlQuery(p): UrlQuery<HeightParams>) -> Response {
    if let Some(above) = p.above {
        let mut rx = s.store.subscribe();
        let wait = Duration::from_millis(p.timeout_ms.unwrap_or(10_000).min(30_000));
        let _ = tokio::time::timeout(wait, rx.wait_for(|h| h.is_some_and(|h| h > above))).await;
    }
    Json(height_json(&s)).into_response()
}

fn height_json(s: &AppState) -> Value {
    let st = &s.store;
    json!({
        "height": st.last().map(|h| h.number),
        "chain_head": st.chain_head.load(Relaxed),
        "finalized_block": st.finalized.load(Relaxed),
        "first_block": st.first_block(),
        "chunked_to": st.chunk_end(),
        "chain_id": st.meta.chain_id,
    })
}

async fn status(State(s): State<Shared>) -> Response {
    let (chunks, rows, filter_bytes) = s.store.chunk_stats();
    let mut v = height_json(&s);
    v["chunks"] = json!(chunks);
    v["chunk_rows"] = json!(rows);
    v["filter_bytes"] = json!(filter_bytes);
    v["hot_blocks"] = json!(s.store.hot.len());
    v["version"] = json!(env!("CARGO_PKG_VERSION"));
    Json(v).into_response()
}

async fn ready(State(s): State<Shared>) -> Response {
    let h = s.store.last().map(|h| h.number).unwrap_or(0);
    let head = s.store.chain_head.load(Relaxed);
    if head > 0 && head.saturating_sub(h) <= s.ready_max_lag {
        (StatusCode::OK, "ready").into_response()
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, format!("behind: {} of {}", h, head)).into_response()
    }
}

async fn metrics(State(s): State<Shared>) -> Response {
    let st = &s.store;
    let (chunks, rows, fb) = st.chunk_stats();
    let h = st.last().map(|h| h.number).unwrap_or(0) as f64;
    let gauges = [
        ("fsync_height", "Last stored block", h),
        ("fsync_chain_head", "Node head", st.chain_head.load(Relaxed) as f64),
        ("fsync_finalized", "Node finalized block", st.finalized.load(Relaxed) as f64),
        ("fsync_chunks", "Published chunks", chunks as f64),
        ("fsync_chunk_rows", "Rows in chunks", rows as f64),
        ("fsync_filter_bytes", "Bytes of row-group filters in memory", fb as f64),
        ("fsync_hot_blocks", "Blocks in the hot tier", st.hot.len() as f64),
    ];
    let mut r = Response::new(Body::from(s.metrics.render(&gauges)));
    r.headers_mut().insert(header::CONTENT_TYPE, HeaderValue::from_static("text/plain; version=0.0.4"));
    r
}

// ── access control ──────────────────────────────────────────────────────

#[derive(Clone)]
pub struct GuardConfig {
    /// Sustained requests per second per client (API key, else IP); 0 = off.
    pub rate: f64,
    pub burst: f64,
    pub trust_proxy: bool,
    pub cors_origins: Vec<String>,
    /// If non-empty, `Authorization: Bearer <key>` must be one of these.
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

fn bearer(req: &Request) -> Option<&str> {
    req.headers().get(header::AUTHORIZATION)?.to_str().ok()?.strip_prefix("Bearer ").map(str::trim)
}

async fn guard_mw(State(g): State<Arc<Guard>>, req: Request, next: Next) -> Response {
    let origin = req.headers().get(header::ORIGIN).cloned();
    let exempt = matches!(req.uri().path(), "/health" | "/ready");
    let mut resp = if req.method() == Method::OPTIONS {
        StatusCode::NO_CONTENT.into_response()
    } else if exempt {
        next.run(req).await
    } else {
        let key = bearer(&req).map(String::from);
        if !g.cfg.api_keys.is_empty() && !key.as_ref().is_some_and(|k| g.cfg.api_keys.iter().any(|a| a == k)) {
            err(StatusCode::UNAUTHORIZED, "missing or unknown API key")
        } else {
            let client = match key {
                Some(k) if !g.cfg.api_keys.is_empty() => format!("k:{k}"),
                _ => g.client_ip(&req).map(|ip| ip.to_string()).unwrap_or_default(),
            };
            if g.allow(&client) {
                next.run(req).await
            } else {
                g.metrics.rate_limited.fetch_add(1, Relaxed);
                let mut r = err(StatusCode::TOO_MANY_REQUESTS, "rate limit exceeded");
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
