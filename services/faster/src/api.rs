//! Explorer REST + WebSocket API. Routes and JSON field names match the old
//! api-go service so the React UI works unchanged.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arc_swap::ArcSwap;
use axum::extract::ws::{Message, Utf8Bytes, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, Request, State};
use axum::http::{header, HeaderValue, Method, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::broadcast;

use crate::indexer::Sink;
use crate::rpc::Rpc;
use crate::store::Store;
use crate::types::*;
use crate::Status;

/// Most blocks / txs pushed to WS clients per commit. The UI keeps 50 blocks
/// and 20 txs, so sending more during catch-up is wasted work.
const WS_MAX_BLOCKS: usize = 50;
const WS_MAX_TXS: usize = 20;
const WS_PING: Duration = Duration::from_secs(20);
const WS_IDLE: Duration = Duration::from_secs(60);
const WS_WRITE_TIMEOUT: Duration = Duration::from_secs(10);
const RPC_CALL_TIMEOUT: Duration = Duration::from_secs(3);

pub struct Hub {
    store: Arc<Store>,
    tx: broadcast::Sender<Utf8Bytes>,
    /// Latest /stats body, recomputed once per commit and shared by everyone.
    stats: ArcSwap<Value>,
}

impl Hub {
    pub fn new(store: Arc<Store>) -> Arc<Hub> {
        let (tx, _) = broadcast::channel(256);
        let h = Arc::new(Hub { store, tx, stats: ArcSwap::from_pointee(Value::Null) });
        h.refresh_stats();
        h
    }

    fn send(&self, v: Value) {
        // No receivers is fine.
        let _ = self.tx.send(Utf8Bytes::from(v.to_string()));
    }

    fn refresh_stats(&self) -> Arc<Value> {
        let v = Arc::new(compute_stats(&self.store).unwrap_or_else(|e| {
            tracing::error!(err = %e, "stats");
            Value::Null
        }));
        self.stats.store(v.clone());
        v
    }

    pub fn stats(&self) -> Arc<Value> {
        self.stats.load_full()
    }

    pub fn init_message(&self) -> anyhow::Result<Value> {
        let blocks: Vec<Value> = self.store.blocks_desc(None, 20)?.iter().map(block_summary_json).collect();
        let txs: Vec<Value> = self.store.latest_txs(20)?.iter().map(tx_summary_json).collect();
        Ok(json!({"type": "init", "data": {"stats": *self.stats(), "blocks": blocks, "txs": txs}}))
    }

    /// Re-sends stats every few seconds so idle chains still look alive.
    pub async fn heartbeat(self: Arc<Self>) {
        let mut t = tokio::time::interval(Duration::from_secs(5));
        loop {
            t.tick().await;
            let s = self.refresh_stats();
            self.send(json!({"type": "stats", "data": *s}));
        }
    }
}

impl Sink for Hub {
    fn committed(&self, blocks: &[FullBlock]) {
        let start = blocks.len().saturating_sub(WS_MAX_BLOCKS);
        for fb in &blocks[start..] {
            self.send(json!({"type": "block", "data": block_summary_json(&fb.block)}));
        }
        // Oldest first: the UI prepends each tx, leaving the newest on top.
        let mut recent: Vec<&TxRec> = blocks.iter().rev().flat_map(|b| b.txs.iter().rev()).take(WS_MAX_TXS).collect();
        recent.reverse();
        for t in recent {
            self.send(json!({"type": "tx", "data": tx_summary_json(t)}));
        }
        let s = self.refresh_stats();
        self.send(json!({"type": "stats", "data": *s}));
    }

    fn reorged(&self, _new_head: Option<Head>) {
        // Clients may be showing orphaned blocks; give them a fresh snapshot.
        self.refresh_stats();
        match self.init_message() {
            Ok(m) => self.send(m),
            Err(e) => tracing::error!(err = %e, "reorg init message"),
        }
    }
}

fn compute_stats(store: &Store) -> anyhow::Result<Value> {
    let c = store.counters()?;
    let recent = store.blocks_desc(None, 101)?;
    let head = recent.first();
    let avg = match (recent.first(), recent.last()) {
        (Some(a), Some(b)) if a.number > b.number && a.timestamp > b.timestamp => {
            (a.timestamp - b.timestamp) as f64 / (a.number - b.number) as f64
        }
        _ => 0.0,
    };
    let last20 = &recent[..recent.len().min(20)];
    let tps = match (last20.first(), last20.last()) {
        (Some(a), Some(b)) if a.number > b.number && a.timestamp > b.timestamp => {
            let bt = (a.timestamp - b.timestamp) as f64 / (a.number - b.number) as f64;
            a.tx_count as f64 / bt
        }
        _ => 0.0,
    };
    Ok(json!({
        "total_blocks": c.blocks.to_string(),
        "block_height": head.map(|b| b.number.to_string()),
        "latest_block_timestamp": head.map(|b| b.timestamp.to_string()),
        "total_transactions": c.txs.to_string(),
        "total_addresses": c.addresses.to_string(),
        "total_logs": c.logs.to_string(),
        "avg_block_time": format!("{avg:.3}"),
        "tps": format!("{tps:.2}"),
    }))
}

// ---- JSON shapes ----

fn opt_dec(v: &Option<Vec<u8>>) -> Value {
    v.as_ref().map(|b| Value::String(dec(b))).unwrap_or(Value::Null)
}

fn block_summary_json(b: &BlockRec) -> Value {
    json!({
        "hash": hex0x(&b.hash),
        "number": b.number,
        "timestamp": b.timestamp,
        "miner": hex0x(&b.miner),
        "gas_used": b.gas_used,
        "transactions_count": b.tx_count,
    })
}

fn block_json(b: &BlockRec) -> Value {
    json!({
        "hash": hex0x(&b.hash),
        "number": b.number,
        "timestamp": b.timestamp,
        "miner": hex0x(&b.miner),
        "gas_used": b.gas_used,
        "gas_limit": b.gas_limit,
        "base_fee_per_gas": opt_dec(&b.base_fee),
        "difficulty": dec(&b.difficulty),
        "total_difficulty": Value::Null,
        "transactions_count": b.tx_count,
        "logs_count": b.log_count,
        "parent_hash": hex0x(&b.parent_hash),
        "state_root": hex0x(&b.state_root),
        "transactions_root": hex0x(&b.transactions_root),
        "receipts_root": hex0x(&b.receipts_root),
        "size": b.size,
    })
}

fn tx_summary_json(t: &TxRec) -> Value {
    json!({
        "hash": hex0x(&t.hash),
        "block_number": t.block_number,
        "from_address": hex0x(&t.from),
        "to_address": t.to.map(|a| hex0x(&a)),
        "value": dec(&t.value),
        "status": t.status,
    })
}

fn tx_json(t: &TxRec) -> Value {
    json!({
        "hash": hex0x(&t.hash),
        "block_number": t.block_number,
        "block_timestamp": t.block_timestamp,
        "transaction_index": t.index,
        "from_address": hex0x(&t.from),
        "to_address": t.to.map(|a| hex0x(&a)),
        "value": dec(&t.value),
        "gas": t.gas,
        // What the sender actually paid per gas; falls back to the tx's own
        // gasPrice when the node has no effectiveGasPrice.
        "gas_price": t.effective_gas_price.as_ref().map(|b| dec(b)).unwrap_or_else(|| dec(&t.gas_price)),
        "effective_gas_price": opt_dec(&t.effective_gas_price),
        "max_fee_per_gas": opt_dec(&t.max_fee_per_gas),
        "max_priority_fee_per_gas": opt_dec(&t.max_priority_fee_per_gas),
        "gas_used": t.gas_used,
        "nonce": t.nonce,
        "input_data": hex0x(&t.input),
        "status": t.status,
        "contract_address": t.contract_address.map(|a| hex0x(&a)),
        "logs_count": t.log_count,
        "transaction_type": t.tx_type,
    })
}

/// RFC 3339 UTC from unix seconds (days-from-civil, no date crate needed).
fn iso(ts: u64) -> String {
    let days = (ts / 86400) as i64;
    let secs = ts % 86400;
    let z = days + 719468;
    let era = z.div_euclid(146097);
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + if m <= 2 { 1 } else { 0 };
    format!("{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z", secs / 3600, secs / 60 % 60, secs % 60)
}

// ---- HTTP ----

#[derive(Clone)]
pub struct AppState {
    pub store: Arc<Store>,
    pub rpc: Arc<Rpc>,
    pub hub: Arc<Hub>,
    pub status: Arc<Status>,
    pub ready_max_lag: u64,
    pub clob: Option<ClobProxy>,
}

#[derive(Clone)]
pub struct ClobProxy {
    pub base: String,
    pub http: reqwest::Client,
}

pub enum ApiError {
    NotFound(&'static str),
    BadRequest(&'static str),
    Internal(anyhow::Error),
    Unavailable(&'static str),
}

impl<E: Into<anyhow::Error>> From<E> for ApiError {
    fn from(e: E) -> Self {
        ApiError::Internal(e.into())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (code, msg) = match self {
            ApiError::NotFound(m) => (StatusCode::NOT_FOUND, m),
            ApiError::BadRequest(m) => (StatusCode::BAD_REQUEST, m),
            ApiError::Unavailable(m) => (StatusCode::SERVICE_UNAVAILABLE, m),
            ApiError::Internal(e) => {
                tracing::error!(err = %e, "request failed");
                (StatusCode::INTERNAL_SERVER_ERROR, "internal error")
            }
        };
        (code, Json(json!({"error": msg}))).into_response()
    }
}

type ApiResult = Result<Json<Value>, ApiError>;

#[derive(Deserialize)]
pub struct ListQ {
    limit: Option<usize>,
    page: Option<usize>,
    pattern: Option<String>,
}

fn limit_or(q: &ListQ, def: usize, max: usize) -> usize {
    match q.limit {
        Some(l) if l > 0 && l <= max => l,
        _ => def,
    }
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/", get(root))
        .route("/health", get(health))
        .route("/ready", get(ready))
        .route("/stats", get(stats))
        .route("/api/blocks", get(blocks_list))
        .route("/api/blocks/range/{start}/{end}", get(blocks_range))
        .route("/api/blocks/{id}", get(block_detail))
        .route("/api/transactions", get(txs_list))
        .route("/api/transactions/pending/list", get(|| async { Json(json!([])) }))
        .route("/api/transactions/address/{addr}", get(txs_by_address))
        .route("/api/transactions/{hash}", get(tx_detail))
        .route("/api/addresses", get(addresses_list))
        .route("/api/addresses/search/by-pattern", get(addresses_search))
        .route("/api/addresses/{addr}", get(address_detail))
        .route("/api/clob/{*rest}", get(clob_proxy))
        .layer(middleware::from_fn(cors))
        .with_state(state)
}

async fn cors(req: Request, next: Next) -> Response {
    if req.method() == Method::OPTIONS {
        let mut r = StatusCode::NO_CONTENT.into_response();
        add_cors(&mut r);
        return r;
    }
    let mut r = next.run(req).await;
    add_cors(&mut r);
    r
}

fn add_cors(r: &mut Response) {
    let h = r.headers_mut();
    h.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, HeaderValue::from_static("*"));
    h.insert(header::ACCESS_CONTROL_ALLOW_METHODS, HeaderValue::from_static("GET, POST, OPTIONS"));
    h.insert(header::ACCESS_CONTROL_ALLOW_HEADERS, HeaderValue::from_static("Content-Type"));
}

async fn health(State(s): State<AppState>) -> Json<Value> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0);
    Json(json!({
        "status": "ok",
        "timestamp": iso(now),
        "connectedClients": s.status.clients.load(Ordering::Relaxed),
    }))
}

/// 200 once the indexer is within READY_MAX_LAG blocks of the node.
async fn ready(State(s): State<AppState>) -> Response {
    let tip = s.status.tip.load(Ordering::Relaxed);
    let head = s.status.head.load(Ordering::Relaxed);
    let lag = tip.saturating_sub(head);
    let ok = tip > 0 && lag <= s.ready_max_lag;
    let code = if ok { StatusCode::OK } else { StatusCode::SERVICE_UNAVAILABLE };
    (code, Json(json!({"ready": ok, "head": head, "tip": tip, "lag": lag}))).into_response()
}

async fn stats(State(s): State<AppState>) -> Json<Value> {
    Json((*s.hub.stats()).clone())
}

async fn blocks_list(State(s): State<AppState>, Query(q): Query<ListQ>) -> ApiResult {
    let l = limit_or(&q, 20, 100);
    Ok(Json(Value::Array(s.store.blocks_desc(None, l)?.iter().map(block_json).collect())))
}

async fn blocks_range(State(s): State<AppState>, Path((start, end)): Path<(u64, u64)>) -> ApiResult {
    if start > end || end - start > 1000 {
        return Err(ApiError::BadRequest("Invalid range"));
    }
    Ok(Json(Value::Array(s.store.blocks_range(start, end)?.iter().map(block_json).collect())))
}

async fn block_detail(State(s): State<AppState>, Path(id): Path<String>) -> ApiResult {
    let b = if id.starts_with("0x") {
        let h = parse_fixed::<32>(&id).ok_or(ApiError::BadRequest("Invalid block hash"))?;
        s.store.block_by_hash(&h)?
    } else {
        let n: u64 = id.parse().map_err(|_| ApiError::BadRequest("Invalid block number"))?;
        s.store.block(n)?
    };
    let b = b.ok_or(ApiError::NotFound("Block not found"))?;
    let mut v = block_json(&b);
    v["transactions"] = Value::Array(s.store.block_txs(b.number)?.iter().map(tx_json).collect());
    Ok(Json(v))
}

async fn txs_list(State(s): State<AppState>, Query(q): Query<ListQ>) -> ApiResult {
    let l = limit_or(&q, 20, 100);
    Ok(Json(Value::Array(s.store.latest_txs(l)?.iter().map(tx_json).collect())))
}

async fn txs_by_address(State(s): State<AppState>, Path(addr): Path<String>, Query(q): Query<ListQ>) -> ApiResult {
    let a = parse_fixed::<20>(&addr).ok_or(ApiError::BadRequest("Invalid address"))?;
    let l = limit_or(&q, 20, 100);
    Ok(Json(Value::Array(s.store.addr_txs(&a, 0, l)?.iter().map(tx_json).collect())))
}

async fn tx_detail(State(s): State<AppState>, Path(hash): Path<String>) -> ApiResult {
    let h = parse_fixed::<32>(&hash).ok_or(ApiError::BadRequest("Invalid transaction hash"))?;
    let t = s.store.tx_by_hash(&h)?.ok_or(ApiError::NotFound("Transaction not found"))?;
    let mut v = tx_json(&t);
    if let Some(b) = s.store.block(t.block_number)? {
        v["block_hash"] = Value::String(hex0x(&b.hash));
    }
    let logs: Vec<Value> = s
        .store
        .tx_logs(&t)?
        .iter()
        .map(|(i, l)| {
            json!({
                "log_index": i,
                "address": hex0x(&l.address),
                "topics": l.topics.iter().map(|t| hex0x(t)).collect::<Vec<_>>(),
                "data": hex0x(&l.data),
            })
        })
        .collect();
    v["logs"] = Value::Array(logs);
    Ok(Json(v))
}

async fn addresses_list() -> ApiError {
    ApiError::NotFound("Address ranking is not available; use /api/addresses/{address}")
}

async fn addresses_search(State(s): State<AppState>, Query(q): Query<ListQ>) -> ApiResult {
    let p = q.pattern.as_deref().unwrap_or("").to_ascii_lowercase();
    let p = p.strip_prefix("0x").unwrap_or(&p);
    if p.len() < 3 || p.len() > 40 || !p.bytes().all(|c| c.is_ascii_hexdigit()) {
        return Err(ApiError::BadRequest("Pattern must be at least 3 hex characters"));
    }
    let rows = s.store.addrs_with_prefix(p, 20)?;
    Ok(Json(Value::Array(
        rows.iter()
            .map(|(a, i)| json!({"address": hex0x(a), "transaction_count": i.tx_count, "last_seen": iso(i.last_ts)}))
            .collect(),
    )))
}

async fn address_detail(State(s): State<AppState>, Path(addr): Path<String>, Query(q): Query<ListQ>) -> ApiResult {
    let a = parse_fixed::<20>(&addr).ok_or(ApiError::BadRequest("Invalid address"))?;
    let addr = hex0x(&a);
    let page = q.page.filter(|p| *p >= 1).unwrap_or(1);
    let limit = limit_or(&q, 25, 50);
    // Node calls run concurrently with the store reads and are bounded, so a
    // slow node degrades the balance/code fields instead of the whole page.
    let rpc = s.rpc.clone();
    let a1 = addr.clone();
    let balance = tokio::spawn(async move { tokio::time::timeout(RPC_CALL_TIMEOUT, rpc.balance(&a1)).await });
    let rpc = s.rpc.clone();
    let a2 = addr.clone();
    let code = tokio::spawn(async move { tokio::time::timeout(RPC_CALL_TIMEOUT, rpc.code(&a2)).await });

    let info = s.store.addr_info(&a)?;
    let txs: Vec<Value> = s.store.addr_txs(&a, (page - 1) * limit, limit)?.iter().map(tx_json).collect();
    let deploy = match info.and_then(|i| i.creator) {
        Some((n, i)) => s.store.tx(n, i)?.map(|t| {
            json!({"hash": hex0x(&t.hash), "block_number": t.block_number,
                   "block_timestamp": t.block_timestamp, "from_address": hex0x(&t.from)})
        }),
        None => None,
    };
    let balance = match balance.await {
        Ok(Ok(Ok(b))) => Value::String(dec(&b)),
        _ => Value::Null,
    };
    let code = match code.await {
        Ok(Ok(Ok(c))) => c,
        _ => Vec::new(),
    };
    let i = info.unwrap_or_default();
    Ok(Json(json!({
        "address": addr,
        "transaction_count": i.tx_count,
        "first_seen": info.map(|i| iso(i.first_ts)),
        "last_seen": info.map(|i| iso(i.last_ts)),
        "is_contract": i.is_contract || !code.is_empty(),
        "bytecode_size": code.len(),
        "bytecode": if code.is_empty() { Value::Null } else { Value::String(hex0x(&code)) },
        "deploy_info": deploy,
        "balance": balance,
        "transactions": txs,
        "total_transactions": i.tx_count,
        "page": page,
        "limit": limit,
    })))
}

async fn clob_proxy(State(s): State<AppState>, req: Request) -> Response {
    let Some(p) = &s.clob else {
        return ApiError::Unavailable("CLOB archive not configured").into_response();
    };
    let pq = req.uri().path_and_query().map(|x| x.as_str()).unwrap_or("/");
    let url = format!("{}{}", p.base.trim_end_matches('/'), pq);
    match p.http.get(&url).send().await {
        Ok(r) => {
            let code = StatusCode::from_u16(r.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
            let ct = r.headers().get(reqwest::header::CONTENT_TYPE).and_then(|v| v.to_str().ok()).map(String::from);
            match r.bytes().await {
                Ok(body) => {
                    let mut resp = (code, body).into_response();
                    if let Some(ct) = ct.and_then(|c| HeaderValue::from_str(&c).ok()) {
                        resp.headers_mut().insert(header::CONTENT_TYPE, ct);
                    }
                    resp
                }
                Err(e) => ApiError::Internal(e.into()).into_response(),
            }
        }
        Err(e) => {
            tracing::warn!(err = %e, "clob proxy");
            (StatusCode::BAD_GATEWAY, Json(json!({"error": "CLOB archive unreachable"}))).into_response()
        }
    }
}

// ---- WebSocket ----

async fn root(
    State(s): State<AppState>,
    ws: Result<WebSocketUpgrade, axum::extract::ws::rejection::WebSocketUpgradeRejection>,
) -> Response {
    match ws {
        Ok(ws) => ws.on_upgrade(move |sock| client(s, sock)),
        Err(_) => StatusCode::NOT_FOUND.into_response(),
    }
}

/// One task per client owns the socket, so writes never race. A client that
/// falls behind the broadcast buffer, stops answering pings, or blocks a
/// write is disconnected; the UI reconnects and gets a fresh init.
async fn client(s: AppState, mut sock: WebSocket) {
    s.status.clients.fetch_add(1, Ordering::Relaxed);
    let mut rx = s.hub.tx.subscribe();
    let reason = 'conn: {
        match s.hub.init_message() {
            Ok(m) => {
                if !send(&mut sock, Utf8Bytes::from(m.to_string())).await {
                    break 'conn "init write failed";
                }
            }
            Err(e) => {
                tracing::error!(err = %e, "ws init");
                break 'conn "init failed";
            }
        }
        let mut ping = tokio::time::interval(WS_PING);
        ping.tick().await;
        let mut last_seen = Instant::now();
        loop {
            tokio::select! {
                m = rx.recv() => match m {
                    Ok(m) => if !send(&mut sock, m).await { break 'conn "write failed" },
                    Err(broadcast::error::RecvError::Lagged(_)) => break 'conn "slow client",
                    Err(broadcast::error::RecvError::Closed) => break 'conn "shutdown",
                },
                _ = ping.tick() => {
                    if last_seen.elapsed() > WS_IDLE {
                        break 'conn "idle";
                    }
                    let ok = tokio::time::timeout(WS_WRITE_TIMEOUT, sock.send(Message::Ping(Default::default()))).await;
                    if !matches!(ok, Ok(Ok(()))) { break 'conn "ping failed" }
                }
                m = sock.recv() => match m {
                    Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break 'conn "closed",
                    Some(Ok(Message::Text(t))) => {
                        last_seen = Instant::now();
                        if serde_json::from_str::<Value>(&t).ok().and_then(|v| v.get("type").cloned()) == Some(json!("ping")) {
                            if !send(&mut sock, Utf8Bytes::from_static(r#"{"type":"pong"}"#)).await { break 'conn "write failed" }
                        }
                    }
                    Some(Ok(_)) => last_seen = Instant::now(),
                },
            }
        }
    };
    tracing::debug!(reason, "ws client gone");
    s.status.clients.fetch_sub(1, Ordering::Relaxed);
}

async fn send(sock: &mut WebSocket, m: Utf8Bytes) -> bool {
    matches!(tokio::time::timeout(WS_WRITE_TIMEOUT, sock.send(Message::Text(m))).await, Ok(Ok(())))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn iso_dates() {
        assert_eq!(iso(0), "1970-01-01T00:00:00Z");
        assert_eq!(iso(1_700_000_000), "2023-11-14T22:13:20Z");
        assert_eq!(iso(951_782_400), "2000-02-29T00:00:00Z");
    }
}
