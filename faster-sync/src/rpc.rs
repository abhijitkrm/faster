//! JSON-RPC client for ingest: batched calls, a request-rate limit for
//! public endpoints, and block + receipts (+ traces) fetching.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde_json::{json, Map, Value};

#[derive(Debug, thiserror::Error)]
pub enum RpcError {
    #[error("transport: {0}")]
    Transport(String),
    #[error("http {0}: {1}")]
    Http(u16, String),
    #[error("rpc error {code}: {message}")]
    Rpc { code: i64, message: String },
    #[error("decode: {0}")]
    Decode(String),
    /// The node has not got this block (or its receipts) yet.
    #[error("not ready: {0}")]
    NotReady(String),
}

impl RpcError {
    /// Worth retrying after a pause (rate limits, overloaded node, timeouts).
    pub fn retryable(&self) -> bool {
        match self {
            RpcError::Transport(_) | RpcError::NotReady(_) => true,
            RpcError::Http(s, _) => *s == 429 || *s >= 500,
            RpcError::Rpc { code, message } => {
                let m = message.to_ascii_lowercase();
                *code == -32005
                    || *code == 429
                    || m.contains("rate")
                    || m.contains("limit")
                    || m.contains("timeout")
                    || m.contains("busy")
                    || m.contains("try again")
            }
            RpcError::Decode(_) => true,
        }
    }
}

const METHOD_NOT_FOUND: i64 = -32601;

pub struct RpcConfig {
    pub url: String,
    pub timeout: Duration,
    /// Max calls per HTTP request.
    pub batch: usize,
    /// Max HTTP requests per second (0 = unlimited).
    pub rps: u32,
    pub traces: bool,
}

pub struct Rpc {
    cfg: RpcConfig,
    http: reqwest::Client,
    block_receipts: AtomicBool,
    traces: AtomicBool,
    limiter: Mutex<Instant>,
    pub requests: AtomicU64,
    pub errors: AtomicU64,
}

/// One block as the node returned it.
pub struct RawBlock {
    pub block: Map<String, Value>,
    pub receipts: Vec<Value>,
    pub traces: Option<Vec<Value>>,
}

impl Rpc {
    pub fn new(cfg: RpcConfig) -> anyhow::Result<Rpc> {
        let http = reqwest::Client::builder()
            .timeout(cfg.timeout)
            .pool_max_idle_per_host(64)
            .tcp_nodelay(true)
            .user_agent(concat!("faster-sync/", env!("CARGO_PKG_VERSION")))
            .build()?;
        let traces = cfg.traces;
        Ok(Rpc {
            cfg,
            http,
            block_receipts: AtomicBool::new(true),
            traces: AtomicBool::new(traces),
            limiter: Mutex::new(Instant::now()),
            requests: AtomicU64::new(0),
            errors: AtomicU64::new(0),
        })
    }

    pub fn traces(&self) -> bool {
        self.traces.load(Ordering::Relaxed)
    }

    async fn pace(&self) {
        if self.cfg.rps == 0 {
            return;
        }
        let gap = Duration::from_secs_f64(1.0 / self.cfg.rps as f64);
        let wait = {
            let mut next = self.limiter.lock().unwrap();
            let now = Instant::now();
            let at = (*next).max(now);
            *next = at + gap;
            at - now
        };
        if !wait.is_zero() {
            tokio::time::sleep(wait).await;
        }
    }

    /// Runs `calls` in as many HTTP requests as the batch limit needs and
    /// returns each call's result in order (`Err` per failed call when
    /// `lenient`, otherwise the first error fails the whole call).
    pub async fn call_many(&self, calls: &[(&str, Value)]) -> Result<Vec<Value>, RpcError> {
        let mut out = Vec::with_capacity(calls.len());
        let futs = calls.chunks(self.cfg.batch.max(1)).map(|c| self.one_batch(c));
        for r in futures::future::join_all(futs).await {
            out.extend(r?);
        }
        Ok(out)
    }

    async fn one_batch(&self, calls: &[(&str, Value)]) -> Result<Vec<Value>, RpcError> {
        self.pace().await;
        self.requests.fetch_add(1, Ordering::Relaxed);
        let reqs: Vec<Value> = calls
            .iter()
            .enumerate()
            .map(|(i, (m, p))| json!({"jsonrpc": "2.0", "id": i, "method": m, "params": p}))
            .collect();
        let res = self.http.post(&self.cfg.url).json(&reqs).send().await;
        let resp = match res {
            Ok(r) => r,
            Err(e) => {
                self.errors.fetch_add(1, Ordering::Relaxed);
                return Err(RpcError::Transport(e.to_string()));
            }
        };
        let status = resp.status();
        let body = resp.bytes().await.map_err(|e| RpcError::Transport(e.to_string()))?;
        if !status.is_success() {
            self.errors.fetch_add(1, Ordering::Relaxed);
            return Err(RpcError::Http(status.as_u16(), String::from_utf8_lossy(&body[..body.len().min(200)]).into()));
        }
        let v: Value = serde_json::from_slice(&body).map_err(|e| RpcError::Decode(format!("response: {e}")))?;
        let arr = match v {
            Value::Array(a) => a,
            Value::Object(o) => {
                // Some nodes answer a whole batch with one error object.
                if let Some(e) = o.get("error") {
                    self.errors.fetch_add(1, Ordering::Relaxed);
                    return Err(rpc_err(e));
                }
                return Err(RpcError::Decode("batch answered with a single object".into()));
            }
            _ => return Err(RpcError::Decode("batch response is not a list".into())),
        };
        let mut out: Vec<Option<Value>> = vec![None; calls.len()];
        for mut r in arr {
            let id = r.get("id").and_then(|v| v.as_u64()).unwrap_or(u64::MAX) as usize;
            if id >= out.len() {
                continue;
            }
            if let Some(e) = r.get("error") {
                self.errors.fetch_add(1, Ordering::Relaxed);
                return Err(rpc_err(e));
            }
            out[id] = Some(r.get_mut("result").map(Value::take).unwrap_or(Value::Null));
        }
        out.into_iter()
            .enumerate()
            .map(|(i, v)| v.ok_or_else(|| RpcError::Decode(format!("no response for call {i}"))))
            .collect()
    }

    pub async fn head(&self) -> Result<u64, RpcError> {
        let r = self.call_many(&[("eth_blockNumber", json!([]))]).await?;
        qty_u64(&r[0]).ok_or_else(|| RpcError::Decode("eth_blockNumber".into()))
    }

    pub async fn chain_id(&self) -> Result<u64, RpcError> {
        let r = self.call_many(&[("eth_chainId", json!([]))]).await?;
        qty_u64(&r[0]).ok_or_else(|| RpcError::Decode("eth_chainId".into()))
    }

    /// The node's finalized block, if it supports the "finalized" tag.
    pub async fn finalized(&self) -> Result<Option<u64>, RpcError> {
        match self.call_many(&[("eth_getBlockByNumber", json!(["finalized", false]))]).await {
            Ok(r) => Ok(r[0].get("number").and_then(qty_u64)),
            Err(RpcError::Rpc { .. }) => Ok(None),
            Err(e) => Err(e),
        }
    }

    pub async fn block_hash(&self, n: u64) -> Result<Option<[u8; 32]>, RpcError> {
        let r = self.call_many(&[("eth_getBlockByNumber", json!([format!("0x{n:x}"), false]))]).await?;
        Ok(r[0].get("hash").and_then(|h| h.as_str()).and_then(crate::model::parse_fixed::<32>))
    }

    /// Fetches complete blocks. Fails (NotReady) rather than return a block
    /// with missing receipts or traces.
    pub async fn blocks(&self, numbers: &[u64]) -> Result<Vec<RawBlock>, RpcError> {
        let mut out = self.blocks_receipts(numbers).await?;
        if self.traces.load(Ordering::Relaxed) {
            let calls: Vec<_> = numbers
                .iter()
                .map(|n| ("debug_traceBlockByNumber", json!([format!("0x{n:x}"), {"tracer": "callTracer"}])))
                .collect();
            match self.call_many(&calls).await {
                Ok(res) => {
                    for (b, r) in out.iter_mut().zip(res) {
                        match r {
                            Value::Array(a) => b.traces = Some(a),
                            _ => return Err(RpcError::NotReady("traces: null".into())),
                        }
                    }
                }
                Err(RpcError::Rpc { code, message }) if code == METHOD_NOT_FOUND => {
                    tracing::error!(%message, "TRACES is on but the node has no debug_traceBlockByNumber; traces off");
                    self.traces.store(false, Ordering::Relaxed);
                }
                Err(e) => return Err(e),
            }
        }
        Ok(out)
    }

    async fn blocks_receipts(&self, numbers: &[u64]) -> Result<Vec<RawBlock>, RpcError> {
        if self.block_receipts.load(Ordering::Relaxed) {
            let mut calls = Vec::with_capacity(numbers.len() * 2);
            for n in numbers {
                let h = format!("0x{n:x}");
                calls.push(("eth_getBlockByNumber", json!([h, true])));
                calls.push(("eth_getBlockReceipts", json!([h])));
            }
            match self.call_many(&calls).await {
                Ok(res) => {
                    let mut it = res.into_iter();
                    let mut out = Vec::with_capacity(numbers.len());
                    for n in numbers {
                        let b = block_obj(it.next().unwrap(), *n)?;
                        let rs = match it.next().unwrap() {
                            Value::Array(a) => a,
                            _ => return Err(RpcError::NotReady(format!("receipts {n}: null"))),
                        };
                        out.push(RawBlock { block: b, receipts: rs, traces: None });
                    }
                    return Ok(out);
                }
                Err(RpcError::Rpc { code, message })
                    if code == METHOD_NOT_FOUND || message.to_ascii_lowercase().contains("not supported") =>
                {
                    tracing::warn!(%message, "eth_getBlockReceipts unsupported; fetching receipts per tx");
                    self.block_receipts.store(false, Ordering::Relaxed);
                }
                Err(e) => return Err(e),
            }
        }
        let calls: Vec<_> =
            numbers.iter().map(|n| ("eth_getBlockByNumber", json!([format!("0x{n:x}"), true]))).collect();
        let res = self.call_many(&calls).await?;
        let mut blocks = Vec::with_capacity(numbers.len());
        for (v, n) in res.into_iter().zip(numbers) {
            blocks.push(block_obj(v, *n)?);
        }
        let hashes: Vec<String> = blocks
            .iter()
            .flat_map(|b| {
                b.get("transactions")
                    .and_then(|t| t.as_array())
                    .into_iter()
                    .flatten()
                    .filter_map(|t| t.get("hash").and_then(|h| h.as_str()).map(String::from))
            })
            .collect();
        let calls: Vec<_> = hashes.iter().map(|h| ("eth_getTransactionReceipt", json!([h]))).collect();
        let mut rs = self.call_many(&calls).await?.into_iter();
        let mut out = Vec::with_capacity(blocks.len());
        for b in blocks {
            let n = b.get("transactions").and_then(|t| t.as_array()).map(|a| a.len()).unwrap_or(0);
            let receipts: Vec<Value> = rs.by_ref().take(n).collect();
            if receipts.iter().any(|r| r.is_null()) {
                return Err(RpcError::NotReady("receipt: null".into()));
            }
            out.push(RawBlock { block: b, receipts, traces: None });
        }
        Ok(out)
    }
}

fn rpc_err(e: &Value) -> RpcError {
    RpcError::Rpc {
        code: e.get("code").and_then(|c| c.as_i64()).unwrap_or(0),
        message: e.get("message").and_then(|m| m.as_str()).unwrap_or("").to_string(),
    }
}

fn block_obj(v: Value, n: u64) -> Result<Map<String, Value>, RpcError> {
    match v {
        Value::Object(o) => {
            if o.get("number").and_then(qty_u64) != Some(n) {
                return Err(RpcError::Decode(format!("block {n}: node returned another block")));
            }
            Ok(o)
        }
        _ => Err(RpcError::NotReady(format!("block {n}: null"))),
    }
}

pub fn qty_u64(v: &Value) -> Option<u64> {
    let s = v.as_str()?;
    u64::from_str_radix(s.strip_prefix("0x")?, 16).ok()
}
