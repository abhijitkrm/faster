//! JSON-RPC methods answered from faster-sync.
//!
//! faster-sync stores every key of the node's block, transaction, receipt
//! and log objects, and returns them as the node sent them (`?shape=rpc`).
//! This module adds what is positional (`blockHash` on transactions,
//! receipts and logs), assembles receipts with their logs, and resolves
//! block tags against faster-sync's height.

use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use serde_json::{json, Map, Value};

use crate::sync::{Sync, SyncError};
use crate::upstream::Upstream;

pub struct Config {
    /// eth_getLogs fails with -32005 above this many logs.
    pub max_logs: usize,
    /// eth_getLogs block range limit (0 = none).
    pub max_block_range: u64,
    /// Add `blockTimestamp` to receipts and logs (as recent geth does).
    pub block_timestamp: bool,
    pub cache_blocks: usize,
}

impl Default for Config {
    fn default() -> Self {
        Config { max_logs: 10_000, max_block_range: 0, block_timestamp: false, cache_blocks: 4096 }
    }
}

pub struct Ctx {
    pub sync: Arc<Sync>,
    pub upstream: Option<Upstream>,
    pub cfg: Config,
    /// Final blocks by (number, full transactions), as sent to clients.
    cache: Mutex<lru::LruCache<(u64, bool), Value>>,
}

/// A JSON-RPC error object, or a request to forward the call upstream.
#[derive(Debug)]
pub enum Fail {
    Error(Value),
    /// Not answerable from faster-sync (state method, block newer than the
    /// archive, unknown tx that may be pending).
    Forward,
}

pub type Res = Result<Value, Fail>;

fn err(code: i64, message: impl Into<String>) -> Fail {
    Fail::Error(json!({"code": code, "message": message.into()}))
}

fn invalid(message: impl Into<String>) -> Fail {
    err(-32602, message)
}

impl From<SyncError> for Fail {
    fn from(e: SyncError) -> Fail {
        tracing::error!(error = %e, "faster-sync");
        err(-32603, e.to_string())
    }
}

fn q(n: u64) -> String {
    format!("0x{n:x}")
}

fn parse_qty(v: &Value) -> Option<u64> {
    let s = v.as_str()?;
    u64::from_str_radix(s.strip_prefix("0x")?, 16).ok()
}

fn is_hash(s: &str) -> bool {
    s.len() == 66 && s.starts_with("0x") && s[2..].bytes().all(|b| b.is_ascii_hexdigit())
}

/// A block reference after tag resolution.
enum At {
    Number(u64),
    Hash(String),
}

/// Methods served from faster-sync (everything else is forwarded).
pub const SERVED: &[&str] = &[
    "eth_chainId",
    "net_version",
    "web3_clientVersion",
    "eth_blockNumber",
    "eth_syncing",
    "eth_getBlockByNumber",
    "eth_getBlockByHash",
    "eth_getBlockTransactionCountByNumber",
    "eth_getBlockTransactionCountByHash",
    "eth_getUncleCountByBlockNumber",
    "eth_getUncleCountByBlockHash",
    "eth_getTransactionByHash",
    "eth_getTransactionByBlockNumberAndIndex",
    "eth_getTransactionByBlockHashAndIndex",
    "eth_getTransactionReceipt",
    "eth_getBlockReceipts",
    "eth_getLogs",
];

impl Ctx {
    pub fn new(sync: Arc<Sync>, upstream: Option<Upstream>, cfg: Config) -> Ctx {
        let cap = NonZeroUsize::new(cfg.cache_blocks.max(1)).unwrap();
        Ctx { sync, upstream, cfg, cache: Mutex::new(lru::LruCache::new(cap)) }
    }

    /// Answers one call: from faster-sync, else upstream, else an error.
    pub async fn call(&self, method: &str, params: &Value) -> Result<Value, Value> {
        let r = if SERVED.contains(&method) { self.serve(method, params).await } else { Err(Fail::Forward) };
        match r {
            Ok(v) => Ok(v),
            Err(Fail::Error(e)) => Err(e),
            Err(Fail::Forward) => match &self.upstream {
                Some(u) => u.call(method, params).await,
                None if SERVED.contains(&method) => Ok(Value::Null),
                None => Err(
                    json!({"code": -32601, "message": format!("method {method} not supported (no upstream node configured)")}),
                ),
            },
        }
    }

    fn height(&self) -> Result<u64, Fail> {
        self.sync.heights().height.ok_or_else(|| err(-32000, "no blocks indexed yet"))
    }

    fn resolve(&self, v: &Value) -> Result<At, Fail> {
        match v {
            Value::String(s) => match s.as_str() {
                "latest" | "pending" => Ok(At::Number(self.height()?)),
                "safe" | "finalized" => {
                    let h = self.sync.heights();
                    Ok(At::Number(h.finalized.min(h.height.unwrap_or(0))))
                }
                "earliest" => Ok(At::Number(0)),
                s if is_hash(s) => Ok(At::Hash(s.to_lowercase())),
                _ => parse_qty(v).map(At::Number).ok_or_else(|| invalid(format!("invalid block number {s:?}"))),
            },
            // EIP-1898 object form.
            Value::Object(o) => {
                if let Some(h) = o.get("blockHash").and_then(|h| h.as_str()) {
                    return Ok(At::Hash(h.to_lowercase()));
                }
                match o.get("blockNumber") {
                    Some(n) => self.resolve(n),
                    None => Err(invalid("block object needs blockNumber or blockHash")),
                }
            }
            _ => Err(invalid("invalid block parameter")),
        }
    }

    /// Block number of `at`, or Forward when it is newer than the archive.
    async fn number(&self, at: At) -> Result<Option<u64>, Fail> {
        match at {
            At::Number(n) => {
                if n > self.height()? {
                    return Err(Fail::Forward);
                }
                Ok(Some(n))
            }
            At::Hash(h) => {
                let v = self.sync.query(json!({"from_block": 0, "blocks": [{"hash": [h]}], "join": "nothing"})).await?;
                match v["blocks"][0]["number"].as_str().and_then(|s| u64::from_str_radix(&s[2..], 16).ok()) {
                    Some(n) => Ok(Some(n)),
                    // Unknown hash: maybe newer than the archive.
                    None => Err(Fail::Forward),
                }
            }
        }
    }

    async fn serve(&self, method: &str, p: &Value) -> Res {
        let arg = |i: usize| p.get(i).unwrap_or(&Value::Null);
        match method {
            "eth_chainId" => Ok(json!(q(self.chain_id().await?))),
            "net_version" => Ok(json!(self.chain_id().await?.to_string())),
            "web3_clientVersion" => Ok(json!(concat!("faster-rpc/", env!("CARGO_PKG_VERSION")))),
            "eth_blockNumber" => Ok(json!(q(self.height()?))),
            "eth_syncing" => Ok(json!(false)),
            "eth_getBlockByNumber" | "eth_getBlockByHash" => {
                let full = arg(1).as_bool().unwrap_or(false);
                match self.number(self.resolve(arg(0))?).await? {
                    Some(n) => Ok(self.block(n, full).await?.unwrap_or(Value::Null)),
                    None => Ok(Value::Null),
                }
            }
            "eth_getBlockTransactionCountByNumber" | "eth_getBlockTransactionCountByHash" => {
                match self.number(self.resolve(arg(0))?).await? {
                    Some(n) => Ok(self
                        .block(n, false)
                        .await?
                        .map(|b| json!(q(b["transactions"].as_array().map(|a| a.len()).unwrap_or(0) as u64)))
                        .unwrap_or(Value::Null)),
                    None => Ok(Value::Null),
                }
            }
            "eth_getUncleCountByBlockNumber" | "eth_getUncleCountByBlockHash" => {
                match self.number(self.resolve(arg(0))?).await? {
                    Some(n) => Ok(self
                        .block(n, false)
                        .await?
                        .map(|b| json!(q(b["uncles"].as_array().map(|a| a.len()).unwrap_or(0) as u64)))
                        .unwrap_or(Value::Null)),
                    None => Ok(Value::Null),
                }
            }
            "eth_getTransactionByBlockNumberAndIndex" | "eth_getTransactionByBlockHashAndIndex" => {
                let i = parse_qty(arg(1)).ok_or_else(|| invalid("invalid transaction index"))? as usize;
                match self.number(self.resolve(arg(0))?).await? {
                    Some(n) => Ok(self
                        .block(n, true)
                        .await?
                        .and_then(|b| b["transactions"].get(i).cloned())
                        .unwrap_or(Value::Null)),
                    None => Ok(Value::Null),
                }
            }
            "eth_getTransactionByHash" => {
                let h = tx_hash(arg(0))?;
                let v = self
                    .sync
                    .query(json!({"from_block": 0, "transactions": [{"hash": [h]}], "join": "blocks"}))
                    .await?;
                let Some(t) = v["transactions"].get(0) else { return Err(Fail::Forward) };
                let hash = v["blocks"][0]["hash"].clone();
                Ok(tx_object(t, &hash))
            }
            "eth_getTransactionReceipt" => {
                let h = tx_hash(arg(0))?;
                let v =
                    self.sync.query(json!({"from_block": 0, "transactions": [{"hash": [h]}], "join": "all"})).await?;
                let Some(t) = v["transactions"].get(0) else { return Err(Fail::Forward) };
                let b = &v["blocks"][0];
                let logs: Vec<Value> = v["logs"].as_array().cloned().unwrap_or_default();
                Ok(self.receipt(t, b, logs))
            }
            "eth_getBlockReceipts" => {
                let Some(n) = self.number(self.resolve(arg(0))?).await? else { return Ok(Value::Null) };
                let v = self
                    .sync
                    .query(json!({"from_block": n, "to_block": n + 1, "transactions": [{}], "include_all_blocks": true, "join": "all"}))
                    .await?;
                let Some(b) = v["blocks"].get(0) else { return Ok(Value::Null) };
                let mut by_tx: HashMap<String, Vec<Value>> = HashMap::new();
                for l in v["logs"].as_array().into_iter().flatten() {
                    by_tx.entry(l["transactionIndex"].as_str().unwrap_or("").to_string()).or_default().push(l.clone());
                }
                let rs: Vec<Value> = v["transactions"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .map(|t| {
                        let ti = t["tx"]["transactionIndex"].as_str().unwrap_or("").to_string();
                        self.receipt(t, b, by_tx.remove(&ti).unwrap_or_default())
                    })
                    .collect();
                Ok(Value::Array(rs))
            }
            "eth_getLogs" => self.logs(arg(0)).await,
            _ => Err(Fail::Forward),
        }
    }

    async fn chain_id(&self) -> Result<u64, Fail> {
        let c = self.sync.chain_id.load(std::sync::atomic::Ordering::Relaxed);
        if c != 0 {
            return Ok(c);
        }
        self.sync.fetch_heights().await?;
        Ok(self.sync.chain_id.load(std::sync::atomic::Ordering::Relaxed))
    }

    /// Block `n` as eth_getBlockByNumber returns it.
    pub async fn block(&self, n: u64, full: bool) -> Result<Option<Value>, Fail> {
        if let Some(b) = self.cache.lock().unwrap().get(&(n, full)) {
            return Ok(Some(b.clone()));
        }
        let v = self
            .sync
            .query(json!({"from_block": n, "to_block": n + 1, "include_all_blocks": true, "transactions": [{}], "join": "nothing"}))
            .await?;
        let Some(b) = v["blocks"].get(0) else { return Ok(None) };
        let mut b = b.clone();
        let hash = b["hash"].clone();
        let txs: Vec<Value> = v["transactions"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|t| if full { tx_object(t, &hash) } else { t["tx"]["hash"].clone() })
            .collect();
        b["transactions"] = Value::Array(txs);
        if n <= self.sync.heights().finalized {
            self.cache.lock().unwrap().put((n, full), b.clone());
        }
        Ok(Some(b))
    }

    fn receipt(&self, t: &Value, block: &Value, logs: Vec<Value>) -> Value {
        let tx = &t["tx"];
        let mut r = t["receipt"].as_object().cloned().unwrap_or_default();
        for (k, from) in [
            ("transactionHash", "hash"),
            ("transactionIndex", "transactionIndex"),
            ("blockNumber", "blockNumber"),
            ("from", "from"),
            ("to", "to"),
            ("type", "type"),
        ] {
            r.insert(k.into(), tx.get(from).cloned().unwrap_or(Value::Null));
        }
        r.insert("blockHash".into(), block["hash"].clone());
        if !r.contains_key("contractAddress") {
            r.insert("contractAddress".into(), Value::Null);
        }
        let logs: Vec<Value> = logs.into_iter().map(|l| self.log_object(l, block)).collect();
        r.insert("logs".into(), Value::Array(logs));
        if self.cfg.block_timestamp {
            r.insert("blockTimestamp".into(), block["timestamp"].clone());
        }
        Value::Object(r)
    }

    fn log_object(&self, l: Value, block: &Value) -> Value {
        let mut l = match l {
            Value::Object(m) => m,
            _ => Map::new(),
        };
        l.insert("blockHash".into(), block["hash"].clone());
        if self.cfg.block_timestamp {
            l.insert("blockTimestamp".into(), block["timestamp"].clone());
        }
        Value::Object(l)
    }

    async fn logs(&self, f: &Value) -> Res {
        let f = f.as_object().ok_or_else(|| invalid("filter must be an object"))?;
        let (from, to) = if let Some(h) = f.get("blockHash") {
            if f.contains_key("fromBlock") || f.contains_key("toBlock") {
                return Err(invalid("cannot specify both blockHash and fromBlock/toBlock"));
            }
            match self.number(self.resolve(h)?).await? {
                Some(n) => (n, n),
                None => return Err(err(-32000, "unknown block")),
            }
        } else {
            let h = self.height()?;
            let at = |k: &str| -> Result<u64, Fail> {
                match f.get(k) {
                    None | Some(Value::Null) => Ok(h),
                    Some(v) => match self.resolve(v)? {
                        At::Number(n) => Ok(n),
                        At::Hash(_) => Err(invalid(format!("{k} must be a block number or tag"))),
                    },
                }
            };
            let (a, b) = (at("fromBlock")?, at("toBlock")?);
            if a > h || b > h {
                // Past the archive: the node may have it (we trail by a
                // block or two); otherwise answer as reth/geth do.
                if self.upstream.is_some() {
                    return Err(Fail::Forward);
                }
                return Err(invalid("block range extends beyond current head block"));
            }
            if a > b {
                return Err(invalid("invalid block range params"));
            }
            (a, b)
        };
        if self.cfg.max_block_range > 0 && to - from + 1 > self.cfg.max_block_range {
            return Err(err(-32005, format!("block range exceeds {} blocks", self.cfg.max_block_range)));
        }
        let mut sel = Map::new();
        match f.get("address") {
            None | Some(Value::Null) => {}
            Some(Value::String(a)) => {
                sel.insert("address".into(), json!([a]));
            }
            Some(Value::Array(a)) => {
                sel.insert("address".into(), Value::Array(a.clone()));
            }
            _ => return Err(invalid("invalid address")),
        }
        // As geth and reth: N topic positions (wildcards included) require a
        // log to have at least N topics.
        let min_topics = f.get("topics").and_then(|t| t.as_array()).map(|t| t.len()).unwrap_or(0);
        if let Some(Value::Array(ts)) = f.get("topics") {
            if ts.len() > 4 {
                return Err(invalid("too many topics"));
            }
            let pos: Vec<Value> = ts
                .iter()
                .map(|t| match t {
                    Value::Null => Ok(json!([])),
                    Value::String(s) => Ok(json!([s])),
                    Value::Array(a) => Ok(Value::Array(a.iter().filter(|x| !x.is_null()).cloned().collect())),
                    _ => Err(invalid("invalid topic")),
                })
                .collect::<Result<_, _>>()?;
            sel.insert("topics".into(), Value::Array(pos));
        }
        let qv = json!({"from_block": from, "to_block": to + 1, "logs": [sel], "join": "blocks"});
        let v = self.sync.query_all(qv, self.cfg.max_logs).await.map_err(|e| match e {
            SyncError::Status(400, m) => invalid(m),
            e => e.into(),
        })?;
        let logs = v["logs"].as_array().cloned().unwrap_or_default();
        if logs.len() > self.cfg.max_logs {
            return Err(err(-32005, format!("query returned more than {} results", self.cfg.max_logs)));
        }
        let blocks: HashMap<String, &Value> = v["blocks"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|b| (b["number"].as_str().unwrap_or("").to_string(), b))
            .collect();
        let empty = json!({});
        Ok(Value::Array(
            logs.into_iter()
                .filter(|l| l["topics"].as_array().map(|t| t.len()).unwrap_or(0) >= min_topics)
                .map(|l| {
                    let b = blocks.get(l["blockNumber"].as_str().unwrap_or("")).copied().unwrap_or(&empty);
                    self.log_object(l, b)
                })
                .collect(),
        ))
    }
}

fn tx_hash(v: &Value) -> Result<String, Fail> {
    match v.as_str() {
        Some(s) if is_hash(s) => Ok(s.to_lowercase()),
        _ => Err(invalid("invalid transaction hash")),
    }
}

/// A transaction as eth_getTransactionByHash returns it.
fn tx_object(t: &Value, block_hash: &Value) -> Value {
    let mut o = t["tx"].as_object().cloned().unwrap_or_default();
    o.insert("blockHash".into(), block_hash.clone());
    Value::Object(o)
}
