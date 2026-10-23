//! A deterministic fake JSON-RPC node for tests and benchmarks.
//!
//! Block `n` is a pure function of (n, fork salt): transactions call
//! contracts drawn from a skewed distribution (a few busy contracts, a long
//! tail of rare ones) and emit ERC-20 style Transfer logs. `reorg(from)`
//! changes the salt of blocks `>= from`, so they get new hashes and
//! contents, like a chain reorganisation.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use axum::extract::State;
use axum::routing::post;
use axum::{Json, Router};
use serde_json::{json, Value};

pub const TRANSFER: [u8; 32] = [
    0xdd, 0xf2, 0x52, 0xad, 0x1b, 0xe2, 0xc8, 0x9b, 0x69, 0xc2, 0xb0, 0x68, 0xfc, 0x37, 0x8d, 0xaa, 0x95, 0x2b, 0xa7,
    0xf1, 0x63, 0xc4, 0xa1, 0x16, 0x28, 0xf5, 0x5a, 0x4d, 0xf5, 0x23, 0xb3, 0xef,
];
pub const APPROVAL: [u8; 32] = [0x8c; 32];

#[derive(Clone)]
pub struct Shape {
    pub txs_per_block: u64,
    pub logs_per_tx: u64,
    /// Distinct contracts; contract i is picked with weight ~ 1/(i+1).
    pub contracts: u64,
    pub accounts: u64,
    pub block_time: u64,
}

impl Default for Shape {
    fn default() -> Self {
        Shape { txs_per_block: 50, logs_per_tx: 2, contracts: 10_000, accounts: 100_000, block_time: 2 }
    }
}

pub struct FakeChain {
    pub shape: Shape,
    pub head: AtomicU64,
    /// (first block, salt) pairs, ascending: blocks >= first use salt.
    forks: RwLock<Vec<(u64, u64)>>,
    pub calls: AtomicU64,
    /// Omit eth_getBlockReceipts (fall back to per-tx receipts).
    pub no_block_receipts: bool,
}

fn mix(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E3779B97F4A7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D049BB133111EB);
    x ^ (x >> 31)
}

fn h32(a: u64, b: u64, c: u64) -> [u8; 32] {
    let mut out = [0u8; 32];
    for i in 0..4 {
        out[i * 8..i * 8 + 8].copy_from_slice(&mix(a ^ mix(b ^ mix(c ^ i as u64))).to_be_bytes());
    }
    out
}

pub fn contract_addr(i: u64) -> [u8; 20] {
    let mut a = [0u8; 20];
    a[0] = 0xc0;
    a[12..].copy_from_slice(&i.to_be_bytes());
    a
}

pub fn account_addr(i: u64) -> [u8; 20] {
    let mut a = [0u8; 20];
    a[0] = 0xaa;
    a[12..].copy_from_slice(&i.to_be_bytes());
    a
}

fn hx(b: &[u8]) -> String {
    format!("0x{}", hex::encode(b))
}

fn q(x: u64) -> String {
    format!("0x{x:x}")
}

impl FakeChain {
    pub fn new(shape: Shape, head: u64) -> Arc<FakeChain> {
        Arc::new(FakeChain {
            shape,
            head: AtomicU64::new(head),
            forks: RwLock::new(vec![(0, 0)]),
            calls: AtomicU64::new(0),
            no_block_receipts: false,
        })
    }

    pub fn reorg(&self, from: u64) {
        let mut f = self.forks.write().unwrap();
        let salt = f.len() as u64 * 7919;
        f.retain(|(b, _)| *b < from);
        f.push((from, salt));
    }

    fn salt(&self, n: u64) -> u64 {
        self.forks.read().unwrap().iter().rev().find(|(b, _)| *b <= n).map(|(_, s)| *s).unwrap_or(0)
    }

    pub fn block_hash(&self, n: u64) -> [u8; 32] {
        h32(n, self.salt(n), 1)
    }

    /// Weighted pick in [0, n): Zipf-like (log-uniform) skew.
    fn skewed(r: u64, n: u64) -> u64 {
        let u = (r >> 11) as f64 / (1u64 << 53) as f64;
        ((n as f64).powf(u) - 1.0).floor().min((n - 1) as f64) as u64
    }

    pub fn tx_contract(&self, n: u64, i: u64) -> u64 {
        Self::skewed(mix((n * 1_000_003 + i) ^ self.salt(n)), self.shape.contracts)
    }

    /// (block json with full txs, receipts json)
    pub fn block(&self, n: u64) -> (Value, Value) {
        let s = &self.shape;
        let salt = self.salt(n);
        let hash = self.block_hash(n);
        let parent = if n == 0 { [0u8; 32] } else { self.block_hash(n - 1) };
        let mut txs = Vec::with_capacity(s.txs_per_block as usize);
        let mut rcs = Vec::with_capacity(s.txs_per_block as usize);
        let mut log_index = 0u64;
        let mut cum = 0u64;
        for i in 0..s.txs_per_block {
            let r = mix(n.wrapping_mul(1_000_003).wrapping_add(i) ^ salt);
            let th = h32(n, i, salt ^ 2);
            let from = account_addr(r % s.accounts);
            let c = self.tx_contract(n, i);
            let to = contract_addr(c);
            let create = i == 0 && n % 100 == 7;
            let sig: [u8; 4] = if r.is_multiple_of(3) { [0x09, 0x5e, 0xa7, 0xb3] } else { [0xa9, 0x05, 0x9c, 0xbb] };
            let mut input = sig.to_vec();
            input.extend_from_slice(&[0u8; 12]);
            input.extend_from_slice(&account_addr(mix(r) % s.accounts));
            input.extend_from_slice(&h32(r, 0, 3));
            let gas_used = 21_000 + (r % 50_000);
            cum += gas_used;
            txs.push(json!({
                "hash": hx(&th), "blockHash": hx(&hash), "blockNumber": q(n), "transactionIndex": q(i),
                "from": hx(&from), "to": if create { Value::Null } else { Value::from(hx(&to)) },
                "value": q(r % 1_000_000_000), "gas": q(100_000), "gasPrice": q(1_000_000_000 + r % 1000),
                "maxFeePerGas": q(2_000_000_000), "maxPriorityFeePerGas": q(1_000_000), "nonce": q(r % 5000),
                "input": hx(&input), "type": "0x2", "chainId": "0x7a69", "v": "0x1", "r": hx(&h32(r, 1, 4)),
                "s": hx(&h32(r, 2, 4)), "yParity": "0x1", "accessList": []
            }));
            let mut logs = Vec::new();
            if !create {
                for k in 0..s.logs_per_tx {
                    let src = account_addr(mix(r + k) % s.accounts);
                    let dst = account_addr(mix(r + k + 1) % s.accounts);
                    let topic0 = if k % 4 == 3 { APPROVAL } else { TRANSFER };
                    let mut t1 = [0u8; 32];
                    t1[12..].copy_from_slice(&src);
                    let mut t2 = [0u8; 32];
                    t2[12..].copy_from_slice(&dst);
                    let addr = if k == 0 { to } else { contract_addr(Self::skewed(mix(r ^ k), s.contracts)) };
                    logs.push(json!({
                        "address": hx(&addr), "topics": [hx(&topic0), hx(&t1), hx(&t2)],
                        "data": hx(&h32(r, k, 5)), "blockNumber": q(n), "blockHash": hx(&hash),
                        "transactionHash": hx(&th), "transactionIndex": q(i), "logIndex": q(log_index), "removed": false
                    }));
                    log_index += 1;
                }
            }
            let ca = if create { Value::from(hx(&contract_addr(1_000_000 + n))) } else { Value::Null };
            rcs.push(json!({
                "transactionHash": hx(&th), "transactionIndex": q(i), "blockHash": hx(&hash), "blockNumber": q(n),
                "from": hx(&from), "to": if create { Value::Null } else { Value::from(hx(&to)) },
                "status": if r.is_multiple_of(50) { "0x0" } else { "0x1" }, "gasUsed": q(gas_used), "cumulativeGasUsed": q(cum),
                "effectiveGasPrice": q(1_001_000_000), "contractAddress": ca, "logs": logs,
                "logsBloom": hx(&[0u8; 256]), "type": "0x2"
            }));
        }
        let block = json!({
            "number": q(n), "hash": hx(&hash), "parentHash": hx(&parent), "timestamp": q(1_700_000_000 + n * s.block_time),
            "miner": hx(&account_addr(n % 16)), "gasUsed": q(cum), "gasLimit": q(30_000_000), "baseFeePerGas": q(1_000_000_000),
            "difficulty": "0x0", "size": q(1000 + s.txs_per_block * 200), "extraData": "0x", "nonce": "0x0000000000000000",
            "sha3Uncles": hx(&h32(n, 0, 6)), "logsBloom": hx(&[0u8; 256]), "transactionsRoot": hx(&h32(n, salt, 7)),
            "stateRoot": hx(&h32(n, salt, 8)), "receiptsRoot": hx(&h32(n, salt, 9)), "mixHash": hx(&h32(n, 0, 10)),
            "uncles": [], "transactions": txs
        });
        (block, Value::Array(rcs))
    }

    fn answer(&self, m: &str, p: &Value) -> Result<Value, (i64, String)> {
        let head = self.head.load(Ordering::Relaxed);
        let num = |v: &Value| -> Option<u64> {
            match v.as_str()? {
                "latest" => Some(head),
                "finalized" | "safe" => Some(head.saturating_sub(8)),
                "earliest" => Some(0),
                s => u64::from_str_radix(s.strip_prefix("0x")?, 16).ok(),
            }
        };
        match m {
            "eth_chainId" => Ok(json!("0x7a69")),
            "eth_blockNumber" => Ok(json!(q(head))),
            "eth_getBlockByNumber" => {
                let Some(n) = num(&p[0]).filter(|n| *n <= head) else { return Ok(Value::Null) };
                let (mut b, _) = self.block(n);
                if !p[1].as_bool().unwrap_or(false) {
                    let hs: Vec<Value> =
                        b["transactions"].as_array().unwrap().iter().map(|t| t["hash"].clone()).collect();
                    b["transactions"] = Value::Array(hs);
                }
                Ok(b)
            }
            "eth_getBlockReceipts" if !self.no_block_receipts => {
                let Some(n) = num(&p[0]).filter(|n| *n <= head) else { return Ok(Value::Null) };
                Ok(self.block(n).1)
            }
            "eth_getTransactionReceipt" => {
                // Test helper only: scan recent blocks.
                let h = p[0].as_str().unwrap_or("").to_string();
                for n in (0..=head).rev().take(10_000) {
                    let (_, rs) = self.block(n);
                    if let Some(r) = rs.as_array().unwrap().iter().find(|r| r["transactionHash"] == h) {
                        return Ok(r.clone());
                    }
                }
                Ok(Value::Null)
            }
            _ => Err((-32601, format!("method {m} not found"))),
        }
    }

    pub fn router(self: Arc<Self>) -> Router {
        async fn handle(State(c): State<Arc<FakeChain>>, Json(body): Json<Value>) -> Json<Value> {
            let one = |r: &Value| {
                c.calls.fetch_add(1, Ordering::Relaxed);
                let id = r["id"].clone();
                match c.answer(r["method"].as_str().unwrap_or(""), &r["params"]) {
                    Ok(v) => json!({"jsonrpc": "2.0", "id": id, "result": v}),
                    Err((code, msg)) => json!({"jsonrpc": "2.0", "id": id, "error": {"code": code, "message": msg}}),
                }
            };
            Json(match &body {
                Value::Array(a) => Value::Array(a.iter().map(one).collect()),
                r => one(r),
            })
        }
        Router::new().route("/", post(handle)).with_state(self)
    }

    /// Serves on 127.0.0.1:`port` (0 = any); returns the URL.
    pub async fn serve(self: Arc<Self>, port: u16) -> String {
        let l = tokio::net::TcpListener::bind(("127.0.0.1", port)).await.unwrap();
        let url = format!("http://{}", l.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(l, self.router()).await });
        url
    }
}
