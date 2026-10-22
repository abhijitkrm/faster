//! Synthetic JSON-RPC node for benchmarking the indexer without a real
//! chain's limits. Serves a deterministic chain of BLOCKS blocks with
//! TXS transactions each (20% of them emit two logs, like token transfers).
//!
//!   fakenode <port> <blocks> <txs-per-block>
//!   fakenode <port> live <first-block> <txs-per-block> <block-ms>
//!
//! Live mode grows the chain in real time from <first-block>: one block
//! every <block-ms>, timestamps on the wall clock, tx counts varying around
//! <txs-per-block> (for demos and screenshots).

use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::State;
use axum::response::IntoResponse;
use axum::routing::post;
use axum::Router;
use serde_json::Value;

struct Chain {
    blocks: u64,
    txs: u64,
    live: Option<Live>,
}

struct Live {
    first: u64,
    block_ms: u64,
    start: std::time::Instant,
    start_unix_ms: u64,
}

fn h(tag: u8, n: u64, i: u64) -> String {
    format!("0x{tag:02x}{:030x}{:032x}", n, i)
}

fn addr(x: u64) -> String {
    format!("0x{:040x}", x)
}

/// Transfer value in wei: 0.001 to ~5 ETH, skewed small; contract calls carry 0.
fn value(n: u64, i: u64) -> u128 {
    if i % 5 == 0 {
        return 0;
    }
    let r = (party(n, i, 11) - 0x10_0000) as u128; // 0..200_000, skewed
    (r % 5000 + 1) * 1_000_000_000_000_000
}

/// Cheap deterministic pseudo-random address with a skewed distribution.
fn party(n: u64, i: u64, salt: u64) -> u64 {
    let mut x = n.wrapping_mul(0x9E3779B97F4A7C15) ^ i.wrapping_mul(0xC2B2AE3D27D4EB4F) ^ salt;
    x ^= x >> 31;
    x = x.wrapping_mul(0xBF58476D1CE4E5B9);
    x ^= x >> 29;
    // skew: small ids are much more common
    let r = (x % 1_000_000) as f64 / 1_000_000.0;
    0x10_0000 + (200_000.0 * r * r * r) as u64
}

impl Chain {
    /// Lowest and highest block the node serves.
    fn range(&self) -> (u64, u64) {
        match &self.live {
            Some(l) => (l.first, l.first + l.start.elapsed().as_millis() as u64 / l.block_ms),
            None => (0, self.blocks - 1),
        }
    }

    fn txs_in(&self, n: u64) -> u64 {
        match &self.live {
            // between 60% and 140% of the nominal count
            Some(_) => self.txs * 6 / 10 + party(n, 0, 7) % (self.txs * 8 / 10 + 1),
            None => self.txs,
        }
    }

    fn timestamp(&self, n: u64) -> u64 {
        match &self.live {
            Some(l) => (l.start_unix_ms + (n - l.first) * l.block_ms) / 1000,
            None => 1_700_000_000 + n,
        }
    }

    fn block_hash(&self, n: u64) -> String {
        h(0xb0, n, 0)
    }

    fn block(&self, n: u64, full: bool) -> String {
        let mut txs = String::with_capacity(self.txs as usize * 420);
        let count = self.txs_in(n);
        for i in 0..count {
            if i > 0 {
                txs.push(',');
            }
            if full {
                let to = if i % 5 == 0 { addr(0xc0ffee) } else { addr(party(n, i, 2)) };
                txs.push_str(&format!(
                    r#"{{"hash":"{}","from":"{}","to":"{}","value":"0x{:x}","gas":"0x5208","gasPrice":"0x3b9aca00","maxFeePerGas":"0x77359400","maxPriorityFeePerGas":"0x1","nonce":"0x{:x}","input":"0x","type":"0x2","transactionIndex":"0x{:x}","blockNumber":"0x{:x}"}}"#,
                    h(0x70, n, i), addr(party(n, i, 1)), to, value(n, i), n, i, n
                ));
            } else {
                txs.push_str(&format!("\"{}\"", h(0x70, n, i)));
            }
        }
        let parent = if n == 0 { h(0, 0, 0) } else { self.block_hash(n - 1) };
        format!(
            r#"{{"number":"0x{n:x}","hash":"{}","parentHash":"{parent}","timestamp":"0x{:x}","miner":"{}","gasUsed":"0x{:x}","gasLimit":"0x1c9c380","baseFeePerGas":"0x7","difficulty":"0x0","stateRoot":"{}","transactionsRoot":"{}","receiptsRoot":"{}","size":"0x1000","transactions":[{txs}]}}"#,
            self.block_hash(n),
            self.timestamp(n),
            addr(9),
            21000 * count,
            h(0x51, n, 0),
            h(0x52, n, 0),
            h(0x53, n, 0)
        )
    }

    fn receipts(&self, n: u64) -> String {
        let mut out = String::with_capacity(self.txs as usize * 400);
        out.push('[');
        let mut li = 0;
        for i in 0..self.txs_in(n) {
            if i > 0 {
                out.push(',');
            }
            let logs = if i % 5 == 0 {
                let from = party(n, i, 1);
                let to = party(n, i, 3);
                let mk = |li: u64| {
                    format!(
                        r#"{{"address":"{}","topics":["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef","0x000000000000000000000000{:040x}","0x000000000000000000000000{:040x}"],"data":"0x{:064x}","logIndex":"0x{li:x}","transactionIndex":"0x{i:x}","blockNumber":"0x{n:x}"}}"#,
                        addr(0xc0ffee),
                        from,
                        to,
                        i + 1
                    )
                };
                let s = format!("{},{}", mk(li), mk(li + 1));
                li += 2;
                s
            } else {
                String::new()
            };
            out.push_str(&format!(
                r#"{{"transactionHash":"{}","transactionIndex":"0x{i:x}","status":"0x1","gasUsed":"0x5208","cumulativeGasUsed":"0x{:x}","effectiveGasPrice":"0x3b9aca00","contractAddress":null,"logs":[{logs}]}}"#,
                h(0x70, n, i),
                21000 * (i + 1)
            ));
        }
        out.push(']');
        out
    }
}

async fn rpc(State(c): State<Arc<Chain>>, body: Bytes) -> impl IntoResponse {
    let v: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    let (reqs, batch) = match v {
        Value::Array(a) => (a, true),
        other => (vec![other], false),
    };
    let (first, tip) = c.range();
    let mut parts = Vec::with_capacity(reqs.len());
    for r in &reqs {
        let id = &r["id"];
        let p0 = r["params"][0].as_str().unwrap_or("0x0");
        let n = u64::from_str_radix(p0.trim_start_matches("0x"), 16).unwrap_or(u64::MAX);
        let result = match r["method"].as_str().unwrap_or("") {
            "eth_blockNumber" => format!("\"0x{tip:x}\""),
            "eth_chainId" => "\"0x539\"".into(),
            "eth_getBlockByNumber" if n >= first && n <= tip => c.block(n, r["params"][1].as_bool().unwrap_or(false)),
            "eth_getBlockReceipts" if n >= first && n <= tip => c.receipts(n),
            "eth_getBalance" => "\"0xde0b6b3a7640000\"".into(),
            "eth_getCode" => "\"0x\"".into(),
            _ => "null".into(),
        };
        parts.push(format!(r#"{{"jsonrpc":"2.0","id":{id},"result":{result}}}"#));
    }
    let body = if batch { format!("[{}]", parts.join(",")) } else { parts.pop().unwrap_or_default() };
    ([("content-type", "application/json")], body)
}

#[tokio::main]
async fn main() {
    let a: Vec<String> = std::env::args().collect();
    let port: u16 = a.get(1).and_then(|s| s.parse().ok()).unwrap_or(18546);
    let live = a.get(2).map(|s| s == "live").unwrap_or(false);
    let blocks: u64 = if live { 0 } else { a.get(2).and_then(|s| s.parse().ok()).unwrap_or(10_000) };
    let (txs, live) = if live {
        let first: u64 = a.get(3).and_then(|s| s.parse().ok()).unwrap_or(12_000_000);
        let txs: u64 = a.get(4).and_then(|s| s.parse().ok()).unwrap_or(500);
        let block_ms: u64 = a.get(5).and_then(|s| s.parse().ok()).unwrap_or(400);
        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64;
        (txs, Some(Live { first, block_ms, start: std::time::Instant::now(), start_unix_ms: now }))
    } else {
        (a.get(3).and_then(|s| s.parse().ok()).unwrap_or(500), None)
    };
    let app = Router::new().route("/", post(rpc)).with_state(Arc::new(Chain { blocks, txs, live }));
    let l = tokio::net::TcpListener::bind(("127.0.0.1", port)).await.unwrap();
    eprintln!("fakenode on :{port}, {blocks} blocks x {txs} txs");
    axum::serve(l, app).await.unwrap();
}
