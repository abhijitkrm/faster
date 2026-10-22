//! End-to-end indexer test against an in-process fake JSON-RPC node that can
//! return null blocks, drop receipts and reorganise its chain.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::State;
use axum::routing::post;
use axum::{Json, Router};
use serde_json::{json, Value};

use crate::indexer::{Indexer, IndexerConfig, Sink};
use crate::rpc::Rpc;
use crate::store::{Store, StoreConfig};
use crate::types::*;
use crate::Status;

#[derive(Default)]
struct Chain {
    /// fork id of each canonical block
    forks: Vec<u8>,
    /// block number -> remaining calls that answer null
    null_block: HashMap<u64, usize>,
    /// block number -> remaining calls that answer with no receipts
    no_receipts: HashMap<u64, usize>,
}

fn hash(n: u64, fork: u8, tag: u8) -> String {
    format!("0x{:02x}{:02x}{:060x}", tag, fork, n)
}

fn addr(x: u64) -> String {
    format!("0x{:040x}", x)
}

impl Chain {
    fn block_hash(&self, n: u64) -> String {
        hash(n, self.forks[n as usize], 0xb0)
    }

    fn tx(&self, n: u64) -> Value {
        let f = self.forks[n as usize];
        json!({"hash": hash(n, f, 0x70), "from": addr(n % 5 + 1), "to": addr(100 + f as u64),
               "value": "0x1", "gas": "0x5208", "gasPrice": "0x3b9aca00", "nonce": format!("0x{n:x}"),
               "input": "0x", "type": "0x0"})
    }

    fn block(&self, n: u64) -> Value {
        let parent = if n == 0 { hash(0, 0, 0) } else { self.block_hash(n - 1) };
        json!({"number": format!("0x{n:x}"), "hash": self.block_hash(n), "parentHash": parent,
               "timestamp": format!("0x{:x}", 1_700_000_000 + n), "miner": addr(9), "gasUsed": "0x5208",
               "gasLimit": "0x1c9c380", "baseFeePerGas": "0x7", "transactions": [self.tx(n)]})
    }

    fn receipts(&self, n: u64) -> Value {
        let f = self.forks[n as usize];
        json!([{"transactionHash": hash(n, f, 0x70), "status": "0x1", "gasUsed": "0x5208",
                "effectiveGasPrice": "0x3b9aca00", "contractAddress": null,
                "logs": [{"address": addr(100 + f as u64), "topics": [hash(n, f, 0x10)], "data": "0x"}]}])
    }
}

async fn node(State(c): State<Arc<Mutex<Chain>>>, Json(body): Json<Value>) -> Json<Value> {
    let mut c = c.lock().unwrap();
    let reqs = body.as_array().cloned().unwrap_or_else(|| vec![body.clone()]);
    let mut out = Vec::new();
    for r in reqs {
        let id = r["id"].clone();
        let n = r["params"][0].as_str().and_then(parse_u64).unwrap_or(0);
        let tip = c.forks.len() as u64;
        let result = match r["method"].as_str().unwrap() {
            "eth_blockNumber" => json!(format!("0x{:x}", tip - 1)),
            "eth_getBlockByNumber" if n >= tip => Value::Null,
            "eth_getBlockByNumber" => match c.null_block.get_mut(&n) {
                Some(k) if *k > 0 => {
                    *k -= 1;
                    Value::Null
                }
                _ => c.block(n),
            },
            "eth_getBlockReceipts" if n >= tip => Value::Null,
            "eth_getBlockReceipts" => match c.no_receipts.get_mut(&n) {
                Some(k) if *k > 0 => {
                    *k -= 1;
                    json!([])
                }
                _ => c.receipts(n),
            },
            m => panic!("unexpected method {m}"),
        };
        out.push(json!({"jsonrpc": "2.0", "id": id, "result": result}));
    }
    Json(Value::Array(out))
}

#[derive(Default)]
struct TestSink {
    reorgs: AtomicUsize,
    commits: AtomicUsize,
}

impl Sink for TestSink {
    fn committed(&self, _: &[FullBlock]) {
        self.commits.fetch_add(1, Ordering::Relaxed);
    }
    fn reorged(&self, _: Option<Head>) {
        self.reorgs.fetch_add(1, Ordering::Relaxed);
    }
}

async fn wait_for(store: &Store, n: u64, hash: &str) {
    for _ in 0..500 {
        if let Some(h) = store.head().unwrap() {
            if h.number == n && hex0x(&h.hash) == hash {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("indexer did not reach block {n} {hash}; head {:?}", store.head().unwrap());
}

/// Store state must equal a fresh index of the canonical chain.
fn check_matches_chain(store: &Store, c: &Chain) {
    let tip = c.forks.len() as u64;
    let mut per_addr: HashMap<String, u64> = HashMap::new();
    for n in 0..tip {
        let b = store.block(n).unwrap().expect("block");
        assert_eq!(hex0x(&b.hash), c.block_hash(n), "block {n}");
        let txs = store.block_txs(n).unwrap();
        assert_eq!(txs.len(), 1);
        let t = &txs[0];
        assert_eq!(hex0x(&t.hash), c.tx(n)["hash"].as_str().unwrap());
        assert_eq!(store.tx_logs(t).unwrap().len(), 1);
        let parties: HashSet<String> =
            [c.tx(n)["from"].as_str().unwrap().to_string(), c.tx(n)["to"].as_str().unwrap().to_string()].into();
        for p in parties {
            *per_addr.entry(p).or_default() += 1;
        }
    }
    assert!(store.block(tip).unwrap().is_none());
    let cnt = store.counters().unwrap();
    assert_eq!(cnt, Counters { blocks: tip, txs: tip, logs: tip, addresses: per_addr.len() as u64 });
    for (a, want) in &per_addr {
        let info = store.addr_info(&parse_fixed::<20>(a).unwrap()).unwrap().expect("addr info");
        assert_eq!(info.tx_count, *want, "tx_count of {a}");
        assert_eq!(store.addr_txs(&parse_fixed::<20>(a).unwrap(), 0, 1000).unwrap().len() as u64, *want);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn follows_chain_through_null_blocks_missing_receipts_and_reorgs() {
    let chain = Arc::new(Mutex::new(Chain::default()));
    {
        let mut c = chain.lock().unwrap();
        c.forks = vec![1; 30];
        c.null_block.insert(5, 2);
        c.no_receipts.insert(7, 1);
    }
    let app = Router::new().route("/", post(node)).with_state(chain.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), StoreConfig { cache_bytes: 8 << 20, reorg_depth: 64 }).unwrap();
    let sink = Arc::new(TestSink::default());
    let idx = Indexer {
        store: store.clone(),
        rpc: Arc::new(Rpc::new(&url, Duration::from_secs(5)).unwrap()),
        cfg: IndexerConfig {
            start_block: 0,
            confirmations: 0,
            fetch_batch: 4,
            concurrency: 3,
            window: 10,
            commit_blocks: 4,
            poll: Duration::from_millis(10),
        },
        sink: sink.clone(),
        status: Arc::new(Status::default()),
    };
    let task = tokio::spawn(idx.run());

    let h29 = chain.lock().unwrap().block_hash(29);
    wait_for(&store, 29, &h29).await;
    check_matches_chain(&store, &chain.lock().unwrap());
    assert_eq!(sink.reorgs.load(Ordering::Relaxed), 0);

    // Reorg: blocks 25.. replaced by fork 2, and the new fork is longer.
    {
        let mut c = chain.lock().unwrap();
        c.forks.truncate(25);
        c.forks.extend(std::iter::repeat(2).take(9));
    }
    let h33 = chain.lock().unwrap().block_hash(33);
    wait_for(&store, 33, &h33).await;
    check_matches_chain(&store, &chain.lock().unwrap());
    assert_eq!(sink.reorgs.load(Ordering::Relaxed), 1);
    // orphaned tx is gone
    let orphan = parse_fixed::<32>(&hash(27, 1, 0x70)).unwrap();
    assert!(store.tx_by_hash(&orphan).unwrap().is_none());

    // Shorter replacement fork at the same height: rollback, then refill.
    {
        let mut c = chain.lock().unwrap();
        c.forks.truncate(32);
        c.forks.extend([3, 3]);
    }
    let h33b = chain.lock().unwrap().block_hash(33);
    wait_for(&store, 33, &h33b).await;
    check_matches_chain(&store, &chain.lock().unwrap());
    assert_eq!(sink.reorgs.load(Ordering::Relaxed), 2);
    task.abort();
}
