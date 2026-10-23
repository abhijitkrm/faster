//! End-to-end: a fake node, ingest (backfill → chunks, tail → hot, reorg,
//! restart) and queries checked against a brute-force scan of the chain.

use std::sync::Arc;
use std::time::{Duration, Instant};

use faster_sync::chunk::{Codec, WriteOpts};
use faster_sync::ingest::{Ingest, IngestConfig};
use faster_sync::metrics::Metrics;
use faster_sync::model::{fixed_col, u64_col, Table};
use faster_sync::query::{self, Join, Limits, LogSel, Plan, Query, TxSel};
use faster_sync::rpc::{Rpc, RpcConfig};
use faster_sync::store::Store;
use faster_sync::testnode::{contract_addr, FakeChain, Shape, TRANSFER};
use serde_json::Value;

fn hx(b: &[u8]) -> String {
    format!("0x{}", hex::encode(b))
}

fn cfg() -> IngestConfig {
    IngestConfig {
        window: 10,
        concurrency: 4,
        finality_depth: 8,
        use_finalized_tag: true,
        chunk_rows: 6_000,
        chunk_max_blocks: 1_000,
        hot_max_rows: 3_000,
        hot_max_age: Duration::from_secs(1 << 40),
        poll: Duration::from_millis(20),
        end_block: None,
        write: WriteOpts { row_group_rows: 1024, codec: Codec::Lz4 },
    }
}

async fn start(chain: &Arc<FakeChain>, url: &str, dir: &std::path::Path) -> (Arc<Store>, tokio::task::JoinHandle<()>) {
    let rpc = Arc::new(
        Rpc::new(RpcConfig { url: url.into(), timeout: Duration::from_secs(10), batch: 50, rps: 0, traces: false })
            .unwrap(),
    );
    let store = Arc::new(Store::open(dir, 0x7a69, 0).unwrap());
    let ing = Ingest { store: store.clone(), rpc, cfg: cfg(), stats: Arc::new(Metrics::default()) };
    let h = tokio::spawn(async move {
        ing.run().await.unwrap();
    });
    let _ = chain;
    (store, h)
}

async fn wait_height(store: &Store, h: u64, hash: [u8; 32]) {
    let t = Instant::now();
    loop {
        if let Some(l) = store.last() {
            if l.number == h && l.hash == hash {
                return;
            }
        }
        assert!(t.elapsed() < Duration::from_secs(60), "timeout waiting for block {h}: at {:?}", store.last());
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// (block, log_index) of every log of the chain matching `f`.
fn brute_logs(chain: &FakeChain, head: u64, f: impl Fn(&Value) -> bool) -> Vec<(u64, u64)> {
    let mut out = Vec::new();
    for n in 0..=head {
        let (_, rs) = chain.block(n);
        for r in rs.as_array().unwrap() {
            for l in r["logs"].as_array().unwrap() {
                if f(l) {
                    out.push((n, u64::from_str_radix(&l["logIndex"].as_str().unwrap()[2..], 16).unwrap()));
                }
            }
        }
    }
    out
}

async fn run(store: &Arc<Store>, q: Query) -> query::Response {
    let lim = Limits { max_rows: 1_000_000, ..Default::default() };
    let plan = Plan::new(&q, store, &lim).unwrap();
    query::execute(store.clone(), plan, &Metrics::default()).await.unwrap()
}

/// Runs `q` page by page and returns (block, index) of all rows of `t`.
async fn all_pages(store: &Arc<Store>, mut q: Query, t: Table, max_rows: usize) -> (Vec<(u64, u64)>, usize) {
    let lim = Limits { max_rows, ..Default::default() };
    let end = q.to_block.unwrap();
    let mut out = Vec::new();
    let mut pages = 0;
    while q.from_block < end {
        let plan = Plan::new(&q, store, &lim).unwrap();
        let r = query::execute(store.clone(), plan, &Metrics::default()).await.unwrap();
        out.extend(keys(&r, t));
        assert!(r.next_block > q.from_block, "no progress at {}", q.from_block);
        q.from_block = r.next_block;
        pages += 1;
    }
    (out, pages)
}

fn keys(r: &query::Response, t: Table) -> Vec<(u64, u64)> {
    let idx = match t {
        Table::Logs => "log_index",
        _ => "transaction_index",
    };
    let mut v = Vec::new();
    for b in &r.tables[t.idx()] {
        let (bn, ix) = (u64_col(b, t.block_col()), u64_col(b, idx));
        for i in 0..b.num_rows() {
            v.push((bn.value(i), ix.value(i)));
        }
    }
    v
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ingest_query_reorg_restart() {
    let shape = Shape { txs_per_block: 20, logs_per_tx: 2, contracts: 2_000, accounts: 5_000, block_time: 2 };
    let chain = FakeChain::new(shape, 600);
    let url = chain.clone().serve(0).await;
    let dir = tempfile::tempdir().unwrap();
    let (store, h) = start(&chain, &url, dir.path()).await;
    wait_height(&store, 600, chain.block_hash(600)).await;
    assert!(store.chunk_count() >= 3, "backfill wrote chunks: {}", store.chunk_count());
    assert!(!store.hot.is_empty(), "tail is in the hot tier");

    // A rare contract across all history.
    let rare =
        (100..2000).find(|i| !brute_logs(&chain, 600, |l| l["address"] == hx(&contract_addr(*i))).is_empty()).unwrap();
    let want = brute_logs(&chain, 600, |l| l["address"] == hx(&contract_addr(rare)));
    let q = Query {
        from_block: 0,
        to_block: Some(601),
        logs: vec![LogSel { address: vec![hx(&contract_addr(rare))], topics: vec![] }],
        ..Default::default()
    };
    let r = run(&store, q.clone()).await;
    assert_eq!(keys(&r, Table::Logs), want);
    // Default join: one tx per distinct (block, tx) and their blocks.
    let txs = keys(&r, Table::Transactions);
    let lg = &r.tables[Table::Logs.idx()];
    let mut want_tx: Vec<(u64, u64)> = lg
        .iter()
        .flat_map(|b| {
            let (bn, ti) = (u64_col(b, "block_number"), u64_col(b, "transaction_index"));
            (0..b.num_rows()).map(move |i| (bn.value(i), ti.value(i))).collect::<Vec<_>>()
        })
        .collect();
    want_tx.dedup();
    assert_eq!(txs, want_tx);
    let blocks: usize = r.tables[0].iter().map(|b| b.num_rows()).sum();
    let mut wb: Vec<u64> = want.iter().map(|x| x.0).collect();
    wb.dedup();
    assert_eq!(blocks, wb.len());
    assert_eq!(r.next_block, 601);
    // Chunk-only range whose last units are pruned: still one page.
    let cend = store.chunk_end().unwrap();
    let mut q2 = q.clone();
    q2.to_block = Some(cend);
    assert_eq!(run(&store, q2).await.next_block, cend);

    // Busy contract + Transfer topic, paged.
    let busy = hx(&contract_addr(0));
    let t0 = hx(&TRANSFER);
    let want = brute_logs(&chain, 600, |l| l["address"] == busy && l["topics"][0] == t0);
    let q = Query {
        from_block: 0,
        to_block: Some(601),
        logs: vec![LogSel { address: vec![busy.clone()], topics: vec![vec![t0.clone()]] }],
        join: Join::Nothing,
        ..Default::default()
    };
    let (got, pages) = all_pages(&store, q, Table::Logs, 50).await;
    assert_eq!(got, want);
    assert!(pages > 3, "paged: {pages}");

    // One event signature across all contracts, topic1 constrained.
    let (_, rs) = chain.block(123);
    let some_from = rs[3]["logs"][0]["topics"][1].as_str().unwrap().to_string();
    let want = brute_logs(&chain, 600, |l| l["topics"][0] == t0 && l["topics"][1] == some_from);
    let q = Query {
        from_block: 0,
        to_block: Some(601),
        logs: vec![LogSel { address: vec![], topics: vec![vec![t0.clone()], vec![some_from.clone()]] }],
        join: Join::Nothing,
        ..Default::default()
    };
    assert_eq!(keys(&run(&store, q).await, Table::Logs), want);

    // Transactions by sighash + status, and by hash.
    let (b, _) = chain.block(450);
    let th = b["transactions"][5]["hash"].as_str().unwrap().to_string();
    let q = Query {
        from_block: 0,
        to_block: Some(601),
        transactions: vec![TxSel { hash: vec![th.clone()], ..Default::default() }],
        ..Default::default()
    };
    let r = run(&store, q).await;
    assert_eq!(keys(&r, Table::Transactions), vec![(450, 5)]);
    let hashes = fixed_col(&r.tables[1][0], "hash");
    assert_eq!(hx(hashes.value(0)), th);

    let mut want = Vec::new();
    for n in 0..=600u64 {
        let (b, rs) = chain.block(n);
        for (i, (t, r)) in b["transactions"].as_array().unwrap().iter().zip(rs.as_array().unwrap()).enumerate() {
            if t["input"].as_str().unwrap().starts_with("0x095ea7b3") && r["status"] == "0x0" {
                want.push((n, i as u64));
            }
        }
    }
    let q = Query {
        from_block: 0,
        to_block: Some(601),
        transactions: vec![TxSel { sighash: vec!["0x095ea7b3".into()], status: Some(0), ..Default::default() }],
        ..Default::default()
    };
    assert_eq!(keys(&run(&store, q).await, Table::Transactions), want);

    // Reorg of the last 3 blocks, then 2 new blocks.
    let old_599 = hx(&chain.block_hash(599));
    chain.reorg(598);
    chain.head.store(602, std::sync::atomic::Ordering::Relaxed);
    wait_height(&store, 602, chain.block_hash(602)).await;
    for n in 590..=602 {
        let r =
            run(&store, Query { from_block: n, to_block: Some(n + 1), include_all_blocks: true, ..Default::default() })
                .await;
        let hs = fixed_col(&r.tables[0][0], "hash");
        assert_eq!(hs.value(0), chain.block_hash(n), "block {n} after reorg");
    }
    // A client that saw the old block 599 is told about the reorg.
    let stale = Query { from_block: 600, parent_hash: Some(old_599), ..Default::default() };
    let e = Plan::new(&stale, &store, &Limits::default()).err().expect("reorg detected");
    assert!(e.downcast_ref::<query::Reorg>().is_some(), "{e}");
    let fresh = Query { from_block: 600, parent_hash: Some(hx(&chain.block_hash(599))), ..Default::default() };
    assert!(Plan::new(&fresh, &store, &Limits::default()).is_ok());
    let want = brute_logs(&chain, 602, |l| l["address"] == busy);
    let q = Query {
        from_block: 0,
        to_block: Some(603),
        logs: vec![LogSel { address: vec![busy.clone()], topics: vec![] }],
        join: Join::Nothing,
        ..Default::default()
    };
    assert_eq!(keys(&run(&store, q.clone()).await, Table::Logs), want);

    // Compaction while ingest keeps running: fewer chunks, same answers.
    let before = store.chunk_count();
    let mut c = cfg();
    c.chunk_rows = 40_000;
    while faster_sync::ingest::compact_once(&store, &c).unwrap() {}
    assert!(store.chunk_count() < before, "compacted {before} -> {}", store.chunk_count());
    assert_eq!(keys(&run(&store, q.clone()).await, Table::Logs), want);
    let rare_q = Query {
        from_block: 0,
        to_block: Some(603),
        logs: vec![LogSel { address: vec![hx(&contract_addr(rare))], topics: vec![] }],
        ..Default::default()
    };
    let want_rare = brute_logs(&chain, 602, |l| l["address"] == hx(&contract_addr(rare)));
    assert_eq!(keys(&run(&store, rare_q).await, Table::Logs), want_rare);

    // Restart on the same data dir.
    h.abort();
    let _ = h.await;
    drop(store);
    chain.head.store(640, std::sync::atomic::Ordering::Relaxed);
    let (store, h) = start(&chain, &url, dir.path()).await;
    wait_height(&store, 640, chain.block_hash(640)).await;
    let want = brute_logs(&chain, 640, |l| l["address"] == busy);
    let mut q = q;
    q.to_block = Some(641);
    assert_eq!(keys(&run(&store, q).await, Table::Logs), want);
    h.abort();
}
