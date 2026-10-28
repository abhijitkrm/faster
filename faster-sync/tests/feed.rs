//! Push ingest (Feed): backfill straight to chunks, hot tail, revert +
//! replacement fork, finalization; answers match the chain.

use std::sync::Arc;

use faster_sync::feed::Feed;
use faster_sync::ingest::IngestConfig;
use faster_sync::metrics::Metrics;
use faster_sync::model::{fixed_col, u64_col, Table};
use faster_sync::query::{self, Join, Limits, LogSel, Plan, Query};
use faster_sync::rpc::RawBlock;
use faster_sync::store::Store;
use faster_sync::testnode::{contract_addr, FakeChain, Shape};

fn raw(chain: &FakeChain, n: u64) -> RawBlock {
    let (b, r) = chain.block(n);
    RawBlock { block: b.as_object().unwrap().clone(), receipts: r.as_array().unwrap().clone(), traces: None }
}

async fn count_busy(store: &Arc<Store>, to: u64) -> (usize, Vec<[u8; 32]>) {
    let q = Query {
        from_block: 0,
        to_block: Some(to),
        logs: vec![LogSel { address: vec![format!("0x{}", hex::encode(contract_addr(0)))], topics: vec![] }],
        join: Join::Blocks,
        ..Default::default()
    };
    let plan = Plan::new(&q, store, &Limits { max_rows: 10_000_000, ..Default::default() }).unwrap();
    let r = query::execute(store.clone(), plan, &Metrics::default()).await.unwrap();
    let logs: usize = r.tables[Table::Logs.idx()].iter().map(|b| b.num_rows()).sum();
    let mut hashes = Vec::new();
    for b in &r.tables[Table::Blocks.idx()] {
        let (n, h) = (u64_col(b, "number"), fixed_col(b, "hash"));
        for i in 0..b.num_rows() {
            let _ = n.value(i);
            hashes.push(h.value(i).try_into().unwrap());
        }
    }
    (logs, hashes)
}

fn brute(chain: &FakeChain, to: u64) -> usize {
    let busy = format!("0x{}", hex::encode(contract_addr(0)));
    (0..to)
        .map(|n| {
            chain
                .block(n)
                .1
                .as_array()
                .unwrap()
                .iter()
                .flat_map(|r| r["logs"].as_array().unwrap().clone())
                .filter(|l| l["address"] == busy)
                .count()
        })
        .sum()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn feed_backfill_tail_revert_finalize() {
    let chain = FakeChain::new(Shape { txs_per_block: 15, logs_per_tx: 2, ..Default::default() }, 400);
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(dir.path(), 0x7a69, 0).unwrap());
    let cfg = IngestConfig { chunk_rows: 4_000, hot_max_rows: 1_000, ..Default::default() };
    let mut feed = Feed::new(store.clone(), cfg, Arc::new(Metrics::default()));

    // Backfill 0..300 (final) in batches of 25, then a hot tail to 400.
    for a in (0..300).step_by(25) {
        feed.push((a..a + 25).map(|n| raw(&chain, n)).collect(), 299).await.unwrap();
    }
    feed.flush().await.unwrap();
    assert!(store.chunk_count() >= 2);
    for n in 300..400 {
        feed.push(vec![raw(&chain, n)], 299).await.unwrap();
    }
    assert_eq!(store.hot.len(), 100);
    assert_eq!(count_busy(&store, 400).await.0, brute(&chain, 400));

    // Reorg: blocks 395.. replaced.
    chain.reorg(395);
    feed.revert(394).await.unwrap();
    for n in 395..405 {
        feed.push(vec![raw(&chain, n)], 299).await.unwrap();
    }
    let (logs, hashes) = count_busy(&store, 405).await;
    assert_eq!(logs, brute(&chain, 405));
    assert!(hashes.iter().any(|h| *h == chain.block_hash(400)), "new fork's block 400 is there");

    // Finality moves: final hot blocks become chunks, answers unchanged.
    let before = store.chunk_count();
    feed.finalize(390, 404).await.unwrap();
    assert!(store.chunk_count() > before);
    assert!(store.hot.first().unwrap() > 300);
    assert_eq!(count_busy(&store, 405).await.0, brute(&chain, 405));

    // A block that does not extend the tip is refused.
    assert!(feed.push(vec![raw(&chain, 410)], 390).await.is_err());
    // Reverting into chunked history is refused.
    assert!(feed.revert(100).await.is_err());
}
