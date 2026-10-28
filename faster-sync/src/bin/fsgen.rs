//! Builds a store from the deterministic fake chain (benchmarks).
//! Usage: fsgen <data dir> <blocks> [txs per block] [logs per tx]

use std::sync::Arc;
use std::time::{Duration, Instant};

use faster_sync::ingest::{Ingest, IngestConfig};
use faster_sync::metrics::Metrics;
use faster_sync::rpc::{Rpc, RpcConfig};
use faster_sync::store::Store;
use faster_sync::testnode::{FakeChain, Shape};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_env_filter("warn").init();
    let a: Vec<String> = std::env::args().collect();
    let dir = std::path::PathBuf::from(&a[1]);
    let blocks: u64 = a[2].parse()?;
    let txs: u64 = a.get(3).map(|s| s.parse()).transpose()?.unwrap_or(150);
    let logs: u64 = a.get(4).map(|s| s.parse()).transpose()?.unwrap_or(3);
    let shape = Shape { txs_per_block: txs, logs_per_tx: logs, contracts: 200_000, accounts: 2_000_000, block_time: 2 };
    let chain = FakeChain::new(shape, blocks + 100);
    let url = chain.clone().serve(0).await;
    let rpc =
        Arc::new(Rpc::new(RpcConfig { url, timeout: Duration::from_secs(60), batch: 1000, rps: 0, traces: false })?);
    let store = Arc::new(Store::open(&dir, 0x7a69, 0)?);
    let mut cfg = IngestConfig { window: 20, concurrency: 8, end_block: Some(blocks - 1), ..Default::default() };
    cfg.chunk_rows = std::env::var("CHUNK_ROWS").ok().and_then(|v| v.parse().ok()).unwrap_or(cfg.chunk_rows);
    cfg.write.row_group_rows =
        std::env::var("ROW_GROUP_ROWS").ok().and_then(|v| v.parse().ok()).unwrap_or(cfg.write.row_group_rows);
    if let Ok(c) = std::env::var("COMPRESSION") {
        cfg.write.codec = c.parse().map_err(anyhow::Error::msg)?;
    }
    let m = Arc::new(Metrics::default());
    let t = Instant::now();
    Ingest { store: store.clone(), rpc, cfg, stats: m.clone() }.run().await?;
    let (chunks, rows, fb) = store.chunk_stats();
    let s = t.elapsed().as_secs_f64();
    println!(
        "blocks={} rows={} chunks={} filter_bytes={} secs={:.1} rows/s={:.0}",
        blocks,
        rows,
        chunks,
        fb,
        s,
        rows as f64 / s
    );
    Ok(())
}
