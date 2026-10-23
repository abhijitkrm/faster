//! Runs one query against a data dir in-process and reports engine time.
//! Usage: fsq <data dir> <query json> [reps]

use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use std::time::Instant;

use faster_sync::metrics::Metrics;
use faster_sync::query::{self, Limits, Plan, Query};
use faster_sync::store::Store;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let a: Vec<String> = std::env::args().collect();
    let dir = std::path::PathBuf::from(&a[1]);
    let store = Arc::new(Store::open_read_only(&dir)?);
    let q: Query = serde_json::from_str(&a[2])?;
    let reps: usize = a.get(3).map(|s| s.parse()).transpose()?.unwrap_or(20);
    let lim = Limits {
        max_rows: 10_000_000,
        max_time: std::time::Duration::from_secs(60),
        parallelism: std::env::var("PAR").ok().and_then(|v| v.parse().ok()).unwrap_or(8),
        ..Default::default()
    };
    let mut times = Vec::new();
    let m = Metrics::default();
    let mut rows = 0;
    for _ in 0..reps {
        let t = Instant::now();
        let plan = Plan::new(&q, &store, &lim)?;
        let r = query::execute(store.clone(), plan, &m).await?;
        times.push(t.elapsed().as_secs_f64() * 1000.0);
        rows = r.rows;
    }
    times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    println!(
        "rows={rows} min={:.2}ms median={:.2}ms groups_read/q={} skipped/q={}",
        times[0],
        times[times.len() / 2],
        m.row_groups_read.load(Relaxed) / reps as u64,
        m.row_groups_skipped.load(Relaxed) / reps as u64
    );
    Ok(())
}
