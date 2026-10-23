//! Serves the deterministic fake chain over JSON-RPC (demos, tests of
//! faster-rpc / faster-index, CI smoke tests).
//! Usage: fakechain <port> <head> [block_ms] [txs per block]
//! With block_ms > 0 a new block is added every block_ms milliseconds.

use std::sync::atomic::Ordering;
use std::time::Duration;

use faster_sync::testnode::{FakeChain, Shape};

#[tokio::main]
async fn main() {
    let a: Vec<String> = std::env::args().collect();
    let arg = |i: usize, d: u64| a.get(i).and_then(|s| s.parse().ok()).unwrap_or(d);
    let (port, head, block_ms, txs) = (arg(1, 8545) as u16, arg(2, 1000), arg(3, 0), arg(4, 50));
    let chain = FakeChain::new(Shape { txs_per_block: txs, ..Default::default() }, head);
    let url = chain.clone().serve(port).await;
    println!("fakechain on {url} head={head} block_ms={block_ms}");
    if block_ms > 0 {
        loop {
            tokio::time::sleep(Duration::from_millis(block_ms)).await;
            chain.head.fetch_add(1, Ordering::Relaxed);
        }
    }
    std::future::pending::<()>().await;
}
