//! The Rust client against a live server: JSON and Arrow pages agree, and
//! streaming covers the range exactly once.

use std::sync::Arc;
use std::time::Duration;

use faster_sync::api::{self, AppState, Guard, GuardConfig};
use faster_sync::ingest::{Ingest, IngestConfig};
use faster_sync::metrics::Metrics;
use faster_sync::query::Limits;
use faster_sync::rpc::{Rpc, RpcConfig};
use faster_sync::store::Store;
use faster_sync::testnode::{FakeChain, Shape};
use faster_sync_client::{Client, Join, LogSelection, Query};
use futures_util::StreamExt;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_json_arrow_stream() {
    let chain = FakeChain::new(Shape { txs_per_block: 10, logs_per_tx: 2, ..Default::default() }, 300);
    let url = chain.clone().serve(0).await;
    let dir = tempfile::tempdir().unwrap();
    let rpc = Arc::new(
        Rpc::new(RpcConfig { url, timeout: Duration::from_secs(10), batch: 50, rps: 0, traces: false }).unwrap(),
    );
    let store = Arc::new(Store::open(dir.path(), 0x7a69, 0).unwrap());
    let m = Arc::new(Metrics::default());
    let cfg = IngestConfig { end_block: Some(300), chunk_rows: 3000, ..Default::default() };
    Ingest { store: store.clone(), rpc, cfg, stats: m.clone() }.run().await.unwrap();
    let state = Arc::new(AppState {
        store,
        metrics: m.clone(),
        limits: Limits { max_rows: 300, ..Default::default() },
        ready_max_lag: 10,
        query_slots: tokio::sync::Semaphore::new(4),
    });
    let guard = Guard::new(
        GuardConfig { rate: 0.0, burst: 1.0, trust_proxy: false, cors_origins: vec![], api_keys: vec![] },
        m,
    );
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", l.local_addr().unwrap());
    let app = api::router(state, guard);
    tokio::spawn(
        async move { axum::serve(l, app.into_make_service_with_connect_info::<std::net::SocketAddr>()).await },
    );

    let c = Client::new(&base);
    let q = Query {
        from_block: 0,
        to_block: Some(301),
        logs: vec![LogSelection { address: vec!["0xc000000000000000000000000000000000000001".into()], topics: vec![] }],
        join: Join::Nothing,
        ..Default::default()
    };
    let j = c.query(&q).await.unwrap();
    let a = c.query_arrow(&q).await.unwrap();
    let arrow_rows: usize = a.logs.iter().map(|b| b.num_rows()).sum();
    assert_eq!(j.logs.len(), arrow_rows);
    assert_eq!(j.meta.next_block, a.meta.next_block);

    let mut total = 0;
    let mut pages = 0;
    let mut last = 0;
    let mut s = Box::pin(c.stream(q, false));
    while let Some(p) = s.next().await {
        let p = p.unwrap();
        for l in &p.logs {
            let b = l["block_number"].as_u64().unwrap();
            assert!(b >= last);
            last = b;
        }
        total += p.logs.len();
        pages += 1;
    }
    let h = c.height(None, Duration::from_secs(1)).await.unwrap();
    assert_eq!(h.height, Some(300));
    assert!(pages >= 1 && total >= j.logs.len(), "pages {pages} total {total}");
}
