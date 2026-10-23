//! HTTP API: JSON rows, JSON-RPC shape, Arrow frames, errors, API keys and
//! rate limiting, long-poll on /height.

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use faster_sync::api::{self, AppState, Guard, GuardConfig};
use faster_sync::ingest::{Ingest, IngestConfig};
use faster_sync::metrics::Metrics;
use faster_sync::query::Limits;
use faster_sync::rpc::{Rpc, RpcConfig};
use faster_sync::store::Store;
use faster_sync::testnode::{FakeChain, Shape};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use tower::ServiceExt;

async fn setup(guard: GuardConfig) -> (axum::Router, Arc<FakeChain>, tempfile::TempDir) {
    let chain = FakeChain::new(Shape { txs_per_block: 10, logs_per_tx: 2, ..Default::default() }, 200);
    let url = chain.clone().serve(0).await;
    let dir = tempfile::tempdir().unwrap();
    let rpc = Arc::new(
        Rpc::new(RpcConfig { url, timeout: Duration::from_secs(10), batch: 50, rps: 0, traces: false }).unwrap(),
    );
    let store = Arc::new(Store::open(dir.path(), 0x7a69, 0).unwrap());
    let cfg = IngestConfig { end_block: Some(200), chunk_rows: 2000, ..Default::default() };
    let m = Arc::new(Metrics::default());
    Ingest { store: store.clone(), rpc, cfg, stats: m.clone() }.run().await.unwrap();
    let state = Arc::new(AppState {
        store,
        metrics: m.clone(),
        limits: Limits { max_rows: 500, ..Default::default() },
        ready_max_lag: 10,
        query_slots: tokio::sync::Semaphore::new(4),
    });
    (api::router(state, Guard::new(guard, m)), chain, dir)
}

fn open() -> GuardConfig {
    GuardConfig { rate: 0.0, burst: 10.0, trust_proxy: false, cors_origins: vec![], api_keys: vec![] }
}

async fn post(app: &axum::Router, path: &str, body: Value, key: Option<&str>) -> (StatusCode, Vec<u8>) {
    let mut req = Request::post(path).header("content-type", "application/json");
    if let Some(k) = key {
        req = req.header("authorization", format!("Bearer {k}"));
    }
    let r = app.clone().oneshot(req.body(Body::from(body.to_string())).unwrap()).await.unwrap();
    let status = r.status();
    (status, r.into_body().collect().await.unwrap().to_bytes().to_vec())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn json_rpc_shape_arrow_and_errors() {
    let (app, chain, _d) = setup(open()).await;
    let busy = "0xc000000000000000000000000000000000000000";

    // JSON rows: selected fields only, paged by max_rows.
    let q = json!({"from_block": 0, "to_block": 201, "logs": [{"address": [busy]}], "join": "nothing",
                   "fields": {"log": ["block_number", "log_index", "address"]}, "max_rows": 20});
    let (s, b) = post(&app, "/query", q, None).await;
    assert_eq!(s, StatusCode::OK);
    let v: Value = serde_json::from_slice(&b).unwrap();
    let logs = v["logs"].as_array().unwrap();
    assert!(!logs.is_empty() && logs.len() <= 40, "{}", logs.len());
    assert_eq!(logs[0].as_object().unwrap().len(), 3);
    assert_eq!(logs[0]["address"], busy);
    assert!(v["next_block"].as_u64().unwrap() < 201, "paged");

    // JSON-RPC shape: the node's own objects.
    let (s, b) = post(
        &app,
        "/query?shape=rpc",
        json!({"from_block": 77, "to_block": 78, "transactions": [{}], "include_all_blocks": true}),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    let v: Value = serde_json::from_slice(&b).unwrap();
    let (block, receipts) = chain.block(77);
    let mut nb = block.clone();
    let txs = nb.as_object_mut().unwrap().remove("transactions").unwrap();
    assert_eq!(v["blocks"][0], nb);
    let mut t0 = txs[0].clone();
    t0.as_object_mut().unwrap().remove("blockHash");
    assert_eq!(v["transactions"][0]["tx"], t0);
    assert_eq!(v["transactions"][0]["receipt"]["cumulativeGasUsed"], receipts[0]["cumulativeGasUsed"]);
    assert_eq!(v["transactions"].as_array().unwrap().len(), 10);

    // Arrow frames end with the metadata frame.
    let r = app
        .clone()
        .oneshot(
            Request::post("/query?format=arrow")
                .header("content-type", "application/json")
                .body(Body::from(json!({"from_block": 0, "logs": [{"address": [busy]}]}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let b = r.into_body().collect().await.unwrap().to_bytes();
    let (mut off, mut tags) = (0, vec![]);
    while off < b.len() {
        let len = u32::from_le_bytes(b[off + 1..off + 5].try_into().unwrap()) as usize;
        tags.push(b[off]);
        if b[off] == 2 {
            let rd = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(&b[off + 5..off + 5 + len]), None)
                .unwrap();
            let n: usize = rd.map(|x| x.unwrap().num_rows()).sum();
            assert!(n > 0);
        }
        off += 5 + len;
    }
    assert_eq!(*tags.last().unwrap(), 255);
    assert!(tags.contains(&2) && tags.contains(&1) && tags.contains(&0), "logs, joined txs and blocks: {tags:?}");

    // Errors.
    let (s, b) = post(&app, "/query", json!({"from_block": 0, "logs": [{"address": ["0x12"]}]}), None).await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "{}", String::from_utf8_lossy(&b));
    let (s, _) = post(&app, "/query", json!({"from_block": 0, "fields": {"log": ["nope"]}}), None).await;
    assert_eq!(s, StatusCode::BAD_REQUEST);
    let (s, _) = post(&app, "/query", json!({"from_block": 0, "bogus": 1}), None).await;
    assert_eq!(s, StatusCode::BAD_REQUEST);

    // Long-poll returns at once when already above.
    let r = app
        .clone()
        .oneshot(Request::get("/height?above=10&timeout_ms=5000").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(&r.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(v["height"], 200);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn api_keys_and_rate_limit() {
    let g =
        GuardConfig { rate: 1.0, burst: 2.0, trust_proxy: false, cors_origins: vec![], api_keys: vec!["k1".into()] };
    let (app, _c, _d) = setup(g).await;
    let q = json!({"from_block": 0, "to_block": 1, "include_all_blocks": true});
    assert_eq!(post(&app, "/query", q.clone(), None).await.0, StatusCode::UNAUTHORIZED);
    assert_eq!(post(&app, "/query", q.clone(), Some("wrong")).await.0, StatusCode::UNAUTHORIZED);
    assert_eq!(post(&app, "/query", q.clone(), Some("k1")).await.0, StatusCode::OK);
    assert_eq!(post(&app, "/query", q.clone(), Some("k1")).await.0, StatusCode::OK);
    assert_eq!(post(&app, "/query", q.clone(), Some("k1")).await.0, StatusCode::TOO_MANY_REQUESTS);
    let r = app.clone().oneshot(Request::get("/health").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(r.status(), StatusCode::OK, "probes are exempt");
}
