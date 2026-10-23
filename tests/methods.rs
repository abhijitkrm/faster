//! faster-rpc against a fake faster-sync holding a two-block chain:
//! object assembly (blockHash, receipts with logs), tags, getLogs filters,
//! batches, forwarding, errors and newHeads/logs subscriptions.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::State;
use axum::routing::{get, post};
use axum::{Json, Router};
use faster_rpc::methods::{Config, Ctx};
use faster_rpc::server::{self, App, Guard, GuardConfig, Metrics};
use faster_rpc::sync::Sync;
use faster_rpc::upstream::Upstream;
use futures::{SinkExt, StreamExt};
use serde_json::{json, Value};

const A: &str = "0x00000000000000000000000000000000000000aa";
const T0: &str = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";

fn h(n: u8) -> String {
    format!("0x{}", format!("{n:02x}").repeat(32))
}

/// Chain: blocks 0..=height, block n has one tx (hash h(0x10+n)) with one log.
struct Chain {
    height: u64,
}

fn block(n: u64) -> Value {
    json!({"number": format!("0x{n:x}"), "hash": h(0xb0 + n as u8), "parentHash": if n == 0 { h(0) } else { h(0xb0 + n as u8 - 1) },
           "timestamp": format!("0x{:x}", 1000 + n), "gasUsed": "0x5208", "uncles": []})
}
fn tx(n: u64) -> Value {
    json!({"tx": {"hash": h(0x10 + n as u8), "blockNumber": format!("0x{n:x}"), "transactionIndex": "0x0", "from": A, "to": null, "type": "0x2", "value": "0x1"},
           "receipt": {"status": "0x1", "gasUsed": "0x5208", "cumulativeGasUsed": "0x5208", "contractAddress": null, "logsBloom": "0x00"}})
}
fn log(n: u64) -> Value {
    json!({"address": if n.is_multiple_of(2) { A } else { "0x00000000000000000000000000000000000000bb" }, "topics": [T0], "data": "0x",
           "blockNumber": format!("0x{n:x}"), "transactionHash": h(0x10 + n as u8), "transactionIndex": "0x0", "logIndex": "0x0", "removed": false})
}

async fn fake_query(State(c): State<Arc<Mutex<Chain>>>, Json(q): Json<Value>) -> Json<Value> {
    let height = c.lock().unwrap().height;
    let from = q["from_block"].as_u64().unwrap();
    let to = q["to_block"].as_u64().unwrap_or(height + 1).min(height + 1);
    if let Some(ph) = q["parent_hash"].as_str() {
        if from > 0 && ph != h(0xb0 + from as u8 - 1) {
            return Json(json!({"error": "reorg", "reorg": true, "block": from - 1}));
        }
    }
    let (mut bs, mut ts, mut ls) = (vec![], vec![], vec![]);
    for n in from..to {
        let mut hit_block = q["include_all_blocks"] == true;
        if let Some(sel) = q["blocks"].get(0) {
            if sel["hash"][0] == block(n)["hash"] {
                hit_block = true;
            }
        }
        let tx_sel = q["transactions"].get(0);
        let tx_hit =
            tx_sel.is_some_and(|s| s["hash"].as_array().is_none_or(|a| a.is_empty() || a[0] == tx(n)["tx"]["hash"]));
        let log_hit = q["logs"].get(0).is_some_and(|s| {
            s["address"].as_array().is_none_or(|a| a.iter().any(|x| x.as_str() == log(n)["address"].as_str()))
                && s["topics"].get(0).and_then(|t| t.as_array()).is_none_or(|a| a.is_empty() || a[0] == T0)
        });
        let join = q["join"].as_str().unwrap_or("default");
        if tx_hit || (log_hit && matches!(join, "default" | "all")) {
            ts.push(tx(n));
        }
        if log_hit || (tx_hit && join == "all") {
            ls.push(log(n));
        }
        if hit_block || ((tx_hit || log_hit) && join != "nothing") {
            bs.push(block(n));
        }
    }
    Json(json!({"blocks": bs, "transactions": ts, "logs": ls, "traces": [], "next_block": to, "archive_height": height,
                "chain_head": height, "finalized_block": height.saturating_sub(2), "last_block": null, "rows": 0, "elapsed_ms": 0.1}))
}

async fn fake_height(State(c): State<Arc<Mutex<Chain>>>) -> Json<Value> {
    let height = c.lock().unwrap().height;
    Json(json!({"height": height, "chain_head": height, "finalized_block": height.saturating_sub(2), "chain_id": 7}))
}

async fn serve(r: Router) -> String {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let u = format!("http://{}", l.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(l, r.into_make_service_with_connect_info::<SocketAddr>()).await });
    u
}

async fn setup() -> (String, Arc<Mutex<Chain>>) {
    let chain = Arc::new(Mutex::new(Chain { height: 5 }));
    let fs = serve(
        Router::new().route("/query", post(fake_query)).route("/height", get(fake_height)).with_state(chain.clone()),
    )
    .await;
    // Upstream node: answers eth_getBalance with a constant.
    let up = serve(Router::new().route(
        "/",
        post(|Json(v): Json<Value>| async move {
            Json(json!({"jsonrpc": "2.0", "id": v["id"], "result": if v["method"] == "eth_getBalance" { json!("0x2a") } else { json!(format!("upstream:{}", v["method"].as_str().unwrap())) }}))
        }),
    ))
    .await;
    let sync = Sync::new(&fs, None);
    sync.fetch_heights().await.unwrap();
    tokio::spawn(sync.clone().watch());
    let ctx = Arc::new(Ctx::new(sync, Some(Upstream::new(&up)), Config { max_logs: 3, ..Default::default() }));
    let m = Arc::new(Metrics::default());
    let app = Arc::new(App { ctx, metrics: m.clone(), max_batch: 10, ready_max_lag: 5 });
    let g = Guard::new(
        GuardConfig { rate: 0.0, burst: 1.0, trust_proxy: false, cors_origins: vec![], api_keys: vec![] },
        m,
    );
    (serve(server::router(app, g)).await, chain)
}

async fn rpc(url: &str, method: &str, params: Value) -> Value {
    let body = json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params});
    reqwest::Client::new().post(url).json(&body).send().await.unwrap().json().await.unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn methods() {
    let (u, _c) = setup().await;
    assert_eq!(rpc(&u, "eth_chainId", json!([])).await["result"], "0x7");
    assert_eq!(rpc(&u, "net_version", json!([])).await["result"], "7");
    assert_eq!(rpc(&u, "eth_blockNumber", json!([])).await["result"], "0x5");

    let b = rpc(&u, "eth_getBlockByNumber", json!(["0x3", true])).await["result"].clone();
    assert_eq!(b["hash"], h(0xb3));
    assert_eq!(b["transactions"][0]["blockHash"], h(0xb3));
    assert_eq!(b["transactions"][0]["to"], Value::Null);
    let b = rpc(&u, "eth_getBlockByNumber", json!(["latest", false])).await["result"].clone();
    assert_eq!(b["number"], "0x5");
    assert_eq!(b["transactions"], json!([h(0x15)]));
    let b = rpc(&u, "eth_getBlockByHash", json!([h(0xb2), false])).await["result"].clone();
    assert_eq!(b["number"], "0x2");
    assert_eq!(rpc(&u, "eth_getBlockTransactionCountByNumber", json!(["0x2"])).await["result"], "0x1");

    let t = rpc(&u, "eth_getTransactionByHash", json!([h(0x14)])).await["result"].clone();
    assert_eq!(t["blockHash"], h(0xb4));
    let r = rpc(&u, "eth_getTransactionReceipt", json!([h(0x14)])).await["result"].clone();
    assert_eq!(r["transactionHash"], h(0x14));
    assert_eq!(r["blockHash"], h(0xb4));
    assert_eq!(r["from"], A);
    assert_eq!(r["contractAddress"], Value::Null);
    assert_eq!(r["logs"][0]["blockHash"], h(0xb4));
    let rs = rpc(&u, "eth_getBlockReceipts", json!(["0x4"])).await["result"].clone();
    assert_eq!(rs.as_array().unwrap().len(), 1);
    assert_eq!(rs[0]["logs"].as_array().unwrap().len(), 1);

    // Block newer than the archive and unknown tx: forwarded upstream.
    assert_eq!(rpc(&u, "eth_getBlockByNumber", json!(["0x9", false])).await["result"], "upstream:eth_getBlockByNumber");
    assert_eq!(
        rpc(&u, "eth_getTransactionByHash", json!([h(0x99)])).await["result"],
        "upstream:eth_getTransactionByHash"
    );
    assert_eq!(rpc(&u, "eth_getBalance", json!([A, "latest"])).await["result"], "0x2a");

    // getLogs: address filter, blockHash, range errors, result cap.
    let l =
        rpc(&u, "eth_getLogs", json!([{"fromBlock": "0x0", "toBlock": "0x5", "address": A}])).await["result"].clone();
    assert_eq!(l.as_array().unwrap().len(), 3);
    assert!(l.as_array().unwrap().iter().all(|x| x["address"] == A && x["blockHash"].is_string()));
    // Two topic positions: logs need two topics (ours have one).
    let l = rpc(&u, "eth_getLogs", json!([{"fromBlock": "0x0", "toBlock": "0x1", "topics": [null, null]}])).await
        ["result"]
        .clone();
    assert_eq!(l, json!([]));
    let l = rpc(&u, "eth_getLogs", json!([{"blockHash": h(0xb1)}])).await["result"].clone();
    assert_eq!(l[0]["blockNumber"], "0x1");
    assert_eq!(rpc(&u, "eth_getLogs", json!([{"fromBlock": "0x4", "toBlock": "0x1"}])).await["error"]["code"], -32602);
    assert_eq!(
        rpc(&u, "eth_getLogs", json!([{"fromBlock": "0x0", "toBlock": "latest"}])).await["error"]["code"],
        -32005
    );
    // Past the archive head: forwarded to the node.
    assert_eq!(rpc(&u, "eth_getLogs", json!([{"fromBlock": "0x50"}])).await["result"], "upstream:eth_getLogs");
    assert_eq!(rpc(&u, "eth_getBlockByNumber", json!(["bogus", false])).await["error"]["code"], -32602);

    // Batch with a notification (no id → no response entry).
    let batch = json!([
        {"jsonrpc": "2.0", "id": 1, "method": "eth_blockNumber", "params": []},
        {"jsonrpc": "2.0", "method": "eth_blockNumber", "params": []},
        {"jsonrpc": "2.0", "id": "x", "method": "eth_chainId"}
    ]);
    let v: Value = reqwest::Client::new().post(&u).json(&batch).send().await.unwrap().json().await.unwrap();
    assert_eq!(v.as_array().unwrap().len(), 2);
    assert_eq!(v[1]["id"], "x");
    let v: Value = reqwest::Client::new().post(&u).body("not json").send().await.unwrap().json().await.unwrap();
    assert_eq!(v["error"]["code"], -32700);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscriptions() {
    let (u, chain) = setup().await;
    let (mut ws, _) = tokio_tungstenite::connect_async(u.replace("http", "ws")).await.unwrap();
    use tokio_tungstenite::tungstenite::Message;
    ws.send(Message::text(
        json!({"jsonrpc": "2.0", "id": 1, "method": "eth_subscribe", "params": ["newHeads"]}).to_string(),
    ))
    .await
    .unwrap();
    ws.send(Message::text(
        json!({"jsonrpc": "2.0", "id": 2, "method": "eth_subscribe", "params": ["logs", {"address": A}]}).to_string(),
    ))
    .await
    .unwrap();
    let mut ids = vec![];
    while ids.len() < 2 {
        let m: Value = serde_json::from_str(ws.next().await.unwrap().unwrap().to_text().unwrap()).unwrap();
        ids.push(m["result"].as_str().unwrap().to_string());
    }
    chain.lock().unwrap().height = 8;
    let (mut heads, mut logs) = (vec![], vec![]);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while heads.len() < 3 || logs.is_empty() {
        let m = tokio::time::timeout_at(deadline, ws.next()).await.expect("notifications").unwrap().unwrap();
        let v: Value = serde_json::from_str(m.to_text().unwrap()).unwrap();
        let r = &v["params"]["result"];
        if v["params"]["subscription"] == ids[0] {
            heads.push(r["number"].as_str().unwrap().to_string());
        } else {
            logs.push(r.clone());
        }
    }
    assert_eq!(heads, vec!["0x6", "0x7", "0x8"]);
    assert_eq!(logs[0]["blockNumber"], "0x6");
    assert_eq!(logs[0]["blockHash"], h(0xb6));
}
