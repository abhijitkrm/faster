//! WebSocket: JSON-RPC over the socket plus `eth_subscribe` for `newHeads`
//! and `logs`. Subscriptions follow faster-sync's height; on a reorg, logs
//! already sent for dropped blocks are sent again with `removed: true`.

use std::collections::VecDeque;
use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket};
use futures::{SinkExt, StreamExt};
use serde_json::{json, Map, Value};
use tokio::sync::mpsc;

use crate::methods::Ctx;
use crate::sync::SyncError;

const MAX_SUBS: usize = 64;
/// Blocks of sent history kept per subscription for reorg handling.
const HISTORY: usize = 128;

pub async fn serve(socket: WebSocket, ctx: Arc<Ctx>) {
    let (mut sink, mut stream) = socket.split();
    let (tx, mut rx) = mpsc::channel::<String>(1024);
    let writer = tokio::spawn(async move {
        while let Some(m) = rx.recv().await {
            if sink.send(Message::Text(m.into())).await.is_err() {
                break;
            }
        }
    });
    let mut subs: std::collections::HashMap<String, tokio::task::JoinHandle<()>> = Default::default();
    let mut next_id = 1u64;
    while let Some(Ok(msg)) = stream.next().await {
        let text = match msg {
            Message::Text(t) => t.to_string(),
            Message::Binary(b) => String::from_utf8_lossy(&b).to_string(),
            Message::Close(_) => break,
            _ => continue,
        };
        let req: Value = match serde_json::from_str(&text) {
            Ok(v) => v,
            Err(_) => {
                let _ = tx
                    .send(
                        json!({"jsonrpc": "2.0", "id": null, "error": {"code": -32700, "message": "parse error"}})
                            .to_string(),
                    )
                    .await;
                continue;
            }
        };
        let id = req.get("id").cloned().unwrap_or(Value::Null);
        let method = req["method"].as_str().unwrap_or("");
        let params = req.get("params").cloned().unwrap_or(json!([]));
        let reply = match method {
            "eth_subscribe" => {
                if subs.len() >= MAX_SUBS {
                    Err(json!({"code": -32005, "message": "too many subscriptions"}))
                } else {
                    let sid = format!("0x{:x}", next_id);
                    next_id += 1;
                    match params[0].as_str() {
                        Some("newHeads") => {
                            subs.insert(sid.clone(), tokio::spawn(follow(ctx.clone(), sid.clone(), None, tx.clone())));
                            Ok(json!(sid))
                        }
                        Some("logs") => {
                            let f = params.get(1).cloned().unwrap_or(json!({}));
                            match log_selection(&f) {
                                Ok(sel) => {
                                    subs.insert(
                                        sid.clone(),
                                        tokio::spawn(follow(ctx.clone(), sid.clone(), Some(sel), tx.clone())),
                                    );
                                    Ok(json!(sid))
                                }
                                Err(e) => Err(e),
                            }
                        }
                        _ => Err(json!({"code": -32602, "message": "unsupported subscription (newHeads, logs)"})),
                    }
                }
            }
            "eth_unsubscribe" => {
                let sid = params[0].as_str().unwrap_or("").to_string();
                Ok(json!(subs.remove(&sid).map(|h| h.abort()).is_some()))
            }
            m => ctx.call(m, &params).await,
        };
        let resp = match reply {
            Ok(r) => json!({"jsonrpc": "2.0", "id": id, "result": r}),
            Err(e) => json!({"jsonrpc": "2.0", "id": id, "error": e}),
        };
        if tx.send(resp.to_string()).await.is_err() {
            break;
        }
    }
    for (_, h) in subs {
        h.abort();
    }
    drop(tx);
    let _ = writer.await;
}

/// faster-sync log selection from an eth_subscribe logs filter.
fn log_selection(f: &Value) -> Result<Value, Value> {
    let mut sel = Map::new();
    match f.get("address") {
        None | Some(Value::Null) => {}
        Some(Value::String(a)) => {
            sel.insert("address".into(), json!([a]));
        }
        Some(Value::Array(a)) => {
            sel.insert("address".into(), Value::Array(a.clone()));
        }
        _ => return Err(json!({"code": -32602, "message": "invalid address"})),
    }
    if let Some(Value::Array(ts)) = f.get("topics") {
        let pos: Vec<Value> = ts
            .iter()
            .map(|t| match t {
                Value::String(s) => json!([s]),
                Value::Array(a) => Value::Array(a.iter().filter(|x| !x.is_null()).cloned().collect()),
                _ => json!([]),
            })
            .collect();
        sel.insert("topics".into(), Value::Array(pos));
    }
    Ok(Value::Object(sel))
}

struct Sent {
    number: u64,
    hash: String,
    logs: Vec<Value>,
}

/// One subscription: from the current height on, sends each new block's
/// header (`sel == None`) or matching logs.
async fn follow(ctx: Arc<Ctx>, sid: String, sel: Option<Value>, out: mpsc::Sender<String>) {
    let mut heights = ctx.sync.subscribe();
    let mut next = ctx.sync.heights().height.map(|h| h + 1).unwrap_or(0);
    let mut history: VecDeque<Sent> = VecDeque::new();
    let notify = |v: Value| {
        json!({"jsonrpc": "2.0", "method": "eth_subscription", "params": {"subscription": sid, "result": v}})
            .to_string()
    };
    loop {
        if heights.changed().await.is_err() {
            return;
        }
        let Some(h) = heights.borrow().height else { continue };
        while next <= h {
            let parent = history.back().filter(|s| s.number + 1 == next).map(|s| s.hash.clone());
            let mut q = json!({"from_block": next, "to_block": h + 1, "include_all_blocks": true, "join": "nothing", "max_rows": 20_000});
            if let Some(sel) = &sel {
                q["logs"] = json!([sel]);
            }
            if let Some(p) = parent {
                q["parent_hash"] = json!(p);
            }
            let v = match ctx.sync.query(q).await {
                Ok(v) => v,
                Err(SyncError::Reorg(_)) => {
                    // Find the newest block we sent that is still canonical.
                    let lo = history.front().map(|s| s.number).unwrap_or(next);
                    let canon = ctx
                        .sync
                        .query(
                            json!({"from_block": lo, "to_block": next, "include_all_blocks": true, "join": "nothing"}),
                        )
                        .await
                        .map(|v| v["blocks"].as_array().cloned().unwrap_or_default())
                        .unwrap_or_default();
                    while let Some(s) = history.back() {
                        let same = canon.iter().any(|b| b["hash"].as_str() == Some(s.hash.as_str()));
                        if same {
                            break;
                        }
                        let s = history.pop_back().unwrap();
                        for mut l in s.logs.into_iter().rev() {
                            l["removed"] = json!(true);
                            if out.send(notify(l)).await.is_err() {
                                return;
                            }
                        }
                        next = s.number;
                    }
                    if history.is_empty() {
                        next = next.min(lo);
                    }
                    continue;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "subscription query");
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                    continue;
                }
            };
            let end = v["next_block"].as_u64().unwrap_or(next);
            let blocks = v["blocks"].as_array().cloned().unwrap_or_default();
            let logs = v["logs"].as_array().cloned().unwrap_or_default();
            for b in &blocks {
                let n = b["number"].as_str().and_then(|s| u64::from_str_radix(&s[2..], 16).ok()).unwrap_or(0);
                let hash = b["hash"].as_str().unwrap_or("").to_string();
                let mut sent = Vec::new();
                if sel.is_none() {
                    let mut hd = b.clone();
                    if let Some(o) = hd.as_object_mut() {
                        for k in ["transactions", "uncles", "size", "totalDifficulty", "transactionCount"] {
                            o.remove(k);
                        }
                    }
                    if out.send(notify(hd)).await.is_err() {
                        return;
                    }
                } else {
                    for l in logs.iter().filter(|l| l["blockNumber"] == b["number"]) {
                        let mut l = l.clone();
                        l["blockHash"] = json!(hash);
                        if out.send(notify(l.clone())).await.is_err() {
                            return;
                        }
                        sent.push(l);
                    }
                }
                history.push_back(Sent { number: n, hash, logs: sent });
                if history.len() > HISTORY {
                    history.pop_front();
                }
            }
            if end <= next {
                break;
            }
            next = end;
        }
    }
}
