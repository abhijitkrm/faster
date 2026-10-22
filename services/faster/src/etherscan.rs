//! Etherscan-compatible API subset on `GET /api?module=...&action=...`, for
//! wallets, scripts and tools that speak the Etherscan dialect. Responses
//! use Etherscan's envelope and field names; every value is a string.

use std::collections::HashMap;

use axum::extract::{Query, State};
use axum::Json;
use serde_json::{json, Value};

use crate::api::AppState;
use crate::tokens::{Transfer, KIND_ERC1155, KIND_ERC20, KIND_ERC721};
use crate::types::*;

/// Etherscan caps page * offset at 10,000.
const MAX_WINDOW: usize = 10_000;

fn ok(result: Value) -> Json<Value> {
    let empty = result.as_array().is_some_and(|a| a.is_empty());
    let msg = if empty { "No transactions found" } else { "OK" };
    Json(json!({"status": if empty { "0" } else { "1" }, "message": msg, "result": result}))
}

fn notok(msg: &str) -> Json<Value> {
    Json(json!({"status": "0", "message": "NOTOK", "result": msg}))
}

fn qs<'a>(q: &'a HashMap<String, String>, k: &str) -> Option<&'a str> {
    q.get(k).map(|s| s.as_str()).filter(|s| !s.is_empty())
}

fn num(q: &HashMap<String, String>, k: &str, def: u64) -> Result<u64, String> {
    match qs(q, k) {
        None => Ok(def),
        Some(v) => v.parse().map_err(|_| format!("Error! Invalid {k}")),
    }
}

/// (start, end, asc, skip, limit) from startblock/endblock/sort/page/offset.
fn window(q: &HashMap<String, String>) -> Result<(u64, u64, bool, usize, usize), String> {
    let start = num(q, "startblock", 0)?;
    let end = num(q, "endblock", u64::MAX)?;
    let asc = qs(q, "sort").unwrap_or("asc") != "desc";
    let page = num(q, "page", 1)?.max(1) as usize;
    let offset = num(q, "offset", MAX_WINDOW as u64)?.clamp(1, MAX_WINDOW as u64) as usize;
    if page.saturating_mul(offset) > MAX_WINDOW {
        return Err("Result window is too large, PageNo x Offset size must be less than or equal to 10000".into());
    }
    Ok((start, end, asc, (page - 1) * offset, offset))
}

fn addr_param(q: &HashMap<String, String>, k: &str) -> Result<Addr, String> {
    qs(q, k).and_then(parse_fixed::<20>).ok_or_else(|| format!("Error! Invalid {k} format"))
}

pub async fn handler(State(s): State<AppState>, Query(q): Query<HashMap<String, String>>) -> Json<Value> {
    let module = qs(&q, "module").unwrap_or("");
    let action = qs(&q, "action").unwrap_or("");
    let r = match (module, action) {
        ("account", "balance") => balance(&s, &q).await,
        ("account", "balancemulti") => balancemulti(&s, &q).await,
        ("account", "txlist") => txlist(&s, &q),
        ("account", "tokentx") => tokentx(&s, &q, KIND_ERC20),
        ("account", "tokennfttx") => tokentx(&s, &q, KIND_ERC721),
        ("account", "token1155tx") => tokentx(&s, &q, KIND_ERC1155),
        ("account", "txlistinternal") => txlistinternal(&s, &q),
        ("block", "getblocknumberbytime") => blockbytime(&s, &q),
        ("transaction", "gettxreceiptstatus") => receipt_status(&s, &q),
        ("proxy", "eth_blockNumber") => {
            let h = s.store.head().ok().flatten().map_or(0, |h| h.number);
            return Json(json!({"jsonrpc": "2.0", "id": 1, "result": format!("0x{h:x}")}));
        }
        _ => Err("Error! Missing Or invalid Module name / Action name".into()),
    };
    match r {
        Ok(v) => v,
        Err(e) => notok(&e),
    }
}

async fn balance(s: &AppState, q: &HashMap<String, String>) -> Result<Json<Value>, String> {
    let a = addr_param(q, "address")?;
    let b = s.rpc.balance(&hex0x(&a)).await.map_err(|_| "Error! Node unavailable".to_string())?;
    Ok(Json(json!({"status": "1", "message": "OK", "result": dec(&b)})))
}

async fn balancemulti(s: &AppState, q: &HashMap<String, String>) -> Result<Json<Value>, String> {
    let list = qs(q, "address").ok_or("Error! Invalid address format")?;
    let addrs: Vec<&str> = list.split(',').map(|x| x.trim()).collect();
    if addrs.len() > 20 {
        return Err("Error! Maximum of 20 addresses".into());
    }
    let mut out = Vec::new();
    for a in addrs {
        let a = parse_fixed::<20>(a).ok_or("Error! Invalid address format")?;
        let b = s.rpc.balance(&hex0x(&a)).await.map_err(|_| "Error! Node unavailable".to_string())?;
        out.push(json!({"account": hex0x(&a), "balance": dec(&b)}));
    }
    Ok(Json(json!({"status": "1", "message": "OK", "result": out})))
}

fn tx_json(t: &TxRec, head: u64, block_hash: &str) -> Value {
    let to = t.to.map(|a| hex0x(&a)).unwrap_or_default();
    let method = if t.input.len() >= 4 { hex0x(&t.input[..4]) } else { "0x".into() };
    json!({
        "blockNumber": t.block_number.to_string(),
        "timeStamp": t.block_timestamp.to_string(),
        "hash": hex0x(&t.hash),
        "nonce": t.nonce.to_string(),
        "blockHash": block_hash,
        "transactionIndex": t.index.to_string(),
        "from": hex0x(&t.from),
        "to": to,
        "value": dec(&t.value),
        "gas": t.gas.to_string(),
        "gasPrice": t.effective_gas_price.as_ref().map(|b| dec(b)).unwrap_or_else(|| dec(&t.gas_price)),
        "isError": if t.status == Some(0) { "1" } else { "0" },
        "txreceipt_status": t.status.map(|x| x.to_string()).unwrap_or_default(),
        "input": hex0x(&t.input),
        "contractAddress": t.contract_address.map(|a| hex0x(&a)).unwrap_or_default(),
        "gasUsed": t.gas_used.to_string(),
        "confirmations": (head.saturating_sub(t.block_number) + 1).to_string(),
        "methodId": method,
    })
}

fn txlist(s: &AppState, q: &HashMap<String, String>) -> Result<Json<Value>, String> {
    let a = addr_param(q, "address")?;
    let (start, end, asc, skip, limit) = window(q)?;
    let txs = s.store.addr_txs_range(&a, start, end, asc, skip, limit).map_err(internal)?;
    let head = s.store.head().ok().flatten().map_or(0, |h| h.number);
    let mut hashes: HashMap<u64, String> = HashMap::new();
    let mut out = Vec::with_capacity(txs.len());
    for t in &txs {
        if !hashes.contains_key(&t.block_number) {
            let h = s.store.block(t.block_number).map_err(internal)?.map(|b| hex0x(&b.hash)).unwrap_or_default();
            hashes.insert(t.block_number, h);
        }
        out.push(tx_json(t, head, &hashes[&t.block_number]));
    }
    Ok(ok(Value::Array(out)))
}

fn tokentx(s: &AppState, q: &HashMap<String, String>, kind: u8) -> Result<Json<Value>, String> {
    let contract = match qs(q, "contractaddress") {
        Some(c) => Some(parse_fixed::<20>(c).ok_or("Error! Invalid contractaddress format")?),
        None => None,
    };
    let address = match qs(q, "address") {
        Some(c) => Some(parse_fixed::<20>(c).ok_or("Error! Invalid address format")?),
        None => None,
    };
    let (start, end, asc, skip, limit) = window(q)?;
    let keep = |t: &Transfer| {
        t.kind == kind && contract.is_none_or(|c| t.token == c) && address.is_none_or(|a| t.from == a || t.to == a)
    };
    let rows = match (address, contract) {
        (Some(a), _) => s.store.transfers_range(&a, false, start, end, asc, skip, limit, &keep),
        (None, Some(c)) => s.store.transfers_range(&c, true, start, end, asc, skip, limit, &keep),
        (None, None) => return Err("Error! Missing address or contractaddress".into()),
    }
    .map_err(internal)?;
    let head = s.store.head().ok().flatten().map_or(0, |h| h.number);
    let mut out = Vec::with_capacity(rows.len());
    for (n, t) in &rows {
        let tx = s.store.tx(*n, t.tx_index).map_err(internal)?;
        let b = s.store.block(*n).map_err(internal)?;
        let meta = s.store.token(&t.token).map_err(internal)?.map(|(_, m)| m).unwrap_or_default();
        let mut v = json!({
            "blockNumber": n.to_string(),
            "timeStamp": b.as_ref().map_or(0, |b| b.timestamp).to_string(),
            "hash": tx.as_ref().map(|t| hex0x(&t.hash)).unwrap_or_default(),
            "nonce": tx.as_ref().map(|t| t.nonce.to_string()).unwrap_or_default(),
            "blockHash": b.as_ref().map(|b| hex0x(&b.hash)).unwrap_or_default(),
            "from": hex0x(&t.from),
            "contractAddress": hex0x(&t.token),
            "to": hex0x(&t.to),
            "tokenName": meta.name.unwrap_or_default(),
            "tokenSymbol": meta.symbol.unwrap_or_default(),
            "transactionIndex": t.tx_index.to_string(),
            "logIndex": t.log_index.to_string(),
            "gas": tx.as_ref().map(|t| t.gas.to_string()).unwrap_or_default(),
            "gasUsed": tx.as_ref().map(|t| t.gas_used.to_string()).unwrap_or_default(),
            "input": "deprecated",
            "confirmations": (head.saturating_sub(*n) + 1).to_string(),
        });
        match kind {
            KIND_ERC20 => {
                v["value"] = json!(dec(&t.value));
                v["tokenDecimal"] = json!(meta.decimals.map(|d| d.to_string()).unwrap_or_default());
            }
            KIND_ERC721 => {
                v["tokenID"] = json!(t.token_id.as_ref().map(|i| dec(i)).unwrap_or_default());
                v["tokenDecimal"] = json!("0");
            }
            _ => {
                v["tokenID"] = json!(t.token_id.as_ref().map(|i| dec(i)).unwrap_or_default());
                v["tokenValue"] = json!(dec(&t.value));
            }
        }
        out.push(v);
    }
    Ok(ok(Value::Array(out)))
}

fn txlistinternal(s: &AppState, q: &HashMap<String, String>) -> Result<Json<Value>, String> {
    let item = |n: u64, ts: u64, h: &H256, c: &InternalTx| {
        json!({
            "blockNumber": n.to_string(),
            "timeStamp": ts.to_string(),
            "hash": hex0x(h),
            "from": hex0x(&c.from),
            "to": c.to.filter(|_| !c.call_type.starts_with("CREATE")).map(|a| hex0x(&a)).unwrap_or_default(),
            "value": dec(&c.value),
            "contractAddress": c.to.filter(|_| c.call_type.starts_with("CREATE")).map(|a| hex0x(&a)).unwrap_or_default(),
            "input": "",
            "type": c.call_type.to_lowercase(),
            "gas": c.gas.to_string(),
            "gasUsed": c.gas_used.to_string(),
            "traceId": format!("{}", c.trace_index),
            "isError": if c.error.is_some() { "1" } else { "0" },
            "errCode": c.error.clone().unwrap_or_default(),
        })
    };
    if let Some(h) = qs(q, "txhash") {
        let h = parse_fixed::<32>(h).ok_or("Error! Invalid txhash format")?;
        let Some(t) = s.store.tx_by_hash(&h).map_err(internal)? else { return Ok(ok(json!([]))) };
        let out: Vec<Value> = s
            .store
            .tx_internals(&t)
            .map_err(internal)?
            .iter()
            .map(|c| item(t.block_number, t.block_timestamp, &h, c))
            .collect();
        return Ok(ok(Value::Array(out)));
    }
    let a = addr_param(q, "address")?;
    let (start, end, _asc, skip, limit) = window(q)?;
    // Newest first; startblock/endblock filter the walk.
    let rows = s
        .store
        .addr_internals(&a, Some((end.min(u64::MAX - 1), u32::MAX, u32::MAX)), skip + limit)
        .map_err(internal)?;
    let mut out = Vec::new();
    for (n, h, c) in rows.iter().filter(|(n, _, _)| *n >= start).skip(skip).take(limit) {
        let ts = s.store.block(*n).map_err(internal)?.map_or(0, |b| b.timestamp);
        out.push(item(*n, ts, h, c));
    }
    Ok(ok(Value::Array(out)))
}

fn blockbytime(s: &AppState, q: &HashMap<String, String>) -> Result<Json<Value>, String> {
    let ts = qs(q, "timestamp").and_then(|v| v.parse().ok()).ok_or("Error! Invalid timestamp")?;
    let after = match qs(q, "closest").unwrap_or("before") {
        "before" => false,
        "after" => true,
        _ => return Err("Error! Invalid closest value".into()),
    };
    match s.store.block_by_time(ts, after).map_err(internal)? {
        Some(n) => Ok(Json(json!({"status": "1", "message": "OK", "result": n.to_string()}))),
        None => Err("Error! No closest block found".into()),
    }
}

fn receipt_status(s: &AppState, q: &HashMap<String, String>) -> Result<Json<Value>, String> {
    let h = qs(q, "txhash").and_then(parse_fixed::<32>).ok_or("Error! Invalid txhash format")?;
    let t = s.store.tx_by_hash(&h).map_err(internal)?;
    let status = t.and_then(|t| t.status).map(|x| x.to_string()).unwrap_or_default();
    Ok(Json(json!({"status": "1", "message": "OK", "result": {"status": status}})))
}

fn internal(e: anyhow::Error) -> String {
    tracing::error!(err = %e, "etherscan api");
    "Error! Internal error".into()
}
