//! Batched JSON-RPC client and decoding of blocks + receipts into store records.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use serde::Deserialize;
use serde_json::value::RawValue;
use serde_json::{json, Value};

use crate::types::*;

#[derive(Debug, thiserror::Error)]
pub enum RpcError {
    #[error("transport: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("decode: {0}")]
    Decode(String),
    #[error("rpc error {code}: {message}")]
    Rpc { code: i64, message: String },
    /// The node has not got this block (or its receipts) yet; retry later.
    #[error("not ready: {0}")]
    NotReady(String),
}

const METHOD_NOT_FOUND: i64 = -32601;

pub struct Rpc {
    url: String,
    http: reqwest::Client,
    /// Cleared after the node answers eth_getBlockReceipts with
    /// "method not found"; we then fetch receipts one tx at a time (batched).
    block_receipts: AtomicBool,
    /// Fetch internal calls with debug_traceBlockByNumber (TRACE_CALLS).
    trace: AtomicBool,
}

#[derive(Deserialize)]
struct Resp<'a> {
    id: u64,
    #[serde(borrow)]
    result: Option<&'a RawValue>,
    error: Option<ErrObj>,
}

#[derive(Deserialize)]
struct ErrObj {
    code: i64,
    message: String,
}

impl Rpc {
    pub fn new(url: &str, timeout: Duration) -> anyhow::Result<Self> {
        let http = reqwest::Client::builder().timeout(timeout).pool_max_idle_per_host(64).tcp_nodelay(true).build()?;
        Ok(Rpc { url: url.to_string(), http, block_receipts: AtomicBool::new(true), trace: AtomicBool::new(false) })
    }

    pub fn set_trace(&self, on: bool) {
        self.trace.store(on, Ordering::Relaxed);
    }

    /// Sends one JSON-RPC batch and returns each call's raw result in request
    /// order. A per-call error fails the whole batch.
    async fn batch(&self, calls: &[(&str, Value)]) -> Result<Vec<Option<Box<RawValue>>>, RpcError> {
        self.batch_with(calls, false).await
    }

    /// `lenient`: a per-call error yields None for that call instead of
    /// failing the batch (e.g. eth_call reverts).
    async fn batch_with(&self, calls: &[(&str, Value)], lenient: bool) -> Result<Vec<Option<Box<RawValue>>>, RpcError> {
        let reqs: Vec<Value> = calls
            .iter()
            .enumerate()
            .map(|(i, (m, p))| json!({"jsonrpc": "2.0", "id": i, "method": m, "params": p}))
            .collect();
        let t = std::time::Instant::now();
        let body = match self.http.post(&self.url).json(&reqs).send().await {
            Ok(r) => r.bytes().await,
            Err(e) => Err(e),
        };
        let m = crate::metrics::get();
        m.rpc_latency.observe(t.elapsed());
        let body = match body {
            Ok(b) => {
                m.rpc_ok.fetch_add(1, Ordering::Relaxed);
                b
            }
            Err(e) => {
                m.rpc_err.fetch_add(1, Ordering::Relaxed);
                return Err(e.into());
            }
        };
        let resps: Vec<Resp> = match serde_json::from_slice(&body) {
            Ok(r) => r,
            Err(_) => {
                // Some nodes answer a whole batch with one error object.
                if let Ok(one) = serde_json::from_slice::<Resp>(&body) {
                    if let Some(e) = one.error {
                        return Err(RpcError::Rpc { code: e.code, message: e.message });
                    }
                }
                return Err(RpcError::Decode(format!(
                    "batch response: {}",
                    String::from_utf8_lossy(&body[..body.len().min(200)])
                )));
            }
        };
        let mut out: Vec<Option<Box<RawValue>>> = (0..calls.len()).map(|_| None).collect();
        let mut seen = 0;
        for r in resps {
            if let Some(e) = r.error {
                if !lenient {
                    return Err(RpcError::Rpc { code: e.code, message: e.message });
                }
                if (r.id as usize) < out.len() {
                    seen += 1;
                }
                continue;
            }
            let i = r.id as usize;
            if i < out.len() {
                out[i] = r.result.map(|v| v.to_owned());
                seen += 1;
            }
        }
        if seen != calls.len() {
            return Err(RpcError::Decode(format!("batch: {} of {} responses", seen, calls.len())));
        }
        Ok(out)
    }

    pub async fn block_number(&self) -> Result<u64, RpcError> {
        let r = self.batch(&[("eth_blockNumber", json!([]))]).await?;
        let s: String = decode(r[0].as_deref(), "eth_blockNumber")?;
        parse_u64(&s).ok_or_else(|| RpcError::Decode("eth_blockNumber".into()))
    }

    /// Hash of the node's canonical block `n`, without transactions.
    pub async fn block_hash(&self, n: u64) -> Result<Option<H256>, RpcError> {
        #[derive(Deserialize)]
        struct H {
            hash: String,
        }
        let r = self.batch(&[("eth_getBlockByNumber", json!([format!("0x{n:x}"), false]))]).await?;
        match r[0].as_deref() {
            Some(v) if v.get() != "null" => {
                let h: H = serde_json::from_str(v.get()).map_err(|e| RpcError::Decode(format!("block {n}: {e}")))?;
                Ok(Some(parse_fixed::<32>(&h.hash).ok_or_else(|| bad("hash", n))?))
            }
            _ => Ok(None),
        }
    }

    /// eth_call of each calldata against `to` at latest; None per failed call.
    pub async fn calls(&self, to: &str, datas: &[&str]) -> Result<Vec<Option<Vec<u8>>>, RpcError> {
        let calls: Vec<_> = datas.iter().map(|d| ("eth_call", json!([{"to": to, "data": d}, "latest"]))).collect();
        let r = self.batch_with(&calls, true).await?;
        Ok(r.into_iter()
            .map(|v| v.and_then(|v| serde_json::from_str::<String>(v.get()).ok()).and_then(|s| parse_hex_bytes(&s)))
            .collect())
    }

    pub async fn balance(&self, addr: &str) -> Result<Vec<u8>, RpcError> {
        let r = self.batch(&[("eth_getBalance", json!([addr, "latest"]))]).await?;
        let s: String = decode(r[0].as_deref(), "eth_getBalance")?;
        parse_quantity(&s).ok_or_else(|| RpcError::Decode("eth_getBalance".into()))
    }

    pub async fn code(&self, addr: &str) -> Result<Vec<u8>, RpcError> {
        let r = self.batch(&[("eth_getCode", json!([addr, "latest"]))]).await?;
        let s: String = decode(r[0].as_deref(), "eth_getCode")?;
        parse_hex_bytes(&s).ok_or_else(|| RpcError::Decode("eth_getCode".into()))
    }

    /// Fetches complete blocks (header, txs, receipts, logs). Fails with
    /// NotReady rather than returning a partial block.
    pub async fn blocks(&self, numbers: &[u64]) -> Result<Vec<FullBlock>, RpcError> {
        let mut out = self.blocks_receipts(numbers).await?;
        if self.trace.load(Ordering::Relaxed) {
            let calls: Vec<_> = numbers
                .iter()
                .map(|n| ("debug_traceBlockByNumber", json!([format!("0x{n:x}"), {"tracer": "callTracer"}])))
                .collect();
            match self.batch(&calls).await {
                Ok(res) => {
                    for (fb, r) in out.iter_mut().zip(res) {
                        fb.internals = decode_traces(r.as_deref(), fb)?;
                    }
                }
                Err(RpcError::Rpc { code, .. }) if code == METHOD_NOT_FOUND => {
                    tracing::error!("TRACE_CALLS is on but the node has no debug_traceBlockByNumber; tracing disabled");
                    self.trace.store(false, Ordering::Relaxed);
                }
                Err(e) => return Err(e),
            }
        }
        Ok(out)
    }

    async fn blocks_receipts(&self, numbers: &[u64]) -> Result<Vec<FullBlock>, RpcError> {
        if self.block_receipts.load(Ordering::Relaxed) {
            let mut calls = Vec::with_capacity(numbers.len() * 2);
            for n in numbers {
                let h = format!("0x{n:x}");
                calls.push(("eth_getBlockByNumber", json!([h, true])));
                calls.push(("eth_getBlockReceipts", json!([h])));
            }
            match self.batch(&calls).await {
                Ok(res) => {
                    let mut out = Vec::with_capacity(numbers.len());
                    for (i, n) in numbers.iter().enumerate() {
                        let b = decode_block(res[2 * i].as_deref(), *n)?;
                        let rs = decode_receipts(res[2 * i + 1].as_deref(), *n)?;
                        out.push(assemble(b, rs)?);
                    }
                    return Ok(out);
                }
                Err(RpcError::Rpc { code, .. }) if code == METHOD_NOT_FOUND => {
                    tracing::warn!("eth_getBlockReceipts unsupported; falling back to per-tx receipts");
                    self.block_receipts.store(false, Ordering::Relaxed);
                }
                Err(e) => return Err(e),
            }
        }
        let calls: Vec<_> =
            numbers.iter().map(|n| ("eth_getBlockByNumber", json!([format!("0x{n:x}"), true]))).collect();
        let res = self.batch(&calls).await?;
        let mut blocks = Vec::with_capacity(numbers.len());
        for (i, n) in numbers.iter().enumerate() {
            blocks.push(decode_block(res[i].as_deref(), *n)?);
        }
        let hashes: Vec<String> = blocks.iter().flat_map(|b| b.transactions.iter().map(|t| t.hash.clone())).collect();
        let mut receipts: Vec<RpcReceipt> = Vec::with_capacity(hashes.len());
        for chunk in hashes.chunks(500) {
            let calls: Vec<_> = chunk.iter().map(|h| ("eth_getTransactionReceipt", json!([h]))).collect();
            for (j, r) in self.batch(&calls).await?.into_iter().enumerate() {
                match r.as_deref() {
                    Some(v) if v.get() != "null" => receipts
                        .push(serde_json::from_str(v.get()).map_err(|e| RpcError::Decode(format!("receipt: {e}")))?),
                    _ => return Err(RpcError::NotReady(format!("receipt {}", chunk[j]))),
                }
            }
        }
        let mut it = receipts.into_iter();
        let mut out = Vec::with_capacity(blocks.len());
        for b in blocks {
            let rs: Vec<RpcReceipt> = it.by_ref().take(b.transactions.len()).collect();
            out.push(assemble(b, rs)?);
        }
        Ok(out)
    }
}

fn decode<T: for<'de> Deserialize<'de>>(v: Option<&RawValue>, what: &str) -> Result<T, RpcError> {
    match v {
        Some(v) if v.get() != "null" => {
            serde_json::from_str(v.get()).map_err(|e| RpcError::Decode(format!("{what}: {e}")))
        }
        _ => Err(RpcError::NotReady(format!("{what}: null"))),
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RpcBlock {
    number: String,
    hash: String,
    parent_hash: String,
    timestamp: String,
    miner: Option<String>,
    gas_used: String,
    gas_limit: String,
    base_fee_per_gas: Option<String>,
    difficulty: Option<String>,
    state_root: Option<String>,
    transactions_root: Option<String>,
    receipts_root: Option<String>,
    size: Option<String>,
    transactions: Vec<RpcTx>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RpcTx {
    hash: String,
    from: String,
    to: Option<String>,
    value: String,
    gas: String,
    gas_price: Option<String>,
    max_fee_per_gas: Option<String>,
    max_priority_fee_per_gas: Option<String>,
    nonce: String,
    input: String,
    #[serde(rename = "type")]
    tx_type: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RpcReceipt {
    transaction_hash: String,
    status: Option<String>,
    gas_used: String,
    effective_gas_price: Option<String>,
    contract_address: Option<String>,
    logs: Vec<RpcLog>,
}

#[derive(Deserialize)]
pub(crate) struct RpcLog {
    address: String,
    topics: Vec<String>,
    data: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Frame {
    #[serde(rename = "type")]
    call_type: Option<String>,
    from: Option<String>,
    to: Option<String>,
    value: Option<String>,
    gas: Option<String>,
    gas_used: Option<String>,
    error: Option<String>,
    #[serde(default)]
    calls: Vec<Frame>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TraceItem {
    tx_hash: Option<String>,
    result: Option<Frame>,
}

/// Flattens callTracer output for a block into internal calls (the top
/// frame of each tx is the tx itself and is skipped).
fn decode_traces(v: Option<&RawValue>, fb: &FullBlock) -> Result<Vec<InternalTx>, RpcError> {
    let n = fb.block.number;
    let items: Vec<TraceItem> = decode(v, &format!("traces {n}"))?;
    if items.len() != fb.txs.len() {
        return Err(RpcError::NotReady(format!("block {n}: {} traces for {} txs", items.len(), fb.txs.len())));
    }
    fn walk(f: &Frame, depth: u16, tx: &TxRec, out: &mut Vec<InternalTx>, n: u64) -> Result<(), RpcError> {
        for c in &f.calls {
            let call_type = c.call_type.clone().unwrap_or_else(|| "CALL".into()).to_uppercase();
            out.push(InternalTx {
                tx_index: tx.index,
                trace_index: out.len() as u32,
                tx_hash8: tx.hash[..8].try_into().unwrap(),
                depth,
                from: c.from.as_deref().and_then(parse_fixed::<20>).ok_or_else(|| bad("trace from", n))?,
                to: c.to.as_deref().and_then(parse_fixed::<20>),
                value: c.value.as_deref().and_then(parse_quantity).unwrap_or_default(),
                gas: c.gas.as_deref().and_then(parse_u64).unwrap_or(0),
                gas_used: c.gas_used.as_deref().and_then(parse_u64).unwrap_or(0),
                error: c.error.clone().map(|e| e.chars().take(200).collect()),
                call_type,
            });
            walk(c, depth + 1, tx, out, n)?;
        }
        Ok(())
    }
    let mut all = Vec::new();
    for (tx, item) in fb.txs.iter().zip(&items) {
        if let Some(h) = item.tx_hash.as_deref().and_then(parse_fixed::<32>) {
            if h != tx.hash {
                return Err(RpcError::NotReady(format!("block {n}: trace for another tx")));
            }
        }
        let mut out = Vec::new();
        if let Some(root) = &item.result {
            walk(root, 1, tx, &mut out, n)?;
        }
        all.extend(out);
    }
    Ok(all)
}

fn decode_block(v: Option<&RawValue>, n: u64) -> Result<RpcBlock, RpcError> {
    let b: RpcBlock = decode(v, &format!("block {n}"))?;
    if parse_u64(&b.number) != Some(n) {
        return Err(RpcError::Decode(format!("block {n}: node returned {}", b.number)));
    }
    Ok(b)
}

fn decode_receipts(v: Option<&RawValue>, n: u64) -> Result<Vec<RpcReceipt>, RpcError> {
    decode(v, &format!("receipts {n}"))
}

fn bad(what: &str, n: u64) -> RpcError {
    RpcError::Decode(format!("block {n}: bad {what}"))
}

fn q(s: &str, what: &str, n: u64) -> Result<Vec<u8>, RpcError> {
    parse_quantity(s).ok_or_else(|| bad(what, n))
}

fn oq(s: &Option<String>, what: &str, n: u64) -> Result<Option<Vec<u8>>, RpcError> {
    s.as_deref().map(|s| q(s, what, n)).transpose()
}

fn h256(s: &str, what: &str, n: u64) -> Result<H256, RpcError> {
    parse_fixed::<32>(s).ok_or_else(|| bad(what, n))
}

fn addr(s: &str, what: &str, n: u64) -> Result<Addr, RpcError> {
    parse_fixed::<20>(s).ok_or_else(|| bad(what, n))
}

fn u(s: &str, what: &str, n: u64) -> Result<u64, RpcError> {
    parse_u64(s).ok_or_else(|| bad(what, n))
}

/// Joins a block with its receipts. Receipts must match the block's
/// transactions one to one, in order, or the block is treated as not ready.
pub(crate) fn assemble(b: RpcBlock, rs: Vec<RpcReceipt>) -> Result<FullBlock, RpcError> {
    let n = u(&b.number, "number", 0)?;
    if rs.len() != b.transactions.len() {
        return Err(RpcError::NotReady(format!("block {n}: {} receipts for {} txs", rs.len(), b.transactions.len())));
    }
    let ts = u(&b.timestamp, "timestamp", n)?;
    let mut txs = Vec::with_capacity(b.transactions.len());
    let mut logs = Vec::new();
    let mut by_hash: HashMap<&str, usize> = HashMap::new();
    for (i, (t, r)) in b.transactions.iter().zip(rs.iter()).enumerate() {
        if !t.hash.eq_ignore_ascii_case(&r.transaction_hash) {
            return Err(RpcError::NotReady(format!("block {n}: receipt {i} is for another tx")));
        }
        by_hash.insert(&t.hash, i);
        let first_log_index = logs.len() as u32;
        for l in &r.logs {
            let mut topics = Vec::with_capacity(l.topics.len());
            for tp in &l.topics {
                topics.push(h256(tp, "log topic", n)?);
            }
            logs.push(LogRec {
                tx_index: i as u32,
                address: addr(&l.address, "log address", n)?,
                topics,
                data: parse_hex_bytes(&l.data).ok_or_else(|| bad("log data", n))?,
            });
        }
        let gas_price = oq(&t.gas_price, "gasPrice", n)?;
        let effective = oq(&r.effective_gas_price, "effectiveGasPrice", n)?;
        txs.push(TxRec {
            hash: h256(&t.hash, "tx hash", n)?,
            block_number: n,
            block_timestamp: ts,
            index: i as u32,
            from: addr(&t.from, "from", n)?,
            to: match t.to.as_deref() {
                Some(s) if !s.is_empty() => Some(addr(s, "to", n)?),
                _ => None,
            },
            value: q(&t.value, "value", n)?,
            gas: u(&t.gas, "gas", n)?,
            gas_price: gas_price.or_else(|| effective.clone()).unwrap_or_default(),
            effective_gas_price: effective,
            max_fee_per_gas: oq(&t.max_fee_per_gas, "maxFeePerGas", n)?,
            max_priority_fee_per_gas: oq(&t.max_priority_fee_per_gas, "maxPriorityFeePerGas", n)?,
            nonce: u(&t.nonce, "nonce", n)?,
            input: parse_hex_bytes(&t.input).ok_or_else(|| bad("input", n))?,
            tx_type: t.tx_type.as_deref().and_then(parse_u64).unwrap_or(0) as u8,
            status: r.status.as_deref().and_then(parse_u64).map(|s| s as u8),
            gas_used: u(&r.gas_used, "gasUsed", n)?,
            contract_address: match r.contract_address.as_deref() {
                Some(s) if !s.is_empty() => Some(addr(s, "contractAddress", n)?),
                _ => None,
            },
            first_log_index,
            log_count: r.logs.len() as u32,
        });
    }
    if by_hash.len() != txs.len() {
        return Err(bad("duplicate tx hash", n));
    }
    let block = BlockRec {
        number: n,
        hash: h256(&b.hash, "hash", n)?,
        parent_hash: h256(&b.parent_hash, "parentHash", n)?,
        timestamp: ts,
        miner: match b.miner.as_deref() {
            Some(m) => addr(m, "miner", n)?,
            None => [0; 20],
        },
        gas_used: u(&b.gas_used, "gasUsed", n)?,
        gas_limit: u(&b.gas_limit, "gasLimit", n)?,
        base_fee: oq(&b.base_fee_per_gas, "baseFeePerGas", n)?,
        difficulty: oq(&b.difficulty, "difficulty", n)?.unwrap_or_default(),
        state_root: b.state_root.as_deref().map(|s| h256(s, "stateRoot", n)).transpose()?.unwrap_or([0; 32]),
        transactions_root: b
            .transactions_root
            .as_deref()
            .map(|s| h256(s, "transactionsRoot", n))
            .transpose()?
            .unwrap_or([0; 32]),
        receipts_root: b.receipts_root.as_deref().map(|s| h256(s, "receiptsRoot", n)).transpose()?.unwrap_or([0; 32]),
        size: b.size.as_deref().and_then(parse_u64).unwrap_or(0),
        tx_count: txs.len() as u32,
        log_count: logs.len() as u32,
    };
    Ok(FullBlock { block, txs, logs, internals: Vec::new() })
}

#[cfg(test)]
mod tests {
    use super::*;

    const BLOCK: &str = r#"{"number":"0x10","hash":"0x1111111111111111111111111111111111111111111111111111111111111111",
      "parentHash":"0x2222222222222222222222222222222222222222222222222222222222222222","timestamp":"0x64",
      "miner":"0x00000000000000000000000000000000000000aa","gasUsed":"0x5208","gasLimit":"0x1c9c380",
      "baseFeePerGas":"0x7","difficulty":"0x0","size":"0x220","transactions":[
      {"hash":"0x3333333333333333333333333333333333333333333333333333333333333333","from":"0x00000000000000000000000000000000000000bb",
       "to":null,"value":"0xde0b6b3a7640000","gas":"0x5208","gasPrice":"0x9","maxFeePerGas":"0xa","maxPriorityFeePerGas":"0x2",
       "nonce":"0x1","input":"0x6080","type":"0x2"}]}"#;
    const RECEIPTS: &str = r#"[{"transactionHash":"0x3333333333333333333333333333333333333333333333333333333333333333",
      "status":"0x1","gasUsed":"0x5208","effectiveGasPrice":"0x8","contractAddress":"0x00000000000000000000000000000000000000cc",
      "logs":[{"address":"0x00000000000000000000000000000000000000cc","topics":["0x4444444444444444444444444444444444444444444444444444444444444444"],"data":"0x01"}]}]"#;

    #[test]
    fn assembles_block() {
        let b: RpcBlock = serde_json::from_str(BLOCK).unwrap();
        let rs: Vec<RpcReceipt> = serde_json::from_str(RECEIPTS).unwrap();
        let fb = assemble(b, rs).unwrap();
        assert_eq!(fb.block.number, 16);
        assert_eq!(fb.block.tx_count, 1);
        assert_eq!(fb.block.log_count, 1);
        let t = &fb.txs[0];
        assert_eq!(t.to, None);
        assert_eq!(t.contract_address.unwrap()[19], 0xcc);
        assert_eq!(t.status, Some(1));
        assert_eq!(t.tx_type, 2);
        assert_eq!(t.gas_price, vec![9]);
        assert_eq!(t.effective_gas_price, Some(vec![8]));
        assert_eq!(dec(&t.value), "1000000000000000000");
        assert_eq!(fb.logs[0].tx_index, 0);
    }

    #[test]
    fn flattens_call_traces() {
        let b: RpcBlock = serde_json::from_str(BLOCK).unwrap();
        let rs: Vec<RpcReceipt> = serde_json::from_str(RECEIPTS).unwrap();
        let fb = assemble(b, rs).unwrap();
        let tr = r#"[{"txHash":"0x3333333333333333333333333333333333333333333333333333333333333333","result":
          {"type":"CREATE","from":"0x00000000000000000000000000000000000000bb","to":"0x00000000000000000000000000000000000000cc","calls":[
            {"type":"CALL","from":"0x00000000000000000000000000000000000000cc","to":"0x00000000000000000000000000000000000000dd","value":"0x5","gasUsed":"0x10","calls":[
              {"type":"staticcall","from":"0x00000000000000000000000000000000000000dd","to":"0x00000000000000000000000000000000000000ee","error":"execution reverted"}]},
            {"type":"DELEGATECALL","from":"0x00000000000000000000000000000000000000cc","to":"0x00000000000000000000000000000000000000ff"}]}}]"#;
        let raw = RawValue::from_string(tr.to_string()).unwrap();
        let it = decode_traces(Some(&raw), &fb).unwrap();
        let got: Vec<_> =
            it.iter().map(|c| (c.trace_index, c.depth, c.call_type.as_str(), c.to.unwrap()[19])).collect();
        assert_eq!(got, vec![(0, 1, "CALL", 0xdd), (1, 2, "STATICCALL", 0xee), (2, 1, "DELEGATECALL", 0xff)]);
        assert_eq!(it[0].value, vec![5]);
        assert_eq!(it[1].error.as_deref(), Some("execution reverted"));
        assert_eq!(it[0].tx_hash8, [0x33; 8]);
        let short = RawValue::from_string("[]".into()).unwrap();
        assert!(matches!(decode_traces(Some(&short), &fb), Err(RpcError::NotReady(_))));
    }

    #[test]
    fn missing_receipts_are_not_ready() {
        let b: RpcBlock = serde_json::from_str(BLOCK).unwrap();
        assert!(matches!(assemble(b, vec![]), Err(RpcError::NotReady(_))));
        assert!(matches!(decode::<RpcBlock>(None, "x"), Err(RpcError::NotReady(_))));
    }
}
