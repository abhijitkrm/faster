//! Table schemas and the conversion between JSON-RPC objects and Arrow rows.
//!
//! Each table is a list of columns. A column either maps one key of the
//! node's JSON object (`key`) or is derived (`key == ""`). Keys the node sends
//! that no column maps are kept as a JSON object in the table's `extra`
//! column, so chain-specific fields (L2 fees, deposit fields, …) survive the
//! round trip and faster-rpc can answer with the node's exact objects.

use std::sync::{Arc, OnceLock};

use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use serde_json::{Map, Value};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    /// Unsigned integer that fits 64 bits (block number, gas, nonce).
    U64,
    /// Arbitrary-size quantity (wei values, difficulty), minimal big-endian bytes.
    Qty,
    /// Variable-length bytes.
    Data,
    H256,
    Addr,
    /// First 4 bytes of call data.
    Sig,
    /// Raw JSON (lists and objects the query engine never filters on).
    Json,
    Str,
}

impl Kind {
    pub fn data_type(self) -> DataType {
        match self {
            Kind::U64 => DataType::UInt64,
            Kind::Qty | Kind::Data => DataType::Binary,
            Kind::H256 => DataType::FixedSizeBinary(32),
            Kind::Addr => DataType::FixedSizeBinary(20),
            Kind::Sig => DataType::FixedSizeBinary(4),
            Kind::Json | Kind::Str => DataType::Utf8,
        }
    }
}

#[derive(Debug)]
pub struct Col {
    pub name: &'static str,
    /// JSON-RPC key; empty for derived columns.
    pub key: &'static str,
    pub kind: Kind,
}

const fn c(name: &'static str, key: &'static str, kind: Kind) -> Col {
    Col { name, key, kind }
}

use Kind::*;

pub static BLOCK_COLS: &[Col] = &[
    c("number", "number", U64),
    c("hash", "hash", H256),
    c("parent_hash", "parentHash", H256),
    c("nonce", "nonce", Data),
    c("sha3_uncles", "sha3Uncles", H256),
    c("logs_bloom", "logsBloom", Data),
    c("transactions_root", "transactionsRoot", H256),
    c("state_root", "stateRoot", H256),
    c("receipts_root", "receiptsRoot", H256),
    c("miner", "miner", Addr),
    c("difficulty", "difficulty", Qty),
    c("total_difficulty", "totalDifficulty", Qty),
    c("extra_data", "extraData", Data),
    c("size", "size", U64),
    c("gas_limit", "gasLimit", U64),
    c("gas_used", "gasUsed", U64),
    c("timestamp", "timestamp", U64),
    c("uncles", "uncles", Json),
    c("base_fee_per_gas", "baseFeePerGas", Qty),
    c("mix_hash", "mixHash", H256),
    c("withdrawals_root", "withdrawalsRoot", H256),
    c("withdrawals", "withdrawals", Json),
    c("blob_gas_used", "blobGasUsed", U64),
    c("excess_blob_gas", "excessBlobGas", U64),
    c("parent_beacon_block_root", "parentBeaconBlockRoot", H256),
    c("requests_hash", "requestsHash", H256),
    c("transaction_count", "", U64),
    c("extra", "", Json),
];

/// Transaction columns followed by receipt columns (`RECEIPT_FROM` on).
pub static TX_COLS: &[Col] = &[
    c("block_number", "blockNumber", U64),
    c("transaction_index", "transactionIndex", U64),
    c("hash", "hash", H256),
    c("from", "from", Addr),
    c("to", "to", Addr),
    c("value", "value", Qty),
    c("gas", "gas", U64),
    c("gas_price", "gasPrice", Qty),
    c("max_fee_per_gas", "maxFeePerGas", Qty),
    c("max_priority_fee_per_gas", "maxPriorityFeePerGas", Qty),
    c("max_fee_per_blob_gas", "maxFeePerBlobGas", Qty),
    c("nonce", "nonce", U64),
    c("input", "input", Data),
    c("type", "type", U64),
    c("chain_id", "chainId", U64),
    c("v", "v", Qty),
    c("r", "r", Qty),
    c("s", "s", Qty),
    c("y_parity", "yParity", U64),
    c("access_list", "accessList", Json),
    c("blob_versioned_hashes", "blobVersionedHashes", Json),
    c("authorization_list", "authorizationList", Json),
    c("sighash", "", Sig),
    c("extra", "", Json),
    // receipt
    c("status", "status", U64),
    c("cumulative_gas_used", "cumulativeGasUsed", U64),
    c("gas_used", "gasUsed", U64),
    c("effective_gas_price", "effectiveGasPrice", Qty),
    c("contract_address", "contractAddress", Addr),
    c("logs_bloom", "logsBloom", Data),
    c("root", "root", H256),
    c("blob_gas_used", "blobGasUsed", U64),
    c("blob_gas_price", "blobGasPrice", Qty),
    c("receipt_extra", "", Json),
];
pub const RECEIPT_FROM: usize = 24;

pub static LOG_COLS: &[Col] = &[
    c("block_number", "blockNumber", U64),
    c("log_index", "logIndex", U64),
    c("transaction_index", "transactionIndex", U64),
    c("transaction_hash", "transactionHash", H256),
    c("address", "address", Addr),
    c("topic0", "", H256),
    c("topic1", "", H256),
    c("topic2", "", H256),
    c("topic3", "", H256),
    c("data", "data", Data),
    c("extra", "", Json),
    // 64-bit hashes of the topics (filter::key): topic filters compare these
    // 8-byte keys first and check the 32-byte topics only for survivors.
    c("topic0_key", "", U64),
    c("topic1_key", "", U64),
    c("topic2_key", "", U64),
    c("topic3_key", "", U64),
];

/// Internal columns, never returned for `"*"`.
pub fn hidden(name: &str) -> bool {
    name.ends_with("_key")
}

pub static TRACE_COLS: &[Col] = &[
    c("block_number", "", U64),
    c("transaction_index", "", U64),
    c("transaction_hash", "", H256),
    c("trace_address", "", Str),
    c("type", "type", Str),
    c("from", "from", Addr),
    c("to", "to", Addr),
    c("value", "value", Qty),
    c("gas", "gas", U64),
    c("gas_used", "gasUsed", U64),
    c("input", "input", Data),
    c("output", "output", Data),
    c("error", "error", Str),
    c("revert_reason", "revertReason", Str),
    c("sighash", "", Sig),
];

/// Keys that appear on the node's objects but are implied by the row's
/// position (block hash on a tx, …) and are not kept in `extra`.
const BLOCK_SKIP: &[&str] = &["transactions"];
const TX_SKIP: &[&str] = &["blockHash"];
const RECEIPT_SKIP: &[&str] =
    &["transactionHash", "transactionIndex", "blockHash", "blockNumber", "from", "to", "type", "logs"];
const LOG_SKIP: &[&str] = &["topics", "blockHash", "removed"];

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
pub enum Table {
    Blocks,
    Transactions,
    Logs,
    Traces,
}

pub const TABLES: [Table; 4] = [Table::Blocks, Table::Transactions, Table::Logs, Table::Traces];

impl Table {
    pub fn cols(self) -> &'static [Col] {
        match self {
            Table::Blocks => BLOCK_COLS,
            Table::Transactions => TX_COLS,
            Table::Logs => LOG_COLS,
            Table::Traces => TRACE_COLS,
        }
    }
    pub fn name(self) -> &'static str {
        match self {
            Table::Blocks => "blocks",
            Table::Transactions => "transactions",
            Table::Logs => "logs",
            Table::Traces => "traces",
        }
    }
    pub fn idx(self) -> usize {
        self as usize
    }
    /// Name of the column holding the block number.
    pub fn block_col(self) -> &'static str {
        match self {
            Table::Blocks => "number",
            _ => "block_number",
        }
    }
    pub fn schema(self) -> SchemaRef {
        static S: OnceLock<[SchemaRef; 4]> = OnceLock::new();
        S.get_or_init(|| {
            TABLES.map(|t| {
                Arc::new(Schema::new(
                    t.cols().iter().map(|c| Field::new(c.name, c.kind.data_type(), true)).collect::<Vec<_>>(),
                ))
            })
        })[self.idx()]
        .clone()
    }
    pub fn col_index(self, name: &str) -> Option<usize> {
        self.cols().iter().position(|c| c.name == name)
    }
}

// ── hex helpers ─────────────────────────────────────────────────────────

pub fn parse_hex(s: &str) -> Option<Vec<u8>> {
    let s = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")).unwrap_or(s);
    if s.len() % 2 == 1 {
        let mut p = String::with_capacity(s.len() + 1);
        p.push('0');
        p.push_str(s);
        return hex::decode(p).ok();
    }
    hex::decode(s).ok()
}

pub fn parse_fixed<const N: usize>(s: &str) -> Option<[u8; N]> {
    let v = parse_hex(s)?;
    v.try_into().ok()
}

fn json_u64(v: &Value) -> Option<u64> {
    match v {
        Value::String(s) => {
            let h = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X"));
            match h {
                Some("") => Some(0),
                Some(h) => u64::from_str_radix(h, 16).ok(),
                None => s.parse().ok(),
            }
        }
        Value::Number(n) => n.as_u64(),
        _ => None,
    }
}

/// Minimal big-endian bytes of a hex quantity ("0x0" → empty).
fn json_qty(v: &Value) -> Option<Vec<u8>> {
    match v {
        Value::String(s) => {
            let mut b = parse_hex(s)?;
            let z = b.iter().take_while(|x| **x == 0).count();
            b.drain(..z);
            Some(b)
        }
        Value::Number(n) => {
            let x = n.as_u64()?;
            let b = x.to_be_bytes();
            let z = b.iter().take_while(|x| **x == 0).count();
            Some(b[z..].to_vec())
        }
        _ => None,
    }
}

pub fn hex0x(b: &[u8]) -> String {
    let mut s = String::with_capacity(2 + b.len() * 2);
    s.push_str("0x");
    s.push_str(&hex::encode(b));
    s
}

pub fn qty_hex(b: &[u8]) -> String {
    let z = b.iter().take_while(|x| **x == 0).count();
    let b = &b[z..];
    if b.is_empty() {
        return "0x0".into();
    }
    let h = hex::encode(b);
    format!("0x{}", h.trim_start_matches('0'))
}

// ── builders ────────────────────────────────────────────────────────────

pub enum ColBuilder {
    U64(UInt64Builder),
    Bin(BinaryBuilder),
    Fixed(FixedSizeBinaryBuilder),
    Str(StringBuilder),
}

impl ColBuilder {
    fn new(k: Kind) -> Self {
        match k {
            U64 => ColBuilder::U64(UInt64Builder::new()),
            Qty | Data => ColBuilder::Bin(BinaryBuilder::new()),
            H256 => ColBuilder::Fixed(FixedSizeBinaryBuilder::new(32)),
            Addr => ColBuilder::Fixed(FixedSizeBinaryBuilder::new(20)),
            Sig => ColBuilder::Fixed(FixedSizeBinaryBuilder::new(4)),
            Json | Str => ColBuilder::Str(StringBuilder::new()),
        }
    }
    fn null(&mut self) {
        match self {
            ColBuilder::U64(b) => b.append_null(),
            ColBuilder::Bin(b) => b.append_null(),
            ColBuilder::Fixed(b) => b.append_null(),
            ColBuilder::Str(b) => b.append_null(),
        }
    }
    fn finish(&mut self) -> ArrayRef {
        match self {
            ColBuilder::U64(b) => Arc::new(b.finish()),
            ColBuilder::Bin(b) => Arc::new(b.finish()),
            ColBuilder::Fixed(b) => Arc::new(b.finish()),
            ColBuilder::Str(b) => Arc::new(b.finish()),
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct DecodeError(pub String);

fn derr(s: impl Into<String>) -> DecodeError {
    DecodeError(s.into())
}

pub struct TableBuilder {
    pub table: Table,
    cols: Vec<ColBuilder>,
    pub rows: usize,
}

impl TableBuilder {
    pub fn new(table: Table) -> Self {
        TableBuilder { table, cols: table.cols().iter().map(|c| ColBuilder::new(c.kind)).collect(), rows: 0 }
    }

    /// Appends `v` (a JSON value, or None for null) to column `i`.
    fn put(&mut self, i: usize, v: Option<&Value>) -> Result<(), DecodeError> {
        let col = &self.table.cols()[i];
        let v = match v {
            None | Some(Value::Null) => {
                self.cols[i].null();
                return Ok(());
            }
            Some(v) => v,
        };
        let bad = || derr(format!("{}.{}: bad value {}", self.table.name(), col.name, short(v)));
        match (&mut self.cols[i], col.kind) {
            (ColBuilder::U64(b), _) => b.append_value(json_u64(v).ok_or_else(bad)?),
            (ColBuilder::Bin(b), Qty) => b.append_value(json_qty(v).ok_or_else(bad)?),
            (ColBuilder::Bin(b), _) => b.append_value(v.as_str().and_then(parse_hex).ok_or_else(bad)?),
            (ColBuilder::Fixed(b), k) => {
                let s = v.as_str().ok_or_else(bad)?;
                if s.is_empty() || s == "0x" {
                    b.append_null();
                } else {
                    let x = parse_hex(s).ok_or_else(bad)?;
                    let n = if k == H256 {
                        32
                    } else if k == Addr {
                        20
                    } else {
                        4
                    };
                    if x.len() != n {
                        return Err(bad());
                    }
                    b.append_value(&x).map_err(|e| derr(e.to_string()))?;
                }
            }
            (ColBuilder::Str(b), Json) => b.append_value(v.to_string()),
            (ColBuilder::Str(b), _) => match v {
                Value::String(s) => b.append_value(s),
                other => b.append_value(other.to_string()),
            },
        }
        Ok(())
    }

    fn put_u64(&mut self, i: usize, x: u64) {
        if let ColBuilder::U64(b) = &mut self.cols[i] {
            b.append_value(x)
        }
    }
    fn put_u64_opt(&mut self, i: usize, x: Option<u64>) {
        if let ColBuilder::U64(b) = &mut self.cols[i] {
            b.append_option(x)
        }
    }
    fn put_fixed(&mut self, i: usize, x: Option<&[u8]>) {
        let n = match self.table.cols()[i].kind {
            H256 => 32,
            Addr => 20,
            _ => 4,
        };
        if let ColBuilder::Fixed(b) = &mut self.cols[i] {
            match x {
                Some(x) if x.len() == n => b.append_value(x).unwrap(),
                _ => b.append_null(),
            }
        }
    }
    fn put_str(&mut self, i: usize, x: Option<&str>) {
        if let ColBuilder::Str(b) = &mut self.cols[i] {
            match x {
                Some(x) => b.append_value(x),
                None => b.append_null(),
            }
        }
    }

    /// Fills the key-mapped columns in `range` from `obj` (removing the keys)
    /// and returns nothing; leftover keys stay in `obj`.
    fn fill(&mut self, range: std::ops::Range<usize>, obj: &mut Map<String, Value>) -> Result<(), DecodeError> {
        for i in range {
            let key = self.table.cols()[i].key;
            if key.is_empty() {
                continue;
            }
            let v = obj.remove(key);
            self.put(i, v.as_ref())?;
        }
        Ok(())
    }

    fn put_extra(&mut self, i: usize, obj: Map<String, Value>, skip: &[&str]) {
        let rest: Map<String, Value> = obj.into_iter().filter(|(k, _)| !skip.contains(&k.as_str())).collect();
        if rest.is_empty() {
            self.put_str(i, None)
        } else {
            self.put_str(i, Some(&Value::Object(rest).to_string()))
        }
    }

    pub fn finish(&mut self) -> RecordBatch {
        let arrays: Vec<ArrayRef> = self.cols.iter_mut().map(|c| c.finish()).collect();
        self.rows = 0;
        RecordBatch::try_new(self.table.schema(), arrays).expect("schema matches builders")
    }
}

fn short(v: &Value) -> String {
    let s = v.to_string();
    s.chars().take(80).collect()
}

/// Rows of one or more consecutive blocks, one batch per table.
#[derive(Clone, Debug)]
pub struct Batches {
    pub tables: [RecordBatch; 4],
}

impl Batches {
    pub fn empty() -> Self {
        Batches { tables: TABLES.map(|t| RecordBatch::new_empty(t.schema())) }
    }
    pub fn get(&self, t: Table) -> &RecordBatch {
        &self.tables[t.idx()]
    }
    pub fn rows(&self) -> usize {
        self.tables.iter().map(|b| b.num_rows()).sum()
    }
    pub fn concat(parts: &[Batches]) -> Batches {
        Batches {
            tables: TABLES.map(|t| {
                let bs: Vec<&RecordBatch> = parts.iter().map(|p| p.get(t)).collect();
                arrow::compute::concat_batches(&t.schema(), bs).expect("same schema")
            }),
        }
    }
    pub fn byte_size(&self) -> usize {
        self.tables.iter().map(|b| b.get_array_memory_size()).sum()
    }
}

/// Header facts the ingest path needs without reading Arrow columns.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct BlockHead {
    pub number: u64,
    pub hash: [u8; 32],
    pub parent_hash: [u8; 32],
    pub timestamp: u64,
}

/// Builds rows for consecutive blocks from the node's JSON objects.
pub struct Builder {
    t: [TableBuilder; 4],
    pub heads: Vec<BlockHead>,
}

impl Default for Builder {
    fn default() -> Self {
        Self::new()
    }
}

impl Builder {
    pub fn new() -> Self {
        Builder { t: TABLES.map(TableBuilder::new), heads: Vec::new() }
    }

    pub fn rows(&self) -> usize {
        self.t.iter().map(|t| t.rows).sum()
    }

    /// Adds one block: `block` from eth_getBlockByNumber(n, true), `receipts`
    /// from eth_getBlockReceipts (same order as the block's transactions),
    /// `traces` from debug_traceBlockByNumber with callTracer, if fetched.
    /// Nothing is appended when the inputs are inconsistent.
    pub fn add_block(
        &mut self,
        mut block: Map<String, Value>,
        receipts: Vec<Value>,
        traces: Option<Vec<Value>>,
    ) -> Result<BlockHead, DecodeError> {
        let number = block.get("number").and_then(json_u64).ok_or_else(|| derr("block: no number"))?;
        let txs = match block.remove("transactions") {
            Some(Value::Array(a)) => a,
            None | Some(Value::Null) => Vec::new(),
            Some(_) => return Err(derr(format!("block {number}: transactions is not a list"))),
        };
        if receipts.len() != txs.len() {
            return Err(derr(format!("block {number}: {} receipts for {} txs", receipts.len(), txs.len())));
        }
        let hash = block.get("hash").and_then(|v| v.as_str()).and_then(parse_fixed::<32>);
        let parent = block.get("parentHash").and_then(|v| v.as_str()).and_then(parse_fixed::<32>);
        let ts = block.get("timestamp").and_then(json_u64);
        let (hash, parent_hash, timestamp) = match (hash, parent, ts) {
            (Some(h), Some(p), Some(t)) => (h, p, t),
            _ => return Err(derr(format!("block {number}: missing hash, parentHash or timestamp"))),
        };
        let mut tx_objs = Vec::with_capacity(txs.len());
        for (i, (tx, r)) in txs.into_iter().zip(receipts).enumerate() {
            let (Value::Object(tx), Value::Object(r)) = (tx, r) else {
                return Err(derr(format!("block {number}: tx {i} is not an object (fetch with full txs)")));
            };
            let th = tx.get("hash").and_then(|v| v.as_str()).unwrap_or("");
            let rh = r.get("transactionHash").and_then(|v| v.as_str()).unwrap_or("");
            if th.is_empty() || !th.eq_ignore_ascii_case(rh) {
                return Err(derr(format!("block {number}: receipt {i} does not belong to tx {th}")));
            }
            tx_objs.push((tx, r));
        }
        if let Some(tr) = &traces {
            if tr.len() != tx_objs.len() {
                return Err(derr(format!("block {number}: {} traces for {} txs", tr.len(), tx_objs.len())));
            }
        }
        // Validated: append. Decode errors past this point can leave a partial
        // row in some builder, so the caller discards the whole Builder.
        let ntx = tx_objs.len() as u64;
        let bt = &mut self.t[0];
        bt.fill(0..BLOCK_COLS.len(), &mut block)?;
        bt.put_u64(BLOCK_COLS.len() - 2, ntx);
        bt.put_extra(BLOCK_COLS.len() - 1, block, BLOCK_SKIP);
        bt.rows += 1;

        let mut log_index_fallback = 0u64;
        for (i, (mut tx, mut r)) in tx_objs.into_iter().enumerate() {
            let tx_hash = parse_fixed::<32>(tx.get("hash").and_then(|v| v.as_str()).unwrap_or(""))
                .ok_or_else(|| derr(format!("block {number}: bad tx hash")))?;
            let logs = match r.remove("logs") {
                Some(Value::Array(a)) => a,
                _ => Vec::new(),
            };
            let sig = tx
                .get("input")
                .and_then(|v| v.as_str())
                .and_then(parse_hex)
                .filter(|b| b.len() >= 4)
                .map(|b| [b[0], b[1], b[2], b[3]]);
            // Some nodes omit blockNumber/transactionIndex on txs from a block.
            tx.entry("blockNumber").or_insert(Value::from(number));
            tx.entry("transactionIndex").or_insert(Value::from(i as u64));
            let tt = &mut self.t[1];
            tt.fill(0..RECEIPT_FROM - 2, &mut tx)?;
            tt.put_fixed(RECEIPT_FROM - 2, sig.as_ref().map(|s| &s[..]));
            tt.put_extra(RECEIPT_FROM - 1, tx, TX_SKIP);
            tt.fill(RECEIPT_FROM..TX_COLS.len() - 1, &mut r)?;
            tt.put_extra(TX_COLS.len() - 1, r, RECEIPT_SKIP);
            tt.rows += 1;

            let lt = &mut self.t[2];
            for l in logs {
                let Value::Object(mut l) = l else { return Err(derr(format!("block {number}: log is not an object"))) };
                l.insert("blockNumber".into(), Value::from(number));
                l.insert("transactionIndex".into(), Value::from(i as u64));
                l.insert("transactionHash".into(), Value::from(hex0x(&tx_hash)));
                l.entry("logIndex").or_insert(Value::from(log_index_fallback));
                log_index_fallback += 1;
                let topics: Vec<[u8; 32]> = match l.get("topics") {
                    Some(Value::Array(a)) => a
                        .iter()
                        .map(|t| t.as_str().and_then(parse_fixed::<32>))
                        .collect::<Option<Vec<_>>>()
                        .ok_or_else(|| derr(format!("block {number}: bad log topic")))?,
                    _ => Vec::new(),
                };
                if topics.len() > 4 {
                    return Err(derr(format!("block {number}: log with {} topics", topics.len())));
                }
                lt.fill(0..5, &mut l)?;
                for k in 0..4 {
                    lt.put_fixed(5 + k, topics.get(k).map(|t| &t[..]));
                }
                lt.fill(9..10, &mut l)?;
                lt.put_extra(10, l, LOG_SKIP);
                for k in 0..4 {
                    lt.put_u64_opt(11 + k, topics.get(k).map(|t| crate::filter::key(t)));
                }
                lt.rows += 1;
            }
            if let Some(tr) = &traces {
                let frame = tr[i].get("result").unwrap_or(&tr[i]);
                let mut path = Vec::new();
                add_frames(&mut self.t[3], frame, number, i as u64, &tx_hash, &mut path)?;
            }
        }
        let head = BlockHead { number, hash, parent_hash, timestamp };
        self.heads.push(head);
        Ok(head)
    }

    pub fn finish(&mut self) -> Batches {
        self.heads.clear();
        Batches { tables: [self.t[0].finish(), self.t[1].finish(), self.t[2].finish(), self.t[3].finish()] }
    }
}

fn add_frames(
    t: &mut TableBuilder,
    f: &Value,
    number: u64,
    txi: u64,
    tx_hash: &[u8; 32],
    path: &mut Vec<usize>,
) -> Result<(), DecodeError> {
    let Value::Object(o) = f else { return Ok(()) };
    let mut o = o.clone();
    let calls = match o.remove("calls") {
        Some(Value::Array(a)) => a,
        _ => Vec::new(),
    };
    o.remove("logs");
    if let Some(Value::String(s)) = o.get_mut("type") {
        *s = s.to_uppercase();
    }
    let sig = o
        .get("input")
        .and_then(|v| v.as_str())
        .and_then(parse_hex)
        .filter(|b| b.len() >= 4)
        .map(|b| [b[0], b[1], b[2], b[3]]);
    t.put_u64(0, number);
    t.put_u64(1, txi);
    t.put_fixed(2, Some(tx_hash));
    let p: Vec<String> = path.iter().map(|x| x.to_string()).collect();
    t.put_str(3, Some(&p.join(",")));
    t.fill(4..TRACE_COLS.len() - 1, &mut o)?;
    t.put_fixed(TRACE_COLS.len() - 1, sig.as_ref().map(|s| &s[..]));
    t.rows += 1;
    for (k, child) in calls.iter().enumerate() {
        path.push(k);
        add_frames(t, child, number, txi, tx_hash, path)?;
        path.pop();
    }
    Ok(())
}

// ── reading values back ─────────────────────────────────────────────────

/// JSON value of row `row` in column `col` (by schema position) of `b`.
/// `rpc`: quantities as hex strings (JSON-RPC style) instead of numbers.
pub fn cell_json(t: Table, b: &RecordBatch, col: usize, row: usize, rpc: bool) -> Value {
    let kind = t.cols()[col].kind;
    cell_json_kind(kind, b.column(col).as_ref(), row, rpc)
}

pub fn cell_json_kind(kind: Kind, a: &dyn Array, row: usize, rpc: bool) -> Value {
    if a.is_null(row) {
        return Value::Null;
    }
    match kind {
        U64 => {
            let x = a.as_any().downcast_ref::<UInt64Array>().unwrap().value(row);
            if rpc {
                Value::String(format!("0x{x:x}"))
            } else {
                Value::from(x)
            }
        }
        Qty => Value::String(qty_hex(a.as_any().downcast_ref::<BinaryArray>().unwrap().value(row))),
        Data => Value::String(hex0x(a.as_any().downcast_ref::<BinaryArray>().unwrap().value(row))),
        H256 | Addr | Sig => {
            Value::String(hex0x(a.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap().value(row)))
        }
        Json => {
            let s = a.as_any().downcast_ref::<StringArray>().unwrap().value(row);
            serde_json::from_str(s).unwrap_or(Value::Null)
        }
        Str => Value::String(a.as_any().downcast_ref::<StringArray>().unwrap().value(row).to_string()),
    }
}

pub fn u64_col<'a>(b: &'a RecordBatch, name: &str) -> &'a UInt64Array {
    b.column_by_name(name).and_then(|a| a.as_any().downcast_ref::<UInt64Array>()).expect("u64 column")
}

pub fn fixed_col<'a>(b: &'a RecordBatch, name: &str) -> &'a FixedSizeBinaryArray {
    b.column_by_name(name).and_then(|a| a.as_any().downcast_ref::<FixedSizeBinaryArray>()).expect("fixed column")
}

/// Rebuilds the node-shaped JSON object of row `row` (block header without
/// transactions, transaction, receipt without logs, or log).
pub fn rpc_object(t: Table, b: &RecordBatch, row: usize, part: RpcPart) -> Map<String, Value> {
    let cols = t.cols();
    let range = match (t, part) {
        (Table::Transactions, RpcPart::Tx) => 0..RECEIPT_FROM,
        (Table::Transactions, RpcPart::Receipt) => RECEIPT_FROM..cols.len(),
        _ => 0..cols.len(),
    };
    let mut o = Map::new();
    for i in range {
        let col = &cols[i];
        if col.key.is_empty() {
            match col.name {
                "extra" | "receipt_extra" => {
                    if let Value::Object(m) = cell_json(t, b, i, row, true) {
                        o.extend(m);
                    }
                }
                _ => {}
            }
            continue;
        }
        let v = cell_json(t, b, i, row, true);
        if v.is_null() && !keeps_null(t, col.key) {
            continue;
        }
        o.insert(col.key.to_string(), v);
    }
    if t == Table::Logs {
        let topics: Vec<Value> = (5..9).map(|i| cell_json(t, b, i, row, true)).take_while(|v| !v.is_null()).collect();
        o.insert("topics".into(), Value::Array(topics));
        o.insert("removed".into(), Value::Bool(false));
    }
    o
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RpcPart {
    Whole,
    Tx,
    Receipt,
}

/// Keys JSON-RPC sends as an explicit null.
fn keeps_null(t: Table, key: &str) -> bool {
    matches!((t, key), (Table::Transactions, "to") | (Table::Transactions, "contractAddress"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    pub fn sample_block() -> (Map<String, Value>, Vec<Value>) {
        let block = json!({
            "number": "0x10", "hash": format!("0x{}", "11".repeat(32)), "parentHash": format!("0x{}", "22".repeat(32)),
            "timestamp": "0x64", "miner": format!("0x{}", "aa".repeat(20)), "gasUsed": "0x5208", "gasLimit": "0x1c9c380",
            "baseFeePerGas": "0x7", "difficulty": "0x0", "size": "0x220", "extraData": "0x", "uncles": [],
            "l1BlockNumber": "0x99",
            "transactions": [{
                "hash": format!("0x{}", "33".repeat(32)), "from": format!("0x{}", "bb".repeat(20)),
                "to": format!("0x{}", "cc".repeat(20)), "value": "0xde0b6b3a7640000", "gas": "0x5208",
                "gasPrice": "0x9", "nonce": "0x1", "input": "0xa9059cbb0000", "type": "0x2", "blockHash": "0x00",
                "accessList": [], "chainId": "0x1", "v": "0x0", "r": "0x1", "s": "0x2", "yParity": "0x0",
                "maxFeePerGas": "0x10", "maxPriorityFeePerGas": "0x1"
            }]
        });
        let receipt = json!({
            "transactionHash": format!("0x{}", "33".repeat(32)), "status": "0x1", "gasUsed": "0x5208",
            "cumulativeGasUsed": "0x5208", "effectiveGasPrice": "0x8", "contractAddress": null, "l1Fee": "0x5",
            "logs": [{"address": format!("0x{}", "dd".repeat(20)), "topics": [format!("0x{}", "ee".repeat(32)), format!("0x{}", "ff".repeat(32))],
                      "data": "0x0102", "logIndex": "0x0", "removed": false}]
        });
        (block.as_object().unwrap().clone(), vec![receipt])
    }

    #[test]
    fn roundtrip_rpc_objects() {
        let (b, r) = sample_block();
        let mut bl = Builder::new();
        let h = bl.add_block(b, r, None).unwrap();
        assert_eq!(h.number, 16);
        let out = bl.finish();
        assert_eq!(out.get(Table::Blocks).num_rows(), 1);
        assert_eq!(out.get(Table::Transactions).num_rows(), 1);
        assert_eq!(out.get(Table::Logs).num_rows(), 1);
        let blk = rpc_object(Table::Blocks, out.get(Table::Blocks), 0, RpcPart::Whole);
        assert_eq!(blk["l1BlockNumber"], "0x99");
        assert_eq!(blk["gasUsed"], "0x5208");
        let tx = rpc_object(Table::Transactions, out.get(Table::Transactions), 0, RpcPart::Tx);
        assert_eq!(tx["value"], "0xde0b6b3a7640000");
        assert_eq!(tx["accessList"], json!([]));
        assert_eq!(tx["v"], "0x0");
        assert!(tx.get("blockHash").is_none());
        let rc = rpc_object(Table::Transactions, out.get(Table::Transactions), 0, RpcPart::Receipt);
        assert_eq!(rc["l1Fee"], "0x5");
        assert_eq!(rc["contractAddress"], Value::Null);
        let lg = rpc_object(Table::Logs, out.get(Table::Logs), 0, RpcPart::Whole);
        assert_eq!(lg["topics"].as_array().unwrap().len(), 2);
        assert_eq!(lg["data"], "0x0102");
        let sig = fixed_col(out.get(Table::Transactions), "sighash");
        assert_eq!(sig.value(0), &[0xa9, 0x05, 0x9c, 0xbb]);
    }

    #[test]
    fn rejects_mismatched_receipts() {
        let (b, mut r) = sample_block();
        r[0]["transactionHash"] = json!(format!("0x{}", "44".repeat(32)));
        assert!(Builder::new().add_block(b, r, None).is_err());
    }
}
