//! Query engine.
//!
//! A query selects rows from four tables over a block range. Within one
//! selection all given fields must match (AND); each field matches any of
//! its listed values (OR); selections of a table are alternatives (OR).
//! Joins then pull related rows: by default the transaction behind each
//! selected log or trace, and the header of every block with a result row.
//!
//! Each chunk is processed independently (in parallel): its row-group
//! filters decide which row groups can match, only those are read, and only
//! the needed columns. Results come back in block order; a page ends at a
//! block boundary once the row or time budget is used, and `next_block`
//! tells the client where to continue.

use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Result};
use arrow::array::*;
use arrow::compute::filter_record_batch;
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use serde::{Deserialize, Serialize};

use crate::chunk::Chunk;
use crate::filter::{filtered_cols, key};
use crate::hot::HotBlock;
use crate::metrics::Metrics;
use crate::model::{parse_hex, Batches, Table, TABLES};
use crate::store::Store;

// ── request ─────────────────────────────────────────────────────────────

#[derive(Deserialize, Default, Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct LogSel {
    #[serde(default)]
    pub address: Vec<String>,
    /// topics[i]: accepted values at position i; empty = any.
    #[serde(default)]
    pub topics: Vec<Vec<String>>,
}

#[derive(Deserialize, Default, Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct TxSel {
    #[serde(default)]
    pub from: Vec<String>,
    #[serde(default)]
    pub to: Vec<String>,
    #[serde(default)]
    pub sighash: Vec<String>,
    #[serde(default)]
    pub contract_address: Vec<String>,
    #[serde(default)]
    pub hash: Vec<String>,
    pub status: Option<u64>,
}

#[derive(Deserialize, Default, Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct TraceSel {
    #[serde(default)]
    pub from: Vec<String>,
    #[serde(default)]
    pub to: Vec<String>,
    #[serde(default)]
    pub sighash: Vec<String>,
    #[serde(default)]
    pub call_type: Vec<String>,
}

#[derive(Deserialize, Default, Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct BlockSel {
    #[serde(default)]
    pub hash: Vec<String>,
    #[serde(default)]
    pub miner: Vec<String>,
}

#[derive(Deserialize, Default, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Join {
    /// Only the selected rows (and block headers if `include_all_blocks`).
    Nothing,
    /// Selected rows, the transaction of each selected log/trace, and the
    /// header of each block with a result row.
    #[default]
    Default,
    /// Default, plus every log and trace of each selected transaction.
    All,
}

#[derive(Deserialize, Default, Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct Fields {
    pub block: Option<Vec<String>>,
    pub transaction: Option<Vec<String>>,
    pub log: Option<Vec<String>>,
    pub trace: Option<Vec<String>>,
}

#[derive(Deserialize, Default, Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct Query {
    pub from_block: u64,
    /// Exclusive; None = up to the newest stored block.
    pub to_block: Option<u64>,
    #[serde(default)]
    pub logs: Vec<LogSel>,
    #[serde(default)]
    pub transactions: Vec<TxSel>,
    #[serde(default)]
    pub traces: Vec<TraceSel>,
    #[serde(default)]
    pub blocks: Vec<BlockSel>,
    #[serde(default)]
    pub include_all_blocks: bool,
    #[serde(default)]
    pub join: Join,
    #[serde(default)]
    pub fields: Fields,
    pub max_rows: Option<usize>,
    pub max_ms: Option<u64>,
}

pub struct Limits {
    pub max_rows: usize,
    pub max_time: Duration,
    pub parallelism: usize,
    pub max_selection_values: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Limits { max_rows: 200_000, max_time: Duration::from_secs(5), parallelism: 8, max_selection_values: 10_000 }
    }
}

// ── compiled request ────────────────────────────────────────────────────

type Set = Option<HashSet<Vec<u8>>>;

struct CField {
    col: &'static str,
    set: HashSet<Vec<u8>>,
    keys: Vec<u64>,
}

/// One selection: every field must match.
struct CSel {
    fields: Vec<CField>,
    status: Option<u64>,
    call_types: Option<HashSet<String>>,
    /// Hash lookups resolved to block numbers (None = no hash constraint).
    blocks: Option<BTreeSet<u64>>,
    hashes: Set,
}

impl CSel {
    fn needs(&self, cols: &mut BTreeSet<&'static str>, t: Table) {
        for f in &self.fields {
            cols.insert(f.col);
        }
        if self.status.is_some() {
            cols.insert("status");
        }
        if self.call_types.is_some() {
            cols.insert("type");
        }
        if self.hashes.is_some() {
            cols.insert("hash");
        }
        cols.insert(t.block_col());
    }
}

fn values<const N: usize>(field: &str, v: &[String]) -> Result<HashSet<Vec<u8>>> {
    v.iter()
        .map(|s| match parse_hex(s) {
            Some(b) if b.len() == N => Ok(b),
            _ => Err(anyhow!("{field}: {s:?} is not {N} bytes of hex")),
        })
        .collect()
}

fn field<const N: usize>(out: &mut Vec<CField>, col: &'static str, v: &[String]) -> Result<()> {
    if v.is_empty() {
        return Ok(());
    }
    let set = values::<N>(col, v)?;
    let keys = set.iter().map(|b| key(b)).collect();
    out.push(CField { col, set, keys });
    Ok(())
}

pub struct Plan {
    from: u64,
    to: u64,
    logs: Vec<CSel>,
    txs: Vec<CSel>,
    traces: Vec<CSel>,
    blocks: Vec<CSel>,
    include_all_blocks: bool,
    join: Join,
    out_cols: [Vec<&'static str>; 4],
    max_rows: usize,
    deadline: Instant,
    parallelism: usize,
}

const DEFAULT_FIELDS: [&[&str]; 4] = [
    &["number", "hash", "parent_hash", "timestamp"],
    &["block_number", "transaction_index", "hash", "from", "to", "value", "input", "status", "gas_used"],
    &["block_number", "log_index", "transaction_index", "transaction_hash", "address", "topic0", "topic1", "topic2", "topic3", "data"],
    &["block_number", "transaction_index", "trace_address", "type", "from", "to", "value", "input", "output", "error"],
];

fn out_cols(t: Table, f: &Option<Vec<String>>) -> Result<Vec<&'static str>> {
    let names: Vec<&'static str> = match f {
        None => DEFAULT_FIELDS[t.idx()].to_vec(),
        Some(v) if v.iter().any(|s| s == "*") => t.cols().iter().map(|c| c.name).collect(),
        Some(v) => v
            .iter()
            .map(|s| {
                t.cols()
                    .iter()
                    .find(|c| c.name == s)
                    .map(|c| c.name)
                    .ok_or_else(|| anyhow!("unknown {} field {s:?}", t.name()))
            })
            .collect::<Result<_>>()?,
    };
    // Schema order, no duplicates.
    Ok(t.cols().iter().map(|c| c.name).filter(|n| names.contains(n)).collect())
}

impl Plan {
    pub fn new(q: &Query, store: &Store, lim: &Limits) -> Result<Plan> {
        let last = store.last().map(|h| h.number + 1).unwrap_or(store.start_block());
        let to = q.to_block.unwrap_or(last).min(last);
        let n_values: usize = q.logs.iter().map(|s| s.address.len() + s.topics.iter().map(|t| t.len()).sum::<usize>()).sum::<usize>()
            + q.transactions.iter().map(|s| s.from.len() + s.to.len() + s.sighash.len() + s.hash.len() + s.contract_address.len()).sum::<usize>()
            + q.traces.iter().map(|s| s.from.len() + s.to.len() + s.sighash.len()).sum::<usize>()
            + q.blocks.iter().map(|s| s.hash.len() + s.miner.len()).sum::<usize>();
        if n_values > lim.max_selection_values {
            bail!("too many selection values ({n_values} > {})", lim.max_selection_values);
        }
        let mut logs = Vec::new();
        for s in &q.logs {
            if s.topics.len() > 4 {
                bail!("logs: at most 4 topic positions");
            }
            let mut f = Vec::new();
            field::<20>(&mut f, "address", &s.address)?;
            for (i, t) in s.topics.iter().enumerate() {
                field::<32>(&mut f, ["topic0", "topic1", "topic2", "topic3"][i], t)?;
            }
            logs.push(CSel { fields: f, status: None, call_types: None, blocks: None, hashes: None });
        }
        let mut txs = Vec::new();
        for s in &q.transactions {
            let mut f = Vec::new();
            field::<20>(&mut f, "from", &s.from)?;
            field::<20>(&mut f, "to", &s.to)?;
            field::<4>(&mut f, "sighash", &s.sighash)?;
            field::<20>(&mut f, "contract_address", &s.contract_address)?;
            let (blocks, hashes) = if s.hash.is_empty() {
                (None, None)
            } else {
                let set = values::<32>("hash", &s.hash)?;
                let mut bs = BTreeSet::new();
                for h in &set {
                    if let Some((b, _)) = store.hot.tx_location(h.as_slice().try_into().unwrap())? {
                        bs.insert(b);
                    }
                }
                (Some(bs), Some(set))
            };
            txs.push(CSel { fields: f, status: s.status, call_types: None, blocks, hashes });
        }
        let mut traces = Vec::new();
        for s in &q.traces {
            let mut f = Vec::new();
            field::<20>(&mut f, "from", &s.from)?;
            field::<20>(&mut f, "to", &s.to)?;
            field::<4>(&mut f, "sighash", &s.sighash)?;
            let ct = (!s.call_type.is_empty()).then(|| s.call_type.iter().map(|c| c.to_uppercase()).collect());
            traces.push(CSel { fields: f, status: None, call_types: ct, blocks: None, hashes: None });
        }
        let mut blocks = Vec::new();
        for s in &q.blocks {
            let mut f = Vec::new();
            field::<20>(&mut f, "miner", &s.miner)?;
            let (bs, hashes) = if s.hash.is_empty() {
                (None, None)
            } else {
                let set = values::<32>("hash", &s.hash)?;
                let mut bs = BTreeSet::new();
                for h in &set {
                    if let Some(n) = store.hot.block_number(h.as_slice().try_into().unwrap())? {
                        bs.insert(n);
                    }
                }
                (Some(bs), Some(set))
            };
            blocks.push(CSel { fields: f, status: None, call_types: None, blocks: bs, hashes });
        }
        let f = &q.fields;
        let out_cols = [
            out_cols(Table::Blocks, &f.block)?,
            out_cols(Table::Transactions, &f.transaction)?,
            out_cols(Table::Logs, &f.log)?,
            out_cols(Table::Traces, &f.trace)?,
        ];
        let max_rows = q.max_rows.unwrap_or(lim.max_rows).clamp(1, lim.max_rows);
        let max_time = q.max_ms.map(Duration::from_millis).unwrap_or(lim.max_time).min(lim.max_time);
        Ok(Plan {
            from: q.from_block,
            to,
            logs,
            txs,
            traces,
            blocks,
            include_all_blocks: q.include_all_blocks,
            join: q.join,
            out_cols,
            max_rows,
            deadline: Instant::now() + max_time,
            parallelism: lim.parallelism.max(1),
        })
    }
}

// ── data sources: a chunk, or the hot blocks ────────────────────────────

trait Source: Send + Sync {
    /// Row groups of table `t`: (min block, max block), or None if unknown.
    fn groups(&self, t: Table) -> usize;
    fn group_range(&self, t: Table, g: usize) -> (u64, u64);
    /// Might group `g` contain a row with value key `k` in column `col`?
    fn may_contain(&self, t: Table, g: usize, col: &str, keys: &[u64]) -> bool;
    fn read(&self, t: Table, groups: &[usize], cols: &[usize]) -> Result<Vec<RecordBatch>>;
}

impl Source for Chunk {
    fn groups(&self, t: Table) -> usize {
        self.index.tables[t.idx()].row_groups.len()
    }
    fn group_range(&self, t: Table, g: usize) -> (u64, u64) {
        let r = &self.index.tables[t.idx()].row_groups[g];
        (r.min_block, r.max_block)
    }
    fn may_contain(&self, t: Table, g: usize, col: &str, keys: &[u64]) -> bool {
        let r = &self.index.tables[t.idx()].row_groups[g];
        match filtered_cols(t).iter().position(|c| *c == col) {
            Some(i) => r.filters[i].any(keys),
            None => true,
        }
    }
    fn read(&self, t: Table, groups: &[usize], cols: &[usize]) -> Result<Vec<RecordBatch>> {
        Chunk::read(self, t, groups, cols)
    }
}

struct HotSource {
    batches: Batches,
    ranges: [(u64, u64); 4],
}

impl HotSource {
    fn new(blocks: &[Arc<HotBlock>]) -> HotSource {
        let parts: Vec<Batches> = blocks.iter().map(|b| b.batches.clone()).collect();
        let lo = blocks.first().map(|b| b.head.number).unwrap_or(0);
        let hi = blocks.last().map(|b| b.head.number).unwrap_or(0);
        HotSource { batches: Batches::concat(&parts), ranges: [(lo, hi); 4] }
    }
}

impl Source for HotSource {
    fn groups(&self, t: Table) -> usize {
        usize::from(self.batches.get(t).num_rows() > 0)
    }
    fn group_range(&self, t: Table, _g: usize) -> (u64, u64) {
        self.ranges[t.idx()]
    }
    fn may_contain(&self, _t: Table, _g: usize, _col: &str, _keys: &[u64]) -> bool {
        true
    }
    fn read(&self, t: Table, _groups: &[usize], cols: &[usize]) -> Result<Vec<RecordBatch>> {
        Ok(vec![self.batches.get(t).project(cols)?])
    }
}

// ── per-source execution ────────────────────────────────────────────────

/// Rows matched in one source, per table, in block order.
#[derive(Default)]
pub struct Part {
    pub tables: [Vec<RecordBatch>; 4],
    pub rows: usize,
    groups_read: u64,
    groups_skipped: u64,
}

/// (block, tx index) keys for joins.
type TxKeys = HashSet<(u64, u64)>;

struct Ctx<'a> {
    plan: &'a Plan,
    src: &'a dyn Source,
    from: u64,
    to: u64,
    read: u64,
    skipped: u64,
}

fn overlaps(r: (u64, u64), from: u64, to: u64) -> bool {
    r.1 >= from && r.0 < to
}

fn any_in(set: &BTreeSet<u64>, r: (u64, u64)) -> bool {
    set.range(r.0..=r.1).next().is_some()
}

impl Ctx<'_> {
    /// Row groups of `t` that may hold a row matching a selection, or a
    /// joined key in `join_blocks`.
    fn candidates(&mut self, t: Table, sels: &[CSel], all: bool, join_blocks: Option<&BTreeSet<u64>>) -> Vec<usize> {
        let mut out = Vec::new();
        for g in 0..self.src.groups(t) {
            let r = self.src.group_range(t, g);
            if !overlaps(r, self.from, self.to) {
                continue;
            }
            let hit = all
                || join_blocks.is_some_and(|b| any_in(b, r))
                || sels.iter().any(|s| {
                    s.blocks.as_ref().is_none_or(|b| any_in(b, r))
                        && s.fields.iter().all(|f| self.src.may_contain(t, g, f.col, &f.keys))
                });
            if hit {
                out.push(g);
            } else {
                self.skipped += 1;
            }
        }
        self.read += out.len() as u64;
        out
    }

    /// Reads candidate groups of `t` and keeps rows that match a selection
    /// (or all rows when `all`) or whose (block, tx index) is in `join`.
    fn scan(
        &mut self,
        t: Table,
        sels: &[CSel],
        all: bool,
        join: Option<(&TxKeys, &BTreeSet<u64>)>,
        extra_cols: &[&'static str],
    ) -> Result<Vec<RecordBatch>> {
        if !all && sels.is_empty() && join.is_none_or(|(k, _)| k.is_empty()) {
            return Ok(Vec::new());
        }
        let groups = self.candidates(t, sels, all, join.map(|j| j.1));
        if groups.is_empty() {
            return Ok(Vec::new());
        }
        let mut need: BTreeSet<&'static str> = self.plan.out_cols[t.idx()].iter().copied().collect();
        need.insert(t.block_col());
        need.extend(extra_cols.iter().copied());
        for s in sels {
            s.needs(&mut need, t);
        }
        if join.is_some() || t != Table::Blocks {
            if t != Table::Blocks {
                need.insert("transaction_index");
            }
        }
        let idx: Vec<usize> = t.cols().iter().enumerate().filter(|(_, c)| need.contains(c.name)).map(|(i, _)| i).collect();
        let mut out = Vec::new();
        for b in self.src.read(t, &groups, &idx)? {
            let mask = mask(t, &b, sels, all, join.map(|j| j.0), self.from, self.to)?;
            let f = filter_record_batch(&b, &mask)?;
            if f.num_rows() > 0 {
                out.push(f);
            }
        }
        Ok(out)
    }
}

fn fixed<'a>(b: &'a RecordBatch, n: &str) -> Option<&'a FixedSizeBinaryArray> {
    b.column_by_name(n).and_then(|a| a.as_any().downcast_ref())
}
fn u64s<'a>(b: &'a RecordBatch, n: &str) -> Option<&'a UInt64Array> {
    b.column_by_name(n).and_then(|a| a.as_any().downcast_ref())
}
fn strs<'a>(b: &'a RecordBatch, n: &str) -> Option<&'a StringArray> {
    b.column_by_name(n).and_then(|a| a.as_any().downcast_ref())
}

fn mask(
    t: Table,
    b: &RecordBatch,
    sels: &[CSel],
    all: bool,
    join: Option<&TxKeys>,
    from: u64,
    to: u64,
) -> Result<BooleanArray> {
    let blocks = u64s(b, t.block_col()).ok_or_else(|| anyhow!("no block column"))?;
    let txi = u64s(b, "transaction_index");
    // Resolve columns once per selection.
    struct R<'a> {
        fields: Vec<(&'a FixedSizeBinaryArray, &'a HashSet<Vec<u8>>)>,
        status: Option<(&'a UInt64Array, u64)>,
        types: Option<(&'a StringArray, &'a HashSet<String>)>,
        hashes: Option<(&'a FixedSizeBinaryArray, &'a HashSet<Vec<u8>>)>,
        blocks: Option<&'a BTreeSet<u64>>,
    }
    let mut rs = Vec::with_capacity(sels.len());
    for s in sels {
        let mut fields = Vec::new();
        for f in &s.fields {
            fields.push((fixed(b, f.col).ok_or_else(|| anyhow!("column {}", f.col))?, &f.set));
        }
        rs.push(R {
            fields,
            status: s.status.map(|v| (u64s(b, "status").expect("status"), v)),
            types: s.call_types.as_ref().map(|c| (strs(b, "type").expect("type"), c)),
            hashes: s.hashes.as_ref().map(|h| (fixed(b, "hash").expect("hash"), h)),
            blocks: s.blocks.as_ref(),
        });
    }
    let mut m = BooleanBufferBuilder::new(b.num_rows());
    for i in 0..b.num_rows() {
        let bn = blocks.value(i);
        if bn < from || bn >= to {
            m.append(false);
            continue;
        }
        let mut hit = all;
        if !hit {
            if let (Some(keys), Some(txi)) = (join, txi) {
                hit = keys.contains(&(bn, txi.value(i)));
            }
        }
        if !hit {
            hit = rs.iter().any(|r| {
                r.blocks.is_none_or(|s| s.contains(&bn))
                    && r.fields.iter().all(|(a, set)| a.is_valid(i) && set.contains(a.value(i)))
                    && r.status.is_none_or(|(a, v)| a.is_valid(i) && a.value(i) == v)
                    && r.types.is_none_or(|(a, set)| a.is_valid(i) && set.contains(a.value(i)))
                    && r.hashes.is_none_or(|(a, set)| a.is_valid(i) && set.contains(a.value(i)))
            });
        }
        m.append(hit);
    }
    Ok(BooleanArray::new(m.finish(), None))
}

fn keys_of(bs: &[RecordBatch], t: Table) -> (TxKeys, BTreeSet<u64>) {
    let mut k = HashSet::new();
    let mut blocks = BTreeSet::new();
    for b in bs {
        let (Some(bn), Some(ti)) = (u64s(b, t.block_col()), u64s(b, "transaction_index")) else { continue };
        for i in 0..b.num_rows() {
            k.insert((bn.value(i), ti.value(i)));
            blocks.insert(bn.value(i));
        }
    }
    (k, blocks)
}

fn block_set(bs: &[RecordBatch], t: Table, into: &mut BTreeSet<u64>) {
    for b in bs {
        if let Some(bn) = u64s(b, t.block_col()) {
            into.extend(bn.values().iter().copied());
        }
    }
}

fn run_source(plan: &Plan, src: &dyn Source, from: u64, to: u64) -> Result<Part> {
    let mut c = Ctx { plan, src, from, to, read: 0, skipped: 0 };
    let mut part = Part::default();
    let join = plan.join;

    // 1. Transactions selected by their own fields (needed first for join=all).
    let tx_sel = if join == Join::All && !plan.txs.is_empty() {
        c.scan(Table::Transactions, &plan.txs, false, None, &[])?
    } else {
        Vec::new()
    };
    let (tx_sel_keys, tx_sel_blocks) = keys_of(&tx_sel, Table::Transactions);
    let tx_join = (join == Join::All).then_some((&tx_sel_keys, &tx_sel_blocks));

    // 2. Logs and traces: own selections, plus (join=all) rows of selected txs.
    let logs = c.scan(Table::Logs, &plan.logs, false, tx_join, &[])?;
    let traces = c.scan(Table::Traces, &plan.traces, false, tx_join, &[])?;

    // 3. Transactions: own selections, plus the tx of every log/trace.
    let txs = if join == Join::Nothing {
        c.scan(Table::Transactions, &plan.txs, false, None, &[])?
    } else {
        let (mut k, mut bs) = keys_of(&logs, Table::Logs);
        let (k2, b2) = keys_of(&traces, Table::Traces);
        k.extend(k2);
        bs.extend(b2);
        if join == Join::All {
            // Already selected: keep them without re-reading.
            k.extend(tx_sel_keys.iter().copied());
            bs.extend(tx_sel_blocks.iter().copied());
            c.scan(Table::Transactions, &[], false, Some((&k, &bs)), &[])?
        } else {
            c.scan(Table::Transactions, &plan.txs, false, Some((&k, &bs)), &[])?
        }
    };

    // 4. Block headers.
    let blocks = if plan.include_all_blocks {
        c.scan(Table::Blocks, &[], true, None, &[])?
    } else {
        let mut bs = BTreeSet::new();
        if join != Join::Nothing {
            block_set(&logs, Table::Logs, &mut bs);
            block_set(&traces, Table::Traces, &mut bs);
            block_set(&txs, Table::Transactions, &mut bs);
        }
        let sel = c.scan(Table::Blocks, &plan.blocks, false, None, &[])?;
        if bs.is_empty() {
            sel
        } else {
            // Selected blocks or blocks with results: one more pass with
            // a block-number predicate.
            block_set(&sel, Table::Blocks, &mut bs);
            let pseudo = CSel { fields: vec![], status: None, call_types: None, blocks: Some(bs), hashes: None };
            c.scan(Table::Blocks, std::slice::from_ref(&pseudo), false, None, &[])?
        }
    };

    for (t, v) in [(Table::Blocks, blocks), (Table::Transactions, txs), (Table::Logs, logs), (Table::Traces, traces)] {
        let cols = &plan.out_cols[t.idx()];
        let mut out = Vec::with_capacity(v.len());
        for b in v {
            let idx: Vec<usize> = cols.iter().map(|n| b.schema().index_of(n)).collect::<Result<_, _>>()?;
            let p = b.project(&idx)?;
            part.rows += p.num_rows();
            out.push(p);
        }
        part.tables[t.idx()] = out;
    }
    part.groups_read = c.read;
    part.groups_skipped = c.skipped;
    Ok(part)
}

// ── result ──────────────────────────────────────────────────────────────

#[derive(Serialize, Clone, Debug)]
pub struct Guard {
    pub block_number: u64,
    pub hash: String,
}

pub struct Response {
    pub tables: [Vec<RecordBatch>; 4],
    pub rows: usize,
    pub next_block: u64,
    pub archive_height: Option<u64>,
    pub chain_head: u64,
    pub finalized: u64,
    /// Hash of the last block covered, when it is in the (reorg-able) hot tier.
    pub last_block: Option<Guard>,
    pub elapsed: Duration,
}

/// Cuts `part` so that only rows of blocks `< end` remain.
fn cut(part: &mut Part, end: u64) {
    part.rows = 0;
    for t in TABLES {
        let v = std::mem::take(&mut part.tables[t.idx()]);
        let mut out = Vec::new();
        for b in v {
            let bn = u64s(&b, t.block_col()).cloned();
            let b = match bn {
                Some(bn) => {
                    let m: BooleanArray = bn.iter().map(|x| Some(x.unwrap_or(0) < end)).collect();
                    filter_record_batch(&b, &m).unwrap_or(b)
                }
                None => b,
            };
            part.rows += b.num_rows();
            out.push(b);
        }
        part.tables[t.idx()] = out;
    }
}

/// Smallest block `e` such that rows of blocks `< e` exceed `budget`, or
/// None if the whole part fits.
fn overflow_block(part: &Part, budget: usize) -> Option<u64> {
    if part.rows <= budget {
        return None;
    }
    let mut counts: std::collections::BTreeMap<u64, usize> = Default::default();
    for t in TABLES {
        for b in &part.tables[t.idx()] {
            if let Some(bn) = u64s(b, t.block_col()) {
                for x in bn.values() {
                    *counts.entry(*x).or_default() += 1;
                }
            }
        }
    }
    let mut acc = 0;
    for (bn, c) in counts {
        acc += c;
        if acc > budget {
            return Some(bn);
        }
    }
    None
}

pub async fn execute(store: Arc<Store>, plan: Plan, metrics: &Metrics) -> Result<Response> {
    let t0 = Instant::now();
    let plan = Arc::new(plan);
    let (from, to) = (plan.from, plan.to);
    let view = store.view(from, to.max(from));
    let mut tables: [Vec<RecordBatch>; 4] = Default::default();
    let mut rows = 0usize;
    let mut next = from;
    let mut done = false;

    let mut tasks = futures::stream::iter(view.chunks.clone())
        .map(|c| {
            let plan = plan.clone();
            tokio::task::spawn_blocking(move || {
                let (a, b) = (plan.from.max(c.from_block()), plan.to.min(c.to_block()));
                run_source(&plan, c.as_ref(), a, b).map(|p| (p, b))
            })
        })
        .buffered(plan.parallelism);

    let mut take = |mut part: Part, end: u64, rows: &mut usize, next: &mut u64| -> bool {
        metrics.row_groups_read.fetch_add(part.groups_read, std::sync::atomic::Ordering::Relaxed);
        metrics.row_groups_skipped.fetch_add(part.groups_skipped, std::sync::atomic::Ordering::Relaxed);
        let budget = plan.max_rows.saturating_sub(*rows);
        let mut stop = false;
        let mut end = end;
        if let Some(e) = overflow_block(&part, budget) {
            // Always return at least one block's rows.
            let e = if *rows == 0 && e == *next { e + 1 } else { e };
            if e <= *next {
                return true;
            }
            cut(&mut part, e);
            end = e;
            stop = true;
        }
        for t in TABLES {
            tables[t.idx()].append(&mut part.tables[t.idx()]);
        }
        *rows += part.rows;
        *next = end;
        stop || Instant::now() >= plan.deadline
    };

    while let Some(r) = tasks.next().await {
        let (part, end) = r??;
        if take(part, end, &mut rows, &mut next) {
            done = true;
            break;
        }
    }
    drop(tasks);
    let mut last_block = None;
    if !done && !view.hot.is_empty() {
        let hot = view.hot.clone();
        let plan2 = plan.clone();
        let (a, b) = (next.max(from), view.end);
        let part = tokio::task::spawn_blocking(move || {
            let src = HotSource::new(&hot);
            run_source(&plan2, &src, a, b)
        })
        .await??;
        take(part, view.end, &mut rows, &mut next);
    }
    if let Some(h) = view.hot.iter().find(|b| b.head.number + 1 == next) {
        last_block = Some(Guard { block_number: h.head.number, hash: crate::model::hex0x(&h.head.hash) });
    }
    if next == from && !done {
        next = view.end.max(from);
    }
    metrics.query_rows.fetch_add(rows as u64, std::sync::atomic::Ordering::Relaxed);
    Ok(Response {
        tables,
        rows,
        next_block: next,
        archive_height: store.last().map(|h| h.number),
        chain_head: store.chain_head.load(std::sync::atomic::Ordering::Relaxed),
        finalized: store.finalized.load(std::sync::atomic::Ordering::Relaxed),
        last_block,
        elapsed: t0.elapsed(),
    })
}
