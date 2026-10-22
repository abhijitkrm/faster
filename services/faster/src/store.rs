//! RocksDB hot store. Key layout (all integers big-endian so keys sort
//! numerically; `^x` is the bitwise complement, giving newest-first order):
//!
//! | CF         | key                              | value                    |
//! |------------|----------------------------------|--------------------------|
//! | meta       | "head" / "counters" / "version"  | Head / Counters / u32    |
//! | blocks     | number u64                       | BlockRec                 |
//! | block_hash | hash 32                          | number u64               |
//! | txs        | number u64 ‖ index u32           | TxRec                    |
//! | tx_hash    | hash 32                          | number u64 ‖ index u32   |
//! | logs       | number u64 ‖ log index u32       | LogRec                   |
//! | addr_tx    | addr 20 ‖ ^number u64 ‖ ^idx u32 | role bits u8             |
//! | addr       | addr 20                          | AddrInfo                 |
//! | undo       | number u64                       | Undo (prior addr/counter state) |
//!
//! Every block — its rows, address updates, counters, undo record and the
//! head pointer — is written in one atomic WriteBatch, so the head is always
//! consistent with the data (there is no separate checkpoint to lose).

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};
use rocksdb::{
    BlockBasedOptions, Cache, ColumnFamily, ColumnFamilyDescriptor, DBCompressionType, Direction,
    IteratorMode, Options, ReadOptions, WriteBatch, WriteOptions, DB,
};
use serde::{Deserialize, Serialize};

use crate::types::*;

const SCHEMA_VERSION: u32 = 1;

pub const ROLE_FROM: u8 = 1;
pub const ROLE_TO: u8 = 2;
pub const ROLE_CREATED: u8 = 4;

const CF_META: &str = "meta";
const CF_BLOCKS: &str = "blocks";
const CF_BLOCK_HASH: &str = "block_hash";
const CF_TXS: &str = "txs";
const CF_TX_HASH: &str = "tx_hash";
const CF_LOGS: &str = "logs";
const CF_ADDR_TX: &str = "addr_tx";
const CF_ADDR: &str = "addr";
const CF_UNDO: &str = "undo";
const CFS: [&str; 9] = [CF_META, CF_BLOCKS, CF_BLOCK_HASH, CF_TXS, CF_TX_HASH, CF_LOGS, CF_ADDR_TX, CF_ADDR, CF_UNDO];

#[derive(Serialize, Deserialize, Default)]
struct Undo {
    counters: Counters,
    /// State of every address the block touched, before the block.
    addrs: Vec<(Addr, Option<AddrInfo>)>,
}

pub struct Store {
    db: DB,
    /// Blocks of undo history kept; deeper reorgs need a resync.
    reorg_depth: u64,
}

pub struct StoreConfig {
    pub cache_bytes: usize,
    pub reorg_depth: u64,
}

fn enc<T: Serialize>(v: &T) -> Vec<u8> {
    bincode::serialize(v).expect("bincode serialize")
}

fn dec_rec<T: for<'de> Deserialize<'de>>(b: &[u8]) -> Result<T> {
    bincode::deserialize(b).map_err(|e| anyhow!("corrupt record: {e}"))
}

pub fn k_num(n: u64) -> [u8; 8] {
    n.to_be_bytes()
}

pub fn k_num_idx(n: u64, i: u32) -> [u8; 12] {
    let mut k = [0u8; 12];
    k[..8].copy_from_slice(&n.to_be_bytes());
    k[8..].copy_from_slice(&i.to_be_bytes());
    k
}

pub fn k_addr_tx(a: &Addr, n: u64, i: u32) -> [u8; 32] {
    let mut k = [0u8; 32];
    k[..20].copy_from_slice(a);
    k[20..28].copy_from_slice(&(!n).to_be_bytes());
    k[28..].copy_from_slice(&(!i).to_be_bytes());
    k
}

fn split_num_idx(k: &[u8]) -> (u64, u32) {
    (u64::from_be_bytes(k[..8].try_into().unwrap()), u32::from_be_bytes(k[8..12].try_into().unwrap()))
}

/// Addresses a tx touches, with role bits; one entry per distinct address so
/// a self-transfer counts once.
fn tx_parties(t: &TxRec) -> Vec<(Addr, u8)> {
    let mut v: Vec<(Addr, u8)> = vec![(t.from, ROLE_FROM)];
    let mut add = |a: Addr, role: u8| match v.iter_mut().find(|(x, _)| *x == a) {
        Some(e) => e.1 |= role,
        None => v.push((a, role)),
    };
    if let Some(to) = t.to {
        add(to, ROLE_TO);
    }
    if let Some(c) = t.contract_address {
        add(c, ROLE_CREATED);
    }
    v
}

impl Store {
    pub fn open(path: &Path, cfg: StoreConfig) -> Result<Arc<Store>> {
        let cache = Cache::new_lru_cache(cfg.cache_bytes);
        let table = |bloom: bool| {
            let mut t = BlockBasedOptions::default();
            t.set_block_cache(&cache);
            t.set_cache_index_and_filter_blocks(true);
            t.set_pin_l0_filter_and_index_blocks_in_cache(true);
            t.set_format_version(6);
            if bloom {
                t.set_bloom_filter(10.0, false);
                t.set_optimize_filters_for_memory(true);
            }
            t
        };
        let cf_opts = |bloom: bool| {
            let mut o = Options::default();
            o.set_compression_type(DBCompressionType::Lz4);
            o.set_block_based_table_factory(&table(bloom));
            o.set_level_compaction_dynamic_level_bytes(true);
            o.set_write_buffer_size(64 << 20);
            o
        };
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        opts.increase_parallelism(std::thread::available_parallelism().map(|n| n.get() as i32).unwrap_or(4));
        opts.set_max_background_jobs(4);
        opts.set_bytes_per_sync(1 << 20);
        // Hash lookups are random point reads: bloom filters avoid touching
        // SST files that cannot hold the key.
        let descs = CFS.iter().map(|name| {
            let bloom = matches!(*name, CF_BLOCK_HASH | CF_TX_HASH | CF_ADDR);
            ColumnFamilyDescriptor::new(*name, cf_opts(bloom))
        });
        let db = DB::open_cf_descriptors(&opts, path, descs).with_context(|| format!("open {}", path.display()))?;
        let s = Store { db, reorg_depth: cfg.reorg_depth };
        match s.get_meta::<u32>("version")? {
            None => {
                let mut wb = WriteBatch::default();
                wb.put_cf(&s.cf(CF_META), "version", enc(&SCHEMA_VERSION));
                s.db.write(wb)?;
            }
            Some(v) if v == SCHEMA_VERSION => {}
            Some(v) => bail!("store schema version {v}, binary expects {SCHEMA_VERSION}; resync required"),
        }
        Ok(Arc::new(s))
    }

    fn cf(&self, name: &str) -> &ColumnFamily {
        self.db.cf_handle(name).expect("column family")
    }

    fn get_meta<T: for<'de> Deserialize<'de>>(&self, key: &str) -> Result<Option<T>> {
        self.db.get_pinned_cf(&self.cf(CF_META), key)?.map(|v| dec_rec(&v)).transpose()
    }

    fn get_rec<T: for<'de> Deserialize<'de>>(&self, cf: &str, key: &[u8]) -> Result<Option<T>> {
        self.db.get_pinned_cf(&self.cf(cf), key)?.map(|v| dec_rec(&v)).transpose()
    }

    pub fn head(&self) -> Result<Option<Head>> {
        self.get_meta("head")
    }

    pub fn counters(&self) -> Result<Counters> {
        Ok(self.get_meta("counters")?.unwrap_or_default())
    }

    /// Appends consecutive blocks on top of the current head in one atomic,
    /// durable write. The caller has checked that they chain onto the head.
    pub fn apply(&self, blocks: &[FullBlock]) -> Result<()> {
        if blocks.is_empty() {
            return Ok(());
        }
        let head = self.head()?;
        let mut expect = head.map(|h| (h.number + 1, h.hash));
        let mut counters = self.counters()?;
        let mut addrs: HashMap<Addr, Option<AddrInfo>> = HashMap::new();
        let mut wb = WriteBatch::default();
        let (cf_blocks, cf_bh, cf_txs, cf_th, cf_logs, cf_at, cf_addr, cf_undo) = (
            self.cf(CF_BLOCKS),
            self.cf(CF_BLOCK_HASH),
            self.cf(CF_TXS),
            self.cf(CF_TX_HASH),
            self.cf(CF_LOGS),
            self.cf(CF_ADDR_TX),
            self.cf(CF_ADDR),
            self.cf(CF_UNDO),
        );
        for fb in blocks {
            let b = &fb.block;
            if let Some((n, parent)) = expect {
                if b.number != n || b.parent_hash != parent {
                    bail!("block {} does not chain onto {} (internal error)", b.number, n - 1);
                }
            }
            let mut undo = Undo { counters, addrs: Vec::new() };
            wb.put_cf(&cf_blocks, k_num(b.number), enc(b));
            wb.put_cf(&cf_bh, b.hash, k_num(b.number));
            for t in &fb.txs {
                let k = k_num_idx(b.number, t.index);
                wb.put_cf(&cf_txs, k, enc(t));
                wb.put_cf(&cf_th, t.hash, k);
                for (a, role) in tx_parties(t) {
                    wb.put_cf(&cf_at, k_addr_tx(&a, b.number, t.index), [role]);
                    let cur = match addrs.get(&a) {
                        Some(v) => *v,
                        None => self.get_rec::<AddrInfo>(CF_ADDR, &a)?,
                    };
                    if !undo.addrs.iter().any(|(x, _)| *x == a) {
                        undo.addrs.push((a, cur));
                    }
                    let mut info = cur.unwrap_or_else(|| {
                        counters.addresses += 1;
                        AddrInfo { first_block: b.number, first_ts: b.timestamp, ..Default::default() }
                    });
                    info.tx_count += 1;
                    info.last_block = b.number;
                    info.last_ts = b.timestamp;
                    if role & ROLE_CREATED != 0 {
                        info.is_contract = true;
                        info.creator = Some((b.number, t.index));
                    }
                    addrs.insert(a, Some(info));
                }
            }
            for (i, l) in fb.logs.iter().enumerate() {
                wb.put_cf(&cf_logs, k_num_idx(b.number, i as u32), enc(l));
            }
            counters.blocks += 1;
            counters.txs += fb.txs.len() as u64;
            counters.logs += fb.logs.len() as u64;
            wb.put_cf(&cf_undo, k_num(b.number), enc(&undo));
            if b.number >= self.reorg_depth {
                wb.delete_cf(&cf_undo, k_num(b.number - self.reorg_depth));
            }
            expect = Some((b.number + 1, b.hash));
        }
        for (a, info) in &addrs {
            if let Some(info) = info {
                wb.put_cf(&cf_addr, a, enc(info));
            }
        }
        let last = &blocks.last().unwrap().block;
        let cf_meta = self.cf(CF_META);
        wb.put_cf(&cf_meta, "counters", enc(&counters));
        wb.put_cf(&cf_meta, "head", enc(&Head { number: last.number, hash: last.hash }));
        let mut wo = WriteOptions::default();
        wo.set_sync(true);
        self.db.write_opt(wb, &wo)?;
        Ok(())
    }

    /// Removes the head block and restores the state before it. Returns the
    /// new head (None if the store is now empty).
    pub fn rollback_head(&self) -> Result<Option<Head>> {
        let head = self.head()?.ok_or_else(|| anyhow!("rollback on empty store"))?;
        let n = head.number;
        let undo: Undo = self
            .get_rec(CF_UNDO, &k_num(n))?
            .ok_or_else(|| anyhow!("no undo record for block {n}: reorg deeper than REORG_DEPTH, resync required"))?;
        let b: BlockRec = self.get_rec(CF_BLOCKS, &k_num(n))?.ok_or_else(|| anyhow!("block {n} missing"))?;
        let mut wb = WriteBatch::default();
        for i in 0..b.tx_count {
            let k = k_num_idx(n, i);
            let t: TxRec = self.get_rec(CF_TXS, &k)?.ok_or_else(|| anyhow!("tx {n}/{i} missing"))?;
            wb.delete_cf(&self.cf(CF_TXS), k);
            wb.delete_cf(&self.cf(CF_TX_HASH), t.hash);
            for (a, _) in tx_parties(&t) {
                wb.delete_cf(&self.cf(CF_ADDR_TX), k_addr_tx(&a, n, i));
            }
        }
        wb.delete_range_cf(&self.cf(CF_LOGS), k_num_idx(n, 0), k_num_idx(n + 1, 0));
        for (a, prev) in &undo.addrs {
            match prev {
                Some(info) => wb.put_cf(&self.cf(CF_ADDR), a, enc(info)),
                None => wb.delete_cf(&self.cf(CF_ADDR), a),
            }
        }
        wb.delete_cf(&self.cf(CF_BLOCKS), k_num(n));
        wb.delete_cf(&self.cf(CF_BLOCK_HASH), b.hash);
        wb.delete_cf(&self.cf(CF_UNDO), k_num(n));
        let cf_meta = self.cf(CF_META);
        wb.put_cf(&cf_meta, "counters", enc(&undo.counters));
        let new_head = if n == 0 {
            None
        } else {
            match self.block(n - 1)? {
                Some(p) => Some(Head { number: p.number, hash: p.hash }),
                None => None,
            }
        };
        match &new_head {
            Some(h) => wb.put_cf(&cf_meta, "head", enc(h)),
            None => wb.delete_cf(&cf_meta, "head"),
        }
        let mut wo = WriteOptions::default();
        wo.set_sync(true);
        self.db.write_opt(wb, &wo)?;
        Ok(new_head)
    }

    // ---- reads ----

    pub fn block(&self, n: u64) -> Result<Option<BlockRec>> {
        self.get_rec(CF_BLOCKS, &k_num(n))
    }

    pub fn block_by_hash(&self, h: &H256) -> Result<Option<BlockRec>> {
        match self.db.get_pinned_cf(&self.cf(CF_BLOCK_HASH), h)? {
            Some(v) => self.block(u64::from_be_bytes(v[..8].try_into()?)),
            None => Ok(None),
        }
    }

    /// Newest-first blocks with number <= `from` (or from the head).
    pub fn blocks_desc(&self, from: Option<u64>, limit: usize) -> Result<Vec<BlockRec>> {
        let start = from.map(k_num);
        let mode = match &start {
            Some(k) => IteratorMode::From(k, Direction::Reverse),
            None => IteratorMode::End,
        };
        let mut out = Vec::with_capacity(limit);
        for kv in self.db.iterator_cf(&self.cf(CF_BLOCKS), mode).take(limit) {
            let (_, v) = kv?;
            out.push(dec_rec(&v)?);
        }
        Ok(out)
    }

    pub fn blocks_range(&self, start: u64, end: u64) -> Result<Vec<BlockRec>> {
        let mut ro = ReadOptions::default();
        ro.set_iterate_upper_bound(k_num(end.saturating_add(1)).to_vec());
        let mut out = Vec::new();
        let k = k_num(start);
        for kv in self.db.iterator_cf_opt(&self.cf(CF_BLOCKS), ro, IteratorMode::From(&k, Direction::Forward)) {
            let (_, v) = kv?;
            out.push(dec_rec(&v)?);
        }
        Ok(out)
    }

    pub fn block_txs(&self, n: u64) -> Result<Vec<TxRec>> {
        let mut ro = ReadOptions::default();
        ro.set_iterate_upper_bound(k_num_idx(n + 1, 0).to_vec());
        let k = k_num_idx(n, 0);
        let mut out = Vec::new();
        for kv in self.db.iterator_cf_opt(&self.cf(CF_TXS), ro, IteratorMode::From(&k, Direction::Forward)) {
            let (_, v) = kv?;
            out.push(dec_rec(&v)?);
        }
        Ok(out)
    }

    /// Newest-first transactions across all blocks.
    pub fn latest_txs(&self, limit: usize) -> Result<Vec<TxRec>> {
        let mut out = Vec::with_capacity(limit);
        for kv in self.db.iterator_cf(&self.cf(CF_TXS), IteratorMode::End).take(limit) {
            let (_, v) = kv?;
            out.push(dec_rec(&v)?);
        }
        Ok(out)
    }

    pub fn tx(&self, n: u64, i: u32) -> Result<Option<TxRec>> {
        self.get_rec(CF_TXS, &k_num_idx(n, i))
    }

    pub fn tx_by_hash(&self, h: &H256) -> Result<Option<TxRec>> {
        match self.db.get_pinned_cf(&self.cf(CF_TX_HASH), h)? {
            Some(v) => {
                let (n, i) = split_num_idx(&v);
                self.tx(n, i)
            }
            None => Ok(None),
        }
    }

    pub fn tx_logs(&self, t: &TxRec) -> Result<Vec<(u32, LogRec)>> {
        if t.log_count == 0 {
            return Ok(Vec::new());
        }
        let keys: Vec<_> =
            (t.first_log_index..t.first_log_index + t.log_count).map(|i| k_num_idx(t.block_number, i)).collect();
        let cf = self.cf(CF_LOGS);
        let mut out = Vec::with_capacity(keys.len());
        for (k, v) in keys.iter().zip(self.db.batched_multi_get_cf(&cf, keys.iter(), true)) {
            if let Some(v) = v? {
                out.push((split_num_idx(k).1, dec_rec(&v)?));
            }
        }
        Ok(out)
    }

    pub fn addr_info(&self, a: &Addr) -> Result<Option<AddrInfo>> {
        self.get_rec(CF_ADDR, a)
    }

    /// Newest-first transactions involving `a`, skipping `offset`.
    pub fn addr_txs(&self, a: &Addr, offset: usize, limit: usize) -> Result<Vec<TxRec>> {
        let mut ro = ReadOptions::default();
        let mut upper = [0u8; 20];
        upper.copy_from_slice(a);
        // Upper bound = address + 1 (addresses are fixed width, so the
        // carry never needs to spill past byte 0 for a real key).
        let mut carry = true;
        for b in upper.iter_mut().rev() {
            if carry {
                let (v, c) = b.overflowing_add(1);
                *b = v;
                carry = c;
            }
        }
        if !carry {
            ro.set_iterate_upper_bound(upper.to_vec());
        }
        let mut keys = Vec::with_capacity(limit);
        for kv in self
            .db
            .iterator_cf_opt(&self.cf(CF_ADDR_TX), ro, IteratorMode::From(a, Direction::Forward))
            .skip(offset)
            .take(limit)
        {
            let (k, _) = kv?;
            let n = !u64::from_be_bytes(k[20..28].try_into()?);
            let i = !u32::from_be_bytes(k[28..32].try_into()?);
            keys.push(k_num_idx(n, i));
        }
        let cf = self.cf(CF_TXS);
        let mut out = Vec::with_capacity(keys.len());
        for v in self.db.batched_multi_get_cf(&cf, keys.iter(), true) {
            if let Some(v) = v? {
                out.push(dec_rec(&v)?);
            }
        }
        Ok(out)
    }

    /// Addresses whose hex form starts with `prefix` (lower-case, no 0x).
    pub fn addrs_with_prefix(&self, prefix: &str, limit: usize) -> Result<Vec<(Addr, AddrInfo)>> {
        let even = &prefix[..prefix.len() & !1];
        let start = hex::decode(even).map_err(|_| anyhow!("bad hex prefix"))?;
        let mut out = Vec::new();
        for kv in self.db.iterator_cf(&self.cf(CF_ADDR), IteratorMode::From(&start, Direction::Forward)) {
            let (k, v) = kv?;
            if !k.starts_with(&start) {
                break;
            }
            if !hex::encode(&k).starts_with(prefix) {
                continue;
            }
            out.push((k[..20].try_into()?, dec_rec(&v)?));
            if out.len() >= limit {
                break;
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn h(n: u64, fork: u8) -> H256 {
        let mut x = [fork; 32];
        x[..8].copy_from_slice(&n.to_be_bytes());
        x
    }

    fn a(x: u8) -> Addr {
        [x; 20]
    }

    fn block(n: u64, fork: u8, parent_fork: u8, txs: Vec<(Addr, Option<Addr>, Option<Addr>)>) -> FullBlock {
        let parent = if n == 0 { [0; 32] } else { h(n - 1, parent_fork) };
        let mut recs = Vec::new();
        let mut logs = Vec::new();
        for (i, (from, to, created)) in txs.into_iter().enumerate() {
            let mut th = h(n, fork);
            th[30] = i as u8;
            th[31] = 0xee;
            logs.push(LogRec { tx_index: i as u32, address: from, topics: vec![th], data: vec![1] });
            recs.push(TxRec {
                hash: th,
                block_number: n,
                block_timestamp: 1000 + n,
                index: i as u32,
                from,
                to,
                value: vec![1],
                gas: 21000,
                gas_price: vec![1],
                effective_gas_price: None,
                max_fee_per_gas: None,
                max_priority_fee_per_gas: None,
                nonce: 0,
                input: vec![],
                tx_type: 0,
                status: Some(1),
                gas_used: 21000,
                contract_address: created,
                first_log_index: i as u32,
                log_count: 1,
            });
        }
        FullBlock {
            block: BlockRec {
                number: n,
                hash: h(n, fork),
                parent_hash: parent,
                timestamp: 1000 + n,
                miner: a(0),
                gas_used: 0,
                gas_limit: 0,
                base_fee: None,
                difficulty: vec![],
                state_root: [0; 32],
                transactions_root: [0; 32],
                receipts_root: [0; 32],
                size: 0,
                tx_count: recs.len() as u32,
                log_count: logs.len() as u32,
            },
            txs: recs,
            logs,
        }
    }

    fn open(dir: &Path) -> Arc<Store> {
        Store::open(dir, StoreConfig { cache_bytes: 8 << 20, reorg_depth: 16 }).unwrap()
    }

    #[test]
    fn apply_counts_distinct_txs_per_address() {
        let d = tempfile::tempdir().unwrap();
        let s = open(d.path());
        s.apply(&[
            block(0, 1, 1, vec![(a(1), Some(a(2)), None), (a(1), Some(a(1)), None)]),
            block(1, 1, 1, vec![(a(2), None, Some(a(9)))]),
        ])
        .unwrap();
        assert_eq!(s.head().unwrap().unwrap(), Head { number: 1, hash: h(1, 1) });
        assert_eq!(s.counters().unwrap(), Counters { blocks: 2, txs: 3, logs: 3, addresses: 3 });
        // self-transfer counts once
        assert_eq!(s.addr_info(&a(1)).unwrap().unwrap().tx_count, 2);
        let a2 = s.addr_info(&a(2)).unwrap().unwrap();
        assert_eq!((a2.tx_count, a2.first_block, a2.last_block), (2, 0, 1));
        let c = s.addr_info(&a(9)).unwrap().unwrap();
        assert!(c.is_contract);
        assert_eq!(c.creator, Some((1, 0)));
        // newest first
        let txs = s.addr_txs(&a(2), 0, 10).unwrap();
        assert_eq!(txs.iter().map(|t| (t.block_number, t.index)).collect::<Vec<_>>(), vec![(1, 0), (0, 0)]);
        assert_eq!(s.addr_txs(&a(2), 1, 10).unwrap().len(), 1);
        let t = s.tx_by_hash(&txs[0].hash).unwrap().unwrap();
        assert_eq!(s.tx_logs(&t).unwrap().len(), 1);
        assert_eq!(s.block_txs(0).unwrap().len(), 2);
        assert_eq!(s.latest_txs(2).unwrap()[0].block_number, 1);
        assert_eq!(s.blocks_desc(None, 5).unwrap().len(), 2);
        assert_eq!(s.addrs_with_prefix("0909", 5).unwrap().len(), 1);
        assert_eq!(s.addrs_with_prefix("090", 5).unwrap().len(), 1);
        assert_eq!(s.addrs_with_prefix("0a", 5).unwrap().len(), 0);
    }

    #[test]
    fn rollback_restores_previous_state_exactly() {
        let d = tempfile::tempdir().unwrap();
        let s = open(d.path());
        s.apply(&[block(0, 1, 1, vec![(a(1), Some(a(2)), None)])]).unwrap();
        let before_counters = s.counters().unwrap();
        let before_a1 = s.addr_info(&a(1)).unwrap();
        s.apply(&[
            block(1, 1, 1, vec![(a(1), Some(a(3)), None)]),
            block(2, 1, 1, vec![(a(3), None, Some(a(4)))]),
        ])
        .unwrap();
        let tx2 = s.block_txs(2).unwrap()[0].hash;
        assert_eq!(s.rollback_head().unwrap(), Some(Head { number: 1, hash: h(1, 1) }));
        assert_eq!(s.rollback_head().unwrap(), Some(Head { number: 0, hash: h(0, 1) }));
        assert_eq!(s.counters().unwrap(), before_counters);
        assert_eq!(s.addr_info(&a(1)).unwrap(), before_a1);
        assert!(s.addr_info(&a(3)).unwrap().is_none());
        assert!(s.addr_info(&a(4)).unwrap().is_none());
        assert!(s.tx_by_hash(&tx2).unwrap().is_none());
        assert!(s.block(1).unwrap().is_none());
        assert!(s.block_by_hash(&h(1, 1)).unwrap().is_none());
        assert_eq!(s.addr_txs(&a(1), 0, 10).unwrap().len(), 1);
        // the replacement fork applies cleanly on top
        s.apply(&[block(1, 2, 1, vec![(a(5), Some(a(1)), None)])]).unwrap();
        assert_eq!(s.addr_info(&a(1)).unwrap().unwrap().tx_count, 2);
        assert_eq!(s.counters().unwrap().blocks, 2);
    }

    #[test]
    fn undo_is_pruned_beyond_reorg_depth() {
        let d = tempfile::tempdir().unwrap();
        let s = open(d.path());
        let blocks: Vec<_> = (0..20).map(|n| block(n, 1, 1, vec![])).collect();
        s.apply(&blocks).unwrap();
        for _ in 0..16 {
            s.rollback_head().unwrap();
        }
        assert!(s.rollback_head().is_err(), "rollback past REORG_DEPTH must fail loudly");
    }

    #[test]
    fn rejects_unchained_blocks() {
        let d = tempfile::tempdir().unwrap();
        let s = open(d.path());
        s.apply(&[block(0, 1, 1, vec![])]).unwrap();
        assert!(s.apply(&[block(1, 2, 2, vec![])]).is_err());
        assert!(s.apply(&[block(2, 1, 1, vec![])]).is_err());
    }
}
