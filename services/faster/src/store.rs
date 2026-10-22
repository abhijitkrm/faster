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
//! | undo       | number u64                       | Undo (prior addr/counter/token state) |
//! | addr_rank  | ^tx_count u64 ‖ addr 20          | (empty)                  |
//! | tt         | number u64 ‖ log u32 ‖ sub u16   | token Transfer           |
//! | tt_token   | token 20 ‖ ^number ‖ ^log ‖ ^sub | (empty)                  |
//! | tt_addr    | addr 20 ‖ ^number ‖ ^log ‖ ^sub  | (empty)                  |
//! | bal        | token 20 ‖ holder 20 ‖ id 32     | amount (min. big-endian) |
//! | bal_rank   | token 20 ‖ ^amount 32 ‖ holder 20 | (empty); id 0 only      |
//! | holder_tok | holder 20 ‖ token 20 ‖ id 32     | (empty) while amount > 0 |
//! | token      | token 20                         | TokenStats               |
//! | token_rank | ^transfers u64 ‖ token 20        | (empty)                  |
//! | token_meta | token 20                         | TokenMeta (not reorg-tracked) |
//! | token_name | lowercase name/symbol ‖ 0 ‖ token | (empty), for search     |
//!
//! Token balances are derived from transfers (ERC-20 amount, ERC-721 count
//! of NFTs held, ERC-1155 amount per id), so they are exact only when
//! indexing starts at the token's first transfer (START_BLOCK=0).
//!
//! Every block — its rows, address updates, counters, undo record and the
//! head pointer — is written in one atomic WriteBatch, so the head is always
//! consistent with the data (there is no separate checkpoint to lose).

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};
use bincode::Options as _;
use rocksdb::{
    BlockBasedOptions, Cache, ColumnFamily, ColumnFamilyDescriptor, DBCompressionType, Direction, IteratorMode,
    Options, ReadOptions, WriteBatch, WriteOptions, DB,
};
use serde::{Deserialize, Serialize};

use crate::types::*;

const SCHEMA_VERSION: u32 = 3;

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
const CF_ADDR_RANK: &str = "addr_rank";
const CF_TT: &str = "tt";
const CF_TT_TOKEN: &str = "tt_token";
const CF_TT_ADDR: &str = "tt_addr";
const CF_BAL: &str = "bal";
const CF_BAL_RANK: &str = "bal_rank";
const CF_HOLDER_TOK: &str = "holder_tok";
const CF_TOKEN: &str = "token";
const CF_TOKEN_RANK: &str = "token_rank";
const CF_TOKEN_META: &str = "token_meta";
const CF_TOKEN_NAME: &str = "token_name";
const CFS: [&str; 20] = [
    CF_META,
    CF_BLOCKS,
    CF_BLOCK_HASH,
    CF_TXS,
    CF_TX_HASH,
    CF_LOGS,
    CF_ADDR_TX,
    CF_ADDR,
    CF_UNDO,
    CF_ADDR_RANK,
    CF_TT,
    CF_TT_TOKEN,
    CF_TT_ADDR,
    CF_BAL,
    CF_BAL_RANK,
    CF_HOLDER_TOK,
    CF_TOKEN,
    CF_TOKEN_RANK,
    CF_TOKEN_META,
    CF_TOKEN_NAME,
];

#[derive(Serialize, Deserialize, Default)]
struct Undo {
    counters: Counters,
    /// State of every address the block touched, before the block.
    addrs: Vec<(Addr, Option<AddrInfo>)>,
    /// Token balances (bal keys) the block changed, before the block.
    bals: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    tokens: Vec<(Addr, Option<TokenStats>)>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenStats {
    pub kind: u8,
    pub transfers: u64,
    /// Holders with a non-zero balance ((holder, id) pairs for ERC-1155).
    pub holders: u64,
    pub first_block: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenMeta {
    pub name: Option<String>,
    pub symbol: Option<String>,
    pub decimals: Option<u8>,
}

/// Transfer position key: block, log index, position in an ERC-1155 batch.
pub fn k_tt(n: u64, log: u32, sub: u16) -> [u8; 14] {
    let mut k = [0u8; 14];
    k[..8].copy_from_slice(&n.to_be_bytes());
    k[8..12].copy_from_slice(&log.to_be_bytes());
    k[12..].copy_from_slice(&sub.to_be_bytes());
    k
}

/// Newest-first transfer index under a 20-byte prefix (token or address).
pub fn k_tt_by(a: &Addr, n: u64, log: u32, sub: u16) -> [u8; 34] {
    let mut k = [0u8; 34];
    k[..20].copy_from_slice(a);
    k[20..28].copy_from_slice(&(!n).to_be_bytes());
    k[28..32].copy_from_slice(&(!log).to_be_bytes());
    k[32..].copy_from_slice(&(!sub).to_be_bytes());
    k
}

fn id32(id: &Option<Vec<u8>>) -> [u8; 32] {
    let mut w = [0u8; 32];
    if let Some(b) = id {
        let b = &b[b.len().saturating_sub(32)..];
        w[32 - b.len()..].copy_from_slice(b);
    }
    w
}

/// bal key for a transfer side. ERC-20 and ERC-721 use id 0 (721 counts
/// NFTs held); ERC-1155 keeps one balance per id.
fn k_bal(token: &Addr, holder: &Addr, kind: u8, id: &Option<Vec<u8>>) -> Vec<u8> {
    let mut k = Vec::with_capacity(72);
    k.extend_from_slice(token);
    k.extend_from_slice(holder);
    k.extend_from_slice(&if kind == crate::tokens::KIND_ERC1155 { id32(id) } else { [0u8; 32] });
    k
}

fn u256(b: &[u8]) -> ruint::aliases::U256 {
    ruint::aliases::U256::try_from_be_slice(&b[b.len().saturating_sub(32)..]).unwrap_or_default()
}

fn min_bytes(v: ruint::aliases::U256) -> Vec<u8> {
    let b: [u8; 32] = v.to_be_bytes();
    let lead = b.iter().take_while(|&&x| x == 0).count();
    b[lead..].to_vec()
}

/// bal_rank key (only for id-0 balances): token ‖ ^amount ‖ holder.
fn k_bal_rank(bal_key: &[u8], amount: &[u8]) -> Option<Vec<u8>> {
    if bal_key[40..].iter().any(|&b| b != 0) {
        return None;
    }
    let a: [u8; 32] = u256(amount).to_be_bytes();
    let mut k = Vec::with_capacity(72);
    k.extend_from_slice(&bal_key[..20]);
    k.extend(a.iter().map(|b| !b));
    k.extend_from_slice(&bal_key[20..40]);
    Some(k)
}

fn k_holder_tok(bal_key: &[u8]) -> Vec<u8> {
    let mut k = Vec::with_capacity(72);
    k.extend_from_slice(&bal_key[20..40]);
    k.extend_from_slice(&bal_key[..20]);
    k.extend_from_slice(&bal_key[40..]);
    k
}

fn k_token_rank(transfers: u64, token: &Addr) -> [u8; 28] {
    k_rank(transfers, token)
}

const ZERO: Addr = [0u8; 20];

pub struct Store {
    db: DB,
    /// Blocks of undo history kept; deeper reorgs need a resync.
    reorg_depth: u64,
    /// Cached head; reads never return rows above it.
    head: std::sync::RwLock<Option<Head>>,
}

pub struct StoreConfig {
    pub cache_bytes: usize,
    pub reorg_depth: u64,
}

fn enc<T: Serialize>(v: &T) -> Vec<u8> {
    // Varint integers: most lengths and quantities fit in one or two bytes.
    bincode::DefaultOptions::new().serialize(v).expect("bincode serialize")
}

fn dec_rec<T: for<'de> Deserialize<'de>>(b: &[u8]) -> Result<T> {
    bincode::DefaultOptions::new().deserialize(b).map_err(|e| anyhow!("corrupt record: {e}"))
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

/// Ranking key: highest tx count first, then address.
pub fn k_rank(count: u64, a: &Addr) -> [u8; 28] {
    let mut k = [0u8; 28];
    k[..8].copy_from_slice(&(!count).to_be_bytes());
    k[8..].copy_from_slice(a);
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

/// Index of block `n` in a run of consecutive blocks.
fn blocks_index(blocks: &[FullBlock], n: u64) -> usize {
    (n - blocks[0].block.number) as usize
}

/// A write to a derived index (rankings, holder→token). Derived entries are
/// written before the state batch from parallel threads; readers check them
/// against the authoritative records, so a stale entry is never served.
type DerivedOp = (&'static str, Vec<u8>, bool); // (cf, key, put = true / delete = false)

/// A balance change `old -> new` (None = zero): the balance itself goes in
/// `wb`; its holder-rank and holder-token entries go to `idx`.
fn write_bal(
    wb: &mut WriteBatch,
    cf_bal: &ColumnFamily,
    idx: &mut Vec<DerivedOp>,
    k: &[u8],
    old: Option<&[u8]>,
    new: Option<&[u8]>,
) {
    if let Some(o) = old {
        if let Some(r) = k_bal_rank(k, o) {
            idx.push((CF_BAL_RANK, r, false));
        }
    }
    match new {
        Some(n) => {
            wb.put_cf(cf_bal, k, n);
            if let Some(r) = k_bal_rank(k, n) {
                idx.push((CF_BAL_RANK, r, true));
            }
            if old.is_none() {
                idx.push((CF_HOLDER_TOK, k_holder_tok(k), true));
            }
        }
        None => {
            wb.delete_cf(cf_bal, k);
            idx.push((CF_HOLDER_TOK, k_holder_tok(k), false));
        }
    }
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
        let mut s = Store { db, reorg_depth: cfg.reorg_depth, head: std::sync::RwLock::new(None) };
        match s.get_meta::<u32>("version")? {
            None => {
                let mut wb = WriteBatch::default();
                wb.put_cf(&s.cf(CF_META), "version", enc(&SCHEMA_VERSION));
                s.db.write(wb)?;
            }
            Some(v) if v == SCHEMA_VERSION => {}
            Some(v) => bail!("store schema version {v}, binary expects {SCHEMA_VERSION}; resync required"),
        }
        s.head = std::sync::RwLock::new(s.get_meta("head")?);
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
        Ok(*self.head.read().unwrap())
    }

    fn head_number(&self) -> Option<u64> {
        self.head.read().unwrap().map(|h| h.number)
    }

    fn visible(&self, n: u64) -> bool {
        self.head_number().is_some_and(|h| n <= h)
    }

    pub fn counters(&self) -> Result<Counters> {
        Ok(self.get_meta("counters")?.unwrap_or_default())
    }

    /// Appends consecutive blocks on top of the current head. The caller has
    /// checked that they chain onto the head.
    ///
    /// Two phases, so RocksDB can insert into its memtables from several
    /// threads (a single batch is inserted by one thread, which capped
    /// ingest at ~150K tx/s):
    ///
    /// 1. Data rows (blocks, hashes, txs, logs, address history) are written
    ///    as one batch per column family, concurrently, without fsync. These
    ///    writes are idempotent: the same block always produces the same rows.
    /// 2. State (address info, counters, undo, head) is written in one batch
    ///    with fsync, which also makes phase 1 durable (the WAL is sequential).
    ///
    /// A crash between the phases leaves rows above the head. Reads never look
    /// above the head and verify hash-index hits, and re-indexing overwrites
    /// the rows, so they are invisible.
    #[cfg(test)]
    pub fn apply(&self, blocks: &[FullBlock]) -> Result<()> {
        self.apply_at(blocks, None).map(|_| ())
    }

    /// `apply` for an indexer that knows the node's tip: blocks already more
    /// than REORG_DEPTH below it can never be rolled back, so they get no
    /// undo record (most of the state write during a catch-up).
    /// Returns tokens seen for the first time in these blocks.
    pub fn apply_at(&self, blocks: &[FullBlock], tip: Option<u64>) -> Result<Vec<Addr>> {
        let Some(last) = blocks.last() else { return Ok(Vec::new()) };
        let t = std::time::Instant::now();
        // The state batch is built while the data rows are written, but is
        // only written after them (see above).
        let transfers: Vec<Vec<crate::tokens::Transfer>> = blocks.iter().map(crate::tokens::decode_block).collect();
        let (data, state) = std::thread::scope(|sc| {
            let st = sc.spawn(|| self.build_state(blocks, &transfers, tip));
            let d = self.write_data(blocks, &transfers);
            (d, st.join())
        });
        data?;
        let (wb, derived, new_tokens) = state.map_err(|_| anyhow!("state builder panicked"))??;
        self.write_derived(derived)?;
        let data_ms = t.elapsed().as_millis() as u64;
        let mut wo = WriteOptions::default();
        wo.set_sync(true);
        self.db.write_opt(wb, &wo)?;
        *self.head.write().unwrap() = Some(Head { number: last.block.number, hash: last.block.hash });
        tracing::debug!(blocks = blocks.len(), data_ms, state_ms = t.elapsed().as_millis() as u64 - data_ms, "apply");
        Ok(new_tokens)
    }

    /// Derived-index writes, one thread per column family, no fsync (the
    /// state batch that follows makes them durable).
    fn write_derived(&self, ops: Vec<DerivedOp>) -> Result<()> {
        let mut by_cf: HashMap<&'static str, Vec<(Vec<u8>, bool)>> = HashMap::new();
        for (cf, k, put) in ops {
            by_cf.entry(cf).or_default().push((k, put));
        }
        let mut nosync = WriteOptions::default();
        nosync.set_sync(false);
        let results: Vec<Result<()>> = std::thread::scope(|sc| {
            let hs: Vec<_> = by_cf
                .iter()
                .map(|(cf, ops)| {
                    let nosync = &nosync;
                    sc.spawn(move || -> Result<()> {
                        let cf = self.cf(cf);
                        let mut wb = WriteBatch::default();
                        for (k, put) in ops {
                            if *put {
                                wb.put_cf(cf, k, []);
                            } else {
                                wb.delete_cf(cf, k);
                            }
                        }
                        self.db.write_opt(wb, nosync)?;
                        Ok(())
                    })
                })
                .collect();
            hs.into_iter().map(|h| h.join().unwrap_or_else(|_| Err(anyhow!("writer thread panicked")))).collect()
        });
        results.into_iter().collect()
    }

    fn write_data(&self, blocks: &[FullBlock], transfers: &[Vec<crate::tokens::Transfer>]) -> Result<()> {
        let mut nosync = WriteOptions::default();
        nosync.set_sync(false);
        let write = |fill: &dyn Fn(&mut WriteBatch)| -> Result<()> {
            let mut wb = WriteBatch::default();
            fill(&mut wb);
            self.db.write_opt(wb, &nosync)?;
            Ok(())
        };
        let (cf_blocks, cf_bh, cf_txs, cf_th, cf_logs, cf_at) = (
            self.cf(CF_BLOCKS),
            self.cf(CF_BLOCK_HASH),
            self.cf(CF_TXS),
            self.cf(CF_TX_HASH),
            self.cf(CF_LOGS),
            self.cf(CF_ADDR_TX),
        );
        // The big column families are split into shards so their memtable
        // inserts run on several threads.
        // Measured on 8 cores: one writer each for txs / tx_hash and two for
        // addr_tx beat finer splits (contention in the write group).
        const TX_SHARDS: u32 = 1;
        const ADDR_SHARDS: u32 = 2;
        let mut jobs: Vec<Box<dyn Fn(&mut WriteBatch) + Sync + '_>> = vec![
            Box::new(|wb: &mut WriteBatch| {
                for fb in blocks {
                    wb.put_cf(cf_blocks, k_num(fb.block.number), enc(&fb.block));
                    wb.put_cf(cf_bh, fb.block.hash, k_num(fb.block.number));
                }
            }),
            Box::new(|wb: &mut WriteBatch| {
                for fb in blocks {
                    for (i, l) in fb.logs.iter().enumerate() {
                        wb.put_cf(cf_logs, k_num_idx(fb.block.number, i as u32), enc(l));
                    }
                }
            }),
        ];
        for shard in 0..TX_SHARDS {
            jobs.push(Box::new(move |wb: &mut WriteBatch| {
                for fb in blocks {
                    for t in fb.txs.iter().filter(|t| t.index % TX_SHARDS == shard) {
                        wb.put_cf(cf_txs, k_num_idx(t.block_number, t.index), enc(t));
                    }
                }
            }));
            jobs.push(Box::new(move |wb: &mut WriteBatch| {
                for fb in blocks {
                    for t in fb.txs.iter().filter(|t| t.index % TX_SHARDS == shard) {
                        wb.put_cf(cf_th, t.hash, k_num_idx(t.block_number, t.index));
                    }
                }
            }));
        }
        for shard in 0..ADDR_SHARDS {
            jobs.push(Box::new(move |wb: &mut WriteBatch| {
                for fb in blocks {
                    for t in &fb.txs {
                        for (a, role) in tx_parties(t) {
                            if a[19] as u32 % ADDR_SHARDS == shard {
                                wb.put_cf(cf_at, k_addr_tx(&a, t.block_number, t.index), [role]);
                            }
                        }
                    }
                }
            }));
        }
        // Token transfers: one writer per index.
        let (cf_tt, cf_ttt, cf_tta) = (self.cf(CF_TT), self.cf(CF_TT_TOKEN), self.cf(CF_TT_ADDR));
        jobs.push(Box::new(move |wb: &mut WriteBatch| {
            for (fb, ts) in blocks.iter().zip(transfers) {
                for t in ts {
                    wb.put_cf(cf_tt, k_tt(fb.block.number, t.log_index, t.sub), enc(t));
                }
            }
        }));
        jobs.push(Box::new(move |wb: &mut WriteBatch| {
            for (fb, ts) in blocks.iter().zip(transfers) {
                for t in ts {
                    wb.put_cf(cf_ttt, k_tt_by(&t.token, fb.block.number, t.log_index, t.sub), []);
                }
            }
        }));
        jobs.push(Box::new(move |wb: &mut WriteBatch| {
            for (fb, ts) in blocks.iter().zip(transfers) {
                let n = fb.block.number;
                for t in ts {
                    if t.from != ZERO {
                        wb.put_cf(cf_tta, k_tt_by(&t.from, n, t.log_index, t.sub), []);
                    }
                    if t.to != ZERO && t.to != t.from {
                        wb.put_cf(cf_tta, k_tt_by(&t.to, n, t.log_index, t.sub), []);
                    }
                }
            }
        }));
        let results: Vec<Result<()>> = std::thread::scope(|sc| {
            let hs: Vec<_> = jobs.iter().map(|j| sc.spawn(|| write(j.as_ref()))).collect();
            hs.into_iter().map(|h| h.join().unwrap_or_else(|_| Err(anyhow!("writer thread panicked")))).collect()
        });
        results.into_iter().collect()
    }

    fn build_state(
        &self,
        blocks: &[FullBlock],
        transfers: &[Vec<crate::tokens::Transfer>],
        tip: Option<u64>,
    ) -> Result<(WriteBatch, Vec<DerivedOp>, Vec<Addr>)> {
        let Some(last) = blocks.last() else { return Ok((WriteBatch::default(), Vec::new(), Vec::new())) };
        let mut derived: Vec<DerivedOp> = Vec::new();
        let head = self.head()?;
        let mut expect = head.map(|h| (h.number + 1, h.hash));
        let mut counters = self.counters()?;
        // Current info for every address the blocks touch, in one multi-get.
        let mut addrs: HashMap<Addr, Option<AddrInfo>> = HashMap::new();
        for fb in blocks {
            for t in &fb.txs {
                for (a, _) in tx_parties(t) {
                    addrs.entry(a).or_insert(None);
                }
            }
        }
        let keys: Vec<Addr> = addrs.keys().copied().collect();
        let cf_addr = self.cf(CF_ADDR);
        for (a, v) in keys.iter().zip(self.db.batched_multi_get_cf(cf_addr, keys.iter(), false)) {
            if let Some(v) = v? {
                addrs.insert(*a, Some(dec_rec(&v)?));
            }
        }
        let before = addrs.clone();
        // Current balances and token stats the transfers touch.
        let mut bals: HashMap<Vec<u8>, Option<Vec<u8>>> = HashMap::new();
        let mut toks: HashMap<Addr, Option<TokenStats>> = HashMap::new();
        for t in transfers.iter().flatten() {
            toks.entry(t.token).or_insert(None);
            for h in [t.from, t.to] {
                if h != ZERO {
                    bals.entry(k_bal(&t.token, &h, t.kind, &t.token_id)).or_insert(None);
                }
            }
        }
        let keys: Vec<Vec<u8>> = bals.keys().cloned().collect();
        for (k, v) in keys.iter().zip(self.db.batched_multi_get_cf(self.cf(CF_BAL), keys.iter(), false)) {
            if let Some(v) = v? {
                bals.insert(k.clone(), Some(v.to_vec()));
            }
        }
        let keys: Vec<Addr> = toks.keys().copied().collect();
        for (k, v) in keys.iter().zip(self.db.batched_multi_get_cf(self.cf(CF_TOKEN), keys.iter(), false)) {
            if let Some(v) = v? {
                toks.insert(*k, Some(dec_rec(&v)?));
            }
        }
        let bals_before = bals.clone();
        let toks_before = toks.clone();
        let mut wb = WriteBatch::default();
        let cf_undo = self.cf(CF_UNDO);
        for fb in blocks {
            let b = &fb.block;
            if let Some((n, parent)) = expect {
                if b.number != n || b.parent_hash != parent {
                    bail!("block {} does not chain onto {} (internal error)", b.number, n - 1);
                }
            }
            let keep_undo = tip.is_none_or(|t| b.number + self.reorg_depth > t);
            let mut undo = Undo { counters, ..Default::default() };
            let mut seen: std::collections::HashSet<Addr> = std::collections::HashSet::new();
            for t in &fb.txs {
                for (a, role) in tx_parties(t) {
                    let cur = addrs[&a];
                    if keep_undo && seen.insert(a) {
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
            let mut seen_bal: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
            let mut seen_tok: std::collections::HashSet<Addr> = std::collections::HashSet::new();
            for t in &transfers[blocks_index(blocks, b.number)] {
                let stats = toks[&t.token];
                if keep_undo && seen_tok.insert(t.token) {
                    undo.tokens.push((t.token, stats));
                }
                let mut st = stats.unwrap_or(TokenStats { kind: t.kind, first_block: b.number, ..Default::default() });
                st.transfers += 1;
                let v = u256(&t.value);
                for (h, add) in [(t.from, false), (t.to, true)] {
                    if h == ZERO {
                        continue; // mint / burn side
                    }
                    let k = k_bal(&t.token, &h, t.kind, &t.token_id);
                    let cur = bals[&k].clone();
                    if keep_undo && seen_bal.insert(k.clone()) {
                        undo.bals.push((k.clone(), cur.clone()));
                    }
                    let old = cur.as_deref().map(u256).unwrap_or_default();
                    // Saturating: history before START_BLOCK is unknown.
                    let new = if add { old.saturating_add(v) } else { old.saturating_sub(v) };
                    match (old.is_zero(), new.is_zero()) {
                        (true, false) => st.holders += 1,
                        (false, true) => st.holders = st.holders.saturating_sub(1),
                        _ => {}
                    }
                    bals.insert(k, (!new.is_zero()).then(|| min_bytes(new)));
                }
                toks.insert(t.token, Some(st));
            }
            counters.blocks += 1;
            counters.txs += fb.txs.len() as u64;
            counters.logs += fb.logs.len() as u64;
            if keep_undo {
                wb.put_cf(cf_undo, k_num(b.number), enc(&undo));
            }
            if b.number >= self.reorg_depth {
                wb.delete_cf(cf_undo, k_num(b.number - self.reorg_depth));
            }
            expect = Some((b.number + 1, b.hash));
        }
        for (a, info) in &addrs {
            if let Some(info) = info {
                wb.put_cf(cf_addr, a, enc(info));
                if let Some(Some(old)) = before.get(a) {
                    derived.push((CF_ADDR_RANK, k_rank(old.tx_count, a).to_vec(), false));
                }
                derived.push((CF_ADDR_RANK, k_rank(info.tx_count, a).to_vec(), true));
            }
        }
        let cf_bal = self.cf(CF_BAL);
        for (k, cur) in &bals {
            let old = &bals_before[k];
            if old == cur {
                continue;
            }
            write_bal(&mut wb, cf_bal, &mut derived, k, old.as_deref(), cur.as_deref());
        }
        let cf_tok = self.cf(CF_TOKEN);
        for (a, st) in &toks {
            if let Some(st) = st {
                if let Some(Some(old)) = toks_before.get(a) {
                    derived.push((CF_TOKEN_RANK, k_token_rank(old.transfers, a).to_vec(), false));
                }
                wb.put_cf(cf_tok, a, enc(st));
                derived.push((CF_TOKEN_RANK, k_token_rank(st.transfers, a).to_vec(), true));
            }
        }
        let new_head = Head { number: last.block.number, hash: last.block.hash };
        let cf_meta = self.cf(CF_META);
        wb.put_cf(cf_meta, "counters", enc(&counters));
        wb.put_cf(cf_meta, "head", enc(&new_head));
        let new_tokens = toks.keys().filter(|a| matches!(toks_before.get(*a), Some(None))).copied().collect();
        Ok((wb, derived, new_tokens))
    }

    /// Tokens with no metadata record yet (e.g. the fetch was interrupted).
    pub fn tokens_missing_meta(&self, limit: usize) -> Result<Vec<Addr>> {
        let mut out = Vec::new();
        for kv in self.db.iterator_cf(self.cf(CF_TOKEN), IteratorMode::Start) {
            let (k, _) = kv?;
            if self.db.get_pinned_cf(self.cf(CF_TOKEN_META), &k)?.is_none() {
                out.push(k[..20].try_into()?);
                if out.len() >= limit {
                    break;
                }
            }
        }
        Ok(out)
    }

    #[cfg(test)]
    pub fn apply_data_only(&self, blocks: &[FullBlock]) -> Result<()> {
        let transfers: Vec<_> = blocks.iter().map(crate::tokens::decode_block).collect();
        self.write_data(blocks, &transfers)
    }

    /// Raw transfer rows of block `n` (no visibility checks; for rollback).
    fn block_transfers(&self, n: u64) -> Result<Vec<crate::tokens::Transfer>> {
        let mut ro = ReadOptions::default();
        ro.set_iterate_upper_bound(k_tt(n + 1, 0, 0).to_vec());
        let k = k_tt(n, 0, 0);
        let mut out = Vec::new();
        for kv in self.db.iterator_cf_opt(self.cf(CF_TT), ro, IteratorMode::From(&k, Direction::Forward)) {
            let (_, v) = kv?;
            out.push(dec_rec(&v)?);
        }
        Ok(out)
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
        for t in self.block_transfers(n)? {
            wb.delete_cf(self.cf(CF_TT_TOKEN), k_tt_by(&t.token, n, t.log_index, t.sub));
            for a in [t.from, t.to] {
                if a != ZERO {
                    wb.delete_cf(self.cf(CF_TT_ADDR), k_tt_by(&a, n, t.log_index, t.sub));
                }
            }
        }
        wb.delete_range_cf(self.cf(CF_TT), k_tt(n, 0, 0), k_tt(n + 1, 0, 0));
        let mut derived: Vec<DerivedOp> = Vec::new();
        for (k, prev) in &undo.bals {
            let cur = self.db.get_cf(self.cf(CF_BAL), k)?;
            write_bal(&mut wb, self.cf(CF_BAL), &mut derived, k, cur.as_deref(), prev.as_deref());
        }
        // Rollback is rare: derived entries go in the same atomic batch.
        for (cf, k, put) in derived {
            if put {
                wb.put_cf(self.cf(cf), k, []);
            } else {
                wb.delete_cf(self.cf(cf), k);
            }
        }
        for (a, prev) in &undo.tokens {
            if let Some(cur) = self.get_rec::<TokenStats>(CF_TOKEN, a)? {
                wb.delete_cf(self.cf(CF_TOKEN_RANK), k_token_rank(cur.transfers, a));
            }
            match prev {
                Some(st) => {
                    wb.put_cf(self.cf(CF_TOKEN), a, enc(st));
                    wb.put_cf(self.cf(CF_TOKEN_RANK), k_token_rank(st.transfers, a), []);
                }
                None => wb.delete_cf(self.cf(CF_TOKEN), a),
            }
        }
        let cf_rank = self.cf(CF_ADDR_RANK);
        for (a, prev) in &undo.addrs {
            if let Some(cur) = self.addr_info(a)? {
                wb.delete_cf(cf_rank, k_rank(cur.tx_count, a));
            }
            match prev {
                Some(info) => {
                    wb.put_cf(&self.cf(CF_ADDR), a, enc(info));
                    wb.put_cf(cf_rank, k_rank(info.tx_count, a), []);
                }
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
        *self.head.write().unwrap() = new_head;
        Ok(new_head)
    }

    /// Consistent on-disk snapshot of the store (hard links to SST files, so
    /// it is near-instant and shares disk with the live store). Start a
    /// node with DATA_DIR pointing at a copy of it to restore.
    pub fn checkpoint(&self, dir: &Path) -> Result<Option<Head>> {
        let head = self.head()?;
        std::fs::create_dir_all(dir)?;
        rocksdb::checkpoint::Checkpoint::new(&self.db)?.create_checkpoint(dir.join("hot"))?;
        Ok(head)
    }

    // ---- reads ----

    pub fn block(&self, n: u64) -> Result<Option<BlockRec>> {
        if !self.visible(n) {
            return Ok(None);
        }
        self.get_rec(CF_BLOCKS, &k_num(n))
    }

    pub fn block_by_hash(&self, h: &H256) -> Result<Option<BlockRec>> {
        match self.db.get_pinned_cf(&self.cf(CF_BLOCK_HASH), h)? {
            Some(v) => Ok(self.block(u64::from_be_bytes(v[..8].try_into()?))?.filter(|b| b.hash == *h)),
            None => Ok(None),
        }
    }

    /// Newest-first blocks with number <= `from` (or from the head).
    pub fn blocks_desc(&self, from: Option<u64>, limit: usize) -> Result<Vec<BlockRec>> {
        let Some(head) = self.head_number() else { return Ok(Vec::new()) };
        let k = k_num(from.map_or(head, |f| f.min(head)));
        let mode = IteratorMode::From(&k, Direction::Reverse);
        let mut out = Vec::with_capacity(limit);
        for kv in self.db.iterator_cf(&self.cf(CF_BLOCKS), mode).take(limit) {
            let (_, v) = kv?;
            out.push(dec_rec(&v)?);
        }
        Ok(out)
    }

    pub fn blocks_range(&self, start: u64, end: u64) -> Result<Vec<BlockRec>> {
        let Some(head) = self.head_number() else { return Ok(Vec::new()) };
        let end = end.min(head);
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

    #[cfg(test)]
    pub fn block_txs(&self, n: u64) -> Result<Vec<TxRec>> {
        self.block_txs_page(n, 0, usize::MAX)
    }

    /// Transactions `offset..offset+limit` of block `n`, in index order.
    pub fn block_txs_page(&self, n: u64, offset: usize, limit: usize) -> Result<Vec<TxRec>> {
        let Some(b) = self.block(n)? else { return Ok(Vec::new()) };
        let start = (offset.min(b.tx_count as usize)) as u32;
        let end = (start as usize + limit).min(b.tx_count as usize) as u32;
        let mut ro = ReadOptions::default();
        ro.set_iterate_upper_bound(k_num_idx(n, end).to_vec());
        let k = k_num_idx(n, start);
        let mut out = Vec::with_capacity((end - start) as usize);
        for kv in self.db.iterator_cf_opt(self.cf(CF_TXS), ro, IteratorMode::From(&k, Direction::Forward)) {
            let (_, v) = kv?;
            out.push(dec_rec(&v)?);
        }
        Ok(out)
    }

    /// Newest-first transactions across all blocks.
    pub fn latest_txs(&self, limit: usize) -> Result<Vec<TxRec>> {
        let Some(head) = self.head_number() else { return Ok(Vec::new()) };
        let mut out = Vec::with_capacity(limit);
        let mut n = head;
        // Walk blocks newest-first; stale rows past a block's count are skipped.
        while out.len() < limit {
            let Some(b) = self.block(n)? else { break };
            if b.tx_count > 0 {
                let mut ro = ReadOptions::default();
                ro.set_iterate_lower_bound(k_num_idx(n, 0).to_vec());
                let k = k_num_idx(n, b.tx_count - 1);
                let it = self.db.iterator_cf_opt(self.cf(CF_TXS), ro, IteratorMode::From(&k, Direction::Reverse));
                for kv in it.take(limit - out.len()) {
                    let (_, v) = kv?;
                    out.push(dec_rec(&v)?);
                }
            }
            if n == 0 {
                break;
            }
            n -= 1;
        }
        Ok(out)
    }

    pub fn tx(&self, n: u64, i: u32) -> Result<Option<TxRec>> {
        match self.block(n)? {
            Some(b) if i < b.tx_count => self.get_rec(CF_TXS, &k_num_idx(n, i)),
            _ => Ok(None),
        }
    }

    pub fn tx_by_hash(&self, h: &H256) -> Result<Option<TxRec>> {
        match self.db.get_pinned_cf(&self.cf(CF_TX_HASH), h)? {
            Some(v) => {
                let (n, i) = split_num_idx(&v);
                Ok(self.tx(n, i)?.filter(|t| t.hash == *h))
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
        self.addr_txs_from(a, a, offset, limit)
    }

    fn addr_txs_from(&self, a: &Addr, start: &[u8], offset: usize, limit: usize) -> Result<Vec<TxRec>> {
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
            .iterator_cf_opt(&self.cf(CF_ADDR_TX), ro, IteratorMode::From(start, Direction::Forward))
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
        let mut counts: HashMap<u64, u32> = HashMap::new();
        for (k, v) in keys.iter().zip(self.db.batched_multi_get_cf(&cf, keys.iter(), true)) {
            let (n, i) = split_num_idx(k);
            let count = match counts.get(&n) {
                Some(c) => *c,
                None => {
                    let c = self.block(n)?.map_or(0, |b| b.tx_count);
                    counts.insert(n, c);
                    c
                }
            };
            if i >= count {
                continue;
            }
            if let Some(v) = v? {
                let t: TxRec = dec_rec(&v)?;
                // Skip leftovers of an orphaned block (see `apply`).
                if tx_parties(&t).iter().any(|(x, _)| x == a) {
                    out.push(t);
                }
            }
        }
        Ok(out)
    }

    /// Addresses with the most transactions, highest first.
    pub fn top_addrs(&self, offset: usize, limit: usize) -> Result<Vec<(Addr, AddrInfo)>> {
        let mut out = Vec::with_capacity(limit);
        for kv in self.db.iterator_cf(self.cf(CF_ADDR_RANK), IteratorMode::Start).skip(offset).take(limit) {
            let (k, _) = kv?;
            let a: Addr = k[8..28].try_into()?;
            let count = !u64::from_be_bytes(k[..8].try_into()?);
            // Skip ranking entries that no longer match the record.
            if let Some(i) = self.addr_info(&a)?.filter(|i| i.tx_count == count) {
                out.push((a, i));
            }
        }
        Ok(out)
    }

    /// Newest-first transactions involving `a` strictly older than `before`
    /// (block, index), or from the newest when None. Cost is independent of
    /// how deep the page is.
    pub fn addr_txs_before(&self, a: &Addr, before: Option<(u64, u32)>, limit: usize) -> Result<Vec<TxRec>> {
        let start = match before {
            Some((n, i)) => {
                // Next key after (n, i) in newest-first order.
                let mut k = k_addr_tx(a, n, i).to_vec();
                k.push(0);
                k
            }
            None => a.to_vec(),
        };
        self.addr_txs_from(a, &start, 0, limit)
    }

    // ---- tokens ----

    /// A transfer row is served only if its block is visible and the log it
    /// came from is still there (rows of an orphaned block are skipped).
    fn transfer_ok(&self, n: u64, t: &crate::tokens::Transfer, counts: &mut HashMap<u64, u32>) -> Result<bool> {
        let lc = match counts.get(&n) {
            Some(c) => *c,
            None => {
                let c = self.block(n)?.map_or(0, |b| b.log_count);
                counts.insert(n, c);
                c
            }
        };
        if t.log_index >= lc {
            return Ok(false);
        }
        Ok(self
            .get_rec::<LogRec>(CF_LOGS, &k_num_idx(n, t.log_index))?
            .is_some_and(|l| l.address == t.token && l.tx_index == t.tx_index))
    }

    /// Newest-first transfers of a token (`by_token`) or involving an
    /// address, strictly older than `before` (block, log, sub).
    pub fn transfers_by(
        &self,
        a: &Addr,
        by_token: bool,
        before: Option<(u64, u32, u16)>,
        limit: usize,
    ) -> Result<Vec<(u64, crate::tokens::Transfer)>> {
        let cf = self.cf(if by_token { CF_TT_TOKEN } else { CF_TT_ADDR });
        let start = match before {
            Some((n, l, sb)) => {
                let mut k = k_tt_by(a, n, l, sb).to_vec();
                k.push(0);
                k
            }
            None => a.to_vec(),
        };
        let mut counts = HashMap::new();
        let mut out = Vec::with_capacity(limit);
        // Read a little past `limit` so skipped orphan rows rarely shorten a page.
        for kv in self.db.iterator_cf(cf, IteratorMode::From(&start, Direction::Forward)).take(limit + 16) {
            let (k, _) = kv?;
            if !k.starts_with(a) || out.len() >= limit {
                break;
            }
            let n = !u64::from_be_bytes(k[20..28].try_into()?);
            let l = !u32::from_be_bytes(k[28..32].try_into()?);
            let sb = !u16::from_be_bytes(k[32..34].try_into()?);
            let Some(t) = self.get_rec::<crate::tokens::Transfer>(CF_TT, &k_tt(n, l, sb))? else { continue };
            if self.transfer_ok(n, &t, &mut counts)? {
                out.push((n, t));
            }
        }
        Ok(out)
    }

    /// Token transfers emitted by one transaction.
    pub fn tx_transfers(&self, t: &TxRec) -> Result<Vec<crate::tokens::Transfer>> {
        if t.log_count == 0 {
            return Ok(Vec::new());
        }
        let mut ro = ReadOptions::default();
        ro.set_iterate_upper_bound(k_tt(t.block_number, t.first_log_index + t.log_count, 0).to_vec());
        let k = k_tt(t.block_number, t.first_log_index, 0);
        let mut counts = HashMap::new();
        let mut out = Vec::new();
        for kv in self.db.iterator_cf_opt(self.cf(CF_TT), ro, IteratorMode::From(&k, Direction::Forward)) {
            let (_, v) = kv?;
            let tr: crate::tokens::Transfer = dec_rec(&v)?;
            if tr.tx_index == t.index && self.transfer_ok(t.block_number, &tr, &mut counts)? {
                out.push(tr);
            }
        }
        Ok(out)
    }

    pub fn token(&self, a: &Addr) -> Result<Option<(TokenStats, TokenMeta)>> {
        let Some(st) = self.get_rec::<TokenStats>(CF_TOKEN, a)? else { return Ok(None) };
        Ok(Some((st, self.get_rec(CF_TOKEN_META, a)?.unwrap_or_default())))
    }

    /// Tokens by number of transfers, highest first.
    pub fn top_tokens(&self, offset: usize, limit: usize) -> Result<Vec<(Addr, TokenStats, TokenMeta)>> {
        let mut out = Vec::with_capacity(limit);
        for kv in self.db.iterator_cf(self.cf(CF_TOKEN_RANK), IteratorMode::Start).skip(offset).take(limit) {
            let (k, _) = kv?;
            let a: Addr = k[8..28].try_into()?;
            let count = !u64::from_be_bytes(k[..8].try_into()?);
            if let Some((st, m)) = self.token(&a)?.filter(|(st, _)| st.transfers == count) {
                out.push((a, st, m));
            }
        }
        Ok(out)
    }

    /// Largest holders of a token (ERC-20 balance / ERC-721 NFT count).
    pub fn token_holders(&self, token: &Addr, offset: usize, limit: usize) -> Result<Vec<(Addr, Vec<u8>)>> {
        let mut out = Vec::with_capacity(limit);
        let it = self.db.iterator_cf(self.cf(CF_BAL_RANK), IteratorMode::From(token, Direction::Forward));
        for kv in it.skip(offset).take(limit) {
            let (k, _) = kv?;
            if !k.starts_with(token) {
                break;
            }
            let amount: Vec<u8> = k[20..52].iter().map(|b| !b).collect();
            let lead = amount.iter().take_while(|&&x| x == 0).count();
            let holder: Addr = k[52..72].try_into()?;
            // Serve only entries that match the stored balance.
            let cur = self.db.get_pinned_cf(self.cf(CF_BAL), k_bal(token, &holder, 0, &None))?;
            if cur.as_deref() == Some(&amount[lead..]) {
                out.push((holder, amount[lead..].to_vec()));
            }
        }
        Ok(out)
    }

    /// Tokens an address holds: (token, id for ERC-1155, amount).
    pub fn holder_tokens(&self, holder: &Addr, limit: usize) -> Result<Vec<(Addr, Option<Vec<u8>>, Vec<u8>)>> {
        let mut keys = Vec::new();
        for kv in
            self.db.iterator_cf(self.cf(CF_HOLDER_TOK), IteratorMode::From(holder, Direction::Forward)).take(limit)
        {
            let (k, _) = kv?;
            if !k.starts_with(holder) {
                break;
            }
            let mut bk = Vec::with_capacity(72);
            bk.extend_from_slice(&k[20..40]);
            bk.extend_from_slice(&k[..20]);
            bk.extend_from_slice(&k[40..72]);
            keys.push(bk);
        }
        let mut out = Vec::with_capacity(keys.len());
        for (k, v) in keys.iter().zip(self.db.batched_multi_get_cf(self.cf(CF_BAL), keys.iter(), false)) {
            if let Some(v) = v? {
                let id = &k[40..72];
                let id = (id.iter().any(|&b| b != 0)).then(|| {
                    let lead = id.iter().take_while(|&&x| x == 0).count();
                    id[lead..].to_vec()
                });
                out.push((k[..20].try_into()?, id, v.to_vec()));
            }
        }
        Ok(out)
    }

    /// Stores fetched metadata and indexes name and symbol for search.
    pub fn put_token_meta(&self, token: &Addr, m: &TokenMeta) -> Result<()> {
        let mut wb = WriteBatch::default();
        wb.put_cf(self.cf(CF_TOKEN_META), token, enc(m));
        for s in [&m.symbol, &m.name].into_iter().flatten() {
            let mut k = s.to_lowercase().into_bytes();
            k.truncate(64);
            k.push(0);
            k.extend_from_slice(token);
            wb.put_cf(self.cf(CF_TOKEN_NAME), k, []);
        }
        self.db.write(wb)?;
        Ok(())
    }

    /// Tokens whose lower-cased name or symbol starts with `prefix`.
    pub fn search_tokens(&self, prefix: &str, limit: usize) -> Result<Vec<Addr>> {
        let p = prefix.to_lowercase().into_bytes();
        let mut out: Vec<Addr> = Vec::new();
        for kv in self.db.iterator_cf(self.cf(CF_TOKEN_NAME), IteratorMode::From(&p, Direction::Forward)) {
            let (k, _) = kv?;
            if !k.starts_with(&p) || out.len() >= limit {
                break;
            }
            let a: Addr = k[k.len() - 20..].try_into()?;
            if !out.contains(&a) {
                out.push(a);
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

    pub(super) fn h(n: u64, fork: u8) -> H256 {
        let mut x = [fork; 32];
        x[..8].copy_from_slice(&n.to_be_bytes());
        x
    }

    pub(super) fn a(x: u8) -> Addr {
        [x; 20]
    }

    pub(super) fn block(n: u64, fork: u8, parent_fork: u8, txs: Vec<(Addr, Option<Addr>, Option<Addr>)>) -> FullBlock {
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

    pub(super) fn open(dir: &Path) -> Arc<Store> {
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
        let top: Vec<_> = s.top_addrs(0, 10).unwrap().iter().map(|(a, i)| (a[0], i.tx_count)).collect();
        assert_eq!(top, vec![(1, 2), (2, 2), (9, 1)]);
        let page1 = s.addr_txs_before(&a(2), None, 1).unwrap();
        assert_eq!((page1[0].block_number, page1[0].index), (1, 0));
        let page2 = s.addr_txs_before(&a(2), Some((1, 0)), 5).unwrap();
        assert_eq!(page2.iter().map(|t| (t.block_number, t.index)).collect::<Vec<_>>(), vec![(0, 0)]);
        assert!(s.addr_txs_before(&a(2), Some((0, 0)), 5).unwrap().is_empty());
    }

    #[test]
    fn rollback_restores_previous_state_exactly() {
        let d = tempfile::tempdir().unwrap();
        let s = open(d.path());
        s.apply(&[block(0, 1, 1, vec![(a(1), Some(a(2)), None)])]).unwrap();
        let before_counters = s.counters().unwrap();
        let before_a1 = s.addr_info(&a(1)).unwrap();
        s.apply(&[block(1, 1, 1, vec![(a(1), Some(a(3)), None)]), block(2, 1, 1, vec![(a(3), None, Some(a(4)))])])
            .unwrap();
        let tx2 = s.block_txs(2).unwrap()[0].hash;
        assert_eq!(s.rollback_head().unwrap(), Some(Head { number: 1, hash: h(1, 1) }));
        assert_eq!(s.rollback_head().unwrap(), Some(Head { number: 0, hash: h(0, 1) }));
        assert_eq!(s.counters().unwrap(), before_counters);
        assert_eq!(s.addr_info(&a(1)).unwrap(), before_a1);
        assert!(s.addr_info(&a(3)).unwrap().is_none());
        assert!(s.addr_info(&a(4)).unwrap().is_none());
        let top: Vec<_> = s.top_addrs(0, 10).unwrap().iter().map(|(a, i)| (a[0], i.tx_count)).collect();
        assert_eq!(top, vec![(1, 1), (2, 1)], "ranking not rolled back");
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

#[cfg(test)]
mod crash_tests {
    use super::tests::*;

    /// Phase 1 of a commit lands but the process dies before phase 2, then
    /// the chain reorgs: the orphan rows must never be visible.
    #[test]
    fn orphan_rows_from_interrupted_commit_are_invisible() {
        let d = tempfile::tempdir().unwrap();
        let s = open(d.path());
        s.apply(&[block(0, 1, 1, vec![(a(1), Some(a(2)), None)])]).unwrap();
        // fork 1 block 1 with three txs: data only (crash before state)
        let orphan = block(1, 1, 1, vec![(a(1), Some(a(3)), None), (a(4), Some(a(5)), None), (a(6), None, None)]);
        s.apply_data_only(&[orphan.clone()]).unwrap();
        assert!(s.block(1).unwrap().is_none(), "row above head visible");
        assert!(s.block_by_hash(&orphan.block.hash).unwrap().is_none());
        assert!(s.tx_by_hash(&orphan.txs[0].hash).unwrap().is_none());
        assert_eq!(s.blocks_desc(None, 10).unwrap().len(), 1);
        assert_eq!(s.latest_txs(10).unwrap().len(), 1);
        assert_eq!(s.addr_txs(&a(3), 0, 10).unwrap().len(), 0);
        drop(s);
        // restart, then the canonical block 1 (fork 2, one tx) is indexed
        let s = open(d.path());
        let canon = block(1, 2, 1, vec![(a(7), Some(a(1)), None)]);
        s.apply(&[canon.clone()]).unwrap();
        assert_eq!(s.block(1).unwrap().unwrap().hash, canon.block.hash);
        assert!(s.block_by_hash(&orphan.block.hash).unwrap().is_none());
        // orphan tx 0 shares the (1, 0) slot with the canonical tx: hash check
        assert!(s.tx_by_hash(&orphan.txs[0].hash).unwrap().is_none());
        // orphan txs 1 and 2 sit past the canonical block's tx count
        assert!(s.tx_by_hash(&orphan.txs[1].hash).unwrap().is_none());
        assert_eq!(s.block_txs(1).unwrap().len(), 1);
        assert_eq!(s.latest_txs(10).unwrap().len(), 2);
        assert_eq!(s.addr_txs(&a(3), 0, 10).unwrap().len(), 0, "orphan addr_tx entry leaked");
        assert_eq!(s.addr_txs(&a(4), 0, 10).unwrap().len(), 0, "orphan past tx count leaked");
        assert_eq!(s.addr_txs(&a(1), 0, 10).unwrap().len(), 2);
        assert!(s.addr_info(&a(3)).unwrap().is_none());
    }
}

#[cfg(test)]
mod token_tests {
    use super::tests::*;
    use super::*;
    use crate::tokens::{KIND_ERC20, KIND_ERC721, TRANSFER};

    fn topic(a: Addr) -> H256 {
        let mut t = [0u8; 32];
        t[12..].copy_from_slice(&a);
        t
    }

    fn amt(v: u64) -> Vec<u8> {
        let mut w = vec![0u8; 32];
        w[24..].copy_from_slice(&v.to_be_bytes());
        w
    }

    /// Block with one tx per transfer: (token, from, to, Some(amount) = ERC-20 / None = NFT id 7).
    fn tblock(n: u64, fork: u8, xs: Vec<(Addr, Addr, Addr, Option<u64>)>) -> FullBlock {
        let mut fb = block(n, fork, 1, xs.iter().map(|(_, f, _, _)| (*f, Some(a(200)), None)).collect());
        fb.logs = xs
            .iter()
            .enumerate()
            .map(|(i, (tok, f, t, v))| match v {
                Some(v) => LogRec {
                    tx_index: i as u32,
                    address: *tok,
                    topics: vec![TRANSFER, topic(*f), topic(*t)],
                    data: amt(*v),
                },
                None => LogRec {
                    tx_index: i as u32,
                    address: *tok,
                    topics: vec![TRANSFER, topic(*f), topic(*t), topic(a(7))],
                    data: vec![],
                },
            })
            .collect();
        for (i, t) in fb.txs.iter_mut().enumerate() {
            t.first_log_index = i as u32;
            t.log_count = 1;
        }
        fb.block.log_count = fb.logs.len() as u32;
        fb
    }

    fn bal(s: &Store, tok: Addr, h: Addr) -> u64 {
        s.holder_tokens(&h, 100).unwrap().iter().find(|(t, _, _)| *t == tok).map_or(0, |(_, _, v)| {
            let mut w = [0u8; 8];
            w[8 - v.len()..].copy_from_slice(v);
            u64::from_be_bytes(w)
        })
    }

    #[test]
    fn balances_holders_and_rollback() {
        let d = tempfile::tempdir().unwrap();
        let s = open(d.path());
        let (usd, nft, z) = (a(50), a(51), [0u8; 20]);
        s.apply(&[tblock(0, 1, vec![(usd, z, a(1), Some(100)), (nft, z, a(1), None)])]).unwrap();
        s.apply(&[tblock(1, 1, vec![(usd, a(1), a(2), Some(30))])]).unwrap();
        let snapshot = |s: &Store| {
            (
                bal(s, usd, a(1)),
                bal(s, usd, a(2)),
                s.token(&usd).unwrap().map(|(st, _)| st),
                s.token_holders(&usd, 0, 10).unwrap(),
                s.top_tokens(0, 10).unwrap().iter().map(|(a, st, _)| (a[0], st.transfers)).collect::<Vec<_>>(),
                s.transfers_by(&a(2), false, None, 10).unwrap().len(),
            )
        };
        let before = snapshot(&s);
        assert_eq!((before.0, before.1), (70, 30));
        assert_eq!(before.2.unwrap(), TokenStats { kind: KIND_ERC20, transfers: 2, holders: 2, first_block: 0 });
        assert_eq!(before.3.iter().map(|(h, _)| h[0]).collect::<Vec<_>>(), vec![1, 2], "holders by balance");
        assert_eq!(before.4, vec![(50, 2), (51, 1)]);
        assert_eq!(before.5, 1);
        assert_eq!(s.token(&nft).unwrap().unwrap().0.kind, KIND_ERC721);
        assert_eq!(bal(&s, nft, a(1)), 1, "NFT count");
        // a(1) sends everything and the NFT away; a(2) becomes the only USD holder
        s.apply(&[tblock(2, 1, vec![(usd, a(1), a(2), Some(70)), (nft, a(1), a(3), None)])]).unwrap();
        assert_eq!((bal(&s, usd, a(1)), bal(&s, usd, a(2))), (0, 100));
        assert_eq!(s.token(&usd).unwrap().unwrap().0.holders, 1);
        assert_eq!(s.token_holders(&usd, 0, 10).unwrap().len(), 1);
        assert!(s.holder_tokens(&a(1), 10).unwrap().is_empty());
        let tx = s.block_txs(2).unwrap()[0].clone();
        assert_eq!(s.tx_transfers(&tx).unwrap().len(), 1);
        assert_eq!(
            s.transfers_by(&usd, true, None, 10).unwrap().iter().map(|(n, _)| *n).collect::<Vec<_>>(),
            vec![2, 1, 0]
        );
        let page2 = s.transfers_by(&usd, true, Some((2, 0, 0)), 10).unwrap();
        assert_eq!(page2.iter().map(|(n, _)| *n).collect::<Vec<_>>(), vec![1, 0]);
        // rollback block 2: everything back exactly
        s.rollback_head().unwrap();
        assert_eq!(snapshot(&s), before);
        assert_eq!(bal(&s, nft, a(1)), 1);
        assert_eq!(bal(&s, nft, a(3)), 0);
        assert!(s.transfers_by(&a(3), false, None, 10).unwrap().is_empty());
    }
}
