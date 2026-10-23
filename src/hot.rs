//! RocksDB hot tier: blocks newer than the last chunk (including blocks that
//! can still be reorged), plus the hash indexes used for lookups by tx hash
//! and block hash across all history.
//!
//! Column families:
//! - `hot`: block number (BE) → bincode(HotRecord) with Arrow IPC per table
//! - `tx`:  tx hash → block number (BE) ‖ tx index (BE u32)
//! - `bh`:  block hash → block number (BE)

use std::collections::BTreeMap;
use std::io::Cursor;
use std::path::Path;
use std::sync::{Arc, RwLock};

use anyhow::{Context, Result};
use arrow::array::{Array, FixedSizeBinaryArray, UInt64Array};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
use arrow::record_batch::RecordBatch;
use rocksdb::{ColumnFamilyDescriptor, Options, WriteBatch, WriteOptions, DB};
use serde::{Deserialize, Serialize};

use crate::model::{fixed_col, u64_col, Batches, BlockHead, Table, TABLES};

pub struct HotBlock {
    pub head: BlockHead,
    pub batches: Batches,
}

#[derive(Serialize, Deserialize)]
struct HotRecord {
    head: BlockHead,
    tables: Vec<Vec<u8>>,
}

pub fn encode_batch(b: &RecordBatch) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    let opts = IpcWriteOptions::default().try_with_compression(Some(arrow::ipc::CompressionType::LZ4_FRAME))?;
    let mut w = StreamWriter::try_new_with_options(&mut out, &b.schema(), opts)?;
    w.write(b)?;
    w.finish()?;
    drop(w);
    Ok(out)
}

pub fn decode_batch(t: Table, bytes: &[u8]) -> Result<RecordBatch> {
    let r = StreamReader::try_new(Cursor::new(bytes), None)?;
    let mut parts = Vec::new();
    for b in r {
        parts.push(b?);
    }
    Ok(arrow::compute::concat_batches(&t.schema(), &parts)?)
}

pub struct Hot {
    db: DB,
    mem: RwLock<BTreeMap<u64, Arc<HotBlock>>>,
}

const CFS: [&str; 3] = ["hot", "tx", "bh"];

impl Hot {
    pub fn open(path: &Path) -> Result<Hot> {
        Self::open_mode(path, false)
    }

    /// Opens without taking the write lock (serve-only replicas, tools):
    /// sees the data as of opening.
    pub fn open_read_only(path: &Path) -> Result<Hot> {
        Self::open_mode(path, true)
    }

    fn open_mode(path: &Path, read_only: bool) -> Result<Hot> {
        let mut o = Options::default();
        o.create_if_missing(true);
        o.create_missing_column_families(true);
        o.set_compression_type(rocksdb::DBCompressionType::Lz4);
        o.increase_parallelism(4);
        o.set_max_background_jobs(4);
        let cfs = CFS.iter().map(|n| {
            let mut co = Options::default();
            co.set_compression_type(rocksdb::DBCompressionType::Lz4);
            if *n != "hot" {
                // Hash keys are random: point lookups only.
                let mut bo = rocksdb::BlockBasedOptions::default();
                bo.set_bloom_filter(10.0, false);
                bo.set_cache_index_and_filter_blocks(true);
                co.set_block_based_table_factory(&bo);
                co.set_write_buffer_size(128 << 20);
            }
            ColumnFamilyDescriptor::new(*n, co)
        });
        let db = if read_only {
            DB::open_cf_descriptors_read_only(&o, path, cfs, false).context("open hot store read-only")?
        } else {
            DB::open_cf_descriptors(&o, path, cfs).context("open hot store")?
        };
        let mut mem = BTreeMap::new();
        {
            let cf = db.cf_handle("hot").unwrap();
            for kv in db.iterator_cf(cf, rocksdb::IteratorMode::Start) {
                let (k, v) = kv?;
                let n = u64::from_be_bytes(k[..8].try_into()?);
                let rec: HotRecord = bincode::deserialize(&v)?;
                let mut tables = Vec::with_capacity(4);
                for (t, bytes) in TABLES.iter().zip(&rec.tables) {
                    tables.push(decode_batch(*t, bytes)?);
                }
                let batches = Batches { tables: tables.try_into().ok().unwrap() };
                mem.insert(n, Arc::new(HotBlock { head: rec.head, batches }));
            }
        }
        Ok(Hot { db, mem: RwLock::new(mem) })
    }

    fn cf(&self, n: &str) -> &rocksdb::ColumnFamily {
        self.db.cf_handle(n).expect("cf")
    }

    pub fn first(&self) -> Option<u64> {
        self.mem.read().unwrap().keys().next().copied()
    }

    pub fn last(&self) -> Option<BlockHead> {
        self.mem.read().unwrap().values().next_back().map(|b| b.head)
    }

    pub fn len(&self) -> usize {
        self.mem.read().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn get(&self, n: u64) -> Option<Arc<HotBlock>> {
        self.mem.read().unwrap().get(&n).cloned()
    }

    /// Hot blocks in `[from, to)`, in order.
    pub fn range(&self, from: u64, to: u64) -> Vec<Arc<HotBlock>> {
        if from >= to {
            return Vec::new();
        }
        self.mem.read().unwrap().range(from..to).map(|(_, b)| b.clone()).collect()
    }

    pub fn rows_until(&self, to: u64) -> usize {
        self.mem.read().unwrap().range(..to).map(|(_, b)| b.batches.rows()).sum()
    }

    /// Adds consecutive blocks (the caller checked the parent links).
    pub fn insert(&self, blocks: Vec<HotBlock>) -> Result<()> {
        let mut wb = WriteBatch::default();
        for b in &blocks {
            let tables = b.batches.tables.iter().map(encode_batch).collect::<Result<Vec<_>>>()?;
            let rec = HotRecord { head: b.head, tables };
            wb.put_cf(self.cf("hot"), b.head.number.to_be_bytes(), bincode::serialize(&rec)?);
            self.index_into(&mut wb, &b.batches);
        }
        self.db.write(wb)?;
        let mut mem = self.mem.write().unwrap();
        for b in blocks {
            mem.insert(b.head.number, Arc::new(b));
        }
        Ok(())
    }

    /// Removes hot blocks `>= from` and their index entries (reorg).
    pub fn truncate(&self, from: u64) -> Result<Vec<BlockHead>> {
        let gone: Vec<Arc<HotBlock>> = self.mem.read().unwrap().range(from..).map(|(_, b)| b.clone()).collect();
        let mut wb = WriteBatch::default();
        for b in &gone {
            wb.delete_cf(self.cf("hot"), b.head.number.to_be_bytes());
            wb.delete_cf(self.cf("bh"), b.head.hash);
            let txs = b.batches.get(Table::Transactions);
            let hashes = fixed_col(txs, "hash");
            for i in 0..hashes.len() {
                wb.delete_cf(self.cf("tx"), hashes.value(i));
            }
        }
        let mut wo = WriteOptions::default();
        wo.set_sync(true);
        self.db.write_opt(wb, &wo)?;
        let mut mem = self.mem.write().unwrap();
        let _ = mem.split_off(&from);
        Ok(gone.iter().map(|b| b.head).collect())
    }

    /// Drops hot rows `< to` after they were published in a chunk; their
    /// index entries stay.
    pub fn drop_before(&self, to: u64) -> Result<()> {
        let mut wb = WriteBatch::default();
        wb.delete_range_cf(self.cf("hot"), 0u64.to_be_bytes(), to.to_be_bytes());
        self.db.write(wb)?;
        let mut mem = self.mem.write().unwrap();
        *mem = mem.split_off(&to);
        Ok(())
    }

    fn index_into(&self, wb: &mut WriteBatch, b: &Batches) {
        let blocks = b.get(Table::Blocks);
        let (nums, hashes) = (u64_col(blocks, "number"), fixed_col(blocks, "hash"));
        for i in 0..blocks.num_rows() {
            wb.put_cf(self.cf("bh"), hashes.value(i), nums.value(i).to_be_bytes());
        }
        let txs = b.get(Table::Transactions);
        let (bn, ix, th): (&UInt64Array, &UInt64Array, &FixedSizeBinaryArray) =
            (u64_col(txs, "block_number"), u64_col(txs, "transaction_index"), fixed_col(txs, "hash"));
        for i in 0..txs.num_rows() {
            let mut v = [0u8; 12];
            v[..8].copy_from_slice(&bn.value(i).to_be_bytes());
            v[8..].copy_from_slice(&(ix.value(i) as u32).to_be_bytes());
            wb.put_cf(self.cf("tx"), th.value(i), v);
        }
    }

    /// Indexes the hashes of blocks that go straight into a chunk. Written
    /// without the WAL and flushed, before the chunk is published.
    pub fn index_chunk(&self, b: &Batches) -> Result<()> {
        let mut wb = WriteBatch::default();
        self.index_into(&mut wb, b);
        let mut wo = WriteOptions::default();
        wo.disable_wal(true);
        self.db.write_opt(wb, &wo)?;
        Ok(())
    }

    /// Makes index entries written without the WAL durable.
    pub fn flush_index(&self) -> Result<()> {
        let mut fo = rocksdb::FlushOptions::default();
        fo.set_wait(true);
        self.db.flush_cfs_opt(&[self.cf("tx"), self.cf("bh")], &fo)?;
        Ok(())
    }

    pub fn tx_location(&self, hash: &[u8; 32]) -> Result<Option<(u64, u32)>> {
        Ok(self.db.get_pinned_cf(self.cf("tx"), hash)?.and_then(|v| {
            (v.len() == 12).then(|| {
                (u64::from_be_bytes(v[..8].try_into().unwrap()), u32::from_be_bytes(v[8..].try_into().unwrap()))
            })
        }))
    }

    pub fn block_number(&self, hash: &[u8; 32]) -> Result<Option<u64>> {
        Ok(self
            .db
            .get_pinned_cf(self.cf("bh"), hash)?
            .and_then(|v| (v.len() == 8).then(|| u64::from_be_bytes(v[..8].try_into().unwrap()))))
    }

    pub fn checkpoint(&self, dir: &Path) -> Result<()> {
        rocksdb::checkpoint::Checkpoint::new(&self.db)?.create_checkpoint(dir)?;
        Ok(())
    }
}
