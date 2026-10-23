//! Immutable chunks: a contiguous range of finalized blocks stored as one
//! Parquet file per table plus `index.bin` (per-row-group block ranges and
//! value filters).
//!
//! A chunk is built in `tmp/`, fsynced and renamed into `chunks/` in one
//! step, so a chunk directory either exists complete or not at all.

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use arrow::array::{Array, FixedSizeBinaryArray, UInt64Array};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder};
use parquet::arrow::{ArrowWriter, ProjectionMask};
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use serde::{Deserialize, Serialize};

use crate::filter::{filtered_cols, key, Filter};
use crate::model::{Batches, BlockHead, Table, TABLES};

const INDEX_MAGIC: &[u8; 8] = b"FSYNIDX1";

#[derive(Serialize, Deserialize)]
pub struct RowGroupIndex {
    pub rows: u64,
    pub min_block: u64,
    pub max_block: u64,
    /// One filter per entry of `filtered_cols(table)`.
    pub filters: Vec<Filter>,
}

#[derive(Serialize, Deserialize, Default)]
pub struct TableIndex {
    pub rows: u64,
    pub row_groups: Vec<RowGroupIndex>,
}

#[derive(Serialize, Deserialize)]
pub struct ChunkIndex {
    pub from_block: u64,
    /// Exclusive.
    pub to_block: u64,
    pub last: BlockHead,
    pub tables: Vec<TableIndex>,
}

impl ChunkIndex {
    pub fn filter_bytes(&self) -> usize {
        self.tables.iter().flat_map(|t| &t.row_groups).flat_map(|g| &g.filters).map(|f| f.bytes()).sum()
    }
}

pub struct Chunk {
    pub dir: PathBuf,
    pub index: ChunkIndex,
    /// Parquet footers, parsed once.
    meta: [ArrowReaderMetadata; 4],
}

impl Chunk {
    pub fn from_block(&self) -> u64 {
        self.index.from_block
    }
    pub fn to_block(&self) -> u64 {
        self.index.to_block
    }

    pub fn open(dir: &Path) -> Result<Chunk> {
        let mut buf = Vec::new();
        File::open(dir.join("index.bin"))?.read_to_end(&mut buf)?;
        if buf.len() < 8 || &buf[..8] != INDEX_MAGIC {
            bail!("{}: bad index magic", dir.display());
        }
        let index: ChunkIndex = bincode::deserialize(&buf[8..]).context("index.bin")?;
        let mut meta = Vec::with_capacity(4);
        for t in TABLES {
            let f = File::open(dir.join(format!("{}.parquet", t.name())))?;
            meta.push(ArrowReaderMetadata::load(&f, ArrowReaderOptions::new())?);
        }
        Ok(Chunk { dir: dir.to_path_buf(), index, meta: meta.try_into().ok().unwrap() })
    }

    /// Reads `row_groups` of table `t`, only the columns in `cols`
    /// (schema positions, ascending).
    pub fn read(&self, t: Table, row_groups: &[usize], cols: &[usize]) -> Result<Vec<RecordBatch>> {
        if row_groups.is_empty() {
            return Ok(Vec::new());
        }
        let f = File::open(self.dir.join(format!("{}.parquet", t.name())))?;
        let meta = self.meta[t.idx()].clone();
        let mask = ProjectionMask::roots(meta.parquet_schema(), cols.iter().copied());
        let reader = ParquetRecordBatchReaderBuilder::new_with_metadata(f, meta)
            .with_row_groups(row_groups.to_vec())
            .with_projection(mask)
            .with_batch_size(64 * 1024)
            .build()?;
        let mut out = Vec::new();
        for b in reader {
            out.push(b?);
        }
        Ok(out)
    }
}

pub struct WriteOpts {
    pub row_group_rows: usize,
    pub zstd_level: i32,
}

impl Default for WriteOpts {
    fn default() -> Self {
        WriteOpts { row_group_rows: 32 * 1024, zstd_level: 1 }
    }
}

pub fn chunk_name(from: u64, to: u64) -> String {
    format!("{from:012}-{to:012}")
}

fn props(o: &WriteOpts) -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(o.zstd_level).unwrap_or_default()))
        .set_max_row_group_row_count(Some(o.row_group_rows))
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .build()
}

/// Writes `b` (blocks `from..to`) as a chunk under `root/chunks/` and
/// returns the opened chunk.
pub fn write(root: &Path, b: &Batches, from: u64, to: u64, last: BlockHead, o: &WriteOpts) -> Result<Chunk> {
    let name = chunk_name(from, to);
    let tmp = root.join("tmp").join(&name);
    let _ = fs::remove_dir_all(&tmp);
    fs::create_dir_all(&tmp)?;
    let mut tables = Vec::with_capacity(4);
    for t in TABLES {
        let batch = b.get(t);
        let path = tmp.join(format!("{}.parquet", t.name()));
        let file = File::create(&path)?;
        let mut w = ArrowWriter::try_new(BufWriter::with_capacity(1 << 20, file), t.schema(), Some(props(o)))?;
        let mut ti = TableIndex { rows: batch.num_rows() as u64, row_groups: Vec::new() };
        let mut off = 0;
        while off < batch.num_rows() {
            let len = o.row_group_rows.min(batch.num_rows() - off);
            let s = batch.slice(off, len);
            w.write(&s)?;
            w.flush()?;
            ti.row_groups.push(group_index(t, &s));
            off += len;
        }
        let inner = w.into_inner()?;
        let file = inner.into_inner().map_err(|e| anyhow::anyhow!("flush {}: {}", path.display(), e))?;
        file.sync_all()?;
        tables.push(ti);
    }
    let index = ChunkIndex { from_block: from, to_block: to, last, tables };
    let mut f = BufWriter::new(File::create(tmp.join("index.bin"))?);
    f.write_all(INDEX_MAGIC)?;
    bincode::serialize_into(&mut f, &index)?;
    f.into_inner().map_err(|e| anyhow::anyhow!("index.bin: {e}"))?.sync_all()?;
    sync_dir(&tmp)?;
    let dst = root.join("chunks").join(&name);
    fs::rename(&tmp, &dst)?;
    sync_dir(&root.join("chunks"))?;
    Chunk::open(&dst)
}

fn group_index(t: Table, b: &RecordBatch) -> RowGroupIndex {
    let blocks = b.column_by_name(t.block_col()).unwrap().as_any().downcast_ref::<UInt64Array>().unwrap();
    let (mut lo, mut hi) = (u64::MAX, 0);
    for i in 0..blocks.len() {
        if blocks.is_valid(i) {
            let v = blocks.value(i);
            lo = lo.min(v);
            hi = hi.max(v);
        }
    }
    let filters = filtered_cols(t)
        .iter()
        .map(|c| {
            let a = b.column_by_name(c).unwrap().as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap();
            let mut keys = Vec::with_capacity(a.len());
            for i in 0..a.len() {
                if a.is_valid(i) {
                    keys.push(key(a.value(i)));
                }
            }
            Filter::build(keys)
        })
        .collect();
    RowGroupIndex { rows: b.num_rows() as u64, min_block: lo, max_block: hi, filters }
}

fn sync_dir(p: &Path) -> Result<()> {
    File::open(p)?.sync_all()?;
    Ok(())
}

/// The published chunks, keyed by first block.
pub struct ChunkSet {
    root: PathBuf,
    pub chunks: BTreeMap<u64, Arc<Chunk>>,
}

impl ChunkSet {
    /// Opens every chunk under `root/chunks`, removes leftovers in `tmp/`
    /// and chunks superseded by a compacted chunk, and checks that the
    /// chunks are contiguous.
    pub fn open(root: &Path) -> Result<ChunkSet> {
        fs::create_dir_all(root.join("chunks"))?;
        let _ = fs::remove_dir_all(root.join("tmp"));
        fs::create_dir_all(root.join("tmp"))?;
        let mut ranges: Vec<(u64, u64, PathBuf)> = Vec::new();
        for e in fs::read_dir(root.join("chunks"))? {
            let e = e?;
            let name = e.file_name().to_string_lossy().to_string();
            let Some((a, b)) = name.split_once('-') else { continue };
            let (Ok(a), Ok(b)) = (a.parse::<u64>(), b.parse::<u64>()) else { continue };
            ranges.push((a, b, e.path()));
        }
        // A compaction publishes the merged chunk before deleting its inputs;
        // after a crash in between, the merged (wider) chunk wins.
        ranges.sort_by(|x, y| x.0.cmp(&y.0).then(y.1.cmp(&x.1)));
        let mut chunks = BTreeMap::new();
        let mut end: Option<u64> = None;
        for (a, b, p) in ranges {
            if let Some(e) = end {
                if b <= e {
                    tracing::info!(dir = %p.display(), "removing chunk superseded by compaction");
                    fs::remove_dir_all(&p)?;
                    continue;
                }
                if a != e {
                    bail!("chunks are not contiguous: gap or overlap at block {e} ({})", p.display());
                }
            }
            let c = Chunk::open(&p).with_context(|| format!("open chunk {}", p.display()))?;
            end = Some(b);
            chunks.insert(a, Arc::new(c));
        }
        Ok(ChunkSet { root: root.to_path_buf(), chunks })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn first_block(&self) -> Option<u64> {
        self.chunks.values().next().map(|c| c.from_block())
    }

    /// First block not in any chunk.
    pub fn end(&self) -> Option<u64> {
        self.chunks.values().next_back().map(|c| c.to_block())
    }

    pub fn last_head(&self) -> Option<BlockHead> {
        self.chunks.values().next_back().map(|c| c.index.last)
    }

    /// Chunks overlapping `[from, to)`, in order.
    pub fn range(&self, from: u64, to: u64) -> Vec<Arc<Chunk>> {
        let start = self.chunks.range(..=from).next_back().map(|(k, _)| *k).unwrap_or(from);
        self.chunks.range(start..to).map(|(_, c)| c.clone()).filter(|c| c.to_block() > from).collect()
    }
}
