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
use parquet::arrow::arrow_reader::{
    ArrowPredicateFn, ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder, RowFilter, RowSelection,
};
use parquet::arrow::{ArrowWriter, ProjectionMask};
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use serde::{Deserialize, Serialize};

use crate::filter::{filtered_cols, key, Filter};
use crate::model::{Batches, BlockHead, Table, TABLES};

const INDEX_MAGIC: &[u8; 8] = b"FSYNIDX2";

/// A run of `SUB_ROWS` rows of a row group (page aligned): the unit of
/// filtering, so a hit decodes a few pages rather than a whole row group.
#[derive(Serialize, Deserialize)]
pub struct SubIndex {
    pub rows: u32,
    pub min_block: u64,
    pub max_block: u64,
    /// One filter per entry of `filtered_cols(table)`.
    pub filters: Vec<Filter>,
}

#[derive(Serialize, Deserialize)]
pub struct RowGroupIndex {
    pub rows: u64,
    pub min_block: u64,
    pub max_block: u64,
    pub subs: Vec<SubIndex>,
}

pub const SUB_ROWS: usize = 4096;

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
        self.tables
            .iter()
            .flat_map(|t| &t.row_groups)
            .flat_map(|g| &g.subs)
            .flat_map(|s| &s.filters)
            .map(|f| f.bytes())
            .sum()
    }
}

pub struct Chunk {
    pub dir: PathBuf,
    pub index: ChunkIndex,
    /// Parquet footers, parsed once.
    meta: [ArrowReaderMetadata; 4],
    /// The table files, memory-mapped once (reads are then plain memory
    /// accesses: no per-page open/seek/close).
    files: [bytes::Bytes; 4],
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
        let mut files = Vec::with_capacity(4);
        for t in TABLES {
            let f = File::open(dir.join(format!("{}.parquet", t.name())))?;
            // Safety: chunk files are immutable once published and are only
            // ever deleted (never truncated) after being replaced.
            let map = unsafe { memmap2::Mmap::map(&f)? };
            let f = bytes::Bytes::from_owner(map);
            files.push(f.clone());
            meta.push(ArrowReaderMetadata::load(
                &f,
                ArrowReaderOptions::new().with_page_index_policy(parquet::file::metadata::PageIndexPolicy::Optional),
            )?);
        }
        Ok(Chunk {
            dir: dir.to_path_buf(),
            index,
            meta: meta.try_into().ok().unwrap(),
            files: files.try_into().ok().unwrap(),
        })
    }

    /// Block slices of `[from, to)` that each cover about `groups` row
    /// groups of the chunk's largest table.
    pub fn slices(&self, from: u64, to: u64, groups: usize) -> Vec<(u64, u64)> {
        let t = self.index.tables.iter().max_by_key(|t| t.rows).expect("tables");
        let mut cuts: Vec<u64> =
            t.row_groups.iter().step_by(groups.max(1)).map(|g| g.min_block).filter(|b| *b > from && *b < to).collect();
        cuts.dedup();
        let mut out = Vec::with_capacity(cuts.len() + 1);
        let mut a = from;
        for c in cuts {
            out.push((a, c));
            a = c;
        }
        if a < to {
            out.push((a, to));
        }
        out
    }

    /// Reads rows of `row_groups` of table `t` that pass `pred` (evaluated
    /// on the `pred_cols` columns only), returning the `cols` columns.
    /// Columns outside `pred_cols` are decoded only for passing rows.
    pub fn read_where(
        &self,
        t: Table,
        row_groups: &[usize],
        cols: &[usize],
        selection: Option<RowSelection>,
        pred_cols: &[usize],
        mut pred: impl FnMut(&RecordBatch) -> anyhow::Result<arrow::array::BooleanArray> + Send + 'static,
    ) -> Result<Vec<RecordBatch>> {
        if row_groups.is_empty() {
            return Ok(Vec::new());
        }
        let f = self.files[t.idx()].clone();
        let meta = self.meta[t.idx()].clone();
        let schema = meta.parquet_schema();
        let mask = ProjectionMask::roots(schema, cols.iter().copied());
        let pmask = ProjectionMask::roots(schema, pred_cols.iter().copied());
        let p = ArrowPredicateFn::new(pmask, move |b: RecordBatch| {
            pred(&b).map_err(|e| arrow::error::ArrowError::ComputeError(e.to_string()))
        });
        let mut builder = ParquetRecordBatchReaderBuilder::new_with_metadata(f, meta)
            .with_row_groups(row_groups.to_vec())
            .with_projection(mask)
            .with_row_filter(RowFilter::new(vec![Box::new(p)]))
            .with_batch_size(64 * 1024);
        if let Some(sel) = selection {
            builder = builder.with_row_selection(sel);
        }
        let mut out = Vec::new();
        for b in builder.build()? {
            let b = b?;
            if b.num_rows() > 0 {
                out.push(b);
            }
        }
        Ok(out)
    }

    /// Rows of `row_groups` (in order) on pages of column `col` whose
    /// min/max range can contain one of `values`; None when the column has
    /// no page statistics.
    pub fn page_selection(&self, t: Table, row_groups: &[usize], col: usize, values: &[&[u8]]) -> Option<RowSelection> {
        use parquet::file::page_index::column_index::ColumnIndexMetaData;
        let md = self.meta[t.idx()].metadata();
        let mut ranges = Vec::new();
        let mut base = 0usize;
        for &g in row_groups {
            let rows = md.row_group(g).num_rows() as usize;
            let pi = md.page_index_for_row_group(g);
            let (Some(ColumnIndexMetaData::FIXED_LEN_BYTE_ARRAY(ci)), Some(oi)) =
                (pi.column_index(col), pi.offset_index(col))
            else {
                return None;
            };
            let locs = oi.page_locations();
            for (p, loc) in locs.iter().enumerate() {
                let (Some(lo), Some(hi)) = (ci.min_value(p), ci.max_value(p)) else { continue };
                if values.iter().any(|v| *v >= lo && *v <= hi) {
                    let start = loc.first_row_index as usize;
                    let end = locs.get(p + 1).map(|n| n.first_row_index as usize).unwrap_or(rows);
                    ranges.push(base + start..base + end);
                }
            }
            base += rows;
        }
        Some(RowSelection::from_consecutive_ranges(ranges.into_iter(), base))
    }

    /// Rows of the sub-groups marked in `masks` (one mask per row group of
    /// `row_groups`, in order).
    pub fn sub_selection(&self, t: Table, row_groups: &[usize], masks: &[Vec<bool>]) -> RowSelection {
        let ti = &self.index.tables[t.idx()];
        let mut ranges = Vec::new();
        let mut base = 0usize;
        for (&g, m) in row_groups.iter().zip(masks) {
            let mut off = base;
            for (s, keep) in ti.row_groups[g].subs.iter().zip(m) {
                if *keep {
                    ranges.push(off..off + s.rows as usize);
                }
                off += s.rows as usize;
            }
            base += ti.row_groups[g].rows as usize;
        }
        RowSelection::from_consecutive_ranges(ranges.into_iter(), base)
    }

    /// Reads `row_groups` of table `t`, only the columns in `cols`
    /// (schema positions, ascending).
    pub fn read(&self, t: Table, row_groups: &[usize], cols: &[usize]) -> Result<Vec<RecordBatch>> {
        if row_groups.is_empty() {
            return Ok(Vec::new());
        }
        let f = self.files[t.idx()].clone();
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Codec {
    Lz4,
    Zstd(i32),
    None,
}

impl std::str::FromStr for Codec {
    type Err = String;
    fn from_str(s: &str) -> Result<Codec, String> {
        match s {
            "lz4" => Ok(Codec::Lz4),
            "none" => Ok(Codec::None),
            "zstd" => Ok(Codec::Zstd(1)),
            z if z.starts_with("zstd") => z[4..].parse().map(Codec::Zstd).map_err(|_| format!("bad codec {s}")),
            _ => Err(format!("unknown codec {s} (lz4, zstd, zstd<level>, none)")),
        }
    }
}

#[derive(Clone, Copy)]
pub struct WriteOpts {
    pub row_group_rows: usize,
    pub codec: Codec,
}

impl Default for WriteOpts {
    fn default() -> Self {
        WriteOpts { row_group_rows: 32 * 1024, codec: Codec::Lz4 }
    }
}

pub fn chunk_name(from: u64, to: u64) -> String {
    format!("{from:012}-{to:012}")
}

/// Column whose values cluster each row group of table `t` (rows sorted by
/// it inside the group, with page-level min/max), if any.
pub fn cluster_col(t: Table) -> Option<&'static str> {
    match t {
        Table::Logs => Some("address"),
        _ => None,
    }
}

const PAGE_ROWS: usize = 1024;

fn props(t: Table, o: &WriteOpts) -> WriterProperties {
    // Small pages everywhere: with the offset index, a point lookup decodes
    // only the pages holding its rows.
    let mut b = WriterProperties::builder().set_data_page_row_count_limit(PAGE_ROWS).set_write_batch_size(PAGE_ROWS);
    if let Some(c) = cluster_col(t) {
        b = b.set_column_statistics_enabled(parquet::schema::types::ColumnPath::from(c), EnabledStatistics::Page);
    }
    b.set_compression(match o.codec {
        Codec::Lz4 => Compression::LZ4_RAW,
        Codec::Zstd(l) => Compression::ZSTD(ZstdLevel::try_new(l).unwrap_or_default()),
        Codec::None => Compression::UNCOMPRESSED,
    })
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
        let mut w = ArrowWriter::try_new(BufWriter::with_capacity(1 << 20, file), t.schema(), Some(props(t, o)))?;
        let mut ti = TableIndex { rows: batch.num_rows() as u64, row_groups: Vec::new() };
        let mut off = 0;
        while off < batch.num_rows() {
            let len = o.row_group_rows.min(batch.num_rows() - off);
            let s = match cluster_col(t) {
                Some(c) => cluster(&batch.slice(off, len), c, t.block_col())?,
                None => batch.slice(off, len),
            };
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

/// Sorts a row group by (`col`, block, original order).
fn cluster(b: &RecordBatch, col: &str, block_col: &str) -> Result<RecordBatch> {
    use arrow::compute::{lexsort_to_indices, take_record_batch, SortColumn};
    let pos = arrow::array::UInt32Array::from_iter_values(0..b.num_rows() as u32);
    let cols = vec![
        SortColumn { values: b.column_by_name(col).unwrap().clone(), options: None },
        SortColumn { values: b.column_by_name(block_col).unwrap().clone(), options: None },
        SortColumn { values: Arc::new(pos), options: None },
    ];
    let idx = lexsort_to_indices(&cols, None)?;
    Ok(take_record_batch(b, &idx)?)
}

fn block_range(t: Table, b: &RecordBatch) -> (u64, u64) {
    let blocks = b.column_by_name(t.block_col()).unwrap().as_any().downcast_ref::<UInt64Array>().unwrap();
    let (mut lo, mut hi) = (u64::MAX, 0);
    for i in 0..blocks.len() {
        if blocks.is_valid(i) {
            let v = blocks.value(i);
            lo = lo.min(v);
            hi = hi.max(v);
        }
    }
    (lo, hi)
}

fn group_index(t: Table, b: &RecordBatch) -> RowGroupIndex {
    let (lo, hi) = block_range(t, b);
    let mut subs = Vec::new();
    let mut off = 0;
    while off < b.num_rows() {
        let s = b.slice(off, SUB_ROWS.min(b.num_rows() - off));
        let (slo, shi) = block_range(t, &s);
        let filters = filtered_cols(t)
            .iter()
            .map(|c| {
                let a = s.column_by_name(c).unwrap().as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap();
                let mut keys = Vec::with_capacity(a.len());
                for i in 0..a.len() {
                    if a.is_valid(i) {
                        keys.push(key(a.value(i)));
                    }
                }
                Filter::build(keys)
            })
            .collect();
        subs.push(SubIndex { rows: s.num_rows() as u32, min_block: slo, max_block: shi, filters });
        off += s.num_rows();
    }
    RowGroupIndex { rows: b.num_rows() as u64, min_block: lo, max_block: hi, subs }
}

fn sync_dir(p: &Path) -> Result<()> {
    File::open(p)?.sync_all()?;
    Ok(())
}

impl Chunk {
    /// Every row of every table, in block order.
    pub fn read_all(&self) -> Result<Batches> {
        let mut tables = Vec::with_capacity(4);
        for t in TABLES {
            let groups: Vec<usize> = (0..self.index.tables[t.idx()].row_groups.len()).collect();
            let cols: Vec<usize> = (0..t.cols().len()).collect();
            let parts = self.read(t, &groups, &cols)?;
            let b = if parts.is_empty() {
                RecordBatch::new_empty(t.schema())
            } else {
                arrow::compute::concat_batches(&t.schema(), &parts)?
            };
            tables.push(if cluster_col(t).is_some() { block_order(t, &b)? } else { b });
        }
        Ok(Batches { tables: tables.try_into().ok().unwrap() })
    }
}

/// Rows back in (block, index) order (undoes clustering).
pub fn block_order(t: Table, b: &RecordBatch) -> Result<RecordBatch> {
    use arrow::compute::take_record_batch;
    let idx_col = if t == Table::Logs { "log_index" } else { "transaction_index" };
    let col = |n: &str| b.column_by_name(n).and_then(|a| a.as_any().downcast_ref::<UInt64Array>().cloned());
    let (Some(bn), Some(ix)) = (col(t.block_col()), col(idx_col)) else { return Ok(b.clone()) };
    // One u64 key per row: (block, index) packed; then an unstable sort of
    // (key, position) pairs, much cheaper than a two-column lexsort.
    let lo = bn.values().iter().min().copied().unwrap_or(0);
    let mut keyed: Vec<(u64, u32)> =
        (0..b.num_rows()).map(|i| (((bn.value(i) - lo) << 24) | ix.value(i).min((1 << 24) - 1), i as u32)).collect();
    if keyed.windows(2).all(|w| w[0].0 <= w[1].0) {
        return Ok(b.clone());
    }
    keyed.sort_unstable();
    let idx = arrow::array::UInt32Array::from_iter_values(keyed.into_iter().map(|(_, i)| i));
    Ok(take_record_batch(b, &idx)?)
}

/// Merges adjacent chunks into one new chunk (published under its own
/// name; the caller swaps it in and deletes the inputs).
pub fn merge(root: &Path, chunks: &[Arc<Chunk>], o: &WriteOpts) -> Result<Chunk> {
    let (first, last) = (chunks.first().expect("chunks"), chunks.last().expect("chunks"));
    let mut parts = Vec::with_capacity(chunks.len());
    for w in chunks.windows(2) {
        if w[0].to_block() != w[1].from_block() {
            bail!("merge: chunks are not adjacent");
        }
    }
    for c in chunks {
        parts.push(c.read_all()?);
    }
    let b = Batches::concat(&parts);
    write(root, &b, first.from_block(), last.to_block(), last.index.last, o)
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
        Self::scan(root, true)
    }

    /// Opens the chunks without cleaning up (another process may be writing).
    pub fn open_read_only(root: &Path) -> Result<ChunkSet> {
        Self::scan(root, false)
    }

    fn scan(root: &Path, cleanup: bool) -> Result<ChunkSet> {
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
                    if cleanup {
                        tracing::info!(dir = %p.display(), "removing chunk superseded by compaction");
                        fs::remove_dir_all(&p)?;
                    }
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
