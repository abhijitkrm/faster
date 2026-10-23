//! The store: published chunks (history) plus the RocksDB hot tier (newest
//! blocks), and the facts the API reports about them.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use crate::chunk::{Chunk, ChunkSet};
use crate::hot::{Hot, HotBlock};
use crate::model::BlockHead;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Meta {
    pub chain_id: u64,
    pub start_block: u64,
    pub version: u32,
}

const META_VERSION: u32 = 1;

pub struct Store {
    pub root: PathBuf,
    pub meta: Meta,
    chunks: RwLock<ChunkSet>,
    pub hot: Hot,
    /// Last block stored (chunks or hot); watchers wake on every change.
    height: watch::Sender<Option<u64>>,
    pub chain_head: AtomicU64,
    pub finalized: AtomicU64,
}

/// What a query sees: chunks and hot blocks without overlap or gap.
pub struct View {
    pub chunks: Vec<Arc<Chunk>>,
    pub hot: Vec<Arc<HotBlock>>,
    /// First block not covered by this view.
    pub end: u64,
}

impl Store {
    /// Opens (or creates) the store under `root` for chain `chain_id`.
    pub fn open(root: &Path, chain_id: u64, start_block: u64) -> Result<Store> {
        std::fs::create_dir_all(root)?;
        let mp = root.join("meta.json");
        let meta = if mp.exists() {
            let m: Meta = serde_json::from_slice(&std::fs::read(&mp)?).context("meta.json")?;
            if m.chain_id != chain_id {
                bail!("data dir {} holds chain {} but the node is chain {}", root.display(), m.chain_id, chain_id);
            }
            if m.version != META_VERSION {
                bail!("data dir version {} (this build reads {})", m.version, META_VERSION);
            }
            m
        } else {
            let m = Meta { chain_id, start_block, version: META_VERSION };
            std::fs::write(&mp, serde_json::to_vec_pretty(&m)?)?;
            m
        };
        let chunks = ChunkSet::open(root)?;
        let hot = Hot::open(&root.join("hot"))?;
        if let (Some(end), Some(first)) = (chunks.end(), hot.first()) {
            if first < end {
                // Crash after a chunk was published but before its hot rows
                // were dropped.
                hot.drop_before(end)?;
            } else if first > end {
                bail!("hot tier starts at {first} but chunks end at {end}");
            }
        }
        let s = Store {
            root: root.to_path_buf(),
            meta,
            chunks: RwLock::new(chunks),
            hot,
            height: watch::channel(None).0,
            chain_head: AtomicU64::new(0),
            finalized: AtomicU64::new(0),
        };
        s.height.send_replace(s.last().map(|h| h.number));
        Ok(s)
    }

    pub fn start_block(&self) -> u64 {
        self.meta.start_block
    }

    /// First block not stored yet.
    pub fn next_block(&self) -> u64 {
        if let Some(h) = self.hot.last() {
            return h.number + 1;
        }
        self.chunks.read().unwrap().end().unwrap_or(self.meta.start_block)
    }

    pub fn last(&self) -> Option<BlockHead> {
        self.hot.last().or_else(|| self.chunks.read().unwrap().last_head())
    }

    pub fn chunk_end(&self) -> Option<u64> {
        self.chunks.read().unwrap().end()
    }

    pub fn first_block(&self) -> Option<u64> {
        let c = self.chunks.read().unwrap().first_block();
        c.or_else(|| self.hot.first())
    }

    pub fn chunk_count(&self) -> usize {
        self.chunks.read().unwrap().chunks.len()
    }

    pub fn chunk_stats(&self) -> (usize, u64, usize) {
        let c = self.chunks.read().unwrap();
        let rows: u64 = c.chunks.values().flat_map(|c| c.index.tables.iter()).map(|t| t.rows).sum();
        let fb: usize = c.chunks.values().map(|c| c.index.filter_bytes()).sum();
        (c.chunks.len(), rows, fb)
    }

    pub fn publish(&self, c: Chunk) {
        let mut cs = self.chunks.write().unwrap();
        cs.chunks.insert(c.from_block(), Arc::new(c));
    }

    /// Replaces the chunks merged into `c` (compaction).
    pub fn replace(&self, c: Chunk, merged: &[u64]) {
        let mut cs = self.chunks.write().unwrap();
        for k in merged {
            cs.chunks.remove(k);
        }
        cs.chunks.insert(c.from_block(), Arc::new(c));
    }

    pub fn all_chunks(&self) -> Vec<Arc<Chunk>> {
        self.chunks.read().unwrap().chunks.values().cloned().collect()
    }

    pub fn notify(&self) {
        self.height.send_replace(self.last().map(|h| h.number));
    }

    pub fn subscribe(&self) -> watch::Receiver<Option<u64>> {
        self.height.subscribe()
    }

    /// Chunks and hot blocks covering `[from, to)`.
    ///
    /// The hot snapshot is taken before the chunk snapshot: the chunker
    /// publishes a chunk before dropping its hot rows, so a block missing
    /// from the hot snapshot is always in the chunk snapshot. Hot blocks
    /// below the chunk end are duplicates and ignored.
    pub fn view(&self, from: u64, to: u64) -> View {
        let hot = self.hot.range(from, to);
        let cs = self.chunks.read().unwrap();
        let cend = cs.end().unwrap_or(0);
        let chunks = cs.range(from, to);
        drop(cs);
        let hot: Vec<_> = hot.into_iter().filter(|b| b.head.number >= cend).collect();
        let end = match hot.last() {
            Some(b) => b.head.number + 1,
            None => cend.max(from).min(to),
        };
        View { chunks, hot, end: end.min(to) }
    }

    pub fn chunks_root(&self) -> &Path {
        &self.root
    }

    pub fn set_chain(&self, head: u64, fin: u64) {
        self.chain_head.store(head, Ordering::Relaxed);
        self.finalized.store(fin, Ordering::Relaxed);
    }
}
