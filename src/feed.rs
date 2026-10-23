//! Push ingest: a source that delivers blocks itself (a reth execution
//! extension, a replay tool) appends them here instead of faster-sync
//! polling a node.
//!
//! Finalized blocks arriving while the hot tier is empty (backfill) are
//! buffered and written straight into chunks; others go to the hot tier.
//! `revert` drops blocks after a reorg; `finalize` moves final hot blocks
//! into chunks.

use std::sync::Arc;
use std::time::Instant;

use anyhow::{bail, Result};

use crate::chunk;
use crate::ingest::{build, build_each, chunk_hot, IngestConfig};
use crate::metrics::Metrics;
use crate::model::{Batches, BlockHead};
use crate::rpc::RawBlock;
use crate::store::Store;

pub struct Feed {
    pub store: Arc<Store>,
    pub cfg: IngestConfig,
    pub stats: Arc<Metrics>,
    pending: Vec<Batches>,
    pending_rows: usize,
    pending_from: u64,
    last: Option<BlockHead>,
}

impl Feed {
    pub fn new(store: Arc<Store>, cfg: IngestConfig, stats: Arc<Metrics>) -> Feed {
        let last = store.last();
        Feed { store, cfg, stats, pending: Vec::new(), pending_rows: 0, pending_from: 0, last }
    }

    /// First block the feed expects next.
    pub fn next_block(&self) -> u64 {
        self.last.map(|h| h.number + 1).unwrap_or(self.store.start_block())
    }

    /// Appends consecutive blocks; `finalized` is the source's finalized
    /// block at this point.
    pub async fn push(&mut self, raw: Vec<RawBlock>, finalized: u64) -> Result<()> {
        if raw.is_empty() {
            return Ok(());
        }
        let n = raw.len() as u64;
        let t = Instant::now();
        let all_final = self.store.hot.is_empty() && {
            let last_num = raw.last().and_then(|r| r.block.get("number")).and_then(crate::rpc::qty_u64);
            last_num.is_some_and(|x| x <= finalized)
        };
        if all_final {
            let (batches, heads) = tokio::task::spawn_blocking(move || build(raw)).await??;
            self.stats.decode.observe(t.elapsed());
            for h in &heads {
                self.check_link(h)?;
                self.last = Some(*h);
            }
            if self.pending.is_empty() {
                self.pending_from = heads[0].number;
            }
            self.pending_rows += batches.rows();
            self.stats.rows.fetch_add(batches.rows() as u64, std::sync::atomic::Ordering::Relaxed);
            self.pending.push(batches);
            if self.pending_rows >= self.cfg.chunk_rows
                || self.last.unwrap().number + 1 - self.pending_from >= self.cfg.chunk_max_blocks
            {
                self.flush().await?;
            }
        } else {
            self.flush().await?;
            let blocks = tokio::task::spawn_blocking(move || build_each(raw)).await??;
            self.stats.decode.observe(t.elapsed());
            for b in &blocks {
                self.check_link(&b.head)?;
                self.last = Some(b.head);
            }
            let rows: usize = blocks.iter().map(|b| b.batches.rows()).sum();
            self.stats.rows.fetch_add(rows as u64, std::sync::atomic::Ordering::Relaxed);
            let store = self.store.clone();
            tokio::task::spawn_blocking(move || store.hot.insert(blocks)).await??;
            self.store.notify();
        }
        self.stats.blocks.fetch_add(n, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    fn check_link(&self, h: &BlockHead) -> Result<()> {
        if let Some(p) = self.last {
            if h.number != p.number + 1 || h.parent_hash != p.hash {
                bail!("feed: block {} does not extend {} (missing revert?)", h.number, p.number);
            }
        }
        Ok(())
    }

    /// Writes buffered final blocks as a chunk.
    pub async fn flush(&mut self) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let parts = std::mem::take(&mut self.pending);
        self.pending_rows = 0;
        let (store, opts, stats, from, last) =
            (self.store.clone(), self.cfg.write, self.stats.clone(), self.pending_from, self.last.unwrap());
        let c = tokio::task::spawn_blocking(move || -> Result<chunk::Chunk> {
            let t = Instant::now();
            let b = Batches::concat(&parts);
            store.hot.index_chunk(&b)?;
            store.hot.flush_index()?;
            let c = chunk::write(store.chunks_root(), &b, from, last.number + 1, last, &opts)?;
            stats.chunk_write.observe(t.elapsed());
            tracing::info!(from, to = last.number + 1, rows = b.rows(), "chunk written");
            Ok(c)
        })
        .await??;
        self.store.publish(c);
        self.store.notify();
        Ok(())
    }

    /// Drops every block after `keep` (reorg or revert).
    pub async fn revert(&mut self, keep: u64) -> Result<()> {
        if self.store.chunk_end().is_some_and(|e| keep + 1 < e) || (!self.pending.is_empty()) {
            bail!("feed: revert to {keep} reaches finalized (chunked) blocks");
        }
        let store = self.store.clone();
        let gone = tokio::task::spawn_blocking(move || store.hot.truncate(keep + 1)).await??;
        self.stats.reorgs.fetch_add(gone.len() as u64, std::sync::atomic::Ordering::Relaxed);
        self.last = self.store.last();
        self.store.notify();
        Ok(())
    }

    /// Records the source's finalized block and chunks final hot blocks.
    pub async fn finalize(&mut self, finalized: u64, head: u64) -> Result<()> {
        self.store.set_chain(head, finalized);
        chunk_hot(&self.store, &self.cfg, &self.stats, finalized, false).await
    }
}
