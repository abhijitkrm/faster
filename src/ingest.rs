//! Ingest: parallel backfill of finalized history straight into chunks,
//! then a live tail into the hot tier with reorg handling, and a chunker
//! that moves finalized hot blocks into chunks.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Result};
use futures::StreamExt;

use crate::chunk::{self, WriteOpts};
use crate::hot::HotBlock;
use crate::model::{Batches, BlockHead, Builder};
use crate::rpc::{RawBlock, Rpc, RpcError};
use crate::store::Store;

#[derive(Clone)]
pub struct IngestConfig {
    /// Blocks per fetch task.
    pub window: u64,
    /// Fetch tasks in flight.
    pub concurrency: usize,
    /// Depth below the head treated as final when the node has no
    /// "finalized" tag (or `use_finalized_tag` is off).
    pub finality_depth: u64,
    pub use_finalized_tag: bool,
    /// Rows per chunk during backfill.
    pub chunk_rows: usize,
    pub chunk_max_blocks: u64,
    /// The hot tier is chunked when its finalized part reaches this many
    /// rows, or its oldest finalized block is older than `hot_max_age`.
    pub hot_max_rows: usize,
    pub hot_max_age: Duration,
    pub poll: Duration,
    /// Stop after this block (tests, benchmarks).
    pub end_block: Option<u64>,
    pub write: WriteOpts,
}

impl Default for IngestConfig {
    fn default() -> Self {
        IngestConfig {
            window: 20,
            concurrency: 8,
            finality_depth: 64,
            use_finalized_tag: true,
            chunk_rows: 2_000_000,
            chunk_max_blocks: 250_000,
            hot_max_rows: 500_000,
            hot_max_age: Duration::from_secs(3600),
            poll: Duration::from_millis(250),
            end_block: None,
            write: WriteOpts::default(),
        }
    }
}

pub struct Ingest {
    pub store: Arc<Store>,
    pub rpc: Arc<Rpc>,
    pub cfg: IngestConfig,
    pub stats: Arc<crate::metrics::Metrics>,
}

fn build(raw: Vec<RawBlock>) -> Result<(Batches, Vec<BlockHead>)> {
    let mut b = Builder::new();
    for r in raw {
        b.add_block(r.block, r.receipts, r.traces)?;
    }
    let heads = b.heads.clone();
    Ok((b.finish(), heads))
}

fn build_each(raw: Vec<RawBlock>) -> Result<Vec<HotBlock>> {
    raw.into_iter()
        .map(|r| {
            let mut b = Builder::new();
            let head = b.add_block(r.block, r.receipts, r.traces)?;
            Ok(HotBlock { head, batches: b.finish() })
        })
        .collect()
}

/// Calls `f` until it succeeds. Retryable errors (rate limits, timeouts,
/// node behind) are retried forever with capped backoff: a long outage of
/// the node must not stop ingest. Other errors fail after a few tries.
async fn retry<T, F, Fut>(what: &str, mut f: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, RpcError>>,
{
    let mut delay = Duration::from_millis(200);
    let mut tries = 0u32;
    loop {
        match f().await {
            Ok(v) => return Ok(v),
            Err(e) if e.retryable() || tries < 3 => {
                tries += 1;
                if tries <= 3 || tries.is_power_of_two() {
                    tracing::warn!(error = %e, what, tries, "rpc retry");
                }
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(Duration::from_secs(15));
            }
            Err(e) => bail!("{what}: {e}"),
        }
    }
}

impl Ingest {
    async fn chain(&self) -> Result<(u64, u64)> {
        let head = retry("eth_blockNumber", || self.rpc.head()).await?;
        let mut fin = head.saturating_sub(self.cfg.finality_depth);
        if self.cfg.use_finalized_tag {
            if let Some(f) = retry("finalized", || self.rpc.finalized()).await? {
                fin = f.min(head);
            }
        }
        if let Some(e) = self.cfg.end_block {
            return Ok((head.min(e), fin.min(e)));
        }
        Ok((head, fin))
    }

    pub async fn run(&self) -> Result<()> {
        let mut last_reorg_check = Instant::now();
        loop {
            let (head, fin) = self.chain().await?;
            self.store.set_chain(head, fin);
            let next = self.store.next_block();
            if let Some(e) = self.cfg.end_block {
                if next > e {
                    self.chunk_hot(fin, true).await?;
                    tracing::info!(end = e, "reached END_BLOCK");
                    return Ok(());
                }
            }
            if self.store.hot.is_empty() && next + self.cfg.window <= fin + 1 {
                self.backfill(next, fin + 1).await?;
                continue;
            }
            if next <= head {
                self.tail(next, head).await?;
                self.chunk_hot(fin, false).await?;
                continue;
            }
            self.chunk_hot(fin, false).await?;
            if last_reorg_check.elapsed() > Duration::from_secs(2) {
                last_reorg_check = Instant::now();
                self.check_tip().await?;
            }
            tokio::time::sleep(self.cfg.poll).await;
        }
    }

    /// Fetches `[from, to)` (finalized) in parallel and writes chunks.
    async fn backfill(&self, from: u64, to: u64) -> Result<()> {
        tracing::info!(from, to, "backfill");
        let w = self.cfg.window;
        let windows: Vec<(u64, u64)> = (from..to).step_by(w as usize).map(|a| (a, (a + w).min(to))).collect();
        let rpc = self.rpc.clone();
        let stats = self.stats.clone();
        let mut stream = futures::stream::iter(windows)
            .map(|(a, b)| {
                let rpc = rpc.clone();
                let stats = stats.clone();
                tokio::spawn(async move {
                    let nums: Vec<u64> = (a..b).collect();
                    let raw = retry("blocks", || rpc.blocks(&nums)).await?;
                    let t = Instant::now();
                    let r = tokio::task::spawn_blocking(move || build(raw)).await??;
                    stats.decode.observe(t.elapsed());
                    Ok::<_, anyhow::Error>(r)
                })
            })
            .buffered(self.cfg.concurrency);

        let mut prev = self.store.last();
        let mut parts: Vec<Batches> = Vec::new();
        let (mut rows, mut start) = (0usize, from);
        let mut pending: Option<tokio::task::JoinHandle<Result<chunk::Chunk>>> = None;
        while let Some(r) = stream.next().await {
            let (batches, heads) = r??;
            for h in &heads {
                if let Some(p) = prev {
                    if h.number != p.number + 1 || h.parent_hash != p.hash {
                        bail!("backfill: block {} does not link to {} (finality depth too small?)", h.number, p.number);
                    }
                }
                prev = Some(*h);
            }
            rows += batches.rows();
            self.stats.blocks.fetch_add(heads.len() as u64, std::sync::atomic::Ordering::Relaxed);
            self.stats.rows.fetch_add(batches.rows() as u64, std::sync::atomic::Ordering::Relaxed);
            parts.push(batches);
            let end = prev.unwrap().number + 1;
            if rows >= self.cfg.chunk_rows || end - start >= self.cfg.chunk_max_blocks || end == to {
                let all = std::mem::take(&mut parts);
                if let Some(p) = pending.take() {
                    self.store.publish(p.await??);
                    self.store.notify();
                }
                let (store, last, opts, stats) = (self.store.clone(), prev.unwrap(), self.cfg.write, stats.clone());
                pending = Some(tokio::task::spawn_blocking(move || {
                    let t = Instant::now();
                    let b = Batches::concat(&all);
                    store.hot.index_chunk(&b)?;
                    store.hot.flush_index()?;
                    let c = chunk::write(store.chunks_root(), &b, start, end, last, &opts)?;
                    stats.chunk_write.observe(t.elapsed());
                    tracing::info!(
                        from = start,
                        to = end,
                        rows = b.rows(),
                        ms = t.elapsed().as_millis() as u64,
                        "chunk written"
                    );
                    Ok(c)
                }));
                rows = 0;
                start = end;
            }
        }
        if let Some(p) = pending.take() {
            self.store.publish(p.await??);
            self.store.notify();
        }
        Ok(())
    }

    /// Fetches `[next, head]` into the hot tier, following parent links;
    /// on a fork, drops our divergent blocks and returns (the next round
    /// refetches).
    async fn tail(&self, next: u64, head: u64) -> Result<()> {
        let to = (head + 1).min(next + self.cfg.window * self.cfg.concurrency as u64);
        let w = self.cfg.window;
        let futs = (next..to).step_by(w as usize).map(|a| {
            let nums: Vec<u64> = (a..(a + w).min(to)).collect();
            let rpc = self.rpc.clone();
            async move { retry("blocks", || rpc.blocks(&nums)).await }
        });
        let mut raw = Vec::new();
        for r in futures::future::join_all(futs).await {
            raw.extend(r?);
        }
        let t = Instant::now();
        let blocks = tokio::task::spawn_blocking(move || build_each(raw)).await??;
        self.stats.decode.observe(t.elapsed());
        let mut prev = self.store.last();
        let mut keep = Vec::with_capacity(blocks.len());
        for b in blocks {
            if let Some(p) = prev {
                if b.head.parent_hash != p.hash {
                    if keep.is_empty() {
                        self.rollback(p).await?;
                        return Ok(());
                    }
                    break;
                }
            }
            prev = Some(b.head);
            keep.push(b);
        }
        let n = keep.len() as u64;
        let rows: usize = keep.iter().map(|b| b.batches.rows()).sum();
        let store = self.store.clone();
        tokio::task::spawn_blocking(move || store.hot.insert(keep)).await??;
        self.stats.blocks.fetch_add(n, std::sync::atomic::Ordering::Relaxed);
        self.stats.rows.fetch_add(rows as u64, std::sync::atomic::Ordering::Relaxed);
        self.store.notify();
        Ok(())
    }

    /// Our block `p` is not the parent of the node's next block: drop it.
    async fn rollback(&self, p: BlockHead) -> Result<()> {
        if self.store.hot.get(p.number).is_none() {
            bail!(
                "reorg below the chunked range at block {} (finalized data changed; raise FINALITY_DEPTH and rebuild)",
                p.number
            );
        }
        tracing::warn!(block = p.number, "reorg: dropping block");
        self.stats.reorgs.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let store = self.store.clone();
        tokio::task::spawn_blocking(move || store.hot.truncate(p.number)).await??;
        self.store.notify();
        Ok(())
    }

    /// Idle check: does the node still have our newest block?
    async fn check_tip(&self) -> Result<()> {
        let Some(last) = self.store.hot.last() else { return Ok(()) };
        if let Some(h) = retry("block hash", || self.rpc.block_hash(last.number)).await? {
            if h != last.hash {
                self.rollback(last).await?;
            }
        }
        Ok(())
    }

    /// Moves finalized hot blocks into a chunk when there are enough of them,
    /// they are old enough, or `force`.
    async fn chunk_hot(&self, fin: u64, force: bool) -> Result<()> {
        let Some(first) = self.store.hot.first() else { return Ok(()) };
        if first > fin {
            return Ok(());
        }
        let to = (fin + 1).min(first + self.cfg.chunk_max_blocks);
        let rows = self.store.hot.rows_until(to);
        let old = self
            .store
            .hot
            .get(first)
            .map(|b| now_secs().saturating_sub(b.head.timestamp) > self.cfg.hot_max_age.as_secs())
            .unwrap_or(false);
        if !(force || rows >= self.cfg.hot_max_rows || old) {
            return Ok(());
        }
        let blocks = self.store.hot.range(first, to);
        let Some(last) = blocks.last().map(|b| b.head) else { return Ok(()) };
        let end = last.number + 1;
        let (store, opts, stats) = (self.store.clone(), self.cfg.write, self.stats.clone());
        tokio::task::spawn_blocking(move || -> Result<()> {
            let t = Instant::now();
            let parts: Vec<Batches> = blocks.iter().map(|b| b.batches.clone()).collect();
            let b = Batches::concat(&parts);
            let c = chunk::write(store.chunks_root(), &b, first, end, last, &opts)?;
            store.publish(c);
            store.hot.drop_before(end)?;
            stats.chunk_write.observe(t.elapsed());
            tracing::info!(from = first, to = end, rows = b.rows(), "hot blocks chunked");
            Ok(())
        })
        .await??;
        self.store.notify();
        Ok(())
    }
}

/// Merges runs of small adjacent chunks (the live tail writes many) into
/// chunks of up to `chunk_rows` rows, one merge per call. Returns whether
/// it merged anything.
pub fn compact_once(store: &Store, cfg: &IngestConfig) -> Result<bool> {
    let chunks = store.all_chunks();
    let rows = |c: &chunk::Chunk| c.index.tables.iter().map(|t| t.rows).sum::<u64>() as usize;
    let mut i = 0;
    while i < chunks.len() {
        let (mut sum, mut j) = (0usize, i);
        while j < chunks.len()
            && sum + rows(&chunks[j]) <= cfg.chunk_rows
            && chunks[j].to_block() - chunks[i].from_block() <= cfg.chunk_max_blocks
        {
            sum += rows(&chunks[j]);
            j += 1;
        }
        if j - i >= 2 {
            let run = &chunks[i..j];
            let t = Instant::now();
            let merged = chunk::merge(store.chunks_root(), run, &cfg.write)?;
            let keys: Vec<u64> = run.iter().map(|c| c.from_block()).collect();
            let dirs: Vec<std::path::PathBuf> = run.iter().map(|c| c.dir.clone()).collect();
            tracing::info!(
                from = merged.from_block(),
                to = merged.to_block(),
                inputs = run.len(),
                rows = sum,
                ms = t.elapsed().as_millis() as u64,
                "chunks compacted"
            );
            store.replace(merged, &keys);
            for d in dirs {
                std::fs::remove_dir_all(&d)?;
            }
            return Ok(true);
        }
        i = j.max(i + 1);
    }
    Ok(false)
}

/// Background compaction loop.
pub async fn compactor(store: Arc<Store>, cfg: IngestConfig, every: Duration) {
    loop {
        tokio::time::sleep(every).await;
        loop {
            let (s, c) = (store.clone(), cfg.clone());
            match tokio::task::spawn_blocking(move || compact_once(&s, &c)).await {
                Ok(Ok(true)) => continue,
                Ok(Ok(false)) => break,
                Ok(Err(e)) => {
                    tracing::error!(error = %format!("{e:#}"), "compaction failed");
                    break;
                }
                Err(e) => {
                    tracing::error!(error = %e, "compaction panicked");
                    break;
                }
            }
        }
    }
}

fn now_secs() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}
