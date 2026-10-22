//! Chain follower: parallel batched fetch, in-order atomic commits, reorg
//! detection by parent hash and rollback through the store's undo journal.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Result};
use futures::stream::{self, StreamExt};

use crate::rpc::{Rpc, RpcError};
use crate::store::Store;
use crate::types::{FullBlock, Head};
use crate::Status;

#[derive(Clone, Debug)]
pub struct IndexerConfig {
    pub start_block: u64,
    /// Blocks behind the node's head to stay; 0 follows the tip (reorgs are
    /// then handled by rollback).
    pub confirmations: u64,
    /// Blocks per JSON-RPC batch request.
    pub fetch_batch: usize,
    /// Batch requests in flight at once.
    pub concurrency: usize,
    /// Blocks planned per round before re-reading the node's head.
    pub window: u64,
    /// Blocks per atomic store write while catching up.
    pub commit_blocks: usize,
    pub poll: Duration,
}

/// Receives each committed run of blocks, and a signal after a reorg.
pub trait Sink: Send + Sync {
    fn committed(&self, blocks: &[FullBlock]);
    fn reorged(&self, new_head: Option<Head>);
}

pub struct Indexer {
    pub store: Arc<Store>,
    pub rpc: Arc<Rpc>,
    pub cfg: IndexerConfig,
    pub sink: Arc<dyn Sink>,
    pub status: Arc<Status>,
}

const MAX_BACKOFF: Duration = Duration::from_secs(5);

enum Round {
    Done,
    Reorg,
}

impl Indexer {
    pub async fn run(self) -> Result<()> {
        if let Some(h) = self.store.head()? {
            self.status.head.store(h.number, Ordering::Relaxed);
            tracing::info!(head = h.number, "resuming");
        } else {
            tracing::info!(start = self.cfg.start_block, "empty store, starting");
        }
        let mut last_log = Instant::now();
        let mut last_logged_head = self.store.head()?.map(|h| h.number).unwrap_or(0);
        loop {
            let tip = self.retry("eth_blockNumber", || self.rpc.block_number()).await;
            self.status.tip.store(tip, Ordering::Relaxed);
            let target = tip.saturating_sub(self.cfg.confirmations);
            let head = self.store.head()?;
            let next = head.map(|h| h.number + 1).unwrap_or(self.cfg.start_block);
            if next > target {
                // Caught up. A reorg that replaces our head without adding a
                // block on top is invisible to the parent-hash check, so
                // compare the head hash directly while idle.
                if let Some(h) = head.filter(|h| h.number <= tip) {
                    if let Ok(Some(canon)) = self.rpc.block_hash(h.number).await {
                        if canon != h.hash {
                            tracing::warn!(block = h.number, "head replaced: chain reorganised");
                            self.resolve_reorg().await?;
                            continue;
                        }
                    }
                }
                tokio::time::sleep(self.cfg.poll).await;
                continue;
            }
            let end = target.min(next + self.cfg.window - 1);
            if let Round::Reorg = self.round(head, next, end).await? {
                self.resolve_reorg().await?;
            }
            if last_log.elapsed() >= Duration::from_secs(10) {
                let h = self.store.head()?.map(|h| h.number).unwrap_or(0);
                let rate = (h.saturating_sub(last_logged_head)) as f64 / last_log.elapsed().as_secs_f64();
                tracing::info!(head = h, tip, lag = tip.saturating_sub(h), blocks_per_s = format!("{rate:.1}"), "progress");
                last_log = Instant::now();
                last_logged_head = h;
            }
        }
    }

    /// Fetches [next, end] with up to `concurrency` batch requests in flight
    /// and commits in order. Stops early if a block does not chain on.
    async fn round(&self, head: Option<Head>, next: u64, end: u64) -> Result<Round> {
        let batch = self.cfg.fetch_batch as u64;
        let chunks: Vec<Vec<u64>> = (next..=end)
            .step_by(batch as usize)
            .map(|s| (s..=end.min(s + batch - 1)).collect())
            .collect();
        let mut fetched = stream::iter(chunks)
            .map(|nums| async move { self.retry("blocks", || self.rpc.blocks(&nums)).await })
            .buffered(self.cfg.concurrency);
        let mut prev = head;
        let mut pending: Vec<FullBlock> = Vec::new();
        while let Some(blocks) = fetched.next().await {
            for fb in blocks {
                if let Some(p) = prev {
                    if fb.block.parent_hash != p.hash {
                        tracing::warn!(
                            block = fb.block.number,
                            "parent hash mismatch: chain reorganised"
                        );
                        self.commit(std::mem::take(&mut pending)).await?;
                        return Ok(Round::Reorg);
                    }
                }
                prev = Some(Head { number: fb.block.number, hash: fb.block.hash });
                pending.push(fb);
                if pending.len() >= self.cfg.commit_blocks {
                    self.commit(std::mem::take(&mut pending)).await?;
                }
            }
        }
        self.commit(pending).await?;
        Ok(Round::Done)
    }

    async fn commit(&self, blocks: Vec<FullBlock>) -> Result<()> {
        if blocks.is_empty() {
            return Ok(());
        }
        let store = self.store.clone();
        let blocks = tokio::task::spawn_blocking(move || store.apply(&blocks).map(|_| blocks)).await??;
        let last = blocks.last().unwrap().block.number;
        self.status.head.store(last, Ordering::Relaxed);
        self.sink.committed(&blocks);
        Ok(())
    }

    /// Rolls the store back until its head is on the node's canonical chain.
    async fn resolve_reorg(&self) -> Result<()> {
        let mut depth = 0u64;
        loop {
            let Some(head) = self.store.head()? else { break };
            let nums = [head.number];
            let canon = self.retry("canonical block", || self.rpc.blocks(&nums)).await;
            if canon[0].block.hash == head.hash {
                break;
            }
            let store = self.store.clone();
            let new_head = tokio::task::spawn_blocking(move || store.rollback_head()).await??;
            depth += 1;
            tracing::warn!(rolled_back = head.number, depth, "reorg rollback");
            self.status.head.store(new_head.map(|h| h.number).unwrap_or(0), Ordering::Relaxed);
            if new_head.is_none() {
                break;
            }
        }
        if depth == 0 {
            // The mismatch came from the node switching forks mid-fetch; our
            // head was canonical. Nothing to undo.
            return Ok(());
        }
        let head = self.store.head()?;
        tracing::warn!(depth, new_head = head.map(|h| h.number), "reorg resolved");
        self.sink.reorged(head);
        Ok(())
    }

    /// Retries transient failures (transport, not-ready blocks) forever with
    /// capped exponential backoff. Bad data is retried too but logged as an
    /// error, since it usually needs operator attention.
    async fn retry<T, F, Fut>(&self, what: &str, f: F) -> T
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<T, RpcError>>,
    {
        let mut backoff = Duration::from_millis(100);
        loop {
            match f().await {
                Ok(v) => return v,
                Err(e @ RpcError::NotReady(_)) => tracing::debug!(what, err = %e, "not ready, retrying"),
                Err(e @ (RpcError::Decode(_) | RpcError::Rpc { .. })) => {
                    tracing::error!(what, err = %e, "rpc error, retrying")
                }
                Err(e) => tracing::warn!(what, err = %e, "rpc failed, retrying"),
            }
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(MAX_BACKOFF);
        }
    }
}

/// Fails fast on settings that would make the indexer misbehave.
pub fn validate(cfg: &IndexerConfig) -> Result<()> {
    if cfg.fetch_batch == 0 || cfg.concurrency == 0 || cfg.window == 0 || cfg.commit_blocks == 0 {
        bail!("FETCH_BATCH, FETCH_CONCURRENCY, FETCH_WINDOW and COMMIT_BLOCKS must be > 0");
    }
    Ok(())
}
