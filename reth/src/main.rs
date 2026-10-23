//! A reth node with faster-sync built in.
//!
//! An execution extension receives every committed, reorged and reverted
//! chain from reth and applies it to a faster-sync store; the faster-sync
//! HTTP API is served from the same process. Blocks and receipts are read
//! through reth's own `eth` API handlers in-process (straight from its
//! database, the same objects its JSON-RPC returns), so there is no HTTP,
//! no rate limit and no polling.
//!
//! Usage: like `reth`, e.g. `faster-sync-reth node --dev` or
//! `faster-sync-reth node --chain mainnet --datadir …`. faster-sync settings
//! come from the environment (`FSYNC_DATA_DIR`, `LISTEN`, `CHUNK_ROWS`, …).

use std::sync::Arc;
use std::time::{Duration, Instant};

use alloy_eips::{BlockId, BlockNumHash, BlockNumberOrTag};
use faster_sync::feed::Feed;
use faster_sync::ingest::IngestConfig;
use faster_sync::metrics::Metrics;
use faster_sync::rpc::RawBlock;
use faster_sync::store::Store;
use futures::future::BoxFuture;
use futures::{StreamExt, TryStreamExt};
use reth_ethereum::chainspec::EthChainSpec;
use reth_ethereum::exex::{ExExContext, ExExEvent, ExExHead, ExExNotification, ExExNotificationsStream};
use reth_ethereum::node::api::FullNodeComponents;
use reth_ethereum::node::builder::NodeHandleFor;
use reth_ethereum::node::EthereumNode;
use reth_ethereum::primitives::AlloyBlockHeader;
use reth_ethereum::provider::{BlockIdReader, BlockNumReader};
use reth_ethereum::rpc::api::EthApiServer;
use serde_json::Value;
use tokio::sync::oneshot;

/// A block by number (canonical at read time) or by hash (exact).
#[derive(Clone, Copy, Debug)]
enum At {
    Number(u64),
    Hash(alloy_primitives::B256),
}

/// Reads a block (full transactions) and its receipts as JSON-RPC objects.
type Fetch = Arc<dyn Fn(At) -> BoxFuture<'static, anyhow::Result<RawBlock>> + Send + Sync>;

fn env<T: std::str::FromStr>(k: &str, d: T) -> T {
    std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d)
}

fn main() -> eyre::Result<()> {
    reth_ethereum::cli::Cli::parse_args().run(async move |builder, _| {
        let (fetch_tx, fetch_rx) = oneshot::channel::<Fetch>();
        let mut fetch_tx = Some(fetch_tx);
        let handle: NodeHandleFor<EthereumNode> = builder
            .node(EthereumNode::default())
            .extend_rpc_modules(move |ctx| {
                let api = ctx.registry.eth_api().clone();
                let fetch: Fetch = Arc::new(move |at| {
                    let api = api.clone();
                    Box::pin(async move {
                        let (b, id) = match at {
                            At::Number(n) => (
                                EthApiServer::block_by_number(&api, BlockNumberOrTag::Number(n), true).await,
                                BlockId::Number(BlockNumberOrTag::Number(n)),
                            ),
                            At::Hash(h) => (EthApiServer::block_by_hash(&api, h, true).await, BlockId::Hash(h.into())),
                        };
                        let b = b.map_err(|e| anyhow::anyhow!("block {at:?}: {e}"))?;
                        let r = EthApiServer::block_receipts(&api, id)
                            .await
                            .map_err(|e| anyhow::anyhow!("receipts {at:?}: {e}"))?;
                        let (Some(b), Some(r)) = (b, r) else { anyhow::bail!("block {at:?} not available") };
                        let block = match serde_json::to_value(&b)? {
                            Value::Object(m) => m,
                            _ => anyhow::bail!("block {at:?}: not an object"),
                        };
                        let receipts = match serde_json::to_value(&r)? {
                            Value::Array(a) => a,
                            _ => anyhow::bail!("receipts {at:?}: not a list"),
                        };
                        Ok(RawBlock { block, receipts, traces: None })
                    })
                });
                if let Some(tx) = fetch_tx.take() {
                    let _ = tx.send(fetch);
                }
                Ok(())
            })
            .install_exex("faster-sync", async move |ctx| Ok(run(ctx, fetch_rx)))
            // The debug launcher also runs the dev-mode miner (`--dev`).
            .launch_with_debug_capabilities()
            .await?;
        handle.wait_for_node_exit().await
    })
}

async fn run<Node: FullNodeComponents>(
    mut ctx: ExExContext<Node>,
    fetch_rx: oneshot::Receiver<Fetch>,
) -> eyre::Result<()> {
    let fetch = fetch_rx.await.map_err(|_| eyre::eyre!("eth API was not installed"))?;
    let chain_id = ctx.config.chain.chain().id();
    let dir = std::path::PathBuf::from(std::env::var("FSYNC_DATA_DIR").unwrap_or_else(|_| "./fsync-data".into()));
    let store = Arc::new(Store::open(&dir, chain_id, env("START_BLOCK", 0)).map_err(|e| eyre::eyre!("{e:#}"))?);
    let metrics = Arc::new(Metrics::default());
    faster_sync::api::spawn_from_env(store.clone(), metrics.clone()).await.map_err(|e| eyre::eyre!("{e:#}"))?;
    let d = IngestConfig::default();
    let cfg = IngestConfig {
        chunk_rows: env("CHUNK_ROWS", d.chunk_rows),
        chunk_max_blocks: env("CHUNK_MAX_BLOCKS", d.chunk_max_blocks),
        hot_max_rows: env("HOT_MAX_ROWS", d.hot_max_rows),
        hot_max_age: Duration::from_secs(env("HOT_MAX_AGE_SECS", d.hot_max_age.as_secs())),
        ..d
    };
    let depth: u64 = env("FINALITY_DEPTH", 64);
    let concurrency: usize = env("FETCH_CONCURRENCY", 16);
    tokio::spawn(faster_sync::ingest::compactor(
        store.clone(),
        cfg.clone(),
        Duration::from_secs(env("COMPACT_EVERY_SECS", 300)),
    ));
    let mut feed = Feed::new(store.clone(), cfg, metrics);
    let finalized = |ctx: &ExExContext<Node>, tip: u64| -> u64 {
        ctx.provider().finalized_block_number().ok().flatten().unwrap_or(tip.saturating_sub(depth)).min(tip)
    };
    let err = |e: anyhow::Error| eyre::eyre!("{e:#}");

    // Catch up from reth's database, then follow reth's chain notifications
    // from our head (reth replays what we missed while stopped).
    let t0 = Instant::now();
    let start = feed.next_block();
    catch_up(&ctx, &mut feed, &fetch, concurrency, &finalized).await.map_err(err)?;
    tracing::info!(
        from = start,
        to = feed.next_block(),
        secs = t0.elapsed().as_secs_f64(),
        "faster-sync caught up from reth's database"
    );
    if let Some(h) = store.last() {
        ctx.notifications.set_with_head(ExExHead::new(BlockNumHash::new(h.number, h.hash.into())));
    } else {
        ctx.notifications.set_without_head();
    }
    while let Some(n) = ctx.notifications.try_next().await? {
        let r: anyhow::Result<()> = async {
            match &n {
                ExExNotification::ChainCommitted { new } => {
                    let blocks: Vec<(u64, _)> = new.blocks_iter().map(|b| (b.number(), b.hash())).collect();
                    apply(&mut feed, &fetch, &blocks, &ctx, &finalized).await
                }
                ExExNotification::ChainReorged { old, new } => {
                    tracing::warn!(old = ?old.range(), new = ?new.range(), "reorg");
                    feed.revert(new.first().number() - 1).await?;
                    let blocks: Vec<(u64, _)> = new.blocks_iter().map(|b| (b.number(), b.hash())).collect();
                    apply(&mut feed, &fetch, &blocks, &ctx, &finalized).await
                }
                ExExNotification::ChainReverted { old } => {
                    tracing::warn!(reverted = ?old.range(), "revert");
                    feed.revert(old.first().number() - 1).await
                }
            }
        }
        .await;
        if let Err(e) = r {
            // Never take the node down: rewind the unfinalized blocks and
            // catch up from reth's canonical chain again.
            tracing::error!(error = %format!("{e:#}"), "faster-sync: notification failed; resyncing from reth's database");
            if let Err(e) = resync(&ctx, &mut feed, &fetch, concurrency, &finalized).await {
                tracing::error!(error = %format!("{e:#}"), "faster-sync: resync failed; will retry on the next notification");
            }
        }
        if let Some(c) = n.committed_chain() {
            ctx.events.send(ExExEvent::FinishedHeight(c.tip().num_hash()))?;
        }
    }
    Ok(())
}

/// Appends canonical blocks from reth's database until our head is reth's.
async fn catch_up<Node: FullNodeComponents>(
    ctx: &ExExContext<Node>,
    feed: &mut Feed,
    fetch: &Fetch,
    concurrency: usize,
    finalized: &impl Fn(&ExExContext<Node>, u64) -> u64,
) -> anyhow::Result<()> {
    loop {
        let tip = ctx.provider().best_block_number()?;
        let next = feed.next_block();
        if next > tip {
            break;
        }
        let fin = finalized(ctx, tip);
        let end = tip.min(next + 999);
        let raws: Vec<RawBlock> = futures::stream::iter(next..=end)
            .map(|n| (fetch)(At::Number(n)))
            .buffered(concurrency)
            .try_collect()
            .await?;
        feed.push(raws, fin).await?;
        feed.finalize(fin, tip).await?;
    }
    feed.flush().await
}

/// Drops every unfinalized block and catches up again.
async fn resync<Node: FullNodeComponents>(
    ctx: &ExExContext<Node>,
    feed: &mut Feed,
    fetch: &Fetch,
    concurrency: usize,
    finalized: &impl Fn(&ExExContext<Node>, u64) -> u64,
) -> anyhow::Result<()> {
    let keep = feed.store.chunk_end().map(|e| e.saturating_sub(1)).unwrap_or(0);
    if feed.store.hot.first().is_some() {
        feed.revert(keep).await?;
    }
    catch_up(ctx, feed, fetch, concurrency, finalized).await
}

/// Appends the notified blocks, read by hash (exact even if the canonical
/// chain moves on meanwhile). Blocks we already have are skipped.
async fn apply<Node: FullNodeComponents>(
    feed: &mut Feed,
    fetch: &Fetch,
    blocks: &[(u64, alloy_primitives::B256)],
    ctx: &ExExContext<Node>,
    finalized: &impl Fn(&ExExContext<Node>, u64) -> u64,
) -> anyhow::Result<()> {
    let next = feed.next_block();
    let todo: Vec<_> = blocks.iter().filter(|(n, _)| *n >= next).copied().collect();
    let Some(&(to, _)) = todo.last() else { return Ok(()) };
    let raws: Vec<RawBlock> =
        futures::stream::iter(todo).map(|(_, h)| (fetch)(At::Hash(h))).buffered(16).try_collect().await?;
    let fin = finalized(ctx, to);
    feed.push(raws, fin).await?;
    feed.finalize(fin, to).await
}
