//! `faster`: one process that indexes an EVM chain into an embedded RocksDB
//! store and serves the explorer API and live WebSocket feed from it.

mod api;
#[cfg(test)]
mod e2e_test;
mod indexer;
mod rpc;
mod store;
mod types;

use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use tracing_subscriber::EnvFilter;

/// Live numbers shared by the indexer and the API.
#[derive(Default)]
pub struct Status {
    pub tip: AtomicU64,
    pub head: AtomicU64,
    pub clients: AtomicUsize,
}

fn env<T: FromStr>(key: &str, def: T) -> Result<T>
where
    T::Err: std::fmt::Display,
{
    match std::env::var(key) {
        Ok(v) if !v.trim().is_empty() => v.trim().parse().map_err(|e| anyhow::anyhow!("{key}={v}: {e}")),
        _ => Ok(def),
    }
}

fn init_logging() {
    let filter = EnvFilter::try_from_env("RUST_LOG").unwrap_or_else(|_| {
        let lvl = std::env::var("LOG_LEVEL").unwrap_or_else(|_| "info".into());
        EnvFilter::new(format!("faster={lvl},warn"))
    });
    let b = tracing_subscriber::fmt().with_env_filter(filter);
    if std::env::var("LOG_FORMAT").as_deref() == Ok("text") {
        b.init();
    } else {
        b.json().flatten_event(true).init();
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    init_logging();
    let rpc_url = env("RPC_URL", "http://localhost:8545".to_string())?;
    let data_dir: PathBuf = env("DATA_DIR", PathBuf::from("./data"))?;
    let port: u16 = env("PORT", 3000)?;
    let icfg = indexer::IndexerConfig {
        start_block: env("START_BLOCK", 0)?,
        confirmations: env("CONFIRMATIONS", 0)?,
        fetch_batch: env("FETCH_BATCH", 10)?,
        concurrency: env("FETCH_CONCURRENCY", 16)?,
        window: env("FETCH_WINDOW", 2000)?,
        commit_blocks: env("COMMIT_BLOCKS", 500)?,
        poll: Duration::from_millis(env("POLL_MS", 250)?),
    };
    indexer::validate(&icfg)?;
    let reorg_depth: u64 = env("REORG_DEPTH", 256)?;
    let cache_mb: usize = env("CACHE_MB", 512)?;
    let rpc_timeout = Duration::from_millis(env("RPC_TIMEOUT_MS", 15_000)?);
    let ready_max_lag: u64 = env("READY_MAX_LAG", 10)?;
    let clob_url = env("CLOB_API_URL", String::new())?;

    std::fs::create_dir_all(&data_dir).with_context(|| format!("create {}", data_dir.display()))?;
    let store = store::Store::open(
        &data_dir.join("hot"),
        store::StoreConfig { cache_bytes: cache_mb << 20, reorg_depth },
    )?;
    let rpc = Arc::new(rpc::Rpc::new(&rpc_url, rpc_timeout)?);
    let status = Arc::new(Status::default());
    let hub = api::Hub::new(store.clone());

    let state = api::AppState {
        store: store.clone(),
        rpc: rpc.clone(),
        hub: hub.clone(),
        status: status.clone(),
        ready_max_lag,
        final_depth: reorg_depth,
        node_cache: Default::default(),
        clob: if clob_url.is_empty() {
            None
        } else {
            Some(api::ClobProxy {
                base: clob_url,
                http: reqwest::Client::builder().timeout(Duration::from_secs(30)).build()?,
            })
        },
    };
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port)).await?;
    tracing::info!(port, rpc = %rpc_url, data = %data_dir.display(), ?icfg, "faster starting");

    let idx = indexer::Indexer { store, rpc, cfg: icfg, sink: hub.clone(), status };
    let indexer = tokio::spawn(idx.run());
    tokio::spawn(hub.clone().heartbeat());
    let server = axum::serve(listener, api::router(state)).with_graceful_shutdown(shutdown_signal());

    tokio::select! {
        r = server => r.context("http server")?,
        r = indexer => {
            // The indexer only returns on a fatal error (e.g. a reorg deeper
            // than REORG_DEPTH); exit non-zero so the supervisor notices.
            r.context("indexer task")?.context("indexer")?;
        }
    }
    tracing::info!("shut down");
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    let term = async {
        let mut s = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("SIGTERM handler");
        s.recv().await;
    };
    #[cfg(not(unix))]
    let term = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {},
        _ = term => {},
    }
    tracing::info!("shutdown signal");
}
