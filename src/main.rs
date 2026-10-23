use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use faster_sync::api::{self, AppState, Guard, GuardConfig};
use faster_sync::chunk::WriteOpts;
use faster_sync::ingest::{Ingest, IngestConfig};
use faster_sync::metrics::Metrics;
use faster_sync::query::Limits;
use faster_sync::rpc::{Rpc, RpcConfig};
use faster_sync::store::Store;

fn env<T: std::str::FromStr>(k: &str, d: T) -> T {
    std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d)
}

fn env_list(k: &str) -> Vec<String> {
    std::env::var(k)
        .unwrap_or_default()
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

#[tokio::main]
async fn main() -> Result<()> {
    let json_logs = std::env::var("LOG_FORMAT").is_ok_and(|v| v == "json");
    let filter = tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into());
    if json_logs {
        tracing_subscriber::fmt().json().with_env_filter(filter).init();
    } else {
        tracing_subscriber::fmt().with_env_filter(filter).init();
    }

    let data = PathBuf::from(std::env::var("DATA_DIR").unwrap_or_else(|_| "./data".into()));
    let Ok(rpc_url) = std::env::var("RPC_URL") else {
        // Serve-only: answer queries from an existing data dir (replicas,
        // benchmarks); nothing is ingested.
        let store = Arc::new(Store::open_read_only(&data).context("RPC_URL unset: serve-only needs an existing DATA_DIR")?);
        tracing::info!(chain_id = store.meta.chain_id, "serve-only (no RPC_URL)");
        return serve(store, Arc::new(Metrics::default()), None).await;
    };
    let rpc = Arc::new(Rpc::new(RpcConfig {
        url: rpc_url,
        timeout: Duration::from_millis(env("RPC_TIMEOUT_MS", 30_000)),
        batch: env("RPC_BATCH", 100),
        rps: env("RPC_RPS", 0),
        traces: env("TRACES", false),
    })?);
    let chain_id = loop {
        match rpc.chain_id().await {
            Ok(c) => break c,
            Err(e) => {
                tracing::warn!(error = %e, "waiting for the node");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    };
    let store = Arc::new(Store::open(&data, chain_id, env("START_BLOCK", 0))?);
    tracing::info!(chain_id, data = %data.display(), next = store.next_block(), chunks = store.chunk_count(), "store open");

    let metrics = Arc::new(Metrics::default());
    let d = IngestConfig::default();
    let cfg = IngestConfig {
        window: env("FETCH_WINDOW", d.window).max(1),
        concurrency: env("FETCH_CONCURRENCY", d.concurrency).max(1),
        finality_depth: env("FINALITY_DEPTH", d.finality_depth),
        use_finalized_tag: env("USE_FINALIZED_TAG", d.use_finalized_tag),
        chunk_rows: env("CHUNK_ROWS", d.chunk_rows),
        chunk_max_blocks: env("CHUNK_MAX_BLOCKS", d.chunk_max_blocks).max(1),
        hot_max_rows: env("HOT_MAX_ROWS", d.hot_max_rows),
        hot_max_age: Duration::from_secs(env("HOT_MAX_AGE_SECS", d.hot_max_age.as_secs())),
        poll: Duration::from_millis(env("POLL_MS", d.poll.as_millis() as u64)),
        end_block: std::env::var("END_BLOCK").ok().and_then(|v| v.parse().ok()),
        write: WriteOpts {
            row_group_rows: env("ROW_GROUP_ROWS", d.write.row_group_rows).max(1024),
            codec: env("COMPRESSION", d.write.codec),
        },
    };
    let ingest = Ingest { store: store.clone(), rpc: rpc.clone(), cfg, stats: metrics.clone() };
    let ingest_task = tokio::spawn(async move {
        let r = ingest.run().await;
        if let Err(e) = &r {
            tracing::error!(error = %format!("{e:#}"), "ingest stopped");
        }
        r
    });

    serve(store, metrics, Some(ingest_task)).await
}

async fn serve(
    store: Arc<Store>,
    metrics: Arc<Metrics>,
    ingest: Option<tokio::task::JoinHandle<Result<()>>>,
) -> Result<()> {
    let state = Arc::new(AppState {
        store: store.clone(),
        metrics: metrics.clone(),
        limits: Limits {
            max_rows: env("QUERY_MAX_ROWS", 200_000),
            max_time: Duration::from_millis(env("QUERY_MAX_MS", 5_000)),
            parallelism: env("QUERY_PARALLELISM", std::thread::available_parallelism().map(|n| n.get()).unwrap_or(8)),
            max_selection_values: env("QUERY_MAX_VALUES", 10_000),
        },
        ready_max_lag: env("READY_MAX_LAG", 10),
        query_slots: tokio::sync::Semaphore::new(env("QUERY_CONCURRENCY", 32)),
    });
    let guard = Guard::new(
        GuardConfig {
            rate: env("RATE_LIMIT_RPS", 0.0),
            burst: env("RATE_LIMIT_BURST", 50.0),
            trust_proxy: env("TRUST_PROXY", false),
            cors_origins: env_list("CORS_ORIGINS"),
            api_keys: env_list("API_KEYS"),
        },
        metrics.clone(),
    );
    tokio::spawn(guard.clone().sweep());
    let app = api::router(state.clone(), guard);
    let addr: SocketAddr = std::env::var("LISTEN").unwrap_or_else(|_| "0.0.0.0:3100".into()).parse()?;
    let admin: SocketAddr = std::env::var("ADMIN_LISTEN").unwrap_or_else(|_| "127.0.0.1:9101".into()).parse()?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let admin_l = tokio::net::TcpListener::bind(admin).await?;
    tracing::info!(%addr, %admin, "listening");
    tokio::spawn(async move { axum::serve(admin_l, api::admin_router(state)).await });
    let server = axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>())
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        });
    match ingest {
        Some(ingest) => tokio::select! {
            r = ingest => { r??; }
            r = server => { r?; }
        },
        None => server.await?,
    }
    Ok(())
}
