use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context, Result};
use faster_rpc::methods::{Config, Ctx};
use faster_rpc::server::{self, App, Guard, GuardConfig, Metrics};
use faster_rpc::sync::Sync;
use faster_rpc::upstream::Upstream;

fn env<T: std::str::FromStr>(k: &str, d: T) -> T {
    std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d)
}

fn env_list(k: &str) -> Vec<String> {
    std::env::var(k).unwrap_or_default().split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect()
}

#[tokio::main]
async fn main() -> Result<()> {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into());
    if std::env::var("LOG_FORMAT").is_ok_and(|v| v == "json") {
        tracing_subscriber::fmt().json().with_env_filter(filter).init();
    } else {
        tracing_subscriber::fmt().with_env_filter(filter).init();
    }
    let sync_url = std::env::var("SYNC_URL").context("SYNC_URL (faster-sync) is required")?;
    let sync = Sync::new(&sync_url, std::env::var("SYNC_API_KEY").ok());
    match sync.fetch_heights().await {
        Ok(h) => tracing::info!(height = ?h.height, "faster-sync reachable"),
        Err(e) => tracing::warn!(error = %e, "faster-sync not reachable yet"),
    }
    tokio::spawn(sync.clone().watch());
    let upstream = std::env::var("UPSTREAM_URL").ok().filter(|s| !s.is_empty()).map(|u| Upstream::new(&u));
    if upstream.is_none() {
        tracing::warn!("UPSTREAM_URL unset: state methods (eth_call, eth_getBalance, …) answer 'not supported'");
    }
    let d = Config::default();
    let cfg = Config {
        max_logs: env("MAX_LOGS", d.max_logs),
        max_block_range: env("MAX_BLOCK_RANGE", d.max_block_range),
        block_timestamp: env("BLOCK_TIMESTAMP", d.block_timestamp),
        cache_blocks: env("CACHE_BLOCKS", d.cache_blocks),
        cache_txs: env("CACHE_TXS", d.cache_txs),
    };
    let ctx = Arc::new(Ctx::new(sync, upstream, cfg));
    let metrics = Arc::new(Metrics::default());
    let app = Arc::new(App {
        ctx,
        metrics: metrics.clone(),
        max_batch: env("MAX_BATCH", 100),
        ready_max_lag: env("READY_MAX_LAG", 10),
    });
    let guard = Guard::new(
        GuardConfig {
            rate: env("RATE_LIMIT_RPS", 0.0),
            burst: env("RATE_LIMIT_BURST", 100.0),
            trust_proxy: env("TRUST_PROXY", false),
            cors_origins: env_list("CORS_ORIGINS"),
            api_keys: env_list("API_KEYS"),
        },
        metrics,
    );
    tokio::spawn(guard.clone().sweep());
    let addr: SocketAddr = std::env::var("LISTEN").unwrap_or_else(|_| "0.0.0.0:8545".into()).parse()?;
    let admin: SocketAddr = std::env::var("ADMIN_LISTEN").unwrap_or_else(|_| "127.0.0.1:9102".into()).parse()?;
    let l = tokio::net::TcpListener::bind(addr).await?;
    let al = tokio::net::TcpListener::bind(admin).await?;
    tracing::info!(%addr, %admin, "listening");
    let admin_app = server::admin_router(app.clone());
    tokio::spawn(async move { axum::serve(al, admin_app).await });
    axum::serve(l, server::router(app, guard).into_make_service_with_connect_info::<SocketAddr>())
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}
