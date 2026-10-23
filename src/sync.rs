//! Minimal client for faster-sync: JSON-RPC-shaped query results and the
//! current heights, plus a background watcher of the archive height.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};
use tokio::sync::watch;

#[derive(Debug, thiserror::Error)]
pub enum SyncError {
    #[error("faster-sync unreachable: {0}")]
    Http(#[from] reqwest::Error),
    #[error("faster-sync {0}: {1}")]
    Status(u16, String),
    #[error("reorg at block {0}")]
    Reorg(u64),
}

#[derive(Clone, Copy, Debug, Default)]
pub struct Heights {
    pub height: Option<u64>,
    pub finalized: u64,
    pub chain_head: u64,
}

pub struct Sync {
    url: String,
    key: Option<String>,
    http: reqwest::Client,
    pub chain_id: AtomicU64,
    heights: watch::Sender<Heights>,
}

impl Sync {
    pub fn new(url: &str, key: Option<String>) -> Arc<Sync> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(60))
            .pool_max_idle_per_host(64)
            .tcp_nodelay(true)
            .build()
            .expect("http client");
        Arc::new(Sync {
            url: url.trim_end_matches('/').to_string(),
            key,
            http,
            chain_id: AtomicU64::new(0),
            heights: watch::channel(Heights::default()).0,
        })
    }

    fn req(&self, r: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.key {
            Some(k) => r.bearer_auth(k),
            None => r,
        }
    }

    /// Runs one query with `?shape=rpc` and returns the parsed response.
    pub async fn query(&self, q: Value) -> Result<Value, SyncError> {
        let r = self.req(self.http.post(format!("{}/query?shape=rpc", self.url)).json(&q)).send().await?;
        let status = r.status().as_u16();
        let v: Value = r.json().await?;
        match status {
            200 => Ok(v),
            409 if v["reorg"] == true => Err(SyncError::Reorg(v["block"].as_u64().unwrap_or(0))),
            s => Err(SyncError::Status(s, v["error"].as_str().unwrap_or("").to_string())),
        }
    }

    /// Runs `q` page by page until `to_block` (exclusive), concatenating
    /// rows; stops early once more than `max_rows` rows were collected.
    pub async fn query_all(&self, mut q: Value, max_rows: usize) -> Result<Value, SyncError> {
        let to = q["to_block"].as_u64();
        let mut out = json!({"blocks": [], "transactions": [], "logs": [], "traces": []});
        let mut rows = 0usize;
        loop {
            let v = self.query(q.clone()).await?;
            for t in ["blocks", "transactions", "logs", "traces"] {
                if let (Some(dst), Some(src)) = (out[t].as_array_mut(), v[t].as_array()) {
                    rows += src.len();
                    dst.extend(src.iter().cloned());
                }
            }
            let next = v["next_block"].as_u64().unwrap_or(0);
            let from = q["from_block"].as_u64().unwrap_or(0);
            out["next_block"] = json!(next);
            if rows > max_rows || next <= from || to.is_some_and(|t| next >= t) {
                break;
            }
            let height = v["archive_height"].as_u64().unwrap_or(0);
            if next > height {
                break;
            }
            q["from_block"] = json!(next);
        }
        Ok(out)
    }

    pub async fn fetch_heights(&self) -> Result<Heights, SyncError> {
        let r = self.req(self.http.get(format!("{}/height", self.url))).send().await?;
        let v: Value = r.json().await?;
        if let Some(c) = v["chain_id"].as_u64() {
            self.chain_id.store(c, Ordering::Relaxed);
        }
        let h = Heights {
            height: v["height"].as_u64(),
            finalized: v["finalized_block"].as_u64().unwrap_or(0),
            chain_head: v["chain_head"].as_u64().unwrap_or(0),
        };
        self.heights.send_replace(h);
        Ok(h)
    }

    pub fn heights(&self) -> Heights {
        *self.heights.borrow()
    }

    pub fn subscribe(&self) -> watch::Receiver<Heights> {
        self.heights.subscribe()
    }

    /// Keeps `heights()` current with faster-sync's long-poll.
    pub async fn watch(self: Arc<Self>) {
        let mut backoff = Duration::from_millis(200);
        loop {
            let above = self.heights().height.unwrap_or(0);
            let r = self
                .req(self.http.get(format!("{}/height?above={above}&timeout_ms=10000", self.url)))
                .timeout(Duration::from_secs(15))
                .send()
                .await;
            match r {
                Ok(r) => match r.json::<Value>().await {
                    Ok(v) => {
                        backoff = Duration::from_millis(200);
                        if let Some(c) = v["chain_id"].as_u64() {
                            self.chain_id.store(c, Ordering::Relaxed);
                        }
                        self.heights.send_replace(Heights {
                            height: v["height"].as_u64(),
                            finalized: v["finalized_block"].as_u64().unwrap_or(0),
                            chain_head: v["chain_head"].as_u64().unwrap_or(0),
                        });
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "faster-sync /height");
                        tokio::time::sleep(backoff).await;
                    }
                },
                Err(e) => {
                    tracing::warn!(error = %e, "faster-sync unreachable");
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(5));
                }
            }
        }
    }
}
