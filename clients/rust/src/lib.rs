//! Client for the faster-sync HTTP API.
//!
//! [`Client::query`] returns one page as JSON rows, [`Client::query_arrow`]
//! as Arrow record batches. [`Client::stream`] follows `next_block` page by
//! page; with `follow` it waits for new blocks at the tip and sends the
//! hash of the last block it returned as `parent_hash`, so a reorg ends the
//! stream with [`Error::Reorg`].

use std::io::Cursor;
use std::time::Duration;

use arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("http: {0}")]
    Http(#[from] reqwest::Error),
    #[error("server {status}: {message}")]
    Server { status: u16, message: String },
    #[error("reorg at block {block}")]
    Reorg { block: u64 },
    #[error("decode: {0}")]
    Decode(String),
}

#[derive(Serialize, Default, Clone, Debug)]
pub struct LogSelection {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub address: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub topics: Vec<Vec<String>>,
}

#[derive(Serialize, Default, Clone, Debug)]
pub struct TransactionSelection {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub from: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub to: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub sighash: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub contract_address: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub hash: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<u64>,
}

#[derive(Serialize, Default, Clone, Debug)]
pub struct TraceSelection {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub from: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub to: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub sighash: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub call_type: Vec<String>,
}

#[derive(Serialize, Default, Clone, Debug)]
pub struct BlockSelection {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub hash: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub miner: Vec<String>,
}

#[derive(Serialize, Default, Clone, Debug)]
pub struct Fields {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transaction: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub log: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trace: Option<Vec<String>>,
}

#[derive(Serialize, Clone, Copy, Debug, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Join {
    Nothing,
    #[default]
    Default,
    All,
}

#[derive(Serialize, Default, Clone, Debug)]
pub struct Query {
    pub from_block: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub to_block: Option<u64>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub logs: Vec<LogSelection>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub transactions: Vec<TransactionSelection>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub traces: Vec<TraceSelection>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub blocks: Vec<BlockSelection>,
    pub include_all_blocks: bool,
    pub join: Join,
    pub fields: Fields,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_rows: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_hash: Option<String>,
}

#[derive(Deserialize, Clone, Debug)]
pub struct Guard {
    pub block_number: u64,
    pub hash: String,
}

#[derive(Deserialize, Clone, Debug)]
pub struct Meta {
    pub next_block: u64,
    pub archive_height: Option<u64>,
    pub chain_head: u64,
    pub finalized_block: u64,
    pub last_block: Option<Guard>,
    pub rows: usize,
    pub elapsed_ms: f64,
}

/// A page of JSON rows.
#[derive(Deserialize, Clone, Debug)]
pub struct Page {
    pub blocks: Vec<Value>,
    pub transactions: Vec<Value>,
    pub logs: Vec<Value>,
    pub traces: Vec<Value>,
    #[serde(flatten)]
    pub meta: Meta,
}

/// A page of Arrow record batches (per table, in block order).
#[derive(Clone, Debug)]
pub struct ArrowPage {
    pub blocks: Vec<RecordBatch>,
    pub transactions: Vec<RecordBatch>,
    pub logs: Vec<RecordBatch>,
    pub traces: Vec<RecordBatch>,
    pub meta: Meta,
}

#[derive(Deserialize, Clone, Debug)]
pub struct Height {
    pub height: Option<u64>,
    pub chain_head: u64,
    pub finalized_block: u64,
    pub first_block: Option<u64>,
    pub chunked_to: Option<u64>,
    pub chain_id: u64,
}

#[derive(Clone)]
pub struct Client {
    url: String,
    http: reqwest::Client,
    api_key: Option<String>,
    retries: u32,
}

impl Client {
    pub fn new(url: &str) -> Client {
        let http = reqwest::Client::builder().timeout(Duration::from_secs(120)).build().expect("http client");
        Client { url: url.trim_end_matches('/').to_string(), http, api_key: None, retries: 5 }
    }

    pub fn with_api_key(mut self, key: impl Into<String>) -> Client {
        self.api_key = Some(key.into());
        self
    }

    async fn send(&self, path: &str, body: Option<&Query>) -> Result<bytes::Bytes, Error> {
        let mut delay = Duration::from_millis(200);
        let mut attempt = 0;
        loop {
            let mut req = match body {
                Some(q) => self.http.post(format!("{}{}", self.url, path)).json(q),
                None => self.http.get(format!("{}{}", self.url, path)),
            };
            if let Some(k) = &self.api_key {
                req = req.bearer_auth(k);
            }
            let r = match req.send().await {
                Ok(r) => r,
                Err(e) if attempt < self.retries => {
                    let _ = e;
                    attempt += 1;
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(Duration::from_secs(10));
                    continue;
                }
                Err(e) => return Err(e.into()),
            };
            let status = r.status().as_u16();
            let b = r.bytes().await?;
            if (200..300).contains(&status) {
                return Ok(b);
            }
            let v: Value = serde_json::from_slice(&b).unwrap_or(Value::Null);
            if status == 409 && v["reorg"] == true {
                return Err(Error::Reorg { block: v["block"].as_u64().unwrap_or(0) });
            }
            if (status == 429 || status >= 500) && attempt < self.retries {
                attempt += 1;
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(Duration::from_secs(10));
                continue;
            }
            let message = v["error"].as_str().map(String::from).unwrap_or_else(|| String::from_utf8_lossy(&b).into());
            return Err(Error::Server { status, message });
        }
    }

    pub async fn query(&self, q: &Query) -> Result<Page, Error> {
        let b = self.send("/query", Some(q)).await?;
        serde_json::from_slice(&b).map_err(|e| Error::Decode(e.to_string()))
    }

    pub async fn query_arrow(&self, q: &Query) -> Result<ArrowPage, Error> {
        let b = self.send("/query?format=arrow", Some(q)).await?;
        decode_frames(&b)
    }

    /// Heights; with `above`, waits until the height passes it (or timeout).
    pub async fn height(&self, above: Option<u64>, timeout: Duration) -> Result<Height, Error> {
        let path = match above {
            Some(a) => format!("/height?above={a}&timeout_ms={}", timeout.as_millis()),
            None => "/height".into(),
        };
        let b = self.send(&path, None).await?;
        serde_json::from_slice(&b).map_err(|e| Error::Decode(e.to_string()))
    }

    /// Pages until `q.to_block` (or forever with `follow`), in order.
    pub fn stream(&self, q: Query, follow: bool) -> impl futures::Stream<Item = Result<Page, Error>> + '_ {
        struct S {
            q: Query,
            done: bool,
        }
        futures::stream::unfold(S { q, done: false }, move |mut s| async move {
            loop {
                if s.done || s.q.to_block.is_some_and(|e| s.q.from_block >= e) {
                    return None;
                }
                let page = match self.query(&s.q).await {
                    Ok(p) => p,
                    Err(e) => {
                        s.done = true;
                        return Some((Err(e), s));
                    }
                };
                let from = s.q.from_block;
                if page.meta.next_block > from {
                    s.q.parent_hash = page
                        .meta
                        .last_block
                        .as_ref()
                        .filter(|g| g.block_number + 1 == page.meta.next_block)
                        .map(|g| g.hash.clone());
                    s.q.from_block = page.meta.next_block;
                    return Some((Ok(page), s));
                }
                if !follow {
                    return None;
                }
                if let Err(e) = self.height(Some(from.saturating_sub(1)), Duration::from_secs(10)).await {
                    s.done = true;
                    return Some((Err(e), s));
                }
            }
        })
    }
}

/// Parses the Arrow response frames (`[tag][len u32 LE][bytes]`).
pub fn decode_frames(b: &[u8]) -> Result<ArrowPage, Error> {
    let mut tables: [Vec<RecordBatch>; 4] = Default::default();
    let mut meta = None;
    let mut off = 0;
    while off + 5 <= b.len() {
        let tag = b[off];
        let len = u32::from_le_bytes(b[off + 1..off + 5].try_into().unwrap()) as usize;
        let body = b.get(off + 5..off + 5 + len).ok_or_else(|| Error::Decode("truncated frame".into()))?;
        match tag {
            0..=3 => {
                let r = arrow::ipc::reader::StreamReader::try_new(Cursor::new(body), None)
                    .map_err(|e| Error::Decode(e.to_string()))?;
                for batch in r {
                    tables[tag as usize].push(batch.map_err(|e| Error::Decode(e.to_string()))?);
                }
            }
            254 => {
                let v: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
                return Err(Error::Server { status: 500, message: v["error"].as_str().unwrap_or("").to_string() });
            }
            255 => meta = Some(serde_json::from_slice::<Meta>(body).map_err(|e| Error::Decode(e.to_string()))?),
            _ => {}
        }
        off += 5 + len;
    }
    let meta = meta.ok_or_else(|| Error::Decode("no metadata frame (response cut short)".into()))?;
    let [blocks, transactions, logs, traces] = tables;
    Ok(ArrowPage { blocks, transactions, logs, traces, meta })
}
