//! CLOB history endpoints (/api/clob/*), served from the ClickHouse archive
//! loaded by services/clobarchiver (schema: database/clickhouse/01-clob.sql).
//! Port of api-go's clob.go / clob_store.go with the same routes and JSON.
//!
//! JSON conventions:
//!   - heights, pair_id, idx, seq: JSON numbers (bounded well below 2^53)
//!   - every other chain integer (prices, amounts, ids, ns timestamps, row
//!     counts): decimal strings, since they can exceed 2^53
//!   - hashes and addresses: lowercase 0x hex
//!   - block_time: RFC 3339 UTC with nanoseconds, plus block_time_ns (string)
//!
//! Lists are newest first: pass next_before as ?before= for the next page.
//! A cursor is "<height>-<pos>" (pos = idx, seq, or seq*2+role for trader
//! fills); a bare "<height>" means "strictly below that height".
//!
//! Queries go over ClickHouse's HTTP interface. User input only reaches the
//! server as typed query parameters ({name:Type}); only the configured,
//! validated database name is spliced into SQL.

use std::time::Duration;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::{json, Map, Value};

use crate::api::AppState;

const DEFAULT_LIMIT: u64 = 50;
const MAX_LIMIT: u64 = 500;
const QUERY_TIMEOUT: Duration = Duration::from_secs(15);

pub struct Clob {
    http: reqwest::Client,
    url: String,
    db: String,
    user: String,
    password: String,
}

#[derive(Debug, thiserror::Error)]
pub enum ChError {
    #[error("unavailable: {0}")]
    Unavailable(String),
    #[error("query: {0}")]
    Query(String),
}

fn valid_ident(s: &str) -> bool {
    let mut c = s.chars();
    matches!(c.next(), Some(ch) if ch.is_ascii_alphabetic() || ch == '_') && c.all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
}

impl Clob {
    pub fn new(url: &str, db: &str, user: &str, password: &str) -> anyhow::Result<Clob> {
        if !valid_ident(db) {
            anyhow::bail!("bad ClickHouse database name {db:?}");
        }
        Ok(Clob {
            http: reqwest::Client::builder().timeout(QUERY_TIMEOUT).pool_max_idle_per_host(16).build()?,
            url: url.trim_end_matches('/').to_string(),
            db: db.to_string(),
            user: user.to_string(),
            password: password.to_string(),
        })
    }

    fn t(&self, name: &str) -> String {
        format!("{}.{}", self.db, name)
    }

    /// Runs a query and returns its rows as JSON objects.
    async fn query(&self, sql: &str, params: &[(&str, String)]) -> Result<Vec<Map<String, Value>>, ChError> {
        let mut q: Vec<(String, String)> = vec![
            ("database".into(), self.db.clone()),
            ("default_format".into(), "JSONEachRow".into()),
            ("output_format_json_quote_64bit_integers".into(), "0".into()),
            // Output aliases (hash -> 0x hex, ...) must not shadow the real
            // columns in WHERE / ORDER BY.
            ("prefer_column_name_to_alias".into(), "1".into()),
        ];
        for (k, v) in params {
            q.push((format!("param_{k}"), v.clone()));
        }
        let resp = self
            .http
            .post(&self.url)
            .query(&q)
            .header("X-ClickHouse-User", &self.user)
            .header("X-ClickHouse-Key", &self.password)
            .body(sql.to_string())
            .send()
            .await
            .map_err(|e| ChError::Unavailable(e.to_string()))?;
        let status = resp.status();
        let body = resp.text().await.map_err(|e| ChError::Unavailable(e.to_string()))?;
        if !status.is_success() {
            return Err(ChError::Query(body.chars().take(300).collect()));
        }
        let mut rows = Vec::new();
        for line in body.lines().filter(|l| !l.is_empty()) {
            match serde_json::from_str::<Value>(line) {
                Ok(Value::Object(m)) => rows.push(m),
                _ => return Err(ChError::Query(format!("bad row: {}", &line[..line.len().min(200)]))),
            }
        }
        Ok(rows)
    }
}

/// Go's time.RFC3339Nano in UTC: fractional seconds with trailing zeros
/// trimmed, no dot when they are all zero.
pub fn rfc3339_nano(ns: i64) -> String {
    let secs = ns.div_euclid(1_000_000_000);
    let frac = ns.rem_euclid(1_000_000_000);
    let days = secs.div_euclid(86400);
    let sod = secs.rem_euclid(86400);
    let z = days + 719468;
    let era = z.div_euclid(146097);
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + if m <= 2 { 1 } else { 0 };
    let mut s = format!("{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}", sod / 3600, sod / 60 % 60, sod % 60);
    if frac != 0 {
        let f = format!("{frac:09}");
        s.push('.');
        s.push_str(f.trim_end_matches('0'));
    }
    s.push('Z');
    s
}

/// Adds block_time (from block_time_ns) and `extra` fields to each row.
fn finish(mut rows: Vec<Map<String, Value>>, extra: &[(&str, &str)]) -> Vec<Value> {
    for r in rows.iter_mut() {
        if let Some(ns) = r.get("block_time_ns").and_then(|v| v.as_str()).and_then(|s| s.parse::<i64>().ok()) {
            r.insert("block_time".into(), Value::String(rfc3339_nano(ns)));
        }
        for (k, v) in extra {
            r.insert((*k).into(), Value::String((*v).into()));
        }
    }
    rows.into_iter().map(Value::Object).collect()
}

const ORDER_COLS: &str = "height, idx, toString(toUnixTimestamp64Nano(block_time)) AS block_time_ns, \
    concat('0x', lower(hex(hash))) AS hash, concat('0x', lower(hex(trader))) AS trader, pair_id, \
    toString(client_order_id) AS client_order_id, toString(order_id) AS order_id, is_buy, is_market, \
    toString(price) AS price, toString(amount) AS amount, toString(timestamp_ns) AS timestamp_ns, \
    toString(status) AS status, toString(filled) AS filled, reason";

const CANCEL_COLS: &str = "height, idx, toString(toUnixTimestamp64Nano(block_time)) AS block_time_ns, \
    toString(kind) AS kind, concat('0x', lower(hex(hash))) AS hash, concat('0x', lower(hex(trader))) AS trader, \
    pair_id, toString(client_id) AS client_id, toString(target_order_id) AS target_order_id, \
    toString(target_client) AS target_client, toString(price) AS price, toString(amount) AS amount, \
    toString(status) AS status, reason";

const FILL_COLS: &str = "height, seq, toString(toUnixTimestamp64Nano(block_time)) AS block_time_ns, pair_id, \
    concat('0x', lower(hex(maker))) AS maker, concat('0x', lower(hex(taker))) AS taker, taker_is_buy, \
    toString(maker_order_id) AS maker_order_id, toString(taker_order_id) AS taker_order_id, \
    toString(price) AS price, toString(qty) AS qty";

const TRADER_FILL_COLS: &str = "height, seq, toString(role) AS role, \
    toString(toUnixTimestamp64Nano(block_time)) AS block_time_ns, concat('0x', lower(hex(trader))) AS trader, \
    pair_id, toString(order_id) AS order_id, concat('0x', lower(hex(counterparty))) AS counterparty, \
    toString(counter_order) AS counter_order_id, is_buy, toString(price) AS price, toString(qty) AS qty";

/// WHERE prefix for one block's rows: the block time prunes to one
/// partition, the height to its granules.
const PINNED: &str = "block_time = fromUnixTimestamp64Nano({ts:Int64}, 'UTC') AND height = {h:UInt64}";

// ---- HTTP layer ----

#[derive(serde::Deserialize, Default)]
pub struct ClobQ {
    limit: Option<String>,
    before: Option<String>,
    orders_after: Option<String>,
    cancels_after: Option<String>,
    fills_after: Option<String>,
}

fn err(code: StatusCode, msg: &str) -> Response {
    (code, Json(json!({"error": msg}))).into_response()
}

fn fail(path: &str, e: ChError) -> Response {
    tracing::error!(path, err = %e, "clob query");
    match e {
        ChError::Unavailable(_) => err(StatusCode::SERVICE_UNAVAILABLE, "CLOB archive unavailable"),
        ChError::Query(_) => err(StatusCode::INTERNAL_SERVER_ERROR, "CLOB archive query failed"),
    }
}

fn store(s: &AppState) -> Result<&Clob, Response> {
    s.clob.as_deref().ok_or_else(|| {
        err(StatusCode::SERVICE_UNAVAILABLE, "CLOB archive not configured (set CLICKHOUSE_URL)")
    })
}

fn parse_limit(q: &Option<String>, def: u64) -> Result<u64, Response> {
    match q.as_deref() {
        None | Some("") => Ok(def),
        Some(v) => match v.parse::<u64>() {
            Ok(n) if n >= 1 => Ok(n.min(MAX_LIMIT)),
            _ => Err(err(StatusCode::BAD_REQUEST, "limit must be a positive integer")),
        },
    }
}

/// (height, pos); no cursor = (MAX, MAX).
fn parse_cursor(q: &Option<String>) -> Result<(u64, u64), Response> {
    let bad = || err(StatusCode::BAD_REQUEST, "before must be <height> or <height>-<pos>");
    match q.as_deref() {
        None | Some("") => Ok((u64::MAX, u64::MAX)),
        Some(v) => {
            let (h, p) = match v.split_once('-') {
                Some((h, p)) => (h, Some(p)),
                None => (v, None),
            };
            let digits = |s: &str| !s.is_empty() && s.len() <= 20 && s.bytes().all(|b| b.is_ascii_digit());
            if !digits(h) || p.is_some_and(|p| !digits(p)) {
                return Err(bad());
            }
            let h: u64 = h.parse().map_err(|_| bad())?;
            let p: u64 = match p {
                Some(p) => p.parse().map_err(|_| bad())?,
                None => 0,
            };
            Ok((h, p))
        }
    }
}

fn hex_param(s: &str, len: usize) -> Option<String> {
    let t = s.strip_prefix("0x")?;
    (t.len() == len * 2 && t.bytes().all(|b| b.is_ascii_hexdigit())).then(|| t.to_ascii_lowercase())
}

fn num(v: &Value, k: &str) -> u64 {
    v.get(k).and_then(|x| x.as_u64()).unwrap_or(0)
}

fn list(items: Vec<Value>, limit: u64, pos: impl Fn(&Value) -> (u64, u64)) -> Json<Value> {
    let next = if (items.len() as u64) < limit || items.is_empty() {
        Value::Null
    } else {
        let (h, p) = pos(items.last().unwrap());
        Value::String(format!("{h}-{p}"))
    };
    Json(json!({"items": items, "next_before": next}))
}

pub async fn stats(State(s): State<AppState>) -> Response {
    let c = match store(&s) {
        Ok(c) => c,
        Err(r) => return r,
    };
    let sql = format!(
        "SELECT
           (SELECT argMax(height, updated_at) FROM {ap}) AS latest_loaded_height,
           (SELECT count() FROM {ap}) AS cp_rows,
           (SELECT toString(toUnixTimestamp64Nano(toDateTime64(max(updated_at), 9, 'UTC'))) FROM {ap}) AS cp_at_ns,
           toString((SELECT count() FROM {b})) AS blocks,
           toString((SELECT count() FROM {o})) AS orders,
           toString((SELECT count() FROM {c})) AS cancels,
           toString((SELECT count() FROM {f})) AS fills,
           toString((SELECT count() FROM {g} FINAL)) AS gap_ranges,
           toString((SELECT toUInt64(sum(to_height - from_height + 1)) FROM {g} FINAL)) AS gap_heights,
           toString(ifNull((SELECT toUnixTimestamp64Nano(block_time) FROM {b} ORDER BY height DESC LIMIT 1), 0)) AS latest_ns,
           (SELECT count() > 0 FROM {b}) AS have_blocks",
        ap = c.t("archive_progress"),
        b = c.t("blocks"),
        o = c.t("orders"),
        c = c.t("cancels"),
        f = c.t("fills"),
        g = c.t("gaps"),
    );
    let rows = match c.query(&sql, &[]).await {
        Ok(r) => r,
        Err(e) => return fail("/api/clob/stats", e),
    };
    let r = rows.into_iter().next().unwrap_or_default();
    let get = |k: &str| r.get(k).cloned().unwrap_or(Value::Null);
    let ns = |k: &str| r.get(k).and_then(|v| v.as_str()).and_then(|s| s.parse::<i64>().ok());
    let truthy = |k: &str| r.get(k).is_some_and(|v| v.as_bool() == Some(true) || v.as_u64() == Some(1));
    let cp_rows = r.get("cp_rows").and_then(|v| v.as_u64().or_else(|| v.as_str()?.parse().ok())).unwrap_or(0);
    Json(json!({
        "latest_loaded_height": get("latest_loaded_height"),
        "latest_block_time": if truthy("have_blocks") { ns("latest_ns").map(rfc3339_nano) } else { None },
        "checkpoint_updated_at": if cp_rows > 0 { ns("cp_at_ns").map(rfc3339_nano) } else { None },
        "blocks": get("blocks"),
        "orders": get("orders"),
        "cancels": get("cancels"),
        "fills": get("fills"),
        "gap_ranges": get("gap_ranges"),
        "gap_heights": get("gap_heights"),
    }))
    .into_response()
}

pub async fn trader_list(
    State(s): State<AppState>,
    Path((addr, kind)): Path<(String, String)>,
    Query(q): Query<ClobQ>,
) -> Response {
    let c = match store(&s) {
        Ok(c) => c,
        Err(r) => return r,
    };
    if !matches!(kind.as_str(), "orders" | "cancels" | "fills") {
        return err(StatusCode::NOT_FOUND, "unknown CLOB endpoint");
    }
    let Some(trader) = hex_param(&addr, 20) else {
        return err(StatusCode::BAD_REQUEST, "address must be 0x + 40 hex characters");
    };
    let limit = match parse_limit(&q.limit, DEFAULT_LIMIT) {
        Ok(l) => l,
        Err(r) => return r,
    };
    let (bh, bp) = match parse_cursor(&q.before) {
        Ok(c) => c,
        Err(r) => return r,
    };
    let params = [("trader", trader), ("bh", bh.to_string()), ("bp", bp.to_string()), ("lim", limit.to_string())];
    let path = format!("/api/clob/traders/{kind}");
    match kind.as_str() {
        "orders" => {
            let sql = format!(
                "SELECT {ORDER_COLS} FROM {} WHERE trader = unhex({{trader:String}})
                   AND height <= {{bh:UInt64}} AND (height < {{bh:UInt64}} OR idx < {{bp:UInt64}})
                 ORDER BY height DESC, idx DESC LIMIT 1 BY height, idx LIMIT {{lim:UInt32}}",
                c.t("orders_by_trader")
            );
            match c.query(&sql, &params).await {
                Ok(rows) => list(finish(rows, &[("kind", "order")]), limit, |v| (num(v, "height"), num(v, "idx")))
                    .into_response(),
                Err(e) => fail(&path, e),
            }
        }
        "cancels" => {
            let sql = format!(
                "SELECT {CANCEL_COLS} FROM {} WHERE trader = unhex({{trader:String}})
                   AND height <= {{bh:UInt64}} AND (height < {{bh:UInt64}} OR idx < {{bp:UInt64}})
                 ORDER BY height DESC, idx DESC LIMIT 1 BY height, idx LIMIT {{lim:UInt32}}",
                c.t("cancels_by_trader")
            );
            match c.query(&sql, &params).await {
                Ok(rows) => list(finish(rows, &[]), limit, |v| (num(v, "height"), num(v, "idx"))).into_response(),
                Err(e) => fail(&path, e),
            }
        }
        _ => {
            let sql = format!(
                "SELECT {TRADER_FILL_COLS} FROM {} WHERE trader = unhex({{trader:String}})
                   AND height <= {{bh:UInt64}} AND (height < {{bh:UInt64}} OR seq * 2 + CAST(role, 'Int8') < {{bp:UInt64}})
                 ORDER BY height DESC, seq DESC, role DESC LIMIT 1 BY height, seq, role LIMIT {{lim:UInt32}}",
                c.t("fills_by_trader")
            );
            match c.query(&sql, &params).await {
                Ok(rows) => list(finish(rows, &[]), limit, |v| {
                    let taker = v.get("role").and_then(|r| r.as_str()) == Some("taker");
                    (num(v, "height"), num(v, "seq") * 2 + taker as u64)
                })
                .into_response(),
                Err(e) => fail(&path, e),
            }
        }
    }
}

pub async fn pair_trades(State(s): State<AppState>, Path(pair): Path<String>, Query(q): Query<ClobQ>) -> Response {
    let c = match store(&s) {
        Ok(c) => c,
        Err(r) => return r,
    };
    let Ok(pair) = pair.parse::<u32>() else {
        return err(StatusCode::BAD_REQUEST, "pair id must be a uint32");
    };
    let limit = match parse_limit(&q.limit, DEFAULT_LIMIT) {
        Ok(l) => l,
        Err(r) => return r,
    };
    let (bh, bp) = match parse_cursor(&q.before) {
        Ok(c) => c,
        Err(r) => return r,
    };
    let sql = format!(
        "SELECT {FILL_COLS} FROM {} WHERE pair_id = {{pair:UInt32}}
           AND height <= {{bh:UInt64}} AND (height < {{bh:UInt64}} OR seq < {{bp:UInt64}})
         ORDER BY height DESC, seq DESC LIMIT 1 BY height, seq LIMIT {{lim:UInt32}}",
        c.t("fills")
    );
    let params = [("pair", pair.to_string()), ("bh", bh.to_string()), ("bp", bp.to_string()), ("lim", limit.to_string())];
    match c.query(&sql, &params).await {
        Ok(rows) => list(finish(rows, &[]), limit, |v| (num(v, "height"), num(v, "seq"))).into_response(),
        Err(e) => fail("/api/clob/pairs/trades", e),
    }
}

pub async fn item_by_hash(State(s): State<AppState>, Path(hash): Path<String>) -> Response {
    let c = match store(&s) {
        Ok(c) => c,
        Err(r) => return r,
    };
    let Some(h) = hex_param(&hash, 32) else {
        return err(StatusCode::BAD_REQUEST, "hash must be 0x + 64 hex characters");
    };
    match item(c, &h).await {
        Ok(Some(v)) => Json(v).into_response(),
        Ok(None) => err(StatusCode::NOT_FOUND, "order not found"),
        Err(e) => fail("/api/clob/orders", e),
    }
}

/// The item (order, or cancel/amend) a hash refers to, the fills of that
/// order, and every occurrence of the hash (a duplicate submission of a
/// signed order is executed, and rejected, as its own item).
async fn item(c: &Clob, h: &str) -> Result<Option<Value>, ChError> {
    let refs = c
        .query(
            &format!(
                "SELECT height, idx, toString(kind) AS kind, toString(toUnixTimestamp64Nano(block_time)) AS ts
                 FROM {} WHERE hash = unhex({{hash:String}})
                 ORDER BY height, kind, idx LIMIT 1 BY height, kind, idx LIMIT 100",
                c.t("item_hash")
            ),
            &[("hash", h.to_string())],
        )
        .await?;
    if refs.is_empty() {
        return Ok(None);
    }
    let mut orders: Vec<Value> = Vec::new();
    let mut cancels: Vec<Value> = Vec::new();
    let mut occurrences: Vec<Value> = Vec::new();
    for r in &refs {
        let p = [
            ("ts", r.get("ts").and_then(|v| v.as_str()).unwrap_or("0").to_string()),
            ("h", r.get("height").map(|v| v.to_string()).unwrap_or_default()),
            ("i", r.get("idx").map(|v| v.to_string()).unwrap_or_default()),
        ];
        let is_order = r.get("kind").and_then(|v| v.as_str()) == Some("order");
        let sql = if is_order {
            format!("SELECT {ORDER_COLS} FROM {} WHERE {PINNED} AND idx = {{i:UInt32}} LIMIT 1", c.t("orders"))
        } else {
            format!("SELECT {CANCEL_COLS} FROM {} WHERE {PINNED} AND idx = {{i:UInt32}} LIMIT 1", c.t("cancels"))
        };
        let extra: &[(&str, &str)] = if is_order { &[("kind", "order")] } else { &[] };
        for v in finish(c.query(&sql, &p).await?, extra) {
            occurrences.push(json!({"kind": v["kind"], "height": v["height"], "idx": v["idx"], "status": v["status"]}));
            if is_order {
                orders.push(v);
            } else {
                cancels.push(v);
            }
        }
    }
    // The primary item: the first executed (non-rejected) occurrence, else the first.
    let pick = |items: &[Value]| -> Value {
        items.iter().find(|v| v["status"] != "rejected").unwrap_or(&items[0]).clone()
    };
    let mut res = json!({"hash": format!("0x{h}"), "fills": [], "occurrences": occurrences});
    if !orders.is_empty() {
        let o = pick(&orders);
        if o["order_id"] != "0" {
            let trader = o["trader"].as_str().unwrap_or("0x").trim_start_matches("0x").to_string();
            let p = [
                ("pair", o["pair_id"].to_string()),
                ("oid", o["order_id"].as_str().unwrap_or("0").to_string()),
                ("trader", trader),
                ("h", o["height"].to_string()),
            ];
            let sql = format!(
                "SELECT {TRADER_FILL_COLS} FROM {} WHERE pair_id = {{pair:UInt32}} AND order_id = {{oid:UInt64}}
                   AND trader = unhex({{trader:String}}) AND height >= {{h:UInt64}}
                 ORDER BY height, seq, role LIMIT 1 BY height, seq, role LIMIT 1000",
                c.t("fills_by_order")
            );
            res["fills"] = Value::Array(finish(c.query(&sql, &p).await?, &[]));
        }
        res["kind"] = json!("order");
        res["order"] = o;
    } else if !cancels.is_empty() {
        let k = pick(&cancels);
        res["kind"] = k["kind"].clone();
        res["cancel"] = k;
    } else {
        // hash index ahead of a repaired table; treat as absent
        return Ok(None);
    }
    Ok(Some(res))
}

pub async fn block(State(s): State<AppState>, Path(height): Path<String>, Query(q): Query<ClobQ>) -> Response {
    let c = match store(&s) {
        Ok(c) => c,
        Err(r) => return r,
    };
    let Ok(height) = height.parse::<u64>() else {
        return err(StatusCode::BAD_REQUEST, "height must be a non-negative integer");
    };
    let limit = match parse_limit(&q.limit, 100) {
        Ok(l) => l,
        Err(r) => return r,
    };
    let mut after = [-1i64; 3];
    for (i, (name, v)) in
        [("orders_after", &q.orders_after), ("cancels_after", &q.cancels_after), ("fills_after", &q.fills_after)]
            .iter()
            .enumerate()
    {
        if let Some(v) = v.as_deref().filter(|v| !v.is_empty()) {
            match v.parse::<u32>() {
                Ok(n) => after[i] = n as i64,
                Err(_) => return err(StatusCode::BAD_REQUEST, &format!("{name} must be a non-negative integer")),
            }
        }
    }
    match block_query(c, height, limit, after).await {
        Ok(r) => r,
        Err(e) => fail("/api/clob/blocks", e),
    }
}

async fn block_query(c: &Clob, height: u64, limit: u64, after: [i64; 3]) -> Result<Response, ChError> {
    let hp = [("h", height.to_string())];
    let head = c
        .query(
            &format!(
                "SELECT toString(toUnixTimestamp64Nano(block_time)) AS ts, orders, cancels, fills, rejected
                 FROM {} WHERE height = {{h:UInt64}} LIMIT 1",
                c.t("blocks")
            ),
            &hp,
        )
        .await?;
    let cp = c
        .query(&format!("SELECT argMax(height, updated_at) AS h FROM {}", c.t("archive_progress")), &[])
        .await?
        .first()
        .and_then(|r| r.get("h").and_then(|v| v.as_u64()))
        .unwrap_or(0);
    let Some(head) = head.into_iter().next().filter(|_| height <= cp) else {
        let gap = c
            .query(
                &format!(
                    "SELECT toString(reason) AS reason FROM {} WHERE from_height <= {{h:UInt64}} AND to_height >= {{h:UInt64}} LIMIT 1",
                    c.t("gaps")
                ),
                &hp,
            )
            .await?;
        let gap = gap.first().and_then(|r| r.get("reason").and_then(|v| v.as_str())).unwrap_or("");
        let mut body = json!({"error": "block not in CLOB archive", "latest_loaded_height": cp, "not_loaded_yet": height > cp});
        // Go marshals an empty Gap as "" under this key too.
        body["gap"] = json!(gap);
        return Ok((StatusCode::NOT_FOUND, Json(body)).into_response());
    };
    let ts = head.get("ts").and_then(|v| v.as_str()).unwrap_or("0").to_string();
    let count = |k: &str| head.get(k).and_then(|v| v.as_u64()).unwrap_or(0);
    let (n_orders, n_cancels, n_fills) = (count("orders"), count("cancels"), count("fills"));
    let p = [
        ("ts", ts.clone()),
        ("h", height.to_string()),
        ("lim", limit.to_string()),
        ("oa", after[0].to_string()),
        ("ca", after[1].to_string()),
        ("fa", after[2].to_string()),
    ];
    let orders = if n_orders > 0 {
        let sql = format!(
            "SELECT {ORDER_COLS} FROM {} WHERE {PINNED} AND idx > {{oa:Int64}} ORDER BY idx LIMIT 1 BY idx LIMIT {{lim:UInt32}}",
            c.t("orders")
        );
        finish(c.query(&sql, &p).await?, &[("kind", "order")])
    } else {
        Vec::new()
    };
    let cancels = if n_cancels > 0 {
        let sql = format!(
            "SELECT {CANCEL_COLS} FROM {} WHERE {PINNED} AND idx > {{ca:Int64}} ORDER BY idx LIMIT 1 BY idx LIMIT {{lim:UInt32}}",
            c.t("cancels")
        );
        finish(c.query(&sql, &p).await?, &[])
    } else {
        Vec::new()
    };
    let fills = if n_fills > 0 {
        let sql = format!(
            "SELECT {FILL_COLS} FROM {} WHERE {PINNED} AND seq > {{fa:Int64}} ORDER BY seq LIMIT 1 BY seq LIMIT {{lim:UInt32}}",
            c.t("fills")
        );
        finish(c.query(&sql, &p).await?, &[])
    } else {
        Vec::new()
    };
    let truncated =
        (orders.len() as u64) < n_orders || (cancels.len() as u64) < n_cancels || (fills.len() as u64) < n_fills;
    // idx and seq are dense (0..count-1): more remain past a full page whose
    // last position is below count-1.
    let next = |items: &[Value], key: &str, total: u64| -> Option<u64> {
        let last = num(items.last()?, key);
        (items.len() as u64 == limit && last + 1 < total).then_some(last)
    };
    let mut b = json!({
        "height": height,
        "block_time": rfc3339_nano(ts.parse().unwrap_or(0)),
        "block_time_ns": ts,
        "counts": {"orders": n_orders, "cancels": n_cancels, "fills": n_fills, "rejected": count("rejected")},
        "truncated": truncated,
    });
    if let Some(n) = next(&orders, "idx", n_orders) {
        b["next_orders_after"] = json!(n);
    }
    if let Some(n) = next(&cancels, "idx", n_cancels) {
        b["next_cancels_after"] = json!(n);
    }
    if let Some(n) = next(&fills, "seq", n_fills) {
        b["next_fills_after"] = json!(n);
    }
    b["orders"] = Value::Array(orders);
    b["cancels"] = Value::Array(cancels);
    b["fills"] = Value::Array(fills);
    Ok(Json(b).into_response())
}

pub async fn unknown() -> Response {
    err(StatusCode::NOT_FOUND, "unknown CLOB endpoint")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn go_rfc3339_nano() {
        assert_eq!(rfc3339_nano(0), "1970-01-01T00:00:00Z");
        assert_eq!(rfc3339_nano(1_700_000_000_123_400_000), "2023-11-14T22:13:20.1234Z");
        assert_eq!(rfc3339_nano(1_700_000_000_000_000_001), "2023-11-14T22:13:20.000000001Z");
    }

    #[test]
    fn cursors() {
        assert_eq!(parse_cursor(&None).ok(), Some((u64::MAX, u64::MAX)));
        assert_eq!(parse_cursor(&Some("12-3".into())).ok(), Some((12, 3)));
        assert_eq!(parse_cursor(&Some("12".into())).ok(), Some((12, 0)));
        assert!(parse_cursor(&Some("x".into())).is_err());
        assert!(parse_cursor(&Some("1-".into())).is_err());
        assert!(parse_cursor(&Some("-1".into())).is_err());
        assert_eq!(hex_param("0xAB", 1).as_deref(), Some("ab"));
        assert!(hex_param("AB", 1).is_none());
        assert!(valid_ident("clob_1") && !valid_ident("a;drop") && !valid_ident("1a"));
    }
}
