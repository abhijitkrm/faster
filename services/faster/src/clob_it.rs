//! /api/clob/* against a real ClickHouse (port of api-go's
//! TestClobClickHouseIntegration). Gated: CLOB_CH_TEST=1, CLICKHOUSE_URL
//! (default http://127.0.0.1:8123), CLICKHOUSE_USER / CLICKHOUSE_PASSWORD.

use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};

use crate::api::{self, AppState};
use crate::clob::Clob;
use crate::rpc::Rpc;
use crate::store::{Store, StoreConfig};
use crate::Status;

fn env(k: &str, d: &str) -> String {
    std::env::var(k).ok().filter(|v| !v.is_empty()).unwrap_or_else(|| d.to_string())
}

#[tokio::test]
async fn clob_clickhouse_integration() {
    if std::env::var("CLOB_CH_TEST").as_deref() != Ok("1") {
        eprintln!("skipped: set CLOB_CH_TEST=1 to run against ClickHouse");
        return;
    }
    let url = env("CLICKHOUSE_URL", "http://127.0.0.1:8123");
    let (user, pass) = (env("CLICKHOUSE_USER", "default"), env("CLICKHOUSE_PASSWORD", ""));
    let db = format!("clob_rs_it_{}", std::process::id());
    let http = reqwest::Client::new();
    let ex = |q: String| {
        let (http, url, user, pass) = (http.clone(), url.clone(), user.clone(), pass.clone());
        async move {
            let r = http
                .post(&url)
                .header("X-ClickHouse-User", user)
                .header("X-ClickHouse-Key", pass)
                .body(q.clone())
                .send()
                .await
                .unwrap();
            assert!(r.status().is_success(), "{q}: {}", r.text().await.unwrap());
        }
    };
    let schema = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/../../database/clickhouse/01-clob.sql"))
        .unwrap()
        .replace("clob.", &format!("{db}."))
        .replace("DATABASE IF NOT EXISTS clob", &format!("DATABASE IF NOT EXISTS {db}"));
    let schema: String = schema.lines().filter(|l| !l.trim_start().starts_with("--")).collect::<Vec<_>>().join("\n");
    for st in schema.split(";\n").filter(|s| s.contains("CREATE")) {
        ex(st.to_string()).await;
    }
    let (a, b, c) = ("aa".repeat(20), "bb".repeat(20), "cc".repeat(20));
    let h = |x: &str| x.repeat(32);
    let d1 = "toDateTime64('2026-10-20 10:00:00.5', 9, 'UTC')";
    let d1b = "toDateTime64('2026-10-20 10:00:01', 9, 'UTC')";
    let d1c = "toDateTime64('2026-10-20 10:00:01.5', 9, 'UTC')";
    let d2 = "toDateTime64('2026-10-21 00:00:02', 9, 'UTC')";
    let big = "18446744073709551615";
    let cols = "(height, idx, block_time, hash, trader, pair_id, client_order_id, order_id, is_buy, is_market, price, amount, timestamp_ns, status, filled, reason)";
    let orders = format!(
        "INSERT INTO {db}.orders {cols} VALUES
         (10, 0, {d1}, unhex('{}'), unhex('{a}'), 1, 7, 101, true, false, {big}, 5, 1792600000000000000, 'open', 0, ''),
         (10, 1, {d1}, unhex('{}'), unhex('{b}'), 1, 8, 102, false, false, 1000, 4, 1792600000000000001, 'partial', 2, '')",
        h("11"),
        h("22")
    );
    ex(orders.clone()).await;
    ex(orders).await; // an unmerged duplicate load: reads must still be exact
    ex(format!(
        "INSERT INTO {db}.orders {cols} VALUES
         (11, 0, {d1b}, unhex('{}'), unhex('{a}'), 1, 7, 0, true, false, 1000, 5, 0, 'rejected', 0, 'duplicate order'),
         (12, 0, {d1c}, unhex('{}'), unhex('{c}'), 1, 9, 103, true, true, 1000, 1, 0, 'filled', 1, '')",
        h("11"),
        h("44")
    ))
    .await;
    ex(format!(
        "INSERT INTO {db}.cancels (height, idx, block_time, kind, hash, trader, pair_id, client_id, target_order_id, target_client, price, amount, status, reason) VALUES
         (11, 0, {d1b}, 'amend', unhex('{}'), unhex('{a}'), 1, 70, 101, 0, 999, 3, 'amended', ''),
         (14, 0, {d2}, 'cancel', unhex('{}'), unhex('{a}'), 1, 71, 101, 0, 0, 0, 'cancelled', '')",
        h("33"),
        h("55")
    ))
    .await;
    ex(format!(
        "INSERT INTO {db}.fills (height, seq, block_time, pair_id, maker, taker, taker_is_buy, maker_order_id, taker_order_id, price, qty) VALUES
         (10, 0, {d1}, 1, unhex('{a}'), unhex('{b}'), false, 101, 102, {big}, 2),
         (12, 0, {d1c}, 1, unhex('{a}'), unhex('{c}'), true, 101, 103, 1000, 1)"
    ))
    .await;
    ex(format!(
        "INSERT INTO {db}.blocks (height, block_time, orders, cancels, fills, rejected) VALUES
         (10, {d1}, 2, 0, 1, 0), (11, {d1b}, 1, 1, 0, 1), (12, {d1c}, 1, 0, 1, 0), (14, {d2}, 0, 1, 0, 0)"
    ))
    .await;
    ex(format!(
        "INSERT INTO {db}.gaps (from_height, to_height, reason, detected_at) VALUES (13, 13, 'missing', now64(3))"
    ))
    .await;
    ex(format!("INSERT INTO {db}.archive_progress (id, height, updated_at) VALUES (0, 14, now64(6))")).await;

    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), StoreConfig { cache_bytes: 8 << 20, reorg_depth: 16 }).unwrap();
    let state = AppState {
        store: store.clone(),
        rpc: Arc::new(Rpc::new("http://127.0.0.1:9", Duration::from_secs(1)).unwrap()),
        hub: api::Hub::new(store),
        status: Arc::new(Status::default()),
        ready_max_lag: 10,
        final_depth: 16,
        node_cache: Default::default(),
        max_ws_clients: 10,
        clob: Some(Arc::new(Clob::new(&url, &db, &user, &pass).unwrap())),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        axum::serve(
            listener,
            api::router(
                state,
                crate::guard::Limiter::new(crate::guard::GuardConfig {
                    rate: 0.0,
                    burst: 0.0,
                    trust_proxy: false,
                    cors_origins: vec!["*".into()],
                }),
            ),
        )
        .await
        .unwrap()
    });
    let get = |p: String, code: u16| {
        let (http, base) = (http.clone(), base.clone());
        async move {
            let r = http.get(format!("{base}{p}")).send().await.unwrap();
            assert_eq!(r.status().as_u16(), code, "{p}");
            r.json::<Value>().await.unwrap()
        }
    };

    // Order by hash: the executed occurrence is primary, the rejected
    // duplicate is listed, and the order's fills (as maker, two blocks).
    let v = get(format!("/api/clob/orders/0x{}", h("11")), 200).await;
    assert_eq!(
        (v["kind"].clone(), v["order"]["status"].clone(), v["order"]["price"].clone()),
        (json!("order"), json!("open"), json!(big))
    );
    assert_eq!(v["occurrences"].as_array().unwrap().len(), 2);
    let fills = v["fills"].as_array().unwrap();
    assert_eq!(
        (fills.len(), fills[0]["height"].clone(), fills[1]["counterparty"].clone()),
        (2, json!(10), json!(format!("0x{c}")))
    );
    let v = get(format!("/api/clob/orders/0x{}", h("33")), 200).await;
    assert_eq!(
        (v["kind"].clone(), v["cancel"]["price"].clone(), v["cancel"]["status"].clone()),
        (json!("amend"), json!("999"), json!("amended"))
    );
    get(format!("/api/clob/orders/0x{}", h("99")), 404).await;

    // Trader orders newest first, exact despite the duplicate load, paged.
    let heights =
        |v: &Value| v["items"].as_array().unwrap().iter().map(|i| i["height"].as_u64().unwrap()).collect::<Vec<_>>();
    assert_eq!(heights(&get(format!("/api/clob/traders/0x{a}/orders"), 200).await), vec![11, 10]);
    let v = get(format!("/api/clob/traders/0x{a}/orders?limit=1"), 200).await;
    assert_eq!(v["next_before"], json!("11-0"));
    let v = get(format!("/api/clob/traders/0x{a}/orders?limit=1&before=11-0"), 200).await;
    assert_eq!((heights(&v), v["items"][0]["price"].clone()), (vec![10], json!(big)));
    let v = get(format!("/api/clob/traders/0x{a}/cancels"), 200).await;
    assert_eq!((v["items"][0]["kind"].clone(), v["items"][1]["kind"].clone()), (json!("cancel"), json!("amend")));

    // Trader fills as maker or taker.
    let v = get(format!("/api/clob/traders/0x{a}/fills"), 200).await;
    assert_eq!(heights(&v), vec![12, 10]);
    assert_eq!((v["items"][0]["role"].clone(), v["items"][0]["is_buy"].clone()), (json!("maker"), json!(false)));
    let v = get(format!("/api/clob/traders/0x{b}/fills"), 200).await;
    let i = &v["items"][0];
    assert_eq!(
        (i["role"].clone(), i["is_buy"].clone(), i["counterparty"].clone()),
        (json!("taker"), json!(false), json!(format!("0x{a}")))
    );

    // Pair tape.
    let v = get("/api/clob/pairs/1/trades?limit=1".into(), 200).await;
    assert_eq!((heights(&v), v["next_before"].clone()), (vec![12], json!("12-0")));
    let v = get("/api/clob/pairs/1/trades?before=12-0".into(), 200).await;
    assert_eq!(
        (heights(&v), v["items"][0]["price"].clone(), v["next_before"].clone()),
        (vec![10], json!(big), Value::Null)
    );
    assert!(heights(&get("/api/clob/pairs/2/trades".into(), 200).await).is_empty());

    // Blocks.
    let v = get("/api/clob/blocks/10".into(), 200).await;
    assert_eq!(
        (v["counts"]["orders"].clone(), v["counts"]["fills"].clone(), v["truncated"].clone()),
        (json!(2), json!(1), json!(false))
    );
    assert_eq!((v["orders"].as_array().unwrap().len(), v["fills"].as_array().unwrap().len()), (2, 1));
    let v = get("/api/clob/blocks/10?limit=1".into(), 200).await;
    assert_eq!(
        (v["orders"].as_array().unwrap().len(), v["truncated"].clone(), v["next_orders_after"].clone()),
        (1, json!(true), json!(0))
    );
    let v = get("/api/clob/blocks/14".into(), 200).await;
    assert_eq!((v["cancels"].as_array().unwrap().len(), v["block_time"].clone()), (1, json!("2026-10-21T00:00:02Z")));
    assert_eq!(get("/api/clob/blocks/13".into(), 404).await["gap"], json!("missing"));
    let v = get("/api/clob/blocks/99".into(), 404).await;
    assert_eq!((v["not_loaded_yet"].clone(), v["latest_loaded_height"].clone()), (json!(true), json!(14)));

    // Stats.
    let v = get("/api/clob/stats".into(), 200).await;
    for (k, want) in [
        ("latest_loaded_height", json!(14)),
        ("blocks", json!("4")),
        ("cancels", json!("2")),
        ("fills", json!("2")),
        ("gap_ranges", json!("1")),
        ("gap_heights", json!("1")),
        ("latest_block_time", json!("2026-10-21T00:00:02Z")),
    ] {
        assert_eq!(v[k], want, "stats {k}");
    }
    ex(format!("DROP DATABASE IF EXISTS {db}")).await;
}
