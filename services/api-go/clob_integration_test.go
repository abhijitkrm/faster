package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// TestClobClickHouseIntegration seeds a throw-away database (schema from
// database/clickhouse/01-clob.sql) and checks every endpoint against it.
// Gated: CLOB_CH_TEST=1 (CLICKHOUSE_ADDR/USER/PASSWORD, defaults as in
// docker-compose: 127.0.0.1:9000, clob/clob).
func TestClobClickHouseIntegration(t *testing.T) {
	if os.Getenv("CLOB_CH_TEST") != "1" {
		t.Skip("set CLOB_CH_TEST=1 to run against ClickHouse")
	}
	ctx := context.Background()
	db := fmt.Sprintf("clob_api_it_%d", time.Now().UnixNano())
	addr := getEnv("CLICKHOUSE_ADDR", "127.0.0.1:9000")
	user, pass := getEnv("CLICKHOUSE_USER", "clob"), getEnv("CLICKHOUSE_PASSWORD", "clob")
	conn, err := clickhouse.Open(&clickhouse.Options{Addr: []string{addr}, Auth: clickhouse.Auth{Database: "default", Username: user, Password: pass}})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	schema, err := os.ReadFile("../../database/clickhouse/01-clob.sql")
	if err != nil {
		t.Fatal(err)
	}
	exec := func(q string) {
		t.Helper()
		if err := conn.Exec(ctx, q); err != nil {
			t.Fatalf("%v\n%s", err, q)
		}
	}
	for _, stmt := range strings.Split(strings.ReplaceAll(string(schema), "clob.", db+"."), ";\n") {
		stmt = strings.ReplaceAll(stmt, "DATABASE IF NOT EXISTS clob", "DATABASE IF NOT EXISTS "+db)
		if !strings.Contains(stmt, "CREATE") {
			continue
		}
		exec(stmt)
	}
	defer conn.Exec(ctx, "DROP DATABASE IF EXISTS "+db)

	A, B, C := strings.Repeat("aa", 20), strings.Repeat("bb", 20), strings.Repeat("cc", 20)
	H1, H2, H3, H4, H5 := strings.Repeat("11", 32), strings.Repeat("22", 32), strings.Repeat("33", 32), strings.Repeat("44", 32), strings.Repeat("55", 32)
	day1 := "toDateTime64('2026-10-20 10:00:00.5', 9, 'UTC')"
	day1b := "toDateTime64('2026-10-20 10:00:01', 9, 'UTC')"
	day1c := "toDateTime64('2026-10-20 10:00:01.5', 9, 'UTC')"
	day2 := "toDateTime64('2026-10-21 00:00:02', 9, 'UTC')"
	big := "18446744073709551615"

	ordersSQL := fmt.Sprintf(`INSERT INTO %s.orders (height, idx, block_time, hash, trader, pair_id, client_order_id, order_id, is_buy, is_market, price, amount, timestamp_ns, status, filled, reason) VALUES
		(10, 0, %s, unhex('%s'), unhex('%s'), 1, 7, 101, true, false, %s, 5, 1792600000000000000, 'open', 0, ''),
		(10, 1, %s, unhex('%s'), unhex('%s'), 1, 8, 102, false, false, 1000, 4, 1792600000000000001, 'partial', 2, '')`,
		db, day1, H1, A, big, day1, H2, B)
	exec(ordersSQL)
	exec(ordersSQL) // an unmerged duplicate load: API reads must still be exact
	exec(fmt.Sprintf(`INSERT INTO %s.orders (height, idx, block_time, hash, trader, pair_id, client_order_id, order_id, is_buy, is_market, price, amount, timestamp_ns, status, filled, reason) VALUES
		(11, 0, %s, unhex('%s'), unhex('%s'), 1, 7, 0, true, false, 1000, 5, 0, 'rejected', 0, 'duplicate order'),
		(12, 0, %s, unhex('%s'), unhex('%s'), 1, 9, 103, true, true, 1000, 1, 0, 'filled', 1, '')`,
		db, day1b, H1, A, day1c, H4, C))
	exec(fmt.Sprintf(`INSERT INTO %s.cancels (height, idx, block_time, kind, hash, trader, pair_id, client_id, target_order_id, target_client, price, amount, status, reason) VALUES
		(11, 0, %s, 'amend', unhex('%s'), unhex('%s'), 1, 70, 101, 0, 999, 3, 'amended', ''),
		(14, 0, %s, 'cancel', unhex('%s'), unhex('%s'), 1, 71, 101, 0, 0, 0, 'cancelled', '')`,
		db, day1b, H3, A, day2, H5, A))
	exec(fmt.Sprintf(`INSERT INTO %s.item_hash (hash, height, idx, kind, block_time, trader, pair_id) VALUES
		(unhex('%s'), 10, 0, 'order', %s, unhex('%s'), 1), (unhex('%s'), 10, 1, 'order', %s, unhex('%s'), 1),
		(unhex('%s'), 11, 0, 'order', %s, unhex('%s'), 1), (unhex('%s'), 12, 0, 'order', %s, unhex('%s'), 1),
		(unhex('%s'), 11, 0, 'amend', %s, unhex('%s'), 1), (unhex('%s'), 14, 0, 'cancel', %s, unhex('%s'), 1)`,
		db, H1, day1, A, H2, day1, B, H1, day1b, A, H4, day1c, C, H3, day1b, A, H5, day2, A))
	exec(fmt.Sprintf(`INSERT INTO %s.fills (height, seq, block_time, pair_id, maker, taker, taker_is_buy, maker_order_id, taker_order_id, price, qty) VALUES
		(10, 0, %s, 1, unhex('%s'), unhex('%s'), false, 101, 102, %s, 2),
		(12, 0, %s, 1, unhex('%s'), unhex('%s'), true, 101, 103, 1000, 1)`,
		db, day1, A, B, big, day1c, A, C))
	exec(fmt.Sprintf(`INSERT INTO %s.fills_by_trader (trader, height, seq, role, block_time, pair_id, order_id, counterparty, counter_order, is_buy, price, qty) VALUES
		(unhex('%s'), 10, 0, 'maker', %s, 1, 101, unhex('%s'), 102, true, %s, 2),
		(unhex('%s'), 10, 0, 'taker', %s, 1, 102, unhex('%s'), 101, false, %s, 2),
		(unhex('%s'), 12, 0, 'maker', %s, 1, 101, unhex('%s'), 103, false, 1000, 1),
		(unhex('%s'), 12, 0, 'taker', %s, 1, 103, unhex('%s'), 101, true, 1000, 1)`,
		db, A, day1, B, big, B, day1, A, big, A, day1c, C, C, day1c, A))
	exec(fmt.Sprintf(`INSERT INTO %s.blocks (height, block_time, orders, cancels, fills, rejected) VALUES
		(10, %s, 2, 0, 1, 0), (11, %s, 1, 1, 0, 1), (12, %s, 1, 0, 1, 0), (14, %s, 0, 1, 0, 0)`, db, day1, day1b, day1c, day2))
	exec(fmt.Sprintf(`INSERT INTO %s.gaps (from_height, to_height, reason, detected_at) VALUES (13, 13, 'missing', now64(3))`, db))
	exec(fmt.Sprintf(`INSERT INTO %s.archive_progress (id, height, updated_at) VALUES (0, 14, now64(6))`, db))

	t.Setenv("CLICKHOUSE_ADDR", addr)
	t.Setenv("CLICKHOUSE_DB", db)
	t.Setenv("CLICKHOUSE_USER", user)
	t.Setenv("CLICKHOUSE_PASSWORD", pass)
	store, err := openClobStore()
	if err != nil || store == nil {
		t.Fatal(store, err)
	}
	srv := httptest.NewServer(&clobAPI{store: store})
	defer srv.Close()

	get := func(path string, wantCode int) map[string]any {
		t.Helper()
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var body map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&body)
		if resp.StatusCode != wantCode {
			t.Fatalf("%s: status %d want %d: %v", path, resp.StatusCode, wantCode, body)
		}
		return body
	}
	items := func(b map[string]any) []map[string]any {
		var out []map[string]any
		for _, it := range b["items"].([]any) {
			out = append(out, it.(map[string]any))
		}
		return out
	}

	// Order by hash: the executed occurrence is primary, the rejected
	// duplicate is listed, and the order's fills (as maker, two blocks).
	b := get("/api/clob/orders/0x"+H1, 200)
	if b["kind"] != "order" || b["order"].(map[string]any)["status"] != "open" || b["order"].(map[string]any)["price"] != big {
		t.Fatalf("order: %v", b)
	}
	if occ := b["occurrences"].([]any); len(occ) != 2 {
		t.Fatalf("occurrences: %v", occ)
	}
	if fills := b["fills"].([]any); len(fills) != 2 || fills[0].(map[string]any)["height"] != float64(10) ||
		fills[1].(map[string]any)["counterparty"] != "0x"+C {
		t.Fatalf("order fills: %v", fills)
	}
	b = get("/api/clob/orders/0x"+H3, 200)
	if b["kind"] != "amend" || b["cancel"].(map[string]any)["price"] != "999" || b["cancel"].(map[string]any)["status"] != "amended" {
		t.Fatalf("amend: %v", b)
	}
	get("/api/clob/orders/0x"+strings.Repeat("99", 32), 404)

	// Trader orders newest first, exact despite the duplicate load, paged.
	b = get("/api/clob/traders/0x"+A+"/orders", 200)
	if it := items(b); len(it) != 2 || it[0]["height"] != float64(11) || it[1]["height"] != float64(10) {
		t.Fatalf("trader orders: %v", b)
	}
	b = get("/api/clob/traders/0x"+A+"/orders?limit=1", 200)
	if b["next_before"] != "11-0" {
		t.Fatalf("next_before %v", b["next_before"])
	}
	b = get("/api/clob/traders/0x"+A+"/orders?limit=1&before=11-0", 200)
	if it := items(b); len(it) != 1 || it[0]["height"] != float64(10) || it[0]["price"] != big {
		t.Fatalf("page 2: %v", b)
	}
	b = get("/api/clob/traders/0x"+A+"/cancels", 200)
	if it := items(b); len(it) != 2 || it[0]["kind"] != "cancel" || it[1]["kind"] != "amend" {
		t.Fatalf("trader cancels: %v", b)
	}

	// Trader fills as maker or taker.
	b = get("/api/clob/traders/0x"+A+"/fills", 200)
	if it := items(b); len(it) != 2 || it[0]["height"] != float64(12) || it[0]["role"] != "maker" || it[0]["is_buy"] != false {
		t.Fatalf("A fills: %v", b)
	}
	b = get("/api/clob/traders/0x"+B+"/fills", 200)
	if it := items(b); len(it) != 1 || it[0]["role"] != "taker" || it[0]["is_buy"] != false || it[0]["counterparty"] != "0x"+A {
		t.Fatalf("B fills: %v", b)
	}

	// Pair tape.
	b = get("/api/clob/pairs/1/trades?limit=1", 200)
	if it := items(b); len(it) != 1 || it[0]["height"] != float64(12) || b["next_before"] != "12-0" {
		t.Fatalf("tape: %v", b)
	}
	b = get("/api/clob/pairs/1/trades?before=12-0", 200)
	if it := items(b); len(it) != 1 || it[0]["height"] != float64(10) || it[0]["price"] != big || b["next_before"] != nil {
		t.Fatalf("tape page 2: %v", b)
	}
	if it := items(get("/api/clob/pairs/2/trades", 200)); len(it) != 0 {
		t.Fatalf("pair 2: %v", it)
	}

	// Blocks.
	b = get("/api/clob/blocks/10", 200)
	if c := b["counts"].(map[string]any); c["orders"] != float64(2) || c["fills"] != float64(1) ||
		len(b["orders"].([]any)) != 2 || len(b["fills"].([]any)) != 1 || b["truncated"] != false {
		t.Fatalf("block 10: %v", b)
	}
	b = get("/api/clob/blocks/10?limit=1", 200)
	if len(b["orders"].([]any)) != 1 || b["truncated"] != true {
		t.Fatalf("block 10 limit: %v", b)
	}
	b = get("/api/clob/blocks/14", 200)
	if len(b["cancels"].([]any)) != 1 || b["block_time"] != "2026-10-21T00:00:02Z" {
		t.Fatalf("block 14: %v", b)
	}
	b = get("/api/clob/blocks/13", 404)
	if b["gap"] != "missing" {
		t.Fatalf("gap block: %v", b)
	}
	b = get("/api/clob/blocks/99", 404)
	if b["not_loaded_yet"] != true || b["latest_loaded_height"] != float64(14) {
		t.Fatalf("future block: %v", b)
	}

	// Stats.
	b = get("/api/clob/stats", 200)
	if b["latest_loaded_height"] != float64(14) || b["blocks"] != "4" || b["cancels"] != "2" ||
		b["fills"] != "2" || b["gap_ranges"] != "1" || b["gap_heights"] != "1" || b["latest_block_time"] != "2026-10-21T00:00:02Z" {
		t.Fatalf("stats: %v", b)
	}
}
