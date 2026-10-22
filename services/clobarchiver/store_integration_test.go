package main

import (
	"context"
	"fmt"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"clobarchiver/synth"
)

// TestClickHouseIntegration runs the archiver against a real ClickHouse.
// Gated: CLOB_CH_TEST=1, with CLICKHOUSE_ADDR / CLICKHOUSE_USER /
// CLICKHOUSE_PASSWORD (defaults 127.0.0.1:9000, clob/clob as in
// docker-compose). It applies database/clickhouse/01-clob.sql into a
// throw-away database and drops it at the end.
func TestClickHouseIntegration(t *testing.T) {
	if os.Getenv("CLOB_CH_TEST") != "1" {
		t.Skip("set CLOB_CH_TEST=1 to run against ClickHouse")
	}
	ctx := context.Background()
	db := fmt.Sprintf("clob_it_%d", time.Now().UnixNano())
	cfg := testConfig()
	cfg.CHAddr = getEnv("CLICKHOUSE_ADDR", "127.0.0.1:9000")
	cfg.CHUser = getEnv("CLICKHOUSE_USER", "clob")
	cfg.CHPassword = getEnv("CLICKHOUSE_PASSWORD", "clob")
	cfg.CHDatabase = "default"
	cfg.BatchMaxRows, cfg.BatchMaxHeights = 2000, 120

	admin, err := OpenClickHouse(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema, err := os.ReadFile("../../database/clickhouse/01-clob.sql")
	if err != nil {
		t.Fatal(err)
	}
	for _, stmt := range strings.Split(strings.ReplaceAll(string(schema), "clob.", db+"."), ";\n") {
		stmt = strings.ReplaceAll(stmt, "DATABASE IF NOT EXISTS clob", "DATABASE IF NOT EXISTS "+db)
		if strings.TrimSpace(stripComments(stmt)) == "" {
			continue
		}
		if err := admin.conn.Exec(ctx, stmt); err != nil {
			t.Fatalf("schema: %v\n%s", err, stmt)
		}
	}
	defer admin.conn.Exec(ctx, "DROP DATABASE IF EXISTS "+db)

	cfg.CHDatabase = db
	store, err := OpenClickHouse(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	const latest = 400
	gaps := []uint64{150, 151, 152, 301}
	node := synth.NewNode(1, latest, gaps...)
	srv := httptest.NewServer(node)
	defer srv.Close()

	want := expectedCounts(1, latest, gaps)
	run := func() {
		t.Helper()
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		a := NewArchiver(cfg, newNodeClient(srv.URL, 5*time.Second), store, nil)
		done := make(chan error, 1)
		go func() { done <- a.Run(ctx) }()
		deadline := time.Now().Add(60 * time.Second)
		for {
			cp, _, err := store.Checkpoint(ctx)
			if err == nil && cp >= latest {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("checkpoint stuck at %d", cp)
			}
			time.Sleep(20 * time.Millisecond)
		}
		cancel()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	check := func(stage string) {
		t.Helper()
		for tbl, n := range want {
			var got uint64
			if err := store.conn.QueryRow(ctx, "SELECT count() FROM "+db+"."+tbl).Scan(&got); err != nil {
				t.Fatal(err)
			}
			if got != n {
				t.Errorf("%s: %s has %d rows, want %d", stage, tbl, got, n)
			}
		}
		var ranges, heights uint64
		if err := store.conn.QueryRow(ctx, "SELECT count(), toUInt64(sum(to_height - from_height + 1)) FROM "+db+".gaps FINAL").Scan(&ranges, &heights); err != nil {
			t.Fatal(err)
		}
		if heights != uint64(len(gaps)) {
			t.Errorf("%s: gap heights %d (in %d ranges), want %d", stage, heights, ranges, len(gaps))
		}
		// Exact uint64 round trip above 2^53.
		var amount uint64
		if err := store.conn.QueryRow(ctx, "SELECT amount FROM "+db+".orders WHERE height = 97 AND idx = 0").Scan(&amount); err != nil {
			t.Fatal(err)
		}
		if amount != 1<<62+97 {
			t.Errorf("amount = %d", amount)
		}
	}

	run()
	check("first load")

	// Crash between INSERT and checkpoint: rows above the checkpoint exist.
	extra := &Batch{}
	for h := uint64(latest + 1); h <= latest+10; h++ {
		extra.addBlock(toBlock(t, synth.Gen(h)))
	}
	if err := store.Insert(ctx, extra); err != nil {
		t.Fatal(err)
	}
	// Operator rewinds the checkpoint to 250: everything above is reloaded.
	if err := store.SetCheckpoint(ctx, 250); err != nil {
		t.Fatal(err)
	}
	run()
	check("after rewind + reload")
}

func stripComments(s string) string {
	var b strings.Builder
	for _, line := range strings.Split(s, "\n") {
		if i := strings.Index(line, "--"); i >= 0 {
			line = line[:i]
		}
		b.WriteString(line)
		b.WriteByte('\n')
	}
	return b.String()
}

func toBlock(t *testing.T, sb *synth.Block) *Block {
	t.Helper()
	raw, err := jsonMarshal(sb)
	if err != nil {
		t.Fatal(err)
	}
	b, err := decodeBlock(raw, sb.Height)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// expectedCounts derives per-table row counts from the generator.
func expectedCounts(from, to uint64, gaps []uint64) map[string]uint64 {
	skip := map[uint64]bool{}
	for _, g := range gaps {
		skip[g] = true
	}
	w := map[string]uint64{}
	for h := from; h <= to; h++ {
		if skip[h] {
			continue
		}
		b := synth.Gen(h)
		w["blocks"]++
		w["orders"] += uint64(len(b.Orders))
		w["cancels"] += uint64(len(b.Cancels))
		w["orders_by_trader"] += uint64(len(b.Orders))
		w["cancels_by_trader"] += uint64(len(b.Cancels))
		w["item_hash"] += uint64(len(b.Orders) + len(b.Cancels))
		w["fills"] += uint64(len(b.Fills))
		w["fills_by_trader"] += 2 * uint64(len(b.Fills))
		w["fills_by_order"] += 2 * uint64(len(b.Fills))
	}
	return w
}
