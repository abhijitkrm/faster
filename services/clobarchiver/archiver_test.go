package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"clobarchiver/synth"
)

func init() { slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil))) }

// docExample is the clob_getBlockArchive example from evm-node
// docs/clob-archive-rpc.md with the "0x…" placeholders filled in, plus a
// price at the top of the uint64 range.
const docExample = `{
  "height": 5123,
  "time_ns": 1792600419439000000,
  "orders": [{
    "hash": "0x1111111111111111111111111111111111111111111111111111111111111111", "trader": "0xaAaAaAaaAaAaAaaAaAAAAAAAAaaaAaAaAaaAaaAa", "pair_id": 3, "client_order_id": 77,
    "order_id": 1001, "is_buy": true, "is_market": false, "price": 18446744073709551615, "amount": 5,
    "timestamp_ns": 1792600419000000000, "status": "partial", "filled": 2, "reason": ""
  }],
  "cancels": [{
    "kind": "cancel", "hash": "0x2222222222222222222222222222222222222222222222222222222222222222", "trader": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "pair_id": 3, "client_id": 5,
    "target_order_id": 0, "target_client": 77, "price": 0, "amount": 0,
    "status": "cancelled", "reason": ""
  }],
  "fills": [{
    "seq": 0, "pair_id": 3, "maker": "0xcccccccccccccccccccccccccccccccccccccccc", "taker": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "taker_is_buy": true,
    "maker_order_id": 900, "taker_order_id": 1001, "price": 1000000, "qty": 2
  }]
}`

func TestDecodeDocExample(t *testing.T) {
	b, err := decodeBlock(json.RawMessage(docExample), 5123)
	if err != nil {
		t.Fatal(err)
	}
	if b.Height != 5123 || b.TimeNs != 1792600419439000000 {
		t.Fatalf("header: %+v", b)
	}
	o := b.Orders[0]
	if o.Price != 18446744073709551615 {
		t.Fatalf("uint64 price lost precision: %d", o.Price)
	}
	if o.Trader.String() != "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" || o.Hash[0] != 0x11 {
		t.Fatalf("hex fields: %s %s", o.Trader, o.Hash)
	}
	if o.PairID != 3 || o.ClientOrderID != 77 || o.OrderID != 1001 || !o.IsBuy || o.IsMarket ||
		o.Amount != 5 || o.TimestampNs != 1792600419000000000 || o.Status != "partial" || o.Filled != 2 {
		t.Fatalf("order: %+v", o)
	}
	c := b.Cancels[0]
	if c.Kind != "cancel" || c.ClientID != 5 || c.TargetClient != 77 || c.TargetOrderID != 0 || c.Status != "cancelled" {
		t.Fatalf("cancel: %+v", c)
	}
	f := b.Fills[0]
	if f.Seq != 0 || f.Maker[0] != 0xcc || f.Taker[0] != 0xaa || !f.TakerIsBuy || f.MakerOrderID != 900 ||
		f.TakerOrderID != 1001 || f.Price != 1000000 || f.Qty != 2 {
		t.Fatalf("fill: %+v", f)
	}
	if got := b.Rows(); got != 1+2+2+3 {
		t.Fatalf("rows = %d", got)
	}
}

func TestDecodeNullAndErrors(t *testing.T) {
	if b, err := decodeBlock(json.RawMessage("null"), 7); b != nil || err != nil {
		t.Fatalf("null: %v %v", b, err)
	}
	cases := map[string]string{
		"wrong height":   strings.Replace(docExample, `"height": 5123`, `"height": 5124`, 1),
		"short hash":     strings.Replace(docExample, "0x1111111111111111111111111111111111111111111111111111111111111111", "0x11", 1),
		"no 0x":          strings.Replace(docExample, `"0xcccccccccccccccccccccccccccccccccccccccc"`, `"cccccccccccccccccccccccccccccccccccccccccc"`, 1),
		"bad status":     strings.Replace(docExample, `"partial"`, `"weird"`, 1),
		"bad kind":       strings.Replace(docExample, `"kind": "cancel"`, `"kind": "nope"`, 1),
		"negative uint":  strings.Replace(docExample, `"amount": 5`, `"amount": -5`, 1),
		"float for uint": strings.Replace(docExample, `"amount": 5`, `"amount": 5.5`, 1),
	}
	for name, js := range cases {
		if _, err := decodeBlock(json.RawMessage(js), 5123); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}

// memStore is an in-memory Store recording what the archiver did.
type memStore struct {
	mu         sync.Mutex
	cp         uint64
	hasCP      bool
	batches    []Batch // copies, in insert order
	heights    map[uint64]int
	gaps       []Gap
	repairs    []uint64
	failInsert int // fail this many Insert calls (after partially applying)
}

func newMemStore() *memStore { return &memStore{heights: map[uint64]int{}} }

func (s *memStore) Checkpoint(context.Context) (uint64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cp, s.hasCP, nil
}

func (s *memStore) Repair(_ context.Context, cp uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.repairs = append(s.repairs, cp)
	for h := range s.heights {
		if h > cp {
			delete(s.heights, h)
		}
	}
	var kept []Gap
	for _, g := range s.gaps {
		if g.From > cp {
			continue
		}
		if g.To > cp {
			g.To = cp
		}
		kept = append(kept, g)
	}
	s.gaps = kept
	return nil
}

func (s *memStore) Insert(_ context.Context, b *Batch) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Apply first, then maybe fail: models a batch that reached some tables.
	for _, blk := range b.Blocks {
		s.heights[blk.Height]++
	}
	s.gaps = append(s.gaps, b.Gaps...)
	if s.failInsert > 0 {
		s.failInsert--
		return errors.New("injected insert failure")
	}
	cp := *b
	cp.Blocks = append([]*Block(nil), b.Blocks...)
	cp.Gaps = append([]Gap(nil), b.Gaps...)
	s.batches = append(s.batches, cp)
	return nil
}

func (s *memStore) SetCheckpoint(_ context.Context, h uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cp, s.hasCP = h, true
	return nil
}

func testConfig() Config {
	return Config{FetchWorkers: 8, BatchMaxRows: 200, BatchMaxHeights: 37, BatchMaxWaitMs: 20, MaxRange: 1000, PollMs: 10}
}

// runUntil runs the archiver until the store's checkpoint reaches target.
func runUntil(t *testing.T, a *Archiver, s *memStore, target uint64) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	deadline := time.Now().Add(20 * time.Second)
	for {
		cp, _, _ := s.Checkpoint(ctx)
		if cp >= target {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("checkpoint stuck at %d (want %d)", cp, target)
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func TestArchiverOrderBatchingAndGaps(t *testing.T) {
	gaps := []uint64{50, 51, 52, 120, 299}
	node := synth.NewNode(1, 300, gaps...)
	node.FailEvery = 13 // RPC errors are retried, never skipped
	srv := httptest.NewServer(node)
	defer srv.Close()

	s := newMemStore()
	cfg := testConfig()
	a := NewArchiver(cfg, newNodeClient(srv.URL, 5*time.Second), s, nil)
	runUntil(t, a, s, 300)

	isGap := map[uint64]bool{}
	for _, g := range gaps {
		isGap[g] = true
	}
	var prev uint64
	for i, b := range s.batches {
		if b.First != prev+1 {
			t.Fatalf("batch %d starts at %d, want %d (not contiguous)", i, b.First, prev+1)
		}
		if b.heights() > cfg.BatchMaxHeights {
			t.Fatalf("batch %d has %d heights > %d", i, b.heights(), cfg.BatchMaxHeights)
		}
		maxBlockRows := 0
		last := b.First - 1
		for _, blk := range b.Blocks {
			if blk.Height <= last {
				t.Fatalf("batch %d out of order: %d after %d", i, blk.Height, last)
			}
			last = blk.Height
			maxBlockRows = max(maxBlockRows, blk.Rows())
			if isGap[blk.Height] {
				t.Fatalf("gap height %d inserted as block", blk.Height)
			}
		}
		if b.Rows() >= cfg.BatchMaxRows+maxBlockRows {
			t.Fatalf("batch %d rows %d overshoot max %d", i, b.Rows(), cfg.BatchMaxRows)
		}
		prev = b.Last
	}
	if prev != 300 {
		t.Fatalf("last batch ends at %d", prev)
	}
	for h := uint64(1); h <= 300; h++ {
		want := 1
		if isGap[h] {
			want = 0
		}
		if s.heights[h] != want {
			t.Fatalf("height %d inserted %d times, want %d", h, s.heights[h], want)
		}
	}
	// Gaps are recorded as coalesced ranges.
	var got []Gap
	for _, g := range s.gaps {
		if n := len(got); n > 0 && got[n-1].To+1 == g.From {
			got[n-1].To = g.To // a run split across batches
			continue
		}
		got = append(got, g)
	}
	want := []Gap{{50, 52, "missing"}, {120, 120, "missing"}, {299, 299, "missing"}}
	if len(got) != len(want) {
		t.Fatalf("gaps = %+v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("gap %d = %+v want %+v", i, got[i], want[i])
		}
	}
	if s.repairs[0] != 0 {
		t.Fatalf("startup repair at %d", s.repairs[0])
	}
	if a.m.rpcErrors.Load() == 0 {
		t.Fatal("expected injected RPC errors to be counted")
	}
}

func TestArchiverInsertFailureRepairsBeforeRetry(t *testing.T) {
	node := synth.NewNode(1, 120)
	srv := httptest.NewServer(node)
	defer srv.Close()
	s := newMemStore()
	s.failInsert = 3
	a := NewArchiver(testConfig(), newNodeClient(srv.URL, 5*time.Second), s, nil)
	runUntil(t, a, s, 120)
	for h := uint64(1); h <= 120; h++ {
		if s.heights[h] != 1 {
			t.Fatalf("height %d present %d times after retried inserts", h, s.heights[h])
		}
	}
	if len(s.repairs) != 1+3 {
		t.Fatalf("repairs = %v, want startup + one per failed insert", s.repairs)
	}
}

func TestArchiverResumesFromCheckpointAndFollowsTip(t *testing.T) {
	node := synth.NewNode(1, 150)
	srv := httptest.NewServer(node)
	defer srv.Close()
	s := newMemStore()
	s.cp, s.hasCP = 100, true
	a := NewArchiver(testConfig(), newNodeClient(srv.URL, 5*time.Second), s, nil)
	go func() {
		time.Sleep(100 * time.Millisecond)
		node.SetLatest(170)
	}()
	runUntil(t, a, s, 170)
	if s.repairs[0] != 100 {
		t.Fatalf("startup repair at %d, want 100", s.repairs[0])
	}
	if s.batches[0].First != 101 {
		t.Fatalf("resumed at %d", s.batches[0].First)
	}
	for h := uint64(1); h <= 170; h++ {
		want := 0
		if h > 100 {
			want = 1
		}
		if s.heights[h] != want {
			t.Fatalf("height %d present %d times", h, s.heights[h])
		}
	}
}

func TestArchiverBelowOldestIsOneUnavailableRange(t *testing.T) {
	node := synth.NewNode(5000, 5010)
	srv := httptest.NewServer(node)
	defer srv.Close()
	s := newMemStore()
	a := NewArchiver(testConfig(), newNodeClient(srv.URL, 5*time.Second), s, nil)
	runUntil(t, a, s, 5010)
	if len(s.gaps) != 1 || s.gaps[0] != (Gap{1, 4999, "unavailable"}) {
		t.Fatalf("gaps = %+v", s.gaps)
	}
	if node.Calls.Load() > 20 {
		t.Fatalf("fetched %d heights; unavailable ones must not be fetched", node.Calls.Load())
	}
}

func TestArchiverStartHeight(t *testing.T) {
	node := synth.NewNode(1, 60)
	srv := httptest.NewServer(node)
	defer srv.Close()
	s := newMemStore()
	cfg := testConfig()
	cfg.StartHeight = 40
	a := NewArchiver(cfg, newNodeClient(srv.URL, 5*time.Second), s, nil)
	runUntil(t, a, s, 60)
	if s.batches[0].First != 40 {
		t.Fatalf("started at %d", s.batches[0].First)
	}
}
