package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"
)

// Metrics are exported in Prometheus text format on /metrics and summarized
// in a periodic log line (heights/s, rows/s).
type Metrics struct {
	heights    atomic.Uint64 // heights loaded (blocks + gaps)
	blocks     atomic.Uint64
	orders     atomic.Uint64
	cancels    atomic.Uint64
	fills      atomic.Uint64
	rows       atomic.Uint64 // rows written across all tables
	gapHeights atomic.Uint64
	flushes    atomic.Uint64
	flushNanos atomic.Uint64
	rpcErrors  atomic.Uint64
	chErrors   atomic.Uint64
	checkpoint atomic.Uint64
	nodeLatest atomic.Uint64
	start      time.Time
}

func NewMetrics() *Metrics { return &Metrics{start: time.Now()} }

func (m *Metrics) rpcErr() { m.rpcErrors.Add(1) }
func (m *Metrics) chErr()  { m.chErrors.Add(1) }

func (m *Metrics) observeFlush(b *Batch, took time.Duration) {
	m.flushes.Add(1)
	m.flushNanos.Add(uint64(took))
	if !b.empty() {
		m.heights.Add(uint64(b.heights()))
	}
	m.rows.Add(uint64(b.Rows()))
	m.blocks.Add(uint64(len(b.Blocks)))
	for _, blk := range b.Blocks {
		m.orders.Add(uint64(len(blk.Orders)))
		m.cancels.Add(uint64(len(blk.Cancels)))
		m.fills.Add(uint64(len(blk.Fills)))
	}
}

func (m *Metrics) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	c := func(name, help string, v uint64) {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s counter\n%s %d\n", name, help, name, name, v)
	}
	g := func(name, help string, v uint64) {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s gauge\n%s %d\n", name, help, name, name, v)
	}
	c("clobarchiver_heights_total", "Heights loaded (including gaps).", m.heights.Load())
	c("clobarchiver_blocks_total", "Blocks with an archive loaded.", m.blocks.Load())
	c("clobarchiver_orders_total", "Order rows loaded.", m.orders.Load())
	c("clobarchiver_cancels_total", "Cancel/amend rows loaded.", m.cancels.Load())
	c("clobarchiver_fills_total", "Fill rows loaded.", m.fills.Load())
	c("clobarchiver_rows_total", "Rows written across all tables.", m.rows.Load())
	c("clobarchiver_gap_heights_total", "Heights the node did not hold.", m.gapHeights.Load())
	c("clobarchiver_flushes_total", "Insert batches committed.", m.flushes.Load())
	fmt.Fprintf(w, "# HELP clobarchiver_flush_seconds_total Time spent in committed inserts.\n# TYPE clobarchiver_flush_seconds_total counter\nclobarchiver_flush_seconds_total %.3f\n", float64(m.flushNanos.Load())/1e9)
	c("clobarchiver_rpc_errors_total", "Node RPC errors (retried).", m.rpcErrors.Load())
	c("clobarchiver_clickhouse_errors_total", "ClickHouse errors (retried).", m.chErrors.Load())
	g("clobarchiver_checkpoint_height", "Last fully loaded height.", m.checkpoint.Load())
	g("clobarchiver_node_latest_height", "Node's latest archived height.", m.nodeLatest.Load())
}

// logLoop logs throughput every interval until ctx ends.
func (m *Metrics) logLoop(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	lastH, lastR, last := m.heights.Load(), m.rows.Load(), time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			h, r := m.heights.Load(), m.rows.Load()
			dt := now.Sub(last).Seconds()
			cp, latest := m.checkpoint.Load(), m.nodeLatest.Load()
			var lag uint64
			if latest > cp {
				lag = latest - cp
			}
			slog.Info("progress",
				"heights_per_s", fmt.Sprintf("%.1f", float64(h-lastH)/dt),
				"rows_per_s", fmt.Sprintf("%.0f", float64(r-lastR)/dt),
				"checkpoint", cp, "node_latest", latest, "lag", lag,
				"rpc_errors", m.rpcErrors.Load(), "ch_errors", m.chErrors.Load())
			lastH, lastR, last = h, r, now
		}
	}
}
