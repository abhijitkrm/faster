package main

import (
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"time"
)

// Gap is an inclusive range of heights the node did not hold.
type Gap struct {
	From, To uint64
	Reason   string // "missing" | "unavailable"
}

// Batch is a contiguous run of heights [First, Last] inserted together:
// the blocks the node returned plus the gaps (heights it returned null for).
type Batch struct {
	First, Last uint64
	Blocks      []*Block
	Gaps        []Gap
	rows        int
	started     time.Time
}

func (b *Batch) empty() bool  { return b.Last == 0 }
func (b *Batch) heights() int { return int(b.Last - b.First + 1) }
func (b *Batch) Rows() int    { return b.rows }
func (b *Batch) reset()       { *b = Batch{Blocks: b.Blocks[:0], Gaps: b.Gaps[:0]} }
func (b *Batch) extend(h uint64) {
	if b.empty() {
		b.First = h
		b.started = time.Now()
	}
	b.Last = h
}

func (b *Batch) addBlock(blk *Block) {
	b.extend(blk.Height)
	b.Blocks = append(b.Blocks, blk)
	b.rows += blk.Rows()
}

// addGap records height h as not held by the node, coalescing runs.
func (b *Batch) addGap(h uint64, reason string) {
	b.extend(h)
	if n := len(b.Gaps); n > 0 && b.Gaps[n-1].To+1 == h && b.Gaps[n-1].Reason == reason {
		b.Gaps[n-1].To = h
		return
	}
	b.Gaps = append(b.Gaps, Gap{From: h, To: h, Reason: reason})
}

// Store is the archive's write side (ClickHouse in production).
type Store interface {
	// Checkpoint returns the last fully loaded height; ok is false when no
	// checkpoint was ever written.
	Checkpoint(ctx context.Context) (h uint64, ok bool, err error)
	// Repair removes every row above height cp (a batch inserted but not
	// checkpointed, or a rewound checkpoint), so (re)loading never duplicates.
	Repair(ctx context.Context, cp uint64) error
	// Insert writes the batch's rows and gaps (not the checkpoint).
	Insert(ctx context.Context, b *Batch) error
	// SetCheckpoint records h as fully loaded.
	SetCheckpoint(ctx context.Context, h uint64) error
}

// Node is the archive's read side (the node's clob JSON-RPC namespace).
type Node interface {
	archiveLatest(ctx context.Context) (ArchiveInfo, error)
	blockArchive(ctx context.Context, h uint64) (*Block, error)
}

// Archiver copies the node's per-block CLOB archive into the store, strictly
// in height order: N fetchers run ahead of the inserter inside a bounded
// window; the inserter takes results in order, accumulates many heights per
// batch, inserts, and only then advances the checkpoint.
type Archiver struct {
	cfg   Config
	node  Node
	store Store
	m     *Metrics
	log   *slog.Logger

	cp    uint64
	batch Batch
}

func NewArchiver(cfg Config, node Node, store Store, m *Metrics) *Archiver {
	if cfg.FetchWorkers < 1 {
		cfg.FetchWorkers = 1
	}
	if cfg.Window < cfg.FetchWorkers {
		cfg.Window = cfg.FetchWorkers * 4
	}
	if cfg.BatchMaxHeights < 1 {
		cfg.BatchMaxHeights = 1
	}
	if cfg.BatchMaxRows < 1 {
		cfg.BatchMaxRows = 1
	}
	if cfg.MaxRange < 1 {
		cfg.MaxRange = 100_000
	}
	if m == nil {
		m = NewMetrics()
	}
	return &Archiver{cfg: cfg, node: node, store: store, m: m, log: slog.Default()}
}

// backoff returns the wait before retry attempt n (0-based): 200ms doubling
// to a 10s cap, with ±20% jitter.
func backoff(n int) time.Duration {
	d := 200 * time.Millisecond << min(n, 6)
	if d > 10*time.Second {
		d = 10 * time.Second
	}
	return time.Duration(float64(d) * (0.8 + 0.4*rand.Float64()))
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// retry runs fn until it succeeds or ctx ends.
func retry[T any](ctx context.Context, what string, onErr func(), fn func(context.Context) (T, error)) (T, error) {
	for n := 0; ; n++ {
		v, err := fn(ctx)
		if err == nil {
			return v, nil
		}
		if ctx.Err() != nil {
			return v, ctx.Err()
		}
		if onErr != nil {
			onErr()
		}
		slog.Warn("retrying", "op", what, "attempt", n+1, "err", err)
		if err := sleepCtx(ctx, backoff(n)); err != nil {
			return v, err
		}
	}
}

// Run loads until ctx is cancelled. It returns nil on a clean shutdown.
func (a *Archiver) Run(ctx context.Context) error {
	type cpv struct {
		h  uint64
		ok bool
	}
	got, err := retry(ctx, "read checkpoint", a.m.chErr, func(ctx context.Context) (cpv, error) {
		h, ok, err := a.store.Checkpoint(ctx)
		return cpv{h, ok}, err
	})
	if err != nil {
		return nilIfCanceled(err)
	}
	a.cp = got.h
	if !got.ok && a.cfg.StartHeight > 1 {
		a.cp = a.cfg.StartHeight - 1
	}
	a.log.Info("starting", "checkpoint", a.cp, "had_checkpoint", got.ok)
	if _, err := retry(ctx, "repair", a.m.chErr, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, a.store.Repair(ctx, a.cp)
	}); err != nil {
		return nilIfCanceled(err)
	}
	a.m.checkpoint.Store(a.cp)

	poll := time.Duration(a.cfg.PollMs) * time.Millisecond
	for {
		if ctx.Err() != nil {
			return a.shutdown()
		}
		info, err := retry(ctx, "clob_archiveLatest", a.m.rpcErr, a.node.archiveLatest)
		if err != nil {
			return a.shutdown()
		}
		a.m.nodeLatest.Store(info.Latest)
		if !info.Enabled {
			a.log.Warn("node archive disabled (start the node with CLOB_ARCHIVE=1); waiting")
			if sleepCtx(ctx, 10*poll) != nil {
				return a.shutdown()
			}
			continue
		}
		next := a.nextHeight()
		if info.Latest > 0 && info.Oldest > next {
			// The node no longer (or never) held these heights: one range.
			a.batch.extend(next)
			a.batch.Gaps = append(a.batch.Gaps, Gap{From: next, To: info.Oldest - 1, Reason: "unavailable"})
			a.batch.Last = info.Oldest - 1
			a.m.gapHeights.Add(info.Oldest - next)
			a.log.Warn("heights below node's oldest are unavailable", "from", next, "to", info.Oldest-1)
			if err := a.flush(ctx); err != nil {
				return a.shutdown()
			}
			continue
		}
		if next > info.Latest {
			if !a.batch.empty() && time.Since(a.batch.started) >= a.maxWait() {
				if err := a.flush(ctx); err != nil {
					return a.shutdown()
				}
			}
			if sleepCtx(ctx, poll) != nil {
				return a.shutdown()
			}
			continue
		}
		to := min(info.Latest, next+uint64(a.cfg.MaxRange)-1)
		if err := a.syncRange(ctx, next, to); err != nil {
			return a.shutdown()
		}
	}
}

func nilIfCanceled(err error) error {
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

func (a *Archiver) maxWait() time.Duration {
	return time.Duration(a.cfg.BatchMaxWaitMs) * time.Millisecond
}

// nextHeight is the first height not yet loaded or buffered.
func (a *Archiver) nextHeight() uint64 {
	if !a.batch.empty() {
		return a.batch.Last + 1
	}
	return a.cp + 1
}

// shutdown flushes what is already buffered (one bounded attempt), so a
// clean stop does not refetch it.
func (a *Archiver) shutdown() error {
	if a.batch.empty() {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := a.insertOnce(ctx); err != nil {
		a.log.Warn("final flush failed; batch will be reloaded on restart", "first", a.batch.First, "last", a.batch.Last, "err", err)
		return nil
	}
	a.log.Info("final flush done", "checkpoint", a.cp)
	return nil
}

type fetched struct {
	h   uint64
	blk *Block
}

// syncRange loads heights [from, to] into the batch, flushing as batches fill.
func (a *Archiver) syncRange(ctx context.Context, from, to uint64) error {
	fctx, cancel := context.WithCancel(ctx)
	defer cancel()

	window := make(chan struct{}, a.cfg.Window)
	jobs := make(chan uint64)
	results := make(chan fetched, a.cfg.Window)

	go func() {
		defer close(jobs)
		for h := from; h <= to; h++ {
			select {
			case window <- struct{}{}:
			case <-fctx.Done():
				return
			}
			select {
			case jobs <- h:
			case <-fctx.Done():
				return
			}
		}
	}()
	for range a.cfg.FetchWorkers {
		go func() {
			for h := range jobs {
				blk, err := retry(fctx, "clob_getBlockArchive", a.m.rpcErr, func(ctx context.Context) (*Block, error) {
					return a.node.blockArchive(ctx, h)
				})
				if err != nil {
					return
				}
				results <- fetched{h, blk} // never blocks: buffer == window
			}
		}()
	}

	pending := make(map[uint64]*Block, a.cfg.Window)
	have := make(map[uint64]bool, a.cfg.Window)
	next := from
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for next <= to {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case r := <-results:
			pending[r.h], have[r.h] = r.blk, true
		case <-tick.C:
			if !a.batch.empty() && time.Since(a.batch.started) >= a.maxWait() {
				if err := a.flush(ctx); err != nil {
					return err
				}
			}
			continue
		}
		for have[next] {
			blk := pending[next]
			delete(pending, next)
			delete(have, next)
			<-window
			if blk == nil {
				a.m.gapHeights.Add(1)
				a.log.Warn("node does not hold height; recorded as gap", "height", next)
				a.batch.addGap(next, "missing")
			} else {
				a.batch.addBlock(blk)
			}
			next++
			if a.batch.Rows() >= a.cfg.BatchMaxRows || a.batch.heights() >= a.cfg.BatchMaxHeights {
				if err := a.flush(ctx); err != nil {
					return err
				}
			}
		}
	}
	if !a.batch.empty() && time.Since(a.batch.started) >= a.maxWait() {
		return a.flush(ctx)
	}
	return nil
}

// insertOnce inserts the batch and advances the checkpoint.
func (a *Archiver) insertOnce(ctx context.Context) error {
	t0 := time.Now()
	if err := a.store.Insert(ctx, &a.batch); err != nil {
		return err
	}
	if err := a.store.SetCheckpoint(ctx, a.batch.Last); err != nil {
		return err
	}
	a.m.observeFlush(&a.batch, time.Since(t0))
	a.log.Debug("flushed", "first", a.batch.First, "last", a.batch.Last, "rows", a.batch.Rows(), "took", time.Since(t0))
	a.cp = a.batch.Last
	a.m.checkpoint.Store(a.cp)
	a.batch.reset()
	return nil
}

// flush inserts the batch, retrying with backoff. Before each retry it
// removes whatever the failed attempt left above the checkpoint, so a
// partially applied batch is never duplicated.
func (a *Archiver) flush(ctx context.Context) error {
	if a.batch.empty() {
		return nil
	}
	for n := 0; ; n++ {
		if n > 0 {
			if err := sleepCtx(ctx, backoff(n-1)); err != nil {
				return err
			}
			if err := a.store.Repair(ctx, a.cp); err != nil {
				a.m.chErr()
				a.log.Warn("repair failed; retrying", "checkpoint", a.cp, "err", err)
				continue
			}
		}
		err := a.insertOnce(ctx)
		if err == nil {
			return nil
		}
		a.m.chErr()
		if ctx.Err() != nil {
			return ctx.Err()
		}
		a.log.Warn("insert failed; will repair and retry", "first", a.batch.First, "last", a.batch.Last, "attempt", n+1, "err", err)
	}
}
