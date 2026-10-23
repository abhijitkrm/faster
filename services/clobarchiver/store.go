package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// CHStore writes batches to ClickHouse over the native protocol: one INSERT
// per table per batch (each one part), never per-row inserts.
type CHStore struct {
	conn driver.Conn
	db   string
	desc string
}

var identRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// OpenClickHouse connects using CLICKHOUSE_DSN, or CLICKHOUSE_ADDR plus
// database/user/password.
func OpenClickHouse(ctx context.Context, cfg Config) (*CHStore, error) {
	var opts *clickhouse.Options
	if cfg.CHDSN != "" {
		o, err := clickhouse.ParseDSN(cfg.CHDSN)
		if err != nil {
			return nil, fmt.Errorf("parse CLICKHOUSE_DSN: %w", err)
		}
		opts = o
	} else {
		opts = &clickhouse.Options{
			Addr: []string{cfg.CHAddr},
			Auth: clickhouse.Auth{Database: cfg.CHDatabase, Username: cfg.CHUser, Password: cfg.CHPassword},
		}
	}
	if opts.Auth.Database == "" {
		opts.Auth.Database = "clob"
	}
	if !identRE.MatchString(opts.Auth.Database) {
		return nil, fmt.Errorf("bad database name %q", opts.Auth.Database)
	}
	if opts.DialTimeout == 0 {
		opts.DialTimeout = 10 * time.Second
	}
	if opts.Compression == nil {
		opts.Compression = &clickhouse.Compression{Method: clickhouse.CompressionLZ4}
	}
	if opts.MaxOpenConns == 0 {
		opts.MaxOpenConns = 8
	}
	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, err
	}
	// Do not fail hard if ClickHouse is still starting: the archiver retries.
	pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	_ = conn.Ping(pctx)
	return &CHStore{conn: conn, db: opts.Auth.Database, desc: strings.Join(opts.Addr, ",") + "/" + opts.Auth.Database}, nil
}

func (s *CHStore) Close() error     { return s.conn.Close() }
func (s *CHStore) Describe() string { return s.desc }

func (s *CHStore) t(name string) string { return s.db + "." + name }

func (s *CHStore) Checkpoint(ctx context.Context) (uint64, bool, error) {
	var h, n uint64
	row := s.conn.QueryRow(ctx, "SELECT argMax(height, updated_at), count() FROM "+s.t("archive_progress"))
	if err := row.Scan(&h, &n); err != nil {
		return 0, false, err
	}
	return h, n > 0, nil
}

func (s *CHStore) SetCheckpoint(ctx context.Context, h uint64) error {
	b, err := s.conn.PrepareBatch(ctx, "INSERT INTO "+s.t("archive_progress")+" (id, height, updated_at)")
	if err != nil {
		return err
	}
	defer abortUnsent(b)
	if err := b.Append(uint8(0), h, time.Now().UTC()); err != nil {
		return err
	}
	return b.Send()
}

// dataTables carry a height and a block_time column.
// Materialized views fill the re-sorted copies on insert, but a DELETE on a
// source table does not propagate, so every table is repaired explicitly.
var dataTables = []string{
	"orders", "orders_by_trader", "cancels", "cancels_by_trader",
	"fills", "fills_by_trader", "fills_by_order", "item_hash", "blocks",
}

// Repair deletes rows above cp. Block time is non-decreasing in height, so
// rows above cp have block_time >= the time of the last loaded block; the
// predicate prunes the mutation to the newest partitions. A table is only
// mutated when it actually holds such rows (the common case is none).
func (s *CHStore) Repair(ctx context.Context, cp uint64) error {
	var tmin time.Time
	if err := s.conn.QueryRow(ctx, "SELECT max(block_time) FROM "+s.t("blocks")+" WHERE height <= ?", cp).Scan(&tmin); err != nil {
		return fmt.Errorf("repair: last block time: %w", err)
	}
	// cp and the time bound come from our own state (not user input) and
	// are formatted as integers.
	where := fmt.Sprintf("height > %d AND block_time >= fromUnixTimestamp64Nano(toInt64(%d), 'UTC')", cp, tmin.UnixNano())
	if tmin.IsZero() || tmin.Unix() <= 0 {
		where = fmt.Sprintf("height > %d", cp)
	}
	for _, tbl := range dataTables {
		var one uint8
		err := s.conn.QueryRow(ctx, "SELECT 1 FROM "+s.t(tbl)+" WHERE "+where+" LIMIT 1").Scan(&one)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				continue
			}
			return fmt.Errorf("repair: probe %s: %w", tbl, err)
		}
		if err := s.conn.Exec(ctx, "ALTER TABLE "+s.t(tbl)+" DELETE WHERE "+where+" SETTINGS mutations_sync = 2"); err != nil {
			return fmt.Errorf("repair: delete from %s: %w", tbl, err)
		}
	}
	// Gap ranges reaching above cp: drop them, keeping any part at or
	// below cp.
	rows, err := s.conn.Query(ctx, "SELECT from_height, to_height, toString(reason) FROM "+s.t("gaps")+" FINAL WHERE to_height > ?", cp)
	if err != nil {
		return fmt.Errorf("repair: gaps: %w", err)
	}
	var keep []Gap
	found := false
	for rows.Next() {
		var g Gap
		if err := rows.Scan(&g.From, &g.To, &g.Reason); err != nil {
			rows.Close()
			return err
		}
		found = true
		if g.From <= cp {
			keep = append(keep, Gap{From: g.From, To: cp, Reason: g.Reason})
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	if !found {
		return nil
	}
	if err := s.conn.Exec(ctx, fmt.Sprintf("ALTER TABLE %s DELETE WHERE to_height > %d SETTINGS mutations_sync = 2", s.t("gaps"), cp)); err != nil {
		return fmt.Errorf("repair: delete gaps: %w", err)
	}
	return s.insertGaps(ctx, keep)
}

func (s *CHStore) insertGaps(ctx context.Context, gaps []Gap) error {
	if len(gaps) == 0 {
		return nil
	}
	b, err := s.conn.PrepareBatch(ctx, "INSERT INTO "+s.t("gaps")+" (from_height, to_height, reason, detected_at)")
	if err != nil {
		return err
	}
	defer abortUnsent(b)
	now := time.Now().UTC()
	for _, g := range gaps {
		if err := b.Append(g.From, g.To, g.Reason, now); err != nil {
			return err
		}
	}
	return b.Send()
}

func blockTime(b *Block) time.Time { return time.Unix(0, b.TimeNs).UTC() }

// Insert writes the batch to the base tables, the per-block summary last.
// Materialized views (01-clob.sql) derive orders_by_trader,
// cancels_by_trader, fills_by_trader, fills_by_order and item_hash server
// side from each inserted block, so every row crosses the wire once.
func (s *CHStore) Insert(ctx context.Context, bt *Batch) error {
	// Every prepared batch holds a connection until sent or aborted.
	var open []driver.Batch
	defer func() {
		for _, b := range open {
			abortUnsent(b)
		}
	}()
	prep := func(q string) (driver.Batch, error) {
		b, err := s.conn.PrepareBatch(ctx, q)
		if err == nil {
			open = append(open, b)
		}
		return b, err
	}

	var nOrders, nCancels, nFills int
	for _, b := range bt.Blocks {
		nOrders += len(b.Orders)
		nCancels += len(b.Cancels)
		nFills += len(b.Fills)
	}

	if nOrders > 0 {
		ob, err := prep("INSERT INTO " + s.t("orders") +
			" (height, idx, block_time, hash, trader, pair_id, client_order_id, order_id, is_buy, is_market, price, amount, timestamp_ns, status, filled, reason)")
		if err != nil {
			return err
		}
		for _, b := range bt.Blocks {
			ts := blockTime(b)
			for i := range b.Orders {
				o := &b.Orders[i]
				if err := ob.Append(b.Height, uint32(i), ts, o.Hash[:], o.Trader[:], o.PairID, o.ClientOrderID, o.OrderID,
					o.IsBuy, o.IsMarket, o.Price, o.Amount, o.TimestampNs, o.Status, o.Filled, o.Reason); err != nil {
					return fmt.Errorf("orders append: %w", err)
				}
			}
		}
		if err := ob.Send(); err != nil {
			return fmt.Errorf("insert orders: %w", err)
		}
	}

	if nCancels > 0 {
		cb, err := prep("INSERT INTO " + s.t("cancels") +
			" (height, idx, block_time, kind, hash, trader, pair_id, client_id, target_order_id, target_client, price, amount, status, reason)")
		if err != nil {
			return err
		}
		for _, b := range bt.Blocks {
			ts := blockTime(b)
			for i := range b.Cancels {
				c := &b.Cancels[i]
				if err := cb.Append(b.Height, uint32(i), ts, c.Kind, c.Hash[:], c.Trader[:], c.PairID, c.ClientID,
					c.TargetOrderID, c.TargetClient, c.Price, c.Amount, c.Status, c.Reason); err != nil {
					return fmt.Errorf("cancels append: %w", err)
				}
			}
		}
		if err := cb.Send(); err != nil {
			return fmt.Errorf("insert cancels: %w", err)
		}
	}

	if nFills > 0 {
		fb, err := prep("INSERT INTO " + s.t("fills") +
			" (height, seq, block_time, pair_id, maker, taker, taker_is_buy, maker_order_id, taker_order_id, price, qty)")
		if err != nil {
			return err
		}
		for _, b := range bt.Blocks {
			ts := blockTime(b)
			for i := range b.Fills {
				f := &b.Fills[i]
				if err := fb.Append(b.Height, f.Seq, ts, f.PairID, f.Maker[:], f.Taker[:], f.TakerIsBuy,
					f.MakerOrderID, f.TakerOrderID, f.Price, f.Qty); err != nil {
					return fmt.Errorf("fills append: %w", err)
				}
			}
		}
		if err := fb.Send(); err != nil {
			return fmt.Errorf("insert fills: %w", err)
		}
	}

	if err := s.insertGaps(ctx, bt.Gaps); err != nil {
		return fmt.Errorf("insert gaps: %w", err)
	}

	if len(bt.Blocks) > 0 {
		bb, err := prep("INSERT INTO " + s.t("blocks") + " (height, block_time, orders, cancels, fills, rejected)")
		if err != nil {
			return err
		}
		for _, b := range bt.Blocks {
			var rejected uint32
			for i := range b.Orders {
				if b.Orders[i].Status == "rejected" {
					rejected++
				}
			}
			for i := range b.Cancels {
				if b.Cancels[i].Status == "rejected" {
					rejected++
				}
			}
			if err := bb.Append(b.Height, blockTime(b), uint32(len(b.Orders)), uint32(len(b.Cancels)), uint32(len(b.Fills)), rejected); err != nil {
				return fmt.Errorf("blocks append: %w", err)
			}
		}
		if err := bb.Send(); err != nil {
			return fmt.Errorf("insert blocks: %w", err)
		}
	}
	return nil
}

func abortUnsent(b driver.Batch) {
	if !b.IsSent() {
		_ = b.Abort()
	}
}
