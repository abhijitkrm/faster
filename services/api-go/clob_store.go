package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"regexp"
	"strconv"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// chClobStore implements clobStore on ClickHouse. All user input reaches the
// server as typed query parameters ({name:Type}); only the database name
// (from configuration, validated) is spliced into the SQL text.
type chClobStore struct {
	conn driver.Conn
	db   string
}

var reIdent = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// openClobStore returns nil (CLOB endpoints answer 503) when neither
// CLICKHOUSE_DSN nor CLICKHOUSE_ADDR is set.
func openClobStore() (clobStore, error) {
	dsn, addr := getEnv("CLICKHOUSE_DSN", ""), getEnv("CLICKHOUSE_ADDR", "")
	var opts *clickhouse.Options
	switch {
	case dsn != "":
		o, err := clickhouse.ParseDSN(dsn)
		if err != nil {
			return nil, fmt.Errorf("parse CLICKHOUSE_DSN: %w", err)
		}
		opts = o
	case addr != "":
		opts = &clickhouse.Options{
			Addr: []string{addr},
			Auth: clickhouse.Auth{
				Database: getEnv("CLICKHOUSE_DB", "clob"),
				Username: getEnv("CLICKHOUSE_USER", "default"),
				Password: getEnv("CLICKHOUSE_PASSWORD", ""),
			},
		}
	default:
		return nil, nil
	}
	if opts.Auth.Database == "" {
		opts.Auth.Database = "clob"
	}
	if !reIdent.MatchString(opts.Auth.Database) {
		return nil, fmt.Errorf("bad ClickHouse database name %q", opts.Auth.Database)
	}
	if opts.DialTimeout == 0 {
		opts.DialTimeout = 5 * time.Second
	}
	if opts.MaxOpenConns == 0 {
		opts.MaxOpenConns = 16
	}
	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, err
	}
	return &chClobStore{conn: conn, db: opts.Auth.Database}, nil
}

func (s *chClobStore) t(name string) string { return s.db + "." + name }

func params(kv ...string) clickhouse.QueryOption {
	p := clickhouse.Parameters{}
	for i := 0; i+1 < len(kv); i += 2 {
		p[kv[i]] = kv[i+1]
	}
	return clickhouse.WithParameters(p)
}

func (s *chClobStore) query(ctx context.Context, p clickhouse.QueryOption, sql string) (driver.Rows, error) {
	return s.conn.Query(clickhouse.Context(ctx, p), sql)
}

func u64(v uint64) string   { return strconv.FormatUint(v, 10) }
func hex0x(b string) string { return "0x" + hex.EncodeToString([]byte(b)) }

func timeFields(ns int64) (string, string) {
	return time.Unix(0, ns).UTC().Format(time.RFC3339Nano), strconv.FormatInt(ns, 10)
}

func cursorParams(c clobCursor, kv ...string) clickhouse.QueryOption {
	return params(append(kv, "bh", u64(c.H), "bp", u64(c.P))...)
}

const orderCols = `height, idx, toUnixTimestamp64Nano(block_time), hash, trader, pair_id, client_order_id, order_id,
	is_buy, is_market, price, amount, timestamp_ns, toString(status), filled, reason`

func scanOrders(rows driver.Rows) ([]clobOrder, error) {
	defer rows.Close()
	out := []clobOrder{}
	for rows.Next() {
		var (
			o                                     clobOrder
			ts, tsSigned                          int64
			hash, trader                          string
			clientID, orderID, price, amt, filled uint64
		)
		if err := rows.Scan(&o.Height, &o.Idx, &ts, &hash, &trader, &o.PairID, &clientID, &orderID,
			&o.IsBuy, &o.IsMarket, &price, &amt, &tsSigned, &o.Status, &filled, &o.Reason); err != nil {
			return nil, err
		}
		o.Kind = "order"
		o.BlockTime, o.BlockTimeNs = timeFields(ts)
		o.Hash, o.Trader = hex0x(hash), hex0x(trader)
		o.ClientOrderID, o.OrderID, o.Price, o.Amount, o.Filled = u64(clientID), u64(orderID), u64(price), u64(amt), u64(filled)
		o.TimestampNs = strconv.FormatInt(tsSigned, 10)
		out = append(out, o)
	}
	return out, rows.Err()
}

const cancelCols = `height, idx, toUnixTimestamp64Nano(block_time), toString(kind), hash, trader, pair_id, client_id,
	target_order_id, target_client, price, amount, toString(status), reason`

func scanCancels(rows driver.Rows) ([]clobCancel, error) {
	defer rows.Close()
	out := []clobCancel{}
	for rows.Next() {
		var (
			c                                     clobCancel
			ts                                    int64
			hash, trader                          string
			clientID, tOrder, tClient, price, amt uint64
		)
		if err := rows.Scan(&c.Height, &c.Idx, &ts, &c.Kind, &hash, &trader, &c.PairID, &clientID,
			&tOrder, &tClient, &price, &amt, &c.Status, &c.Reason); err != nil {
			return nil, err
		}
		c.BlockTime, c.BlockTimeNs = timeFields(ts)
		c.Hash, c.Trader = hex0x(hash), hex0x(trader)
		c.ClientID, c.TargetOrderID, c.TargetClient, c.Price, c.Amount = u64(clientID), u64(tOrder), u64(tClient), u64(price), u64(amt)
		out = append(out, c)
	}
	return out, rows.Err()
}

const fillCols = `height, seq, toUnixTimestamp64Nano(block_time), pair_id, maker, taker, taker_is_buy,
	maker_order_id, taker_order_id, price, qty`

func scanFills(rows driver.Rows) ([]clobFill, error) {
	defer rows.Close()
	out := []clobFill{}
	for rows.Next() {
		var (
			f                    clobFill
			ts                   int64
			maker, taker         string
			mID, tID, price, qty uint64
		)
		if err := rows.Scan(&f.Height, &f.Seq, &ts, &f.PairID, &maker, &taker, &f.TakerIsBuy, &mID, &tID, &price, &qty); err != nil {
			return nil, err
		}
		f.BlockTime, f.BlockTimeNs = timeFields(ts)
		f.Maker, f.Taker = hex0x(maker), hex0x(taker)
		f.MakerOrderID, f.TakerOrderID, f.Price, f.Qty = u64(mID), u64(tID), u64(price), u64(qty)
		out = append(out, f)
	}
	return out, rows.Err()
}

const traderFillCols = `height, seq, toString(role), toUnixTimestamp64Nano(block_time), trader, pair_id, order_id,
	counterparty, counter_order, is_buy, price, qty`

func scanTraderFills(rows driver.Rows) ([]clobTraderFill, error) {
	defer rows.Close()
	out := []clobTraderFill{}
	for rows.Next() {
		var (
			f                     clobTraderFill
			ts                    int64
			trader, cp            string
			oid, coid, price, qty uint64
		)
		if err := rows.Scan(&f.Height, &f.Seq, &f.Role, &ts, &trader, &f.PairID, &oid, &cp, &coid, &f.IsBuy, &price, &qty); err != nil {
			return nil, err
		}
		f.BlockTime, f.BlockTimeNs = timeFields(ts)
		f.Trader, f.Counterparty = hex0x(trader), hex0x(cp)
		f.OrderID, f.CounterOrderID, f.Price, f.Qty = u64(oid), u64(coid), u64(price), u64(qty)
		out = append(out, f)
	}
	return out, rows.Err()
}

// Newest-first list queries. "height <= bh" is the indexable bound; the OR
// makes the cursor exclusive at the position. LIMIT 1 BY drops a duplicate
// row that a not-yet-merged re-load could leave (cheap: applied to the few
// rows read in key order, no FINAL).

func (s *chClobStore) TraderOrders(ctx context.Context, trader []byte, before clobCursor, limit int) ([]clobOrder, error) {
	rows, err := s.query(ctx, cursorParams(before, "trader", hex.EncodeToString(trader), "lim", strconv.Itoa(limit)), `
		SELECT `+orderCols+` FROM `+s.t("orders_by_trader")+`
		WHERE trader = unhex({trader:String})
		  AND height <= {bh:UInt64} AND (height < {bh:UInt64} OR idx < {bp:UInt64})
		ORDER BY height DESC, idx DESC
		LIMIT 1 BY height, idx
		LIMIT {lim:UInt32}`)
	if err != nil {
		return nil, err
	}
	return scanOrders(rows)
}

func (s *chClobStore) TraderCancels(ctx context.Context, trader []byte, before clobCursor, limit int) ([]clobCancel, error) {
	rows, err := s.query(ctx, cursorParams(before, "trader", hex.EncodeToString(trader), "lim", strconv.Itoa(limit)), `
		SELECT `+cancelCols+` FROM `+s.t("cancels_by_trader")+`
		WHERE trader = unhex({trader:String})
		  AND height <= {bh:UInt64} AND (height < {bh:UInt64} OR idx < {bp:UInt64})
		ORDER BY height DESC, idx DESC
		LIMIT 1 BY height, idx
		LIMIT {lim:UInt32}`)
	if err != nil {
		return nil, err
	}
	return scanCancels(rows)
}

func (s *chClobStore) TraderFills(ctx context.Context, trader []byte, before clobCursor, limit int) ([]clobTraderFill, error) {
	rows, err := s.query(ctx, cursorParams(before, "trader", hex.EncodeToString(trader), "lim", strconv.Itoa(limit)), `
		SELECT `+traderFillCols+` FROM `+s.t("fills_by_trader")+`
		WHERE trader = unhex({trader:String})
		  AND height <= {bh:UInt64} AND (height < {bh:UInt64} OR seq * 2 + CAST(role, 'Int8') < {bp:UInt64})
		ORDER BY height DESC, seq DESC, role DESC
		LIMIT 1 BY height, seq, role
		LIMIT {lim:UInt32}`)
	if err != nil {
		return nil, err
	}
	return scanTraderFills(rows)
}

func (s *chClobStore) PairTrades(ctx context.Context, pair uint32, before clobCursor, limit int) ([]clobFill, error) {
	rows, err := s.query(ctx, cursorParams(before, "pair", u64(uint64(pair)), "lim", strconv.Itoa(limit)), `
		SELECT `+fillCols+` FROM `+s.t("fills")+`
		WHERE pair_id = {pair:UInt32}
		  AND height <= {bh:UInt64} AND (height < {bh:UInt64} OR seq < {bp:UInt64})
		ORDER BY height DESC, seq DESC
		LIMIT 1 BY height, seq
		LIMIT {lim:UInt32}`)
	if err != nil {
		return nil, err
	}
	return scanFills(rows)
}

// pinned is the WHERE prefix for reading one block's rows: the block time
// prunes to one partition, the height to its granules.
const pinned = `block_time = fromUnixTimestamp64Nano({ts:Int64}, 'UTC') AND height = {h:UInt64}`

func (s *chClobStore) ItemByHash(ctx context.Context, hash []byte) (*clobItemResponse, error) {
	type ref struct {
		height uint64
		idx    uint32
		kind   string
		ts     int64
	}
	rows, err := s.query(ctx, params("hash", hex.EncodeToString(hash)), `
		SELECT height, idx, toString(kind), toUnixTimestamp64Nano(block_time)
		FROM `+s.t("item_hash")+`
		WHERE hash = unhex({hash:String})
		ORDER BY height, kind, idx
		LIMIT 1 BY height, kind, idx
		LIMIT 100`)
	if err != nil {
		return nil, err
	}
	var refs []ref
	for rows.Next() {
		var r ref
		if err := rows.Scan(&r.height, &r.idx, &r.kind, &r.ts); err != nil {
			rows.Close()
			return nil, err
		}
		refs = append(refs, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(refs) == 0 {
		return nil, nil
	}
	res := &clobItemResponse{Hash: "0x" + hex.EncodeToString(hash), Fills: []clobTraderFill{}, Occurrences: []clobOccurrence{}}
	var orders []clobOrder
	var cancels []clobCancel
	for _, r := range refs {
		p := params("ts", strconv.FormatInt(r.ts, 10), "h", u64(r.height), "i", strconv.FormatUint(uint64(r.idx), 10))
		if r.kind == "order" {
			rows, err := s.query(ctx, p, `SELECT `+orderCols+` FROM `+s.t("orders")+` WHERE `+pinned+` AND idx = {i:UInt32} LIMIT 1`)
			if err != nil {
				return nil, err
			}
			got, err := scanOrders(rows)
			if err != nil {
				return nil, err
			}
			for _, o := range got {
				orders = append(orders, o)
				res.Occurrences = append(res.Occurrences, clobOccurrence{"order", o.Height, o.Idx, o.Status})
			}
		} else {
			rows, err := s.query(ctx, p, `SELECT `+cancelCols+` FROM `+s.t("cancels")+` WHERE `+pinned+` AND idx = {i:UInt32} LIMIT 1`)
			if err != nil {
				return nil, err
			}
			got, err := scanCancels(rows)
			if err != nil {
				return nil, err
			}
			for _, c := range got {
				cancels = append(cancels, c)
				res.Occurrences = append(res.Occurrences, clobOccurrence{c.Kind, c.Height, c.Idx, c.Status})
			}
		}
	}
	// The primary item: the first executed (non-rejected) occurrence, else
	// the first one.
	switch {
	case len(orders) > 0:
		pick := orders[0]
		for _, o := range orders {
			if o.Status != "rejected" {
				pick = o
				break
			}
		}
		res.Kind, res.Order = "order", &pick
		if pick.OrderID != "0" {
			rows, err := s.query(ctx, params("pair", u64(uint64(pick.PairID)), "oid", pick.OrderID,
				"trader", pick.Trader[2:], "h", u64(pick.Height)), `
				SELECT `+traderFillCols+` FROM `+s.t("fills_by_order")+`
				WHERE pair_id = {pair:UInt32} AND order_id = {oid:UInt64}
				  AND trader = unhex({trader:String}) AND height >= {h:UInt64}
				ORDER BY height, seq, role
				LIMIT 1 BY height, seq, role
				LIMIT 1000`)
			if err != nil {
				return nil, err
			}
			if res.Fills, err = scanTraderFills(rows); err != nil {
				return nil, err
			}
		}
	case len(cancels) > 0:
		pick := cancels[0]
		for _, c := range cancels {
			if c.Status != "rejected" {
				pick = c
				break
			}
		}
		res.Kind, res.Cancel = pick.Kind, &pick
	default:
		return nil, nil // hash index ahead of a repaired table; treat as absent
	}
	return res, nil
}

func (s *chClobStore) checkpoint(ctx context.Context) (uint64, error) {
	var h uint64
	err := s.conn.QueryRow(ctx, `SELECT argMax(height, updated_at) FROM `+s.t("archive_progress")).Scan(&h)
	return h, err
}

func (s *chClobStore) Block(ctx context.Context, height uint64, page clobBlockPage) (*clobBlockResponse, *clobBlockMissing, error) {
	limit := page.Limit
	rows, err := s.query(ctx, params("h", u64(height)), `
		SELECT toUnixTimestamp64Nano(block_time), orders, cancels, fills, rejected
		FROM `+s.t("blocks")+` WHERE height = {h:UInt64} LIMIT 1`)
	if err != nil {
		return nil, nil, err
	}
	var (
		ts    int64
		found bool
		b     clobBlockResponse
	)
	for rows.Next() {
		found = true
		if err := rows.Scan(&ts, &b.Counts.Orders, &b.Counts.Cancels, &b.Counts.Fills, &b.Counts.Rejected); err != nil {
			rows.Close()
			return nil, nil, err
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	cp, err := s.checkpoint(ctx)
	if err != nil {
		return nil, nil, err
	}
	if !found || height > cp {
		m := &clobBlockMissing{LatestLoaded: cp, BeyondCheckpoint: height > cp}
		grows, err := s.query(ctx, params("h", u64(height)), `
			SELECT toString(reason) FROM `+s.t("gaps")+`
			WHERE from_height <= {h:UInt64} AND to_height >= {h:UInt64} LIMIT 1`)
		if err != nil {
			return nil, nil, err
		}
		for grows.Next() {
			if err := grows.Scan(&m.Gap); err != nil {
				grows.Close()
				return nil, nil, err
			}
		}
		grows.Close()
		return nil, m, grows.Err()
	}
	b.Height = height
	b.BlockTime, b.BlockTimeNs = timeFields(ts)
	p := params("ts", strconv.FormatInt(ts, 10), "h", u64(height), "lim", strconv.Itoa(limit),
		"oa", strconv.FormatInt(page.OrdersAfter, 10), "ca", strconv.FormatInt(page.CancelsAfter, 10),
		"fa", strconv.FormatInt(page.FillsAfter, 10))
	if b.Counts.Orders > 0 {
		r, err := s.query(ctx, p, `SELECT `+orderCols+` FROM `+s.t("orders")+` WHERE `+pinned+`
			AND idx > {oa:Int64} ORDER BY idx LIMIT 1 BY idx LIMIT {lim:UInt32}`)
		if err != nil {
			return nil, nil, err
		}
		if b.Orders, err = scanOrders(r); err != nil {
			return nil, nil, err
		}
	}
	if b.Counts.Cancels > 0 {
		r, err := s.query(ctx, p, `SELECT `+cancelCols+` FROM `+s.t("cancels")+` WHERE `+pinned+`
			AND idx > {ca:Int64} ORDER BY idx LIMIT 1 BY idx LIMIT {lim:UInt32}`)
		if err != nil {
			return nil, nil, err
		}
		if b.Cancels, err = scanCancels(r); err != nil {
			return nil, nil, err
		}
	}
	if b.Counts.Fills > 0 {
		r, err := s.query(ctx, p, `SELECT `+fillCols+` FROM `+s.t("fills")+` WHERE `+pinned+`
			AND seq > {fa:Int64} ORDER BY seq LIMIT 1 BY seq LIMIT {lim:UInt32}`)
		if err != nil {
			return nil, nil, err
		}
		if b.Fills, err = scanFills(r); err != nil {
			return nil, nil, err
		}
	}
	if b.Orders == nil {
		b.Orders = []clobOrder{}
	}
	if b.Cancels == nil {
		b.Cancels = []clobCancel{}
	}
	if b.Fills == nil {
		b.Fills = []clobFill{}
	}
	b.Truncated = len(b.Orders) < int(b.Counts.Orders) || len(b.Cancels) < int(b.Counts.Cancels) || len(b.Fills) < int(b.Counts.Fills)
	// idx and seq are dense (0..count-1): more remain past a full page
	// whose last position is below count-1.
	next := func(n int, last, count uint32) *uint32 {
		if n == limit && last+1 < count {
			return &last
		}
		return nil
	}
	if n := len(b.Orders); n > 0 {
		b.NextOrders = next(n, b.Orders[n-1].Idx, b.Counts.Orders)
	}
	if n := len(b.Cancels); n > 0 {
		b.NextCancels = next(n, b.Cancels[n-1].Idx, b.Counts.Cancels)
	}
	if n := len(b.Fills); n > 0 {
		b.NextFills = next(n, b.Fills[n-1].Seq, b.Counts.Fills)
	}
	return &b, nil, nil
}

// Stats reads table row counts from part metadata (count() without WHERE is
// answered from part metadata, no scan), the checkpoint, and the gap table
// (small, so FINAL is fine there).
func (s *chClobStore) Stats(ctx context.Context) (*clobStats, error) {
	var (
		st                             clobStats
		cp, cpRows                     uint64
		cpAt                           time.Time
		blocks, orders, cancels, fills uint64
		gapRanges, gapHeights          uint64
		latestTs                       int64
		haveBlocks                     uint8
	)
	err := s.conn.QueryRow(ctx, `
		SELECT
		  (SELECT argMax(height, updated_at) FROM `+s.t("archive_progress")+`),
		  (SELECT count() FROM `+s.t("archive_progress")+`),
		  (SELECT max(updated_at) FROM `+s.t("archive_progress")+`),
		  (SELECT count() FROM `+s.t("blocks")+`),
		  (SELECT count() FROM `+s.t("orders")+`),
		  (SELECT count() FROM `+s.t("cancels")+`),
		  (SELECT count() FROM `+s.t("fills")+`),
		  (SELECT count() FROM `+s.t("gaps")+` FINAL),
		  (SELECT toUInt64(sum(to_height - from_height + 1)) FROM `+s.t("gaps")+` FINAL),
		  ifNull((SELECT toUnixTimestamp64Nano(block_time) FROM `+s.t("blocks")+` ORDER BY height DESC LIMIT 1), 0),
		  (SELECT count() > 0 FROM `+s.t("blocks")+`)`).
		Scan(&cp, &cpRows, &cpAt, &blocks, &orders, &cancels, &fills, &gapRanges, &gapHeights, &latestTs, &haveBlocks)
	if err != nil {
		return nil, err
	}
	st.LatestLoadedHeight = cp
	if cpRows > 0 {
		v := cpAt.UTC().Format(time.RFC3339Nano)
		st.CheckpointAt = &v
	}
	if haveBlocks == 1 {
		v, _ := timeFields(latestTs)
		st.LatestBlockTime = &v
	}
	st.Blocks, st.Orders, st.Cancels, st.Fills = u64(blocks), u64(orders), u64(cancels), u64(fills)
	st.GapRanges, st.GapHeights = u64(gapRanges), u64(gapHeights)
	return &st, nil
}
