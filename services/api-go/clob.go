package main

// CLOB history endpoints (/api/clob/*), served from the ClickHouse archive
// loaded by services/clobarchiver (schema: database/clickhouse/01-clob.sql).
//
// JSON conventions:
//   - heights, pair_id, idx, seq: JSON numbers (bounded well below 2^53)
//   - every other integer from the chain (prices, amounts, filled, qty, order
//     and client IDs, nanosecond timestamps, row counts): decimal strings,
//     since uint64/int64 values can exceed 2^53 and lose precision as JS numbers
//   - hashes and addresses: lowercase 0x hex
//   - block_time: RFC 3339 UTC with nanoseconds, plus block_time_ns (string)
//
// Lists are newest first with cursor pagination: pass the response's
// next_before as ?before= to get the next page (null when there is no more).
// A cursor is "<height>-<pos>" (pos = idx, seq, or seq*2+role for trader
// fills); a bare "<height>" means "strictly below that height".

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const (
	clobDefaultLimit = 50
	clobMaxLimit     = 500
)

type clobOrder struct {
	Kind          string `json:"kind"`
	Height        uint64 `json:"height"`
	Idx           uint32 `json:"idx"`
	BlockTime     string `json:"block_time"`
	BlockTimeNs   string `json:"block_time_ns"`
	Hash          string `json:"hash"`
	Trader        string `json:"trader"`
	PairID        uint32 `json:"pair_id"`
	ClientOrderID string `json:"client_order_id"`
	OrderID       string `json:"order_id"`
	IsBuy         bool   `json:"is_buy"`
	IsMarket      bool   `json:"is_market"`
	Price         string `json:"price"`
	Amount        string `json:"amount"`
	TimestampNs   string `json:"timestamp_ns"`
	Status        string `json:"status"`
	Filled        string `json:"filled"`
	Reason        string `json:"reason"`
}

type clobCancel struct {
	Kind          string `json:"kind"` // cancel | amend
	Height        uint64 `json:"height"`
	Idx           uint32 `json:"idx"`
	BlockTime     string `json:"block_time"`
	BlockTimeNs   string `json:"block_time_ns"`
	Hash          string `json:"hash"`
	Trader        string `json:"trader"`
	PairID        uint32 `json:"pair_id"`
	ClientID      string `json:"client_id"`
	TargetOrderID string `json:"target_order_id"`
	TargetClient  string `json:"target_client"`
	Price         string `json:"price"`
	Amount        string `json:"amount"`
	Status        string `json:"status"`
	Reason        string `json:"reason"`
}

type clobFill struct {
	Height       uint64 `json:"height"`
	Seq          uint32 `json:"seq"`
	BlockTime    string `json:"block_time"`
	BlockTimeNs  string `json:"block_time_ns"`
	PairID       uint32 `json:"pair_id"`
	Maker        string `json:"maker"`
	Taker        string `json:"taker"`
	TakerIsBuy   bool   `json:"taker_is_buy"`
	MakerOrderID string `json:"maker_order_id"`
	TakerOrderID string `json:"taker_order_id"`
	Price        string `json:"price"`
	Qty          string `json:"qty"`
}

// clobTraderFill is a fill from one participant's side.
type clobTraderFill struct {
	Height         uint64 `json:"height"`
	Seq            uint32 `json:"seq"`
	Role           string `json:"role"` // maker | taker
	BlockTime      string `json:"block_time"`
	BlockTimeNs    string `json:"block_time_ns"`
	Trader         string `json:"trader"`
	PairID         uint32 `json:"pair_id"`
	OrderID        string `json:"order_id"`
	Counterparty   string `json:"counterparty"`
	CounterOrderID string `json:"counter_order_id"`
	IsBuy          bool   `json:"is_buy"` // the participant's side
	Price          string `json:"price"`
	Qty            string `json:"qty"`
}

type clobOccurrence struct {
	Kind   string `json:"kind"`
	Height uint64 `json:"height"`
	Idx    uint32 `json:"idx"`
	Status string `json:"status"`
}

// clobItemResponse is GET /api/clob/orders/{hash}: the item (order, or
// cancel/amend) the hash refers to, the fills of that order, and every
// occurrence of the hash (a duplicate submission of a signed order is
// executed, and rejected, as its own item).
type clobItemResponse struct {
	Hash        string           `json:"hash"`
	Kind        string           `json:"kind"` // order | cancel | amend
	Order       *clobOrder       `json:"order,omitempty"`
	Cancel      *clobCancel      `json:"cancel,omitempty"`
	Fills       []clobTraderFill `json:"fills"`
	Occurrences []clobOccurrence `json:"occurrences"`
}

type clobBlockCounts struct {
	Orders   uint32 `json:"orders"`
	Cancels  uint32 `json:"cancels"`
	Fills    uint32 `json:"fills"`
	Rejected uint32 `json:"rejected"`
}

// clobBlockResponse is GET /api/clob/blocks/{height}. Each list holds at most
// ?limit= items (in block order); counts are the full totals. A block can hold
// 100K+ items: ?orders_after=, ?cancels_after= (idx) and ?fills_after= (seq)
// page each list, and next_*_after is the cursor for the next page (absent
// when the list is complete).
type clobBlockResponse struct {
	Height      uint64          `json:"height"`
	BlockTime   string          `json:"block_time"`
	BlockTimeNs string          `json:"block_time_ns"`
	Counts      clobBlockCounts `json:"counts"`
	Orders      []clobOrder     `json:"orders"`
	Cancels     []clobCancel    `json:"cancels"`
	Fills       []clobFill      `json:"fills"`
	Truncated   bool            `json:"truncated"`
	NextOrders  *uint32         `json:"next_orders_after,omitempty"`
	NextCancels *uint32         `json:"next_cancels_after,omitempty"`
	NextFills   *uint32         `json:"next_fills_after,omitempty"`
}

// clobBlockPage selects one page of each of a block's lists: items with
// idx/seq greater than the *After cursor (-1 = from the start).
type clobBlockPage struct {
	Limit                                 int
	OrdersAfter, CancelsAfter, FillsAfter int64
}

// clobBlockMissing explains a 404 for a block: never loaded, or a gap.
type clobBlockMissing struct {
	Gap              string `json:"gap,omitempty"` // missing | unavailable
	LatestLoaded     uint64 `json:"latest_loaded_height"`
	BeyondCheckpoint bool   `json:"not_loaded_yet"`
}

type clobStats struct {
	LatestLoadedHeight uint64  `json:"latest_loaded_height"`
	LatestBlockTime    *string `json:"latest_block_time"`
	CheckpointAt       *string `json:"checkpoint_updated_at"`
	Blocks             string  `json:"blocks"`
	Orders             string  `json:"orders"`
	Cancels            string  `json:"cancels"`
	Fills              string  `json:"fills"`
	GapRanges          string  `json:"gap_ranges"`
	GapHeights         string  `json:"gap_heights"`
}

type clobList[T any] struct {
	Items      []T     `json:"items"`
	NextBefore *string `json:"next_before"`
}

// clobCursor positions a newest-first list: items strictly before (H, P).
type clobCursor struct {
	H uint64
	P uint64
}

var clobNoCursor = clobCursor{H: math.MaxUint64, P: math.MaxUint64}

func (c clobCursor) String() string { return fmt.Sprintf("%d-%d", c.H, c.P) }

type clobStore interface {
	ItemByHash(ctx context.Context, hash []byte) (*clobItemResponse, error) // nil: not found
	TraderOrders(ctx context.Context, trader []byte, before clobCursor, limit int) ([]clobOrder, error)
	TraderCancels(ctx context.Context, trader []byte, before clobCursor, limit int) ([]clobCancel, error)
	TraderFills(ctx context.Context, trader []byte, before clobCursor, limit int) ([]clobTraderFill, error)
	PairTrades(ctx context.Context, pair uint32, before clobCursor, limit int) ([]clobFill, error)
	Block(ctx context.Context, height uint64, page clobBlockPage) (*clobBlockResponse, *clobBlockMissing, error)
	Stats(ctx context.Context) (*clobStats, error)
}

// clobAPI routes /api/clob/*. store is nil when ClickHouse is not configured.
type clobAPI struct {
	store clobStore
}

var (
	reHash32 = regexp.MustCompile(`^0x[0-9a-fA-F]{64}$`)
	reAddr   = regexp.MustCompile(`^0x[0-9a-fA-F]{40}$`)
	reCursor = regexp.MustCompile(`^([0-9]{1,20})(?:-([0-9]{1,20}))?$`)
)

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func clobError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// isUnavailable reports errors meaning ClickHouse cannot be reached.
func isUnavailable(err error) bool {
	var ne net.Error
	var oe *net.OpError
	return errors.As(err, &oe) || errors.As(err, &ne) || errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, io.EOF) || errors.Is(err, context.DeadlineExceeded)
}

func (a *clobAPI) fail(w http.ResponseWriter, r *http.Request, err error) {
	if errors.Is(err, context.Canceled) {
		return
	}
	slog.Error("clob query", "path", r.URL.Path, "err", err)
	if isUnavailable(err) {
		clobError(w, http.StatusServiceUnavailable, "CLOB archive unavailable")
		return
	}
	clobError(w, http.StatusInternalServerError, "CLOB archive query failed")
}

func parseLimit(q string) (int, error) {
	if q == "" {
		return clobDefaultLimit, nil
	}
	n, err := strconv.Atoi(q)
	if err != nil || n < 1 {
		return 0, fmt.Errorf("limit must be a positive integer")
	}
	return min(n, clobMaxLimit), nil
}

func parseCursor(q string) (clobCursor, error) {
	if q == "" {
		return clobNoCursor, nil
	}
	m := reCursor.FindStringSubmatch(q)
	if m == nil {
		return clobCursor{}, fmt.Errorf("before must be <height> or <height>-<pos>")
	}
	h, err := strconv.ParseUint(m[1], 10, 64)
	if err != nil {
		return clobCursor{}, fmt.Errorf("before: bad height")
	}
	c := clobCursor{H: h}
	if m[2] != "" {
		if c.P, err = strconv.ParseUint(m[2], 10, 64); err != nil {
			return clobCursor{}, fmt.Errorf("before: bad position")
		}
	}
	return c, nil
}

func decodeHex(s string) []byte {
	b, _ := hex.DecodeString(s[2:])
	return b
}

func nextCursor[T any](items []T, limit int, pos func(T) clobCursor) *string {
	if len(items) < limit || len(items) == 0 {
		return nil
	}
	s := pos(items[len(items)-1]).String()
	return &s
}

func (a *clobAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		clobError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	parts := strings.Split(strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/clob"), "/"), "/")
	if a.store == nil {
		clobError(w, http.StatusServiceUnavailable, "CLOB archive not configured (set CLICKHOUSE_ADDR or CLICKHOUSE_DSN)")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()
	q := r.URL.Query()

	switch {
	case len(parts) == 1 && parts[0] == "stats":
		st, err := a.store.Stats(ctx)
		if err != nil {
			a.fail(w, r, err)
			return
		}
		writeJSON(w, http.StatusOK, st)

	case len(parts) == 2 && parts[0] == "orders":
		if !reHash32.MatchString(parts[1]) {
			clobError(w, http.StatusBadRequest, "hash must be 0x + 64 hex characters")
			return
		}
		item, err := a.store.ItemByHash(ctx, decodeHex(parts[1]))
		if err != nil {
			a.fail(w, r, err)
			return
		}
		if item == nil {
			clobError(w, http.StatusNotFound, "order not found")
			return
		}
		writeJSON(w, http.StatusOK, item)

	case len(parts) == 3 && parts[0] == "traders" && (parts[2] == "orders" || parts[2] == "fills" || parts[2] == "cancels"):
		if !reAddr.MatchString(parts[1]) {
			clobError(w, http.StatusBadRequest, "address must be 0x + 40 hex characters")
			return
		}
		limit, err := parseLimit(q.Get("limit"))
		if err != nil {
			clobError(w, http.StatusBadRequest, err.Error())
			return
		}
		before, err := parseCursor(q.Get("before"))
		if err != nil {
			clobError(w, http.StatusBadRequest, err.Error())
			return
		}
		trader := decodeHex(parts[1])
		switch parts[2] {
		case "orders":
			items, err := a.store.TraderOrders(ctx, trader, before, limit)
			if err != nil {
				a.fail(w, r, err)
				return
			}
			writeJSON(w, http.StatusOK, clobList[clobOrder]{Items: items, NextBefore: nextCursor(items, limit, func(o clobOrder) clobCursor {
				return clobCursor{o.Height, uint64(o.Idx)}
			})})
		case "cancels":
			items, err := a.store.TraderCancels(ctx, trader, before, limit)
			if err != nil {
				a.fail(w, r, err)
				return
			}
			writeJSON(w, http.StatusOK, clobList[clobCancel]{Items: items, NextBefore: nextCursor(items, limit, func(c clobCancel) clobCursor {
				return clobCursor{c.Height, uint64(c.Idx)}
			})})
		case "fills":
			items, err := a.store.TraderFills(ctx, trader, before, limit)
			if err != nil {
				a.fail(w, r, err)
				return
			}
			writeJSON(w, http.StatusOK, clobList[clobTraderFill]{Items: items, NextBefore: nextCursor(items, limit, func(f clobTraderFill) clobCursor {
				return clobCursor{f.Height, traderFillPos(f.Seq, f.Role)}
			})})
		}

	case len(parts) == 3 && parts[0] == "pairs" && parts[2] == "trades":
		pair, err := strconv.ParseUint(parts[1], 10, 32)
		if err != nil {
			clobError(w, http.StatusBadRequest, "pair id must be a uint32")
			return
		}
		limit, err := parseLimit(q.Get("limit"))
		if err != nil {
			clobError(w, http.StatusBadRequest, err.Error())
			return
		}
		before, err := parseCursor(q.Get("before"))
		if err != nil {
			clobError(w, http.StatusBadRequest, err.Error())
			return
		}
		items, err := a.store.PairTrades(ctx, uint32(pair), before, limit)
		if err != nil {
			a.fail(w, r, err)
			return
		}
		writeJSON(w, http.StatusOK, clobList[clobFill]{Items: items, NextBefore: nextCursor(items, limit, func(f clobFill) clobCursor {
			return clobCursor{f.Height, uint64(f.Seq)}
		})})

	case len(parts) == 2 && parts[0] == "blocks":
		h, err := strconv.ParseUint(parts[1], 10, 64)
		if err != nil {
			clobError(w, http.StatusBadRequest, "height must be a non-negative integer")
			return
		}
		limit := 100
		if v := q.Get("limit"); v != "" {
			if limit, err = parseLimit(v); err != nil {
				clobError(w, http.StatusBadRequest, err.Error())
				return
			}
		}
		page := clobBlockPage{Limit: limit, OrdersAfter: -1, CancelsAfter: -1, FillsAfter: -1}
		for _, c := range []struct {
			name string
			dst  *int64
		}{{"orders_after", &page.OrdersAfter}, {"cancels_after", &page.CancelsAfter}, {"fills_after", &page.FillsAfter}} {
			if v := q.Get(c.name); v != "" {
				n, err := strconv.ParseUint(v, 10, 32)
				if err != nil {
					clobError(w, http.StatusBadRequest, c.name+" must be a non-negative integer")
					return
				}
				*c.dst = int64(n)
			}
		}
		blk, missing, err := a.store.Block(ctx, h, page)
		if err != nil {
			a.fail(w, r, err)
			return
		}
		if blk == nil {
			body := map[string]any{"error": "block not in CLOB archive"}
			if missing != nil {
				body["gap"] = missing.Gap
				body["latest_loaded_height"] = missing.LatestLoaded
				body["not_loaded_yet"] = missing.BeyondCheckpoint
			}
			writeJSON(w, http.StatusNotFound, body)
			return
		}
		writeJSON(w, http.StatusOK, blk)

	default:
		clobError(w, http.StatusNotFound, "unknown CLOB endpoint")
	}
}

func traderFillPos(seq uint32, role string) uint64 {
	p := uint64(seq) * 2
	if role == "taker" {
		p++
	}
	return p
}
