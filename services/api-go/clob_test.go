package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func init() { slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil))) }

type fakeClobStore struct {
	err        error
	lastBefore clobCursor
	lastLimit  int
	lastTrader []byte
	lastPair   uint32
	orders     []clobOrder
	fills      []clobTraderFill
	item       *clobItemResponse
	block      *clobBlockResponse
	missing    *clobBlockMissing
}

func (f *fakeClobStore) ItemByHash(_ context.Context, h []byte) (*clobItemResponse, error) {
	return f.item, f.err
}
func (f *fakeClobStore) TraderOrders(_ context.Context, t []byte, b clobCursor, l int) ([]clobOrder, error) {
	f.lastTrader, f.lastBefore, f.lastLimit = t, b, l
	return f.orders, f.err
}
func (f *fakeClobStore) TraderCancels(_ context.Context, t []byte, b clobCursor, l int) ([]clobCancel, error) {
	f.lastTrader, f.lastBefore, f.lastLimit = t, b, l
	return []clobCancel{}, f.err
}
func (f *fakeClobStore) TraderFills(_ context.Context, t []byte, b clobCursor, l int) ([]clobTraderFill, error) {
	f.lastTrader, f.lastBefore, f.lastLimit = t, b, l
	return f.fills, f.err
}
func (f *fakeClobStore) PairTrades(_ context.Context, p uint32, b clobCursor, l int) ([]clobFill, error) {
	f.lastPair, f.lastBefore, f.lastLimit = p, b, l
	return []clobFill{}, f.err
}
func (f *fakeClobStore) Block(_ context.Context, h uint64, p clobBlockPage) (*clobBlockResponse, *clobBlockMissing, error) {
	f.lastLimit = p.Limit
	return f.block, f.missing, f.err
}
func (f *fakeClobStore) Stats(context.Context) (*clobStats, error) {
	return &clobStats{LatestLoadedHeight: 9, Orders: "18446744073709551615"}, f.err
}

const (
	tHash = "0x1111111111111111111111111111111111111111111111111111111111111111"
	tAddr = "0xAAaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
)

func do(t *testing.T, api *clobAPI, method, path string) (int, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(method, path, nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("%s: content-type %q", path, ct)
	}
	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("%s: body %q: %v", path, w.Body.String(), err)
	}
	return w.Code, body
}

var allPaths = []string{
	"/api/clob/orders/" + tHash,
	"/api/clob/traders/" + tAddr + "/orders",
	"/api/clob/traders/" + tAddr + "/fills",
	"/api/clob/pairs/3/trades",
	"/api/clob/blocks/10",
	"/api/clob/stats",
}

func TestClobNotConfigured503(t *testing.T) {
	api := &clobAPI{}
	for _, p := range allPaths {
		code, body := do(t, api, "GET", p)
		if code != http.StatusServiceUnavailable || body["error"] == nil {
			t.Errorf("%s: %d %v", p, code, body)
		}
	}
}

func TestClobValidation(t *testing.T) {
	api := &clobAPI{store: &fakeClobStore{}}
	bad := []string{
		"/api/clob/orders/0x1234",
		"/api/clob/orders/" + strings.Replace(tHash, "1", "g", 1),
		"/api/clob/orders/" + tHash[2:],
		"/api/clob/traders/0x12/orders",
		"/api/clob/traders/" + tAddr + "/orders?limit=0",
		"/api/clob/traders/" + tAddr + "/orders?limit=abc",
		"/api/clob/traders/" + tAddr + "/fills?before=1-2-3",
		"/api/clob/traders/" + tAddr + "/fills?before=-5",
		"/api/clob/traders/" + tAddr + "/fills?before=99999999999999999999",
		"/api/clob/pairs/abc/trades",
		"/api/clob/pairs/4294967296/trades",
		"/api/clob/pairs/-1/trades",
		"/api/clob/blocks/abc",
		"/api/clob/blocks/-1",
		"/api/clob/blocks/5?limit=x",
	}
	for _, p := range bad {
		if code, body := do(t, api, "GET", p); code != http.StatusBadRequest || body["error"] == nil {
			t.Errorf("%s: %d %v", p, code, body)
		}
	}
	if code, _ := do(t, api, "GET", "/api/clob/nope"); code != http.StatusNotFound {
		t.Errorf("unknown endpoint: %d", code)
	}
	if code, _ := do(t, api, "POST", "/api/clob/stats"); code != http.StatusMethodNotAllowed {
		t.Errorf("POST: %d", code)
	}
}

func TestClobLimitAndCursor(t *testing.T) {
	st := &fakeClobStore{}
	api := &clobAPI{store: st}

	do(t, api, "GET", "/api/clob/traders/"+tAddr+"/orders")
	if st.lastLimit != clobDefaultLimit || st.lastBefore != clobNoCursor {
		t.Fatalf("defaults: %d %+v", st.lastLimit, st.lastBefore)
	}
	if got := "0x" + string(hexLower(st.lastTrader)); got != strings.ToLower(tAddr) {
		t.Fatalf("trader %s", got)
	}
	do(t, api, "GET", "/api/clob/traders/"+tAddr+"/orders?limit=100000&before=120-3")
	if st.lastLimit != clobMaxLimit || st.lastBefore != (clobCursor{120, 3}) {
		t.Fatalf("cap/cursor: %d %+v", st.lastLimit, st.lastBefore)
	}
	do(t, api, "GET", "/api/clob/pairs/7/trades?before=120&limit=5")
	if st.lastPair != 7 || st.lastLimit != 5 || st.lastBefore != (clobCursor{120, 0}) {
		t.Fatalf("pair: %d %d %+v", st.lastPair, st.lastLimit, st.lastBefore)
	}
	do(t, api, "GET", "/api/clob/blocks/5")
	if st.lastLimit != 100 {
		t.Fatalf("block default limit %d", st.lastLimit)
	}

	// next_before: set when the page is full, from the last item.
	st.orders = []clobOrder{{Height: 10, Idx: 2, Price: "18446744073709551615"}, {Height: 9, Idx: 0}}
	_, body := do(t, api, "GET", "/api/clob/traders/"+tAddr+"/orders?limit=2")
	if body["next_before"] != "9-0" {
		t.Fatalf("next_before = %v", body["next_before"])
	}
	items := body["items"].([]any)
	if items[0].(map[string]any)["price"] != "18446744073709551615" {
		t.Fatalf("price not an exact string: %v", items[0])
	}
	_, body = do(t, api, "GET", "/api/clob/traders/"+tAddr+"/orders?limit=3")
	if body["next_before"] != nil {
		t.Fatalf("short page next_before = %v", body["next_before"])
	}
	st.fills = []clobTraderFill{{Height: 4, Seq: 3, Role: "taker"}}
	_, body = do(t, api, "GET", "/api/clob/traders/"+tAddr+"/fills?limit=1")
	if body["next_before"] != "4-7" {
		t.Fatalf("fills next_before = %v", body["next_before"])
	}
}

func hexLower(b []byte) []byte {
	const digits = "0123456789abcdef"
	out := make([]byte, 0, 2*len(b))
	for _, c := range b {
		out = append(out, digits[c>>4], digits[c&15])
	}
	return out
}

func TestClobNotFoundAndErrors(t *testing.T) {
	st := &fakeClobStore{missing: &clobBlockMissing{Gap: "missing", LatestLoaded: 300}}
	api := &clobAPI{store: st}
	if code, _ := do(t, api, "GET", "/api/clob/orders/"+tHash); code != http.StatusNotFound {
		t.Fatalf("missing hash: %d", code)
	}
	code, body := do(t, api, "GET", "/api/clob/blocks/151")
	if code != http.StatusNotFound || body["gap"] != "missing" || body["latest_loaded_height"] != float64(300) {
		t.Fatalf("gap block: %d %v", code, body)
	}

	st.err = &net.OpError{Op: "dial", Err: errors.New("connection refused")}
	if code, _ := do(t, api, "GET", "/api/clob/stats"); code != http.StatusServiceUnavailable {
		t.Fatalf("unreachable: %d", code)
	}
	st.err = errors.New("code: 60, table does not exist")
	code, body = do(t, api, "GET", "/api/clob/stats")
	if code != http.StatusInternalServerError || strings.Contains(body["error"].(string), "table") {
		t.Fatalf("query error must be generic: %d %v", code, body)
	}
}

func TestClobCursorString(t *testing.T) {
	if (clobCursor{5, 1}).String() != "5-1" {
		t.Fatal()
	}
	c, err := parseCursor("")
	if err != nil || c.H != math.MaxUint64 {
		t.Fatal(c, err)
	}
	if traderFillPos(3, "maker") != 6 || traderFillPos(3, "taker") != 7 {
		t.Fatal("trader fill pos")
	}
}
