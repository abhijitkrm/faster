// Package synth generates deterministic, contract-shaped CLOB archive data
// (evm-node docs/clob-archive-rpc.md) and serves it as a fake node JSON-RPC
// endpoint. Used by the archiver's tests and by cmd/fakenode for local
// end-to-end runs; it is not part of the production path.
package synth

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"sync"
	"sync/atomic"
)

type Order struct {
	Hash          string `json:"hash"`
	Trader        string `json:"trader"`
	PairID        uint32 `json:"pair_id"`
	ClientOrderID uint64 `json:"client_order_id"`
	OrderID       uint64 `json:"order_id"`
	IsBuy         bool   `json:"is_buy"`
	IsMarket      bool   `json:"is_market"`
	Price         uint64 `json:"price"`
	Amount        uint64 `json:"amount"`
	TimestampNs   int64  `json:"timestamp_ns"`
	Status        string `json:"status"`
	Filled        uint64 `json:"filled"`
	Reason        string `json:"reason"`
}

type Cancel struct {
	Kind          string `json:"kind"`
	Hash          string `json:"hash"`
	Trader        string `json:"trader"`
	PairID        uint32 `json:"pair_id"`
	ClientID      uint64 `json:"client_id"`
	TargetOrderID uint64 `json:"target_order_id"`
	TargetClient  uint64 `json:"target_client"`
	Price         uint64 `json:"price"`
	Amount        uint64 `json:"amount"`
	Status        string `json:"status"`
	Reason        string `json:"reason"`
}

type Fill struct {
	Seq          uint32 `json:"seq"`
	PairID       uint32 `json:"pair_id"`
	Maker        string `json:"maker"`
	Taker        string `json:"taker"`
	TakerIsBuy   bool   `json:"taker_is_buy"`
	MakerOrderID uint64 `json:"maker_order_id"`
	TakerOrderID uint64 `json:"taker_order_id"`
	Price        uint64 `json:"price"`
	Qty          uint64 `json:"qty"`
}

type Block struct {
	Height  uint64   `json:"height"`
	TimeNs  int64    `json:"time_ns"`
	Orders  []Order  `json:"orders"`
	Cancels []Cancel `json:"cancels"`
	Fills   []Fill   `json:"fills"`
}

// BaseTimeNs is the time of height 0; blocks are 500ms apart, so a few
// hundred thousand heights span several day partitions.
const BaseTimeNs int64 = 1_792_600_000_000_000_000

// BlockIntervalNs is the synthetic block time.
const BlockIntervalNs int64 = 500_000_000

// NumTraders is the size of the synthetic trader set.
const NumTraders = 20

func hash32(parts ...uint64) string {
	h := sha256.New()
	var b [8]byte
	for _, p := range parts {
		binary.BigEndian.PutUint64(b[:], p)
		h.Write(b[:])
	}
	return "0x" + hex.EncodeToString(h.Sum(nil))
}

// Trader returns synthetic trader i's address.
func Trader(i int) string { return "0x" + hash32(0xadd, uint64(i))[2:42] }

// OrderID is the chain order ID of the i-th order placed at height h.
func OrderID(h uint64, i int) uint64 { return h*100 + uint64(i) + 1 }

// Gen builds height h's archive. Shape per height:
//   - h%7 orders, statuses cycling open/partial/filled/rejected (rejected
//     orders have order_id 0 and a reason)
//   - one fill per partial/filled order, against order 0 of height h-1
//     (resting maker), so traders appear as both maker and taker
//   - a cancel every 3rd height, an amend every 5th, a rejected cancel every
//     11th
//   - every 50th height a duplicate submission of order 0 (same hash,
//     rejected), to exercise position-based identity
//   - every 97th height an amount above 2^53 (JSON number precision check)
func Gen(h uint64) *Block {
	b := &Block{Height: h, TimeNs: BaseTimeNs + int64(h)*BlockIntervalNs, Orders: []Order{}, Cancels: []Cancel{}, Fills: []Fill{}}
	n := int(h % 7)
	statuses := []string{"open", "partial", "filled", "rejected"}
	for i := 0; i < n; i++ {
		st := statuses[(int(h)+i)%4]
		o := Order{
			Hash:          hash32(1, h, uint64(i)),
			Trader:        Trader(int(h*7+uint64(i)) % NumTraders),
			PairID:        uint32(1 + (h+uint64(i))%3),
			ClientOrderID: h*10 + uint64(i),
			OrderID:       OrderID(h, i),
			IsBuy:         (h+uint64(i))%2 == 0,
			IsMarket:      i == 5,
			Price:         1_000_000 + (h%100)*10,
			Amount:        5 + uint64(i),
			TimestampNs:   b.TimeNs - 250_000_000,
			Status:        st,
		}
		if h%97 == 0 && i == 0 {
			o.Amount = 1<<62 + h // > 2^53: must survive as an exact integer
		}
		switch st {
		case "partial":
			o.Filled = 2
		case "filled":
			o.Filled = o.Amount
		case "rejected":
			o.OrderID = 0
			o.Reason = "insufficient balance"
		}
		b.Orders = append(b.Orders, o)
		if o.Filled > 0 && h > 1 {
			b.Fills = append(b.Fills, Fill{
				Seq:          uint32(len(b.Fills)),
				PairID:       o.PairID,
				Maker:        Trader(int((h-1)*7) % NumTraders),
				Taker:        o.Trader,
				TakerIsBuy:   o.IsBuy,
				MakerOrderID: OrderID(h-1, 0),
				TakerOrderID: o.OrderID,
				Price:        o.Price,
				Qty:          o.Filled,
			})
		}
	}
	if h%50 == 0 && n > 0 {
		dup := b.Orders[0]
		dup.Status, dup.OrderID, dup.Filled, dup.Reason = "rejected", 0, 0, "duplicate order"
		b.Orders = append(b.Orders, dup)
	}
	if h%3 == 0 && h > 1 {
		b.Cancels = append(b.Cancels, Cancel{
			Kind: "cancel", Hash: hash32(2, h), Trader: Trader(int((h-1)*7) % NumTraders), PairID: uint32(1 + (h-1)%3),
			ClientID: h, TargetOrderID: OrderID(h-1, 0), Status: "cancelled",
		})
	}
	if h%5 == 0 && h > 1 {
		b.Cancels = append(b.Cancels, Cancel{
			Kind: "amend", Hash: hash32(3, h), Trader: Trader(int((h-2)*7) % NumTraders), PairID: uint32(1 + (h-2)%3),
			ClientID: h + 1, TargetClient: (h - 2) * 10, Price: 999_000, Amount: 3, Status: "amended",
		})
	}
	if h%11 == 0 {
		b.Cancels = append(b.Cancels, Cancel{
			Kind: "cancel", Hash: hash32(4, h), Trader: Trader(3), PairID: 1,
			ClientID: h + 2, TargetOrderID: 42, Status: "rejected", Reason: "order not found",
		})
	}
	return b
}

// Node is a fake node serving clob_archiveLatest / clob_getBlockArchive for
// heights [Oldest, Latest], returning null for Gaps.
type Node struct {
	mu     sync.Mutex
	oldest uint64
	latest uint64
	gaps   map[uint64]bool
	// Calls counts clob_getBlockArchive requests.
	Calls atomic.Int64
	// FailEvery makes every n-th clob_getBlockArchive call fail with a
	// JSON-RPC error (0 = never), to exercise retries.
	FailEvery int64
	Disabled  bool
}

func NewNode(oldest, latest uint64, gaps ...uint64) *Node {
	n := &Node{oldest: oldest, latest: latest, gaps: map[uint64]bool{}}
	for _, g := range gaps {
		n.gaps[g] = true
	}
	return n
}

// SetLatest moves the tip.
func (n *Node) SetLatest(h uint64) { n.mu.Lock(); n.latest = h; n.mu.Unlock() }

// Range returns [oldest, latest].
func (n *Node) Range() (uint64, uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.oldest, n.latest
}

func (n *Node) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ID     json.RawMessage `json:"id"`
		Method string          `json:"method"`
		Params []uint64        `json:"params"`
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	resp := map[string]any{"jsonrpc": "2.0", "id": req.ID}
	oldest, latest := n.Range()
	switch req.Method {
	case "clob_archiveLatest":
		if n.Disabled {
			resp["result"] = map[string]any{"enabled": false, "oldest": 0, "latest": 0}
		} else {
			resp["result"] = map[string]any{"enabled": true, "oldest": oldest, "latest": latest}
		}
	case "clob_getBlockArchive":
		c := n.Calls.Add(1)
		if len(req.Params) != 1 {
			resp["error"] = map[string]any{"code": -32602, "message": "want [height]"}
			break
		}
		if n.FailEvery > 0 && c%n.FailEvery == 0 {
			resp["error"] = map[string]any{"code": -32000, "message": "injected failure"}
			break
		}
		h := req.Params[0]
		if h < oldest || h > latest || n.gaps[h] {
			resp["result"] = nil
		} else {
			resp["result"] = Gen(h)
		}
	default:
		resp["error"] = map[string]any{"code": -32601, "message": "method not found"}
	}
	_ = json.NewEncoder(w).Encode(resp)
}
