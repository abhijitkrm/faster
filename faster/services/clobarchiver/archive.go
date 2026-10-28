package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
)

// Wire types of the node's clob JSON-RPC namespace (evm-node
// docs/clob-archive-rpc.md). All integers are JSON numbers in uint64/int64
// range; encoding/json parses them straight into the integer field types, so
// no precision is lost through float64.

// ArchiveInfo is the clob_archiveLatest result.
type ArchiveInfo struct {
	Enabled bool   `json:"enabled"`
	Oldest  uint64 `json:"oldest"`
	Latest  uint64 `json:"latest"`
}

// Hash32 is a 0x-prefixed 32-byte hex value.
type Hash32 [32]byte

// Addr is a 0x-prefixed 20-byte hex value.
type Addr [20]byte

func (h *Hash32) UnmarshalJSON(b []byte) error { return decodeHexJSON(b, h[:], "hash") }
func (a *Addr) UnmarshalJSON(b []byte) error   { return decodeHexJSON(b, a[:], "address") }

func (h Hash32) String() string { return "0x" + hex.EncodeToString(h[:]) }
func (a Addr) String() string   { return "0x" + hex.EncodeToString(a[:]) }

func decodeHexJSON(b []byte, dst []byte, what string) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("%s: %w", what, err)
	}
	if !strings.HasPrefix(s, "0x") && !strings.HasPrefix(s, "0X") {
		return fmt.Errorf("%s %q: missing 0x prefix", what, s)
	}
	s = s[2:]
	if len(s) != 2*len(dst) {
		return fmt.Errorf("%s: want %d hex chars, got %d", what, 2*len(dst), len(s))
	}
	if _, err := hex.Decode(dst, []byte(s)); err != nil {
		return fmt.Errorf("%s: %w", what, err)
	}
	return nil
}

// Order is one executed placement.
type Order struct {
	Hash          Hash32 `json:"hash"`
	Trader        Addr   `json:"trader"`
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

// Cancel is one executed cancel or amend.
type Cancel struct {
	Kind          string `json:"kind"`
	Hash          Hash32 `json:"hash"`
	Trader        Addr   `json:"trader"`
	PairID        uint32 `json:"pair_id"`
	ClientID      uint64 `json:"client_id"`
	TargetOrderID uint64 `json:"target_order_id"`
	TargetClient  uint64 `json:"target_client"`
	Price         uint64 `json:"price"`
	Amount        uint64 `json:"amount"`
	Status        string `json:"status"`
	Reason        string `json:"reason"`
}

// Fill is one match; Seq is its index within the block.
type Fill struct {
	Seq          uint32 `json:"seq"`
	PairID       uint32 `json:"pair_id"`
	Maker        Addr   `json:"maker"`
	Taker        Addr   `json:"taker"`
	TakerIsBuy   bool   `json:"taker_is_buy"`
	MakerOrderID uint64 `json:"maker_order_id"`
	TakerOrderID uint64 `json:"taker_order_id"`
	Price        uint64 `json:"price"`
	Qty          uint64 `json:"qty"`
}

// Block is the clob_getBlockArchive result for one height.
type Block struct {
	Height  uint64   `json:"height"`
	TimeNs  int64    `json:"time_ns"`
	Orders  []Order  `json:"orders"`
	Cancels []Cancel `json:"cancels"`
	Fills   []Fill   `json:"fills"`
}

var (
	orderStatuses = map[string]bool{"open": true, "partial": true, "filled": true, "rejected": true}
	cancelStatus  = map[string]bool{"cancelled": true, "amended": true, "rejected": true}
)

// Rows is the number of rows the block sends to ClickHouse (its blocks row
// plus orders, cancels and fills; materialized views add the re-sorted
// copies server side). Used to size insert batches.
func (b *Block) Rows() int {
	return 1 + len(b.Orders) + len(b.Cancels) + len(b.Fills)
}

// Validate checks enum values so a contract change fails loudly instead of
// writing garbage (ClickHouse would reject unknown Enum8 names anyway, but
// only for the whole batch).
func (b *Block) Validate(want uint64) error {
	if b.Height != want {
		return fmt.Errorf("archive for height %d returned height %d", want, b.Height)
	}
	for i := range b.Orders {
		if !orderStatuses[b.Orders[i].Status] {
			return fmt.Errorf("height %d order %d: unknown status %q", want, i, b.Orders[i].Status)
		}
	}
	for i := range b.Cancels {
		c := &b.Cancels[i]
		if c.Kind != "cancel" && c.Kind != "amend" {
			return fmt.Errorf("height %d cancel %d: unknown kind %q", want, i, c.Kind)
		}
		if !cancelStatus[c.Status] {
			return fmt.Errorf("height %d cancel %d: unknown status %q", want, i, c.Status)
		}
	}
	return nil
}

// decodeBlock parses a clob_getBlockArchive result. A JSON null means the node
// does not hold the height: (nil, nil).
func decodeBlock(raw json.RawMessage, want uint64) (*Block, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	var b Block
	if err := json.Unmarshal(raw, &b); err != nil {
		return nil, fmt.Errorf("decode archive %d: %w", want, err)
	}
	if err := b.Validate(want); err != nil {
		return nil, err
	}
	return &b, nil
}

func jsonMarshal(v any) (json.RawMessage, error) { return json.Marshal(v) }
