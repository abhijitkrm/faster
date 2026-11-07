package main

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// ClickHouse layout: txs ordered by (block, idx) for range scans; a separate
// hash-ordered lookup table and an address-ordered history table, the usual
// "one table per access path" ClickHouse design.
type chEng struct{ c driver.Conn }

func newCH() *chEng {
	c, err := clickhouse.Open(&clickhouse.Options{Addr: []string{"127.0.0.1:19000"}, MaxOpenConns: 16})
	must(err)
	ctx := context.Background()
	for _, q := range []string{
		`DROP DATABASE IF EXISTS bench`, `CREATE DATABASE bench`,
		`CREATE TABLE bench.txs (block_number UInt64, tx_index UInt32, hash FixedString(32), from_address FixedString(20), to_address FixedString(20), payload String) ENGINE = MergeTree ORDER BY (block_number, tx_index)`,
		`CREATE TABLE bench.tx_by_hash (hash FixedString(32), block_number UInt64, tx_index UInt32) ENGINE = MergeTree ORDER BY hash SETTINGS index_granularity = 256`,
		`CREATE TABLE bench.addr_txs (address FixedString(20), block_number UInt64, tx_index UInt32) ENGINE = MergeTree ORDER BY (address, block_number, tx_index) SETTINGS index_granularity = 256`,
		`CREATE TABLE bench.blocks (number UInt64, header String) ENGINE = MergeTree ORDER BY number`,
	} {
		must(c.Exec(ctx, q))
	}
	return &chEng{c: c}
}
func (e *chEng) name() string { return "clickhouse" }
func (e *chEng) load(batch []tx) error {
	ctx := context.Background()
	b1, err := e.c.PrepareBatch(ctx, "INSERT INTO bench.txs")
	if err != nil {
		return err
	}
	b2, _ := e.c.PrepareBatch(ctx, "INSERT INTO bench.tx_by_hash")
	b3, _ := e.c.PrepareBatch(ctx, "INSERT INTO bench.addr_txs")
	b4, _ := e.c.PrepareBatch(ctx, "INSERT INTO bench.blocks")
	var last uint64 = 1<<64 - 1
	for _, x := range batch {
		if err := b1.Append(x.block, x.idx, string(x.hash[:]), string(addrBytes(x.from)), string(addrBytes(x.to)), string(x.payload)); err != nil {
			return err
		}
		b2.Append(string(x.hash[:]), x.block, x.idx)
		b3.Append(string(addrBytes(x.from)), x.block, x.idx)
		if x.to != x.from {
			b3.Append(string(addrBytes(x.to)), x.block, x.idx)
		}
		if x.block != last {
			b4.Append(x.block, string(x.payload[:120]))
			last = x.block
		}
	}
	for _, b := range []driver.Batch{b1, b2, b3, b4} {
		if err := b.Send(); err != nil {
			return err
		}
	}
	return nil
}
func (e *chEng) getTx(hash []byte) ([]byte, error) {
	var p string
	err := e.c.QueryRow(context.Background(), `SELECT payload FROM bench.txs WHERE (block_number, tx_index) IN
		(SELECT block_number, tx_index FROM bench.tx_by_hash WHERE hash = ?) LIMIT 1`, string(hash)).Scan(&p)
	return []byte(p), err
}
func (e *chEng) addrPage(addr, before []byte, n int) (int, error) {
	q := `SELECT payload FROM bench.txs WHERE (block_number, tx_index) IN (SELECT block_number, tx_index FROM bench.addr_txs WHERE address = ? %s ORDER BY block_number DESC, tx_index DESC LIMIT ?) ORDER BY block_number DESC, tx_index DESC`
	args := []any{string(addr)}
	cond := ""
	if before != nil {
		cond = "AND (block_number, tx_index) < (?, ?)"
		args = append(args, binary.BigEndian.Uint64(before), binary.BigEndian.Uint32(before[8:]))
	}
	args = append(args, n)
	rows, err := e.c.Query(context.Background(), fmt.Sprintf(q, cond), args...)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	cnt := 0
	for rows.Next() {
		cnt++
	}
	return cnt, rows.Err()
}
func (e *chEng) latestBlocks(n int) (int, error) {
	rows, err := e.c.Query(context.Background(), `SELECT number, header FROM bench.blocks ORDER BY number DESC LIMIT ?`, n)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	cnt := 0
	for rows.Next() {
		cnt++
	}
	return cnt, rows.Err()
}
func (e *chEng) scanTo(addr []byte, from, to uint64) (int, error) {
	var n uint64
	err := e.c.QueryRow(context.Background(), `SELECT count() FROM bench.txs WHERE block_number >= ? AND block_number < ? AND to_address = ?`, from, to, string(addr)).Scan(&n)
	return int(n), err
}
func (e *chEng) agg() (int, error) {
	rows, err := e.c.Query(context.Background(), `SELECT intDiv(block_number, 10000) b, count() FROM bench.txs GROUP BY b`)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		n++
	}
	return n, rows.Err()
}
func (e *chEng) size() int64 {
	var s uint64
	e.c.QueryRow(context.Background(), `SELECT sum(bytes_on_disk) FROM system.parts WHERE database = 'bench' AND active`).Scan(&s)
	return int64(s)
}
func (e *chEng) close() {
	e.c.Exec(context.Background(), `OPTIMIZE TABLE bench.txs FINAL`)
	e.c.Close()
}
