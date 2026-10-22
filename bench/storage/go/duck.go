package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/binary"
	"os"
	"path/filepath"

	"github.com/marcboeker/go-duckdb/v2"
)

// DuckDB: embedded columnar SQL. ART index on hash and on (address, block, idx).
type duckEng struct {
	db   *sql.DB
	conn *duckdb.Connector
	path string
	stmtTx, stmtPage, stmtPageB, stmtBlocks, stmtScan *sql.Stmt
}

func newDuck(path string) *duckEng {
	os.RemoveAll(path)
	os.MkdirAll(path, 0o755)
	f := filepath.Join(path, "bench.duckdb")
	c, err := duckdb.NewConnector(f+"?threads=8&memory_limit=8GB", nil)
	must(err)
	db := sql.OpenDB(c)
	db.SetMaxOpenConns(16)
	for _, q := range []string{
		`CREATE TABLE txs (block_number UBIGINT, tx_index UINTEGER, hash BLOB, from_address BLOB, to_address BLOB, payload BLOB, PRIMARY KEY (block_number, tx_index))`,
		`CREATE TABLE addr_txs (address BLOB, block_number UBIGINT, tx_index UINTEGER)`,
		`CREATE TABLE blocks (number UBIGINT PRIMARY KEY, header BLOB)`,
	} {
		_, err := db.Exec(q)
		must(err)
	}
	return &duckEng{db: db, conn: c, path: path}
}
func (e *duckEng) name() string { return "duckdb" }
func (e *duckEng) load(batch []tx) error {
	conn, err := e.conn.Connect(context.Background())
	if err != nil {
		return err
	}
	defer conn.Close()
	at, err := duckdb.NewAppenderFromConn(conn, "", "txs")
	if err != nil {
		return err
	}
	aa, _ := duckdb.NewAppenderFromConn(conn, "", "addr_txs")
	ab, _ := duckdb.NewAppenderFromConn(conn, "", "blocks")
	var last uint64 = 1<<64 - 1
	for _, x := range batch {
		if err := at.AppendRow(x.block, x.idx, x.hash[:], addrBytes(x.from), addrBytes(x.to), x.payload); err != nil {
			return err
		}
		aa.AppendRow(addrBytes(x.from), x.block, x.idx)
		if x.to != x.from {
			aa.AppendRow(addrBytes(x.to), x.block, x.idx)
		}
		if x.block != last {
			ab.AppendRow(x.block, x.payload[:120])
			last = x.block
		}
	}
	for _, a := range []*duckdb.Appender{at, aa, ab} {
		if err := a.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Indexes are built after loading (DuckDB's recommended bulk path); point
// queries then use ART lookups.
func (e *duckEng) finish() {
	for _, q := range []string{
		`CREATE INDEX txs_hash ON txs (hash)`,
		`CREATE INDEX addr_idx ON addr_txs (address, block_number, tx_index)`,
		`CHECKPOINT`,
	} {
		_, err := e.db.Exec(q)
		must(err)
	}
	var err error
	e.stmtTx, err = e.db.Prepare(`SELECT payload FROM txs WHERE hash = ?`)
	must(err)
	e.stmtPage, err = e.db.Prepare(`SELECT t.payload FROM txs t JOIN (SELECT block_number, tx_index FROM addr_txs WHERE address = ? ORDER BY block_number DESC, tx_index DESC LIMIT ?) a USING (block_number, tx_index)`)
	must(err)
	e.stmtPageB, err = e.db.Prepare(`SELECT t.payload FROM txs t JOIN (SELECT block_number, tx_index FROM addr_txs WHERE address = ? AND (block_number, tx_index) < (?, ?) ORDER BY block_number DESC, tx_index DESC LIMIT ?) a USING (block_number, tx_index)`)
	must(err)
	e.stmtBlocks, err = e.db.Prepare(`SELECT number, header FROM blocks ORDER BY number DESC LIMIT ?`)
	must(err)
	e.stmtScan, err = e.db.Prepare(`SELECT count(*) FROM txs WHERE block_number >= ? AND block_number < ? AND to_address = ?`)
	must(err)
}
func (e *duckEng) getTx(hash []byte) ([]byte, error) {
	var p []byte
	err := e.stmtTx.QueryRow(hash).Scan(&p)
	return p, err
}
func countRows(rows *sql.Rows, err error) (int, error) {
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
func (e *duckEng) addrPage(addr, before []byte, n int) (int, error) {
	if before == nil {
		return countRows(e.stmtPage.Query(addr, n))
	}
	return countRows(e.stmtPageB.Query(addr, binary.BigEndian.Uint64(before), binary.BigEndian.Uint32(before[8:]), n))
}
func (e *duckEng) latestBlocks(n int) (int, error) { return countRows(e.stmtBlocks.Query(n)) }
func (e *duckEng) scanTo(addr []byte, from, to uint64) (int, error) {
	var n int
	err := e.stmtScan.QueryRow(from, to, addr).Scan(&n)
	return n, err
}
func (e *duckEng) agg() (int, error) {
	return countRows(e.db.Query(`SELECT block_number // 10000, count(*) FROM txs GROUP BY 1`))
}
func (e *duckEng) size() int64 { return dirSize(e.path) }
func (e *duckEng) close()      { e.db.Close() }

var _ driver.Valuer = nil
