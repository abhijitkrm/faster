package main

import (
	"database/sql"
	"encoding/binary"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

// TiDB (single-machine playground: 1 PD, 1 TiKV, 1 TiDB). Clustered primary
// keys; multi-row INSERTs (TiDB has no COPY).
type tidbEng struct {
	db                                      *sql.DB
	stTx, stPage, stPageB, stBlocks, stScan *sql.Stmt
}

func newTiDB() *tidbEng {
	root, err := sql.Open("mysql", "root@tcp(127.0.0.1:4000)/")
	must(err)
	for _, q := range []string{`DROP DATABASE IF EXISTS bench`, `CREATE DATABASE bench`} {
		_, err := root.Exec(q)
		must(err)
	}
	root.Close()
	db, err := sql.Open("mysql", "root@tcp(127.0.0.1:4000)/bench?interpolateParams=false")
	must(err)
	db.SetMaxOpenConns(16)
	db.SetMaxIdleConns(16)
	for _, q := range []string{
		`CREATE TABLE txs (block_number BIGINT UNSIGNED, tx_index INT UNSIGNED, hash VARBINARY(32) NOT NULL, from_address VARBINARY(20), to_address VARBINARY(20), payload BLOB, PRIMARY KEY (block_number, tx_index) CLUSTERED, KEY txs_hash (hash))`,
		`CREATE TABLE addr_txs (address VARBINARY(20), block_number BIGINT UNSIGNED, tx_index INT UNSIGNED, PRIMARY KEY (address, block_number, tx_index) CLUSTERED)`,
		`CREATE TABLE blocks (number BIGINT UNSIGNED PRIMARY KEY CLUSTERED, header BLOB)`,
	} {
		_, err := db.Exec(q)
		must(err)
	}
	e := &tidbEng{db: db}
	prep := func(q string) *sql.Stmt { s, err := db.Prepare(q); must(err); return s }
	e.stTx = prep(`SELECT payload FROM txs WHERE hash = ?`)
	e.stPage = prep(`SELECT t.payload FROM addr_txs a JOIN txs t ON t.block_number = a.block_number AND t.tx_index = a.tx_index WHERE a.address = ? ORDER BY a.block_number DESC, a.tx_index DESC LIMIT ?`)
	e.stPageB = prep(`SELECT t.payload FROM addr_txs a JOIN txs t ON t.block_number = a.block_number AND t.tx_index = a.tx_index WHERE a.address = ? AND (a.block_number, a.tx_index) < (?, ?) ORDER BY a.block_number DESC, a.tx_index DESC LIMIT ?`)
	e.stBlocks = prep(`SELECT number, header FROM blocks ORDER BY number DESC LIMIT ?`)
	e.stScan = prep(`SELECT count(*) FROM txs WHERE block_number >= ? AND block_number < ? AND to_address = ?`)
	return e
}
func (e *tidbEng) name() string { return "tidb" }

func multiInsert(t *sql.Tx, table string, cols int, rows [][]any) error {
	const chunk = 500
	for i := 0; i < len(rows); i += chunk {
		end := min(i+chunk, len(rows))
		ph := "(" + strings.TrimSuffix(strings.Repeat("?,", cols), ",") + ")"
		q := "INSERT IGNORE INTO " + table + " VALUES " + strings.TrimSuffix(strings.Repeat(ph+",", end-i), ",")
		args := make([]any, 0, (end-i)*cols)
		for _, r := range rows[i:end] {
			args = append(args, r...)
		}
		if _, err := t.Exec(q, args...); err != nil {
			return err
		}
	}
	return nil
}

func (e *tidbEng) load(batch []tx) error {
	var txr, ar, br [][]any
	var last uint64 = 1<<64 - 1
	for _, x := range batch {
		txr = append(txr, []any{x.block, x.idx, x.hash[:], addrBytes(x.from), addrBytes(x.to), x.payload})
		ar = append(ar, []any{addrBytes(x.from), x.block, x.idx})
		if x.to != x.from {
			ar = append(ar, []any{addrBytes(x.to), x.block, x.idx})
		}
		if x.block != last {
			br = append(br, []any{x.block, x.payload[:120]})
			last = x.block
		}
	}
	t, err := e.db.Begin()
	if err != nil {
		return err
	}
	defer t.Rollback()
	if err := multiInsert(t, "txs", 6, txr); err != nil {
		return err
	}
	if err := multiInsert(t, "addr_txs", 3, ar); err != nil {
		return err
	}
	if err := multiInsert(t, "blocks", 2, br); err != nil {
		return err
	}
	return t.Commit()
}
func (e *tidbEng) finish() {
	for _, q := range []string{`ANALYZE TABLE txs`, `ANALYZE TABLE addr_txs`, `ANALYZE TABLE blocks`} {
		_, err := e.db.Exec(q)
		must(err)
	}
}
func (e *tidbEng) getTx(hash []byte) ([]byte, error) {
	var p []byte
	err := e.stTx.QueryRow(hash).Scan(&p)
	return p, err
}
func (e *tidbEng) addrPage(addr, before []byte, n int) (int, error) {
	if before == nil {
		return countRows(e.stPage.Query(addr, n))
	}
	return countRows(e.stPageB.Query(addr, binary.BigEndian.Uint64(before), binary.BigEndian.Uint32(before[8:]), n))
}
func (e *tidbEng) latestBlocks(n int) (int, error) { return countRows(e.stBlocks.Query(n)) }
func (e *tidbEng) scanTo(addr []byte, from, to uint64) (int, error) {
	var n int
	err := e.stScan.QueryRow(from, to, addr).Scan(&n)
	return n, err
}
func (e *tidbEng) agg() (int, error) {
	return countRows(e.db.Query(`SELECT block_number DIV 10000, count(*) FROM txs GROUP BY 1`))
}
func (e *tidbEng) size() int64 {
	var s sql.NullInt64
	e.db.QueryRow(`SELECT sum(data_length + index_length) FROM information_schema.tables WHERE table_schema = 'bench'`).Scan(&s)
	return s.Int64
}
func (e *tidbEng) close() { e.db.Close() }
