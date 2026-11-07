package main

import (
	"database/sql"
	"encoding/binary"
	"os"
	"path/filepath"

	_ "github.com/mattn/go-sqlite3"
)

// SQLite: WAL, synchronous=FULL (durable commits like the other engines),
// WITHOUT ROWID clustered tables, large page cache + mmap.
type sqliteEng struct {
	db                                          *sql.DB
	path                                        string
	stTx, stPage, stPageB, stBlocks, stScan     *sql.Stmt
}

func newSQLite(path string) *sqliteEng {
	os.RemoveAll(path)
	os.MkdirAll(path, 0o755)
	f := filepath.Join(path, "bench.db")
	db, err := sql.Open("sqlite3", "file:"+f+"?_journal_mode=WAL&_synchronous=FULL&_cache_size=-4000000&_busy_timeout=5000")
	must(err)
	db.SetMaxOpenConns(16)
	for _, q := range []string{
		`PRAGMA mmap_size = 34359738368`,
		`CREATE TABLE txs (block_number INTEGER, tx_index INTEGER, hash BLOB NOT NULL, from_address BLOB, to_address BLOB, payload BLOB, PRIMARY KEY (block_number, tx_index)) WITHOUT ROWID`,
		`CREATE INDEX txs_hash ON txs (hash)`,
		`CREATE TABLE addr_txs (address BLOB, block_number INTEGER, tx_index INTEGER, PRIMARY KEY (address, block_number, tx_index)) WITHOUT ROWID`,
		`CREATE TABLE blocks (number INTEGER PRIMARY KEY, header BLOB)`,
	} {
		_, err := db.Exec(q)
		must(err)
	}
	e := &sqliteEng{db: db, path: path}
	prep := func(q string) *sql.Stmt { s, err := db.Prepare(q); must(err); return s }
	e.stTx = prep(`SELECT payload FROM txs WHERE hash = ?`)
	e.stPage = prep(`SELECT t.payload FROM addr_txs a JOIN txs t ON t.block_number = a.block_number AND t.tx_index = a.tx_index WHERE a.address = ? ORDER BY a.block_number DESC, a.tx_index DESC LIMIT ?`)
	e.stPageB = prep(`SELECT t.payload FROM addr_txs a JOIN txs t ON t.block_number = a.block_number AND t.tx_index = a.tx_index WHERE a.address = ? AND (a.block_number, a.tx_index) < (?, ?) ORDER BY a.block_number DESC, a.tx_index DESC LIMIT ?`)
	e.stBlocks = prep(`SELECT number, header FROM blocks ORDER BY number DESC LIMIT ?`)
	e.stScan = prep(`SELECT count(*) FROM txs WHERE block_number >= ? AND block_number < ? AND to_address = ?`)
	return e
}
func (e *sqliteEng) name() string { return "sqlite" }
func (e *sqliteEng) load(batch []tx) error {
	t, err := e.db.Begin()
	if err != nil {
		return err
	}
	defer t.Rollback()
	it, _ := t.Prepare(`INSERT INTO txs VALUES (?, ?, ?, ?, ?, ?)`)
	ia, _ := t.Prepare(`INSERT OR IGNORE INTO addr_txs VALUES (?, ?, ?)`)
	ib, _ := t.Prepare(`INSERT INTO blocks VALUES (?, ?)`)
	var last uint64 = 1<<64 - 1
	for _, x := range batch {
		if _, err := it.Exec(int64(x.block), x.idx, x.hash[:], addrBytes(x.from), addrBytes(x.to), x.payload); err != nil {
			return err
		}
		ia.Exec(addrBytes(x.from), int64(x.block), x.idx)
		if x.to != x.from {
			ia.Exec(addrBytes(x.to), int64(x.block), x.idx)
		}
		if x.block != last {
			ib.Exec(int64(x.block), x.payload[:120])
			last = x.block
		}
	}
	return t.Commit()
}
func (e *sqliteEng) getTx(hash []byte) ([]byte, error) {
	var p []byte
	err := e.stTx.QueryRow(hash).Scan(&p)
	return p, err
}
func (e *sqliteEng) addrPage(addr, before []byte, n int) (int, error) {
	if before == nil {
		return countRows(e.stPage.Query(addr, n))
	}
	return countRows(e.stPageB.Query(addr, int64(binary.BigEndian.Uint64(before)), binary.BigEndian.Uint32(before[8:]), n))
}
func (e *sqliteEng) latestBlocks(n int) (int, error) { return countRows(e.stBlocks.Query(n)) }
func (e *sqliteEng) scanTo(addr []byte, from, to uint64) (int, error) {
	var n int
	err := e.stScan.QueryRow(int64(from), int64(to), addr).Scan(&n)
	return n, err
}
func (e *sqliteEng) agg() (int, error) {
	return countRows(e.db.Query(`SELECT block_number / 10000, count(*) FROM txs GROUP BY 1`))
}
func (e *sqliteEng) size() int64 { return dirSize(e.path) }
func (e *sqliteEng) close()      { e.db.Close() }
