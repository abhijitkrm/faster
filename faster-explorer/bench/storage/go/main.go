package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/erigontech/mdbx-go/mdbx"
	ep "github.com/fergusstrange/embedded-postgres"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	nBlocks   = flag.Int("blocks", 500000, "blocks")
	txPer     = flag.Int("txper", 10, "txs per block")
	nAddr     = flag.Int("addrs", 1000000, "addresses")
	payloadSz = flag.Int("payload", 180, "tx payload bytes")
	nOps      = flag.Int("ops", 100000, "ops per query class")
	engines   = flag.String("engines", "mdbx,pebble,pg", "engines")
	dir       = flag.String("dir", "./data", "data dir")
	batchBlk  = 1000
)

type tx struct {
	block   uint64
	idx     uint32
	hash    [32]byte
	from    uint32
	to      uint32
	payload []byte
}

func addrBytes(i uint32) []byte {
	b := make([]byte, 20)
	binary.BigEndian.PutUint32(b[16:], i)
	b[0] = 0xaa
	return b
}

func txKey(b uint64, i uint32) []byte {
	k := make([]byte, 12)
	binary.BigEndian.PutUint64(k, b)
	binary.BigEndian.PutUint32(k[8:], i)
	return k
}

func invKey(b uint64, i uint32) []byte { return txKey(^b, ^i) }

// gen streams the chain block-batch by block-batch; deterministic.
func gen(fn func([]tx)) {
	r := rand.New(rand.NewSource(1))
	z := rand.NewZipf(r, 1.2, 1, uint64(*nAddr-1))
	payload := make([]byte, *payloadSz)
	r.Read(payload)
	batch := make([]tx, 0, batchBlk**txPer)
	for b := 0; b < *nBlocks; b++ {
		for i := 0; i < *txPer; i++ {
			var k [12]byte
			binary.BigEndian.PutUint64(k[:], uint64(b))
			binary.BigEndian.PutUint32(k[8:], uint32(i))
			batch = append(batch, tx{block: uint64(b), idx: uint32(i), hash: sha256.Sum256(k[:]),
				from: uint32(z.Uint64()), to: uint32(z.Uint64()), payload: payload})
		}
		if (b+1)%batchBlk == 0 || b == *nBlocks-1 {
			fn(batch)
			batch = batch[:0]
		}
	}
}

func hashOf(b uint64, i uint32) []byte {
	var k [12]byte
	binary.BigEndian.PutUint64(k[:], b)
	binary.BigEndian.PutUint32(k[8:], i)
	h := sha256.Sum256(k[:])
	return h[:]
}

type engine interface {
	name() string
	load(batch []tx) error
	getTx(hash []byte) ([]byte, error)
	addrPage(addr []byte, before []byte, n int) (int, error) // before: 12-byte (block,idx) or nil
	latestBlocks(n int) (int, error)
	scanTo(addr []byte, from, to uint64) (int, error) // filtered range scan (getLogs-like)
	agg() (int, error)                                // tx count per 10k-block bucket, whole history
	size() int64
	close()
}

// ---------------- MDBX ----------------
type mdbxEng struct {
	env                   *mdbx.Env
	txs, hashes, addr, bl mdbx.DBI
	path                  string
}

func newMDBX(path string) *mdbxEng {
	os.RemoveAll(path)
	os.MkdirAll(path, 0o755)
	env, err := mdbx.NewEnv(mdbx.Label("bench"))
	must(err)
	must(env.SetOption(mdbx.OptMaxDB, 16))
	must(env.SetGeometry(-1, -1, 64<<30, 1<<30, -1, 4096))
	must(env.Open(path, mdbx.NoReadahead|mdbx.Durable, 0o644))
	e := &mdbxEng{env: env, path: path}
	must(env.Update(func(t *mdbx.Txn) error {
		var err error
		if e.txs, err = t.OpenDBISimple("txs", mdbx.Create); err != nil {
			return err
		}
		if e.hashes, err = t.OpenDBISimple("hashes", mdbx.Create); err != nil {
			return err
		}
		if e.addr, err = t.OpenDBISimple("addr", mdbx.Create|mdbx.DupSort|mdbx.DupFixed); err != nil {
			return err
		}
		e.bl, err = t.OpenDBISimple("blocks", mdbx.Create)
		return err
	}))
	return e
}
func (e *mdbxEng) name() string { return "mdbx" }
func (e *mdbxEng) load(batch []tx) error {
	return e.env.Update(func(t *mdbx.Txn) error {
		var last uint64 = 1<<64 - 1
		for _, x := range batch {
			k := txKey(x.block, x.idx)
			v := append(append(addrBytes(x.from), addrBytes(x.to)...), x.payload...)
			if err := t.Put(e.txs, k, v, mdbx.Append); err != nil {
				return err
			}
			if err := t.Put(e.hashes, x.hash[:], k, 0); err != nil {
				return err
			}
			ik := invKey(x.block, x.idx)
			if err := t.Put(e.addr, addrBytes(x.from), ik, 0); err != nil {
				return err
			}
			if x.to != x.from {
				if err := t.Put(e.addr, addrBytes(x.to), ik, 0); err != nil {
					return err
				}
			}
			if x.block != last {
				bk := make([]byte, 8)
				binary.BigEndian.PutUint64(bk, x.block)
				if err := t.Put(e.bl, bk, x.payload[:120], mdbx.Append); err != nil {
					return err
				}
				last = x.block
			}
		}
		return nil
	})
}
func (e *mdbxEng) getTx(hash []byte) ([]byte, error) {
	var out []byte
	err := e.env.View(func(t *mdbx.Txn) error {
		k, err := t.Get(e.hashes, hash)
		if err != nil {
			return err
		}
		v, err := t.Get(e.txs, k)
		out = v[:8] // zero-copy view; touch only
		return err
	})
	return out, err
}
func (e *mdbxEng) addrPage(addr, before []byte, n int) (int, error) {
	cnt := 0
	err := e.env.View(func(t *mdbx.Txn) error {
		c, err := t.OpenCursor(e.addr)
		if err != nil {
			return err
		}
		defer c.Close()
		var v []byte
		if before == nil {
			_, v, err = c.Get(addr, nil, mdbx.Set)
		} else {
			inv := invKey(binary.BigEndian.Uint64(before), binary.BigEndian.Uint32(before[8:]))
			_, v, err = c.Get(addr, inv, mdbx.GetBothRange)
			if err == nil && string(v) == string(inv) {
				_, v, err = c.Get(nil, nil, mdbx.NextDup)
			}
		}
		for err == nil && cnt < n {
			k := txKey(^binary.BigEndian.Uint64(v), ^binary.BigEndian.Uint32(v[8:]))
			if _, err = t.Get(e.txs, k); err != nil {
				return err
			}
			cnt++
			_, v, err = c.Get(nil, nil, mdbx.NextDup)
		}
		if mdbx.IsNotFound(err) {
			return nil
		}
		return err
	})
	return cnt, err
}
func (e *mdbxEng) latestBlocks(n int) (int, error) {
	cnt := 0
	err := e.env.View(func(t *mdbx.Txn) error {
		c, err := t.OpenCursor(e.bl)
		if err != nil {
			return err
		}
		defer c.Close()
		_, _, err = c.Get(nil, nil, mdbx.Last)
		for err == nil && cnt < n {
			cnt++
			_, _, err = c.Get(nil, nil, mdbx.Prev)
		}
		return nil
	})
	return cnt, err
}
func (e *mdbxEng) scanTo(addr []byte, from, to uint64) (int, error) {
	cnt := 0
	err := e.env.View(func(t *mdbx.Txn) error {
		c, err := t.OpenCursor(e.txs)
		if err != nil {
			return err
		}
		defer c.Close()
		k, v, err := c.Get(txKey(from, 0), nil, mdbx.SetRange)
		for err == nil && binary.BigEndian.Uint64(k) < to {
			if string(v[20:40]) == string(addr) {
				cnt++
			}
			k, v, err = c.Get(nil, nil, mdbx.Next)
		}
		return nil
	})
	return cnt, err
}
func (e *mdbxEng) agg() (int, error) {
	buckets := map[uint64]int{}
	err := e.env.View(func(t *mdbx.Txn) error {
		c, err := t.OpenCursor(e.txs)
		if err != nil {
			return err
		}
		defer c.Close()
		k, _, err := c.Get(nil, nil, mdbx.First)
		for err == nil {
			buckets[binary.BigEndian.Uint64(k)/10000]++
			k, _, err = c.Get(nil, nil, mdbx.Next)
		}
		return nil
	})
	return len(buckets), err
}
func (e *mdbxEng) size() int64 { return dirSize(e.path) }
func (e *mdbxEng) close()      { e.env.Close() }

// ---------------- Pebble ----------------
type pebEng struct {
	db   *pebble.DB
	path string
}

func newPebble(path string) *pebEng {
	os.RemoveAll(path)
	db, err := pebble.Open(path, &pebble.Options{Cache: pebble.NewCache(4 << 30), MemTableSize: 256 << 20})
	must(err)
	return &pebEng{db: db, path: path}
}
func (e *pebEng) name() string { return "pebble" }
func (e *pebEng) load(batch []tx) error {
	b := e.db.NewBatch()
	var last uint64 = 1<<64 - 1
	for _, x := range batch {
		k := txKey(x.block, x.idx)
		v := append(append(addrBytes(x.from), addrBytes(x.to)...), x.payload...)
		b.Set(append([]byte{'t'}, k...), v, nil)
		b.Set(append([]byte{'h'}, x.hash[:]...), k, nil)
		ik := invKey(x.block, x.idx)
		b.Set(append(append([]byte{'a'}, addrBytes(x.from)...), ik...), nil, nil)
		if x.to != x.from {
			b.Set(append(append([]byte{'a'}, addrBytes(x.to)...), ik...), nil, nil)
		}
		if x.block != last {
			bk := make([]byte, 9)
			bk[0] = 'b'
			binary.BigEndian.PutUint64(bk[1:], x.block)
			b.Set(bk, x.payload[:120], nil)
			last = x.block
		}
	}
	return b.Commit(pebble.Sync)
}
func (e *pebEng) getTx(hash []byte) ([]byte, error) {
	k, c, err := e.db.Get(append([]byte{'h'}, hash...))
	if err != nil {
		return nil, err
	}
	tk := append([]byte{'t'}, k...)
	c.Close()
	v, c2, err := e.db.Get(tk)
	if err != nil {
		return nil, err
	}
	out := v[:8]
	c2.Close()
	return out, nil
}
func (e *pebEng) addrPage(addr, before []byte, n int) (int, error) {
	lo := append([]byte{'a'}, addr...)
	hi := append(append([]byte{}, lo...), 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff)
	start := lo
	if before != nil {
		inv := invKey(binary.BigEndian.Uint64(before), binary.BigEndian.Uint32(before[8:]))
		start = append(append(append([]byte{}, lo...), inv...), 0)
	}
	it, err := e.db.NewIter(&pebble.IterOptions{LowerBound: start, UpperBound: hi})
	if err != nil {
		return 0, err
	}
	defer it.Close()
	cnt := 0
	for ok := it.First(); ok && cnt < n; ok = it.Next() {
		k := it.Key()[21:]
		tk := append([]byte{'t'}, txKey(^binary.BigEndian.Uint64(k), ^binary.BigEndian.Uint32(k[8:]))...)
		_, c, err := e.db.Get(tk)
		if err != nil {
			return cnt, err
		}
		c.Close()
		cnt++
	}
	return cnt, nil
}
func (e *pebEng) latestBlocks(n int) (int, error) {
	it, err := e.db.NewIter(&pebble.IterOptions{LowerBound: []byte{'b'}, UpperBound: []byte{'c'}})
	if err != nil {
		return 0, err
	}
	defer it.Close()
	cnt := 0
	for ok := it.Last(); ok && cnt < n; ok = it.Prev() {
		cnt++
	}
	return cnt, nil
}
func (e *pebEng) scanTo(addr []byte, from, to uint64) (int, error) {
	it, err := e.db.NewIter(&pebble.IterOptions{LowerBound: append([]byte{'t'}, txKey(from, 0)...), UpperBound: append([]byte{'t'}, txKey(to, 0)...)})
	if err != nil {
		return 0, err
	}
	defer it.Close()
	cnt := 0
	for ok := it.First(); ok; ok = it.Next() {
		if string(it.Value()[20:40]) == string(addr) {
			cnt++
		}
	}
	return cnt, nil
}
func (e *pebEng) agg() (int, error) {
	it, err := e.db.NewIter(&pebble.IterOptions{LowerBound: []byte{'t'}, UpperBound: []byte{'u'}})
	if err != nil {
		return 0, err
	}
	defer it.Close()
	buckets := map[uint64]int{}
	for ok := it.First(); ok; ok = it.Next() {
		buckets[binary.BigEndian.Uint64(it.Key()[1:])/10000]++
	}
	return len(buckets), nil
}
func (e *pebEng) size() int64 { return dirSize(e.path) }
func (e *pebEng) close()      { e.db.Close() }

// ---------------- Postgres ----------------
type pgEng struct {
	pg   *ep.EmbeddedPostgres
	db   *pgxpool.Pool
	path string
}

func newPG(path string) *pgEng {
	os.RemoveAll(path)
	cfg := ep.DefaultConfig().Port(55433).Version(ep.V17).DataPath(filepath.Join(path, "data")).
		RuntimePath(filepath.Join(path, "rt")).StartParameters(map[string]string{
		"shared_buffers": "4GB", "effective_cache_size": "8GB", "work_mem": "64MB",
		"max_wal_size": "8GB", "checkpoint_timeout": "30min", "random_page_cost": "1.1", "jit": "off"}).
		StartTimeout(2 * time.Minute)
	pg := ep.NewDatabase(cfg)
	must(pg.Start())
	pc, err := pgxpool.ParseConfig("postgres://postgres:postgres@localhost:55433/postgres?sslmode=disable")
	must(err)
	pc.MaxConns = 16
	db, err := pgxpool.NewWithConfig(context.Background(), pc)
	must(err)
	_, err = db.Exec(context.Background(), `
	CREATE TABLE txs (block_number bigint, tx_index int, hash bytea NOT NULL, from_address bytea, to_address bytea, payload bytea, PRIMARY KEY(block_number, tx_index));
	CREATE INDEX txs_hash ON txs (hash);
	CREATE TABLE addr_txs (address bytea, block_number bigint, tx_index int, PRIMARY KEY (address, block_number, tx_index));
	CREATE TABLE blocks (number bigint PRIMARY KEY, header bytea);`)
	must(err)
	return &pgEng{pg: pg, db: db, path: path}
}
func (e *pgEng) name() string { return "postgres" }
func (e *pgEng) load(batch []tx) error {
	ctx := context.Background()
	t, err := e.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer t.Rollback(ctx)
	if _, err := t.CopyFrom(ctx, pgx.Identifier{"txs"}, []string{"block_number", "tx_index", "hash", "from_address", "to_address", "payload"},
		pgx.CopyFromSlice(len(batch), func(i int) ([]any, error) {
			x := batch[i]
			return []any{int64(x.block), int32(x.idx), x.hash[:], addrBytes(x.from), addrBytes(x.to), x.payload}, nil
		})); err != nil {
		return err
	}
	var rows [][]any
	var blocks [][]any
	var last uint64 = 1<<64 - 1
	for _, x := range batch {
		rows = append(rows, []any{addrBytes(x.from), int64(x.block), int32(x.idx)})
		if x.to != x.from {
			rows = append(rows, []any{addrBytes(x.to), int64(x.block), int32(x.idx)})
		}
		if x.block != last {
			blocks = append(blocks, []any{int64(x.block), x.payload[:120]})
			last = x.block
		}
	}
	if _, err := t.CopyFrom(ctx, pgx.Identifier{"addr_txs"}, []string{"address", "block_number", "tx_index"}, pgx.CopyFromRows(rows)); err != nil {
		return err
	}
	if _, err := t.CopyFrom(ctx, pgx.Identifier{"blocks"}, []string{"number", "header"}, pgx.CopyFromRows(blocks)); err != nil {
		return err
	}
	return t.Commit(ctx)
}
func (e *pgEng) getTx(hash []byte) ([]byte, error) {
	var p []byte
	err := e.db.QueryRow(context.Background(), `SELECT payload FROM txs WHERE hash = $1`, hash).Scan(&p)
	return p, err
}
func (e *pgEng) addrPage(addr, before []byte, n int) (int, error) {
	ctx := context.Background()
	var rows pgx.Rows
	var err error
	if before == nil {
		rows, err = e.db.Query(ctx, `SELECT t.payload FROM addr_txs a JOIN txs t ON t.block_number = a.block_number AND t.tx_index = a.tx_index
			WHERE a.address = $1 ORDER BY a.block_number DESC, a.tx_index DESC LIMIT $2`, addr, n)
	} else {
		rows, err = e.db.Query(ctx, `SELECT t.payload FROM addr_txs a JOIN txs t ON t.block_number = a.block_number AND t.tx_index = a.tx_index
			WHERE a.address = $1 AND (a.block_number, a.tx_index) < ($3, $4) ORDER BY a.block_number DESC, a.tx_index DESC LIMIT $2`,
			addr, n, int64(binary.BigEndian.Uint64(before)), int32(binary.BigEndian.Uint32(before[8:])))
	}
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
func (e *pgEng) latestBlocks(n int) (int, error) {
	rows, err := e.db.Query(context.Background(), `SELECT number, header FROM blocks ORDER BY number DESC LIMIT $1`, n)
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
func (e *pgEng) scanTo(addr []byte, from, to uint64) (int, error) {
	var n int
	err := e.db.QueryRow(context.Background(), `SELECT count(*) FROM txs WHERE block_number >= $1 AND block_number < $2 AND to_address = $3`, int64(from), int64(to), addr).Scan(&n)
	return n, err
}
func (e *pgEng) agg() (int, error) {
	rows, err := e.db.Query(context.Background(), `SELECT block_number / 10000, count(*) FROM txs GROUP BY 1`)
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
func (e *pgEng) size() int64 {
	var s int64
	e.db.QueryRow(context.Background(), `SELECT pg_database_size('postgres')`).Scan(&s)
	return s
}
func (e *pgEng) close() { e.db.Close(); e.pg.Stop() }

// ---------------- harness ----------------
var errNA = fmt.Errorf("n/a")

func parseUsedMemory(info string) int64 {
	for _, l := range strings.Split(info, "\n") {
		if strings.HasPrefix(l, "used_memory:") {
			n, _ := strconv.ParseInt(strings.TrimSpace(l[len("used_memory:"):]), 10, 64)
			return n
		}
	}
	return 0
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func dirSize(p string) int64 {
	var s int64
	filepath.Walk(p, func(_ string, i os.FileInfo, _ error) error {
		if i != nil && !i.IsDir() {
			s += i.Size()
		}
		return nil
	})
	return s
}

func pct(d []time.Duration, p float64) time.Duration {
	return d[int(float64(len(d)-1)*p)]
}

func measure(label string, ops int, f func(i int) error) {
	lat := make([]time.Duration, ops)
	start := time.Now()
	for i := 0; i < ops; i++ {
		t := time.Now()
		if err := f(i); err == errNA {
			fmt.Printf("  %-26s n/a (no ordered scan in this engine)\n", label)
			return
		} else if err != nil {
			panic(fmt.Sprintf("%s: %v", label, err))
		}
		lat[i] = time.Since(t)
	}
	wall := time.Since(start)
	sort.Slice(lat, func(a, b int) bool { return lat[a] < lat[b] })
	fmt.Printf("  %-26s p50 %9s  p99 %9s  p99.9 %9s  1-thread %8.0f ops/s\n", label,
		pct(lat, .5), pct(lat, .99), pct(lat, .999), float64(ops)/wall.Seconds())
}

func parallel(label string, ops, threads int, f func(i int) error) {
	var wg sync.WaitGroup
	start := time.Now()
	per := ops / threads
	for t := 0; t < threads; t++ {
		wg.Add(1)
		go func(t int) {
			defer wg.Done()
			for i := 0; i < per; i++ {
				if err := f(t*per + i); err != nil {
					panic(err)
				}
			}
		}(t)
	}
	wg.Wait()
	fmt.Printf("  %-26s %d threads %10.0f ops/s\n", label, threads, float64(per*threads)/time.Since(start).Seconds())
}

func main() {
	flag.Parse()
	total := *nBlocks * *txPer
	r := rand.New(rand.NewSource(7))
	hashes := make([][]byte, *nOps)
	for i := range hashes {
		n := r.Intn(total)
		hashes[i] = hashOf(uint64(n / *txPer), uint32(n%*txPer))
	}
	warm := make([][]byte, *nOps)
	for i := range warm {
		warm[i] = addrBytes(uint32(100 + r.Intn(10000)))
	}
	hot := addrBytes(0)
	mid := txKey(uint64(*nBlocks/2), 0)

	for _, name := range splitComma(*engines) {
		var e engine
		switch name {
		case "mdbx":
			e = newMDBX(filepath.Join(*dir, "mdbx"))
		case "pebble":
			e = newPebble(filepath.Join(*dir, "pebble"))
		case "pg":
			e = newPG(filepath.Join(*dir, "pg"))
		case "ch":
			e = newCH()
		case "duckdb":
			e = newDuck(filepath.Join(*dir, "duckdb"))
		case "redis":
			e = newRedis()
		case "sqlite":
			e = newSQLite(filepath.Join(*dir, "sqlite"))
		case "tidb":
			e = newTiDB()
		case "pgvac":
			e = pgVac{newPG(filepath.Join(*dir, "pg"))}
		case "lmdb":
			e = newLMDB(filepath.Join(*dir, "lmdb"))
		}
		fmt.Printf("== %s: loading %d blocks / %d txs\n", e.name(), *nBlocks, total)
		st := time.Now()
		gen(func(b []tx) { must(e.load(b)) })
		if f, ok := e.(interface{ finish() }); ok {
			f.finish()
		}
		el := time.Since(st)
		fmt.Printf("  load %.1fs  = %.0f tx/s ingest  size %.2f GB\n", el.Seconds(), float64(total)/el.Seconds(), float64(e.size())/1e9)

		measure("tx by hash", *nOps, func(i int) error { _, err := e.getTx(hashes[i]); return err })
		measure("latest 20 blocks", *nOps/10, func(i int) error { _, err := e.latestBlocks(20); return err })
		measure("addr page warm (25)", *nOps/10, func(i int) error { _, err := e.addrPage(warm[i], nil, 25); return err })
		measure("addr page hot (25)", *nOps/10, func(i int) error { _, err := e.addrPage(hot, nil, 25); return err })
		measure("addr page hot deep (25)", *nOps/10, func(i int) error { _, err := e.addrPage(hot, mid, 25); return err })
		measure("range scan to=X 100k blk", 20, func(i int) error { _, err := e.scanTo(addrBytes(5), 100000, 200000); return err })
		measure("agg full history", 5, func(i int) error { _, err := e.agg(); return err })
		parallel("tx by hash", *nOps*4, 8, func(i int) error { _, err := e.getTx(hashes[i%*nOps]); return err })
		parallel("addr page warm (25)", *nOps, 8, func(i int) error { _, err := e.addrPage(warm[i%*nOps], nil, 25); return err })
		e.close()
	}
}

func splitComma(s string) []string {
	var out []string
	cur := ""
	for _, c := range s {
		if c == ',' {
			out = append(out, cur)
			cur = ""
			continue
		}
		cur += string(c)
	}
	return append(out, cur)
}
