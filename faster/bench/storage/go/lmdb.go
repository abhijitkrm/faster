package main

import (
	"context"
	"encoding/binary"
	"os"

	"github.com/PowerDNS/lmdb-go/lmdb"
)

// ---------------- LMDB ----------------
type lmdbEng struct {
	env                   *lmdb.Env
	txs, hashes, addr, bl lmdb.DBI
	path                  string
}

func newLMDB(path string) *lmdbEng {
	os.RemoveAll(path)
	os.MkdirAll(path, 0o755)
	env, err := lmdb.NewEnv()
	must(err)
	must(env.SetMaxDBs(16))
	must(env.SetMapSize(64 << 30))
	must(env.Open(path, lmdb.NoReadahead|lmdb.NoTLS, 0o644))
	e := &lmdbEng{env: env, path: path}
	must(env.Update(func(t *lmdb.Txn) error {
		var err error
		if e.txs, err = t.OpenDBI("txs", lmdb.Create); err != nil {
			return err
		}
		if e.hashes, err = t.OpenDBI("hashes", lmdb.Create); err != nil {
			return err
		}
		if e.addr, err = t.OpenDBI("addr", lmdb.Create|lmdb.DupSort|lmdb.DupFixed); err != nil {
			return err
		}
		e.bl, err = t.OpenDBI("blocks", lmdb.Create)
		return err
	}))
	return e
}
func (e *lmdbEng) name() string { return "lmdb" }
func (e *lmdbEng) load(batch []tx) error {
	return e.env.Update(func(t *lmdb.Txn) error {
		var last uint64 = 1<<64 - 1
		for _, x := range batch {
			k := txKey(x.block, x.idx)
			v := append(append(addrBytes(x.from), addrBytes(x.to)...), x.payload...)
			if err := t.Put(e.txs, k, v, lmdb.Append); err != nil {
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
				if err := t.Put(e.bl, bk, x.payload[:120], lmdb.Append); err != nil {
					return err
				}
				last = x.block
			}
		}
		return nil
	})
}
func (e *lmdbEng) getTx(hash []byte) ([]byte, error) {
	var out []byte
	err := e.env.View(func(t *lmdb.Txn) error {
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
func (e *lmdbEng) addrPage(addr, before []byte, n int) (int, error) {
	cnt := 0
	err := e.env.View(func(t *lmdb.Txn) error {
		c, err := t.OpenCursor(e.addr)
		if err != nil {
			return err
		}
		defer c.Close()
		var v []byte
		if before == nil {
			_, v, err = c.Get(addr, nil, lmdb.Set)
		} else {
			inv := invKey(binary.BigEndian.Uint64(before), binary.BigEndian.Uint32(before[8:]))
			_, v, err = c.Get(addr, inv, lmdb.GetBothRange)
			if err == nil && string(v) == string(inv) {
				_, v, err = c.Get(nil, nil, lmdb.NextDup)
			}
		}
		for err == nil && cnt < n {
			k := txKey(^binary.BigEndian.Uint64(v), ^binary.BigEndian.Uint32(v[8:]))
			if _, err = t.Get(e.txs, k); err != nil {
				return err
			}
			cnt++
			_, v, err = c.Get(nil, nil, lmdb.NextDup)
		}
		if lmdb.IsNotFound(err) {
			return nil
		}
		return err
	})
	return cnt, err
}
func (e *lmdbEng) latestBlocks(n int) (int, error) {
	cnt := 0
	err := e.env.View(func(t *lmdb.Txn) error {
		c, err := t.OpenCursor(e.bl)
		if err != nil {
			return err
		}
		defer c.Close()
		_, _, err = c.Get(nil, nil, lmdb.Last)
		for err == nil && cnt < n {
			cnt++
			_, _, err = c.Get(nil, nil, lmdb.Prev)
		}
		return nil
	})
	return cnt, err
}
func (e *lmdbEng) scanTo(addr []byte, from, to uint64) (int, error) {
	cnt := 0
	err := e.env.View(func(t *lmdb.Txn) error {
		c, err := t.OpenCursor(e.txs)
		if err != nil {
			return err
		}
		defer c.Close()
		k, v, err := c.Get(txKey(from, 0), nil, lmdb.SetRange)
		for err == nil && binary.BigEndian.Uint64(k) < to {
			if string(v[20:40]) == string(addr) {
				cnt++
			}
			k, v, err = c.Get(nil, nil, lmdb.Next)
		}
		return nil
	})
	return cnt, err
}
func (e *lmdbEng) agg() (int, error) {
	buckets := map[uint64]int{}
	err := e.env.View(func(t *lmdb.Txn) error {
		c, err := t.OpenCursor(e.txs)
		if err != nil {
			return err
		}
		defer c.Close()
		k, _, err := c.Get(nil, nil, lmdb.First)
		for err == nil {
			buckets[binary.BigEndian.Uint64(k)/10000]++
			k, _, err = c.Get(nil, nil, lmdb.Next)
		}
		return nil
	})
	return len(buckets), err
}
func (e *lmdbEng) size() int64 { return dirSize(e.path) }
func (e *lmdbEng) close()      { e.env.Close() }


// pgVac is Postgres with VACUUM ANALYZE + CHECKPOINT after load, so reads are
// measured without post-load autovacuum/checkpoint interference.
type pgVac struct{ *pgEng }

func (e pgVac) name() string { return "postgres-vacuumed" }
func (e pgVac) finish() {
	for _, q := range []string{`VACUUM (ANALYZE)`, `CHECKPOINT`} {
		_, err := e.db.Exec(context.Background(), q)
		must(err)
	}
}
