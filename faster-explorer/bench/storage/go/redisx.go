package main

import (
	"context"
	"encoding/binary"
	"strconv"

	"github.com/redis/go-redis/v9"
)

// Redis: everything in memory. Keys: h:<hash> -> txkey, t:<txkey> -> value,
// a:<addr> sorted set scored by block*2^16+idx, zset "blocks".
type redisEng struct{ c *redis.Client }

func newRedis() *redisEng {
	c := redis.NewClient(&redis.Options{Addr: "127.0.0.1:16379", PoolSize: 16})
	must(c.FlushAll(context.Background()).Err())
	return &redisEng{c: c}
}
func (e *redisEng) name() string { return "redis" }
func score(b uint64, i uint32) float64 { return float64(b<<16 | uint64(i)) }
func (e *redisEng) load(batch []tx) error {
	ctx := context.Background()
	p := e.c.Pipeline()
	var last uint64 = 1<<64 - 1
	for _, x := range batch {
		k := txKey(x.block, x.idx)
		v := append(append(addrBytes(x.from), addrBytes(x.to)...), x.payload...)
		p.Set(ctx, "t:"+string(k), v, 0)
		p.Set(ctx, "h:"+string(x.hash[:]), k, 0)
		m := redis.Z{Score: score(x.block, x.idx), Member: string(k)}
		p.ZAdd(ctx, "a:"+string(addrBytes(x.from)), m)
		if x.to != x.from {
			p.ZAdd(ctx, "a:"+string(addrBytes(x.to)), m)
		}
		if x.block != last {
			p.ZAdd(ctx, "blocks", redis.Z{Score: float64(x.block), Member: strconv.FormatUint(x.block, 10)})
			last = x.block
		}
	}
	_, err := p.Exec(ctx)
	return err
}
func (e *redisEng) getTx(hash []byte) ([]byte, error) {
	ctx := context.Background()
	k, err := e.c.Get(ctx, "h:"+string(hash)).Bytes()
	if err != nil {
		return nil, err
	}
	return e.c.Get(ctx, "t:"+string(k)).Bytes()
}
func (e *redisEng) addrPage(addr, before []byte, n int) (int, error) {
	ctx := context.Background()
	max := "+inf"
	if before != nil {
		max = "(" + strconv.FormatFloat(score(binary.BigEndian.Uint64(before), binary.BigEndian.Uint32(before[8:])), 'f', 0, 64)
	}
	keys, err := e.c.ZRevRangeByScore(ctx, "a:"+string(addr), &redis.ZRangeBy{Max: max, Min: "-inf", Count: int64(n)}).Result()
	if err != nil || len(keys) == 0 {
		return 0, err
	}
	tk := make([]string, len(keys))
	for i, k := range keys {
		tk[i] = "t:" + k
	}
	v, err := e.c.MGet(ctx, tk...).Result()
	return len(v), err
}
func (e *redisEng) latestBlocks(n int) (int, error) {
	v, err := e.c.ZRevRange(context.Background(), "blocks", 0, int64(n-1)).Result()
	return len(v), err
}
func (e *redisEng) scanTo([]byte, uint64, uint64) (int, error) { return -1, errNA }
func (e *redisEng) agg() (int, error)                          { return -1, errNA }
func (e *redisEng) size() int64 {
	s, _ := e.c.Info(context.Background(), "memory").Result()
	return parseUsedMemory(s)
}
func (e *redisEng) close() { e.c.Close() }
