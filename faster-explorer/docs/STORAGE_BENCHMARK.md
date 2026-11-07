# Storage engine benchmark (2026-10-16)

Goal: pick the storage layout for every Faster service (explorer API, faster-rpc,
faster-sync, GraphQL, indexer framework) based on measured latency, not
assumptions.

## Setup

- Data: 500,000 blocks × 10 tx = **5,000,000 txs**, 1M addresses with Zipf(1.2)
  activity (address #0 is party to ~18% of all txs, like a big router/exchange).
  ~10M address→tx index rows. Tx record ≈ 220 bytes.
- Machine: Apple M1 Pro (8 cores), 32 GB RAM, internal NVMe, macOS. Data fits in
  RAM, so these are **warm-cache** numbers — the regime an explorer runs in.
- Durability: each engine commits durably every 1,000 blocks (fsync / sync
  commit), except Redis (in-memory, persistence off).
- Harnesses: `bench/storage/go` (MDBX, LMDB, Pebble, SQLite, Postgres 17,
  ClickHouse 26.11, DuckDB, Redis 8.10, TiDB 8.5.8) and `bench/storage/rust`
  (RocksDB, Fjall, redb, QMDB). Raw output: `bench/storage/results/`.
- Same key layout everywhere: `txs (block, idx) → record`, `hash → (block, idx)`,
  `address → (block, idx)` newest-first, `blocks (number) → header`.

Workloads: tx-by-hash (2 lookups), latest 20 blocks, address history page of 25
(warm address, hot address, hot address deep in history with a keyset cursor),
filtered range scan over 1M txs (`to = X` in 100k blocks, eth_getLogs-like),
and a whole-history aggregate (tx count per 10k-block bucket).

## Results

| Engine | Kind | Ingest tx/s | Size GB | Tx by hash p50 / p99 | Lookups/s 8 thr | Latest 20 p50 | Addr page warm p50 | Addr page hot-deep p50 | Range scan 1M tx | Aggregate 5M tx |
|---|---|---|---|---|---|---|---|---|---|---|
| **QMDB** | hashed KV + Merkle (Rust) | **314K** | 3.09 | **1.9 / 5.4 µs** | 595K | n/a | n/a | n/a | n/a | n/a |
| **MDBX** | mmap B+tree (C) | 17.6K | 3.22 | **3.8 / 8.9 µs** | **1.03M** | 1.7 µs | **31 µs** | **10 µs** | 154 ms | 803 ms |
| redb | mmap-free B+tree (Rust) | 22.6K | 4.30 | 4.7 / 10.6 µs | 508K | 1.7 µs | 29 µs | n/a¹ | 100 ms | 465 ms |
| LMDB | mmap B+tree (C) | 34.6K | 2.33 | 4.8 / 9.9 µs | 234K | 3.0 µs | 36 µs | 13 µs | 115 ms | 585 ms |
| SQLite | embedded SQL B-tree | 23.6K | 2.17 | 5.6 / 10.3 µs | 75K | 8.6 µs | 44 µs | 20 µs | 88 ms | 2.8 s |
| **RocksDB** | LSM (C++) | 223K | 0.52² | 10.1 / 18.2 µs | 458K | 15.8 µs | 92 µs | 45 µs | 167 ms | 814 ms |
| **Fjall** | LSM (Rust) | 191K | 0.78² | 11.6 / 21.5 µs | **1.02M** | 1.9 µs | 84 µs | 16 µs | 113 ms | 688 ms |
| Pebble | LSM (Go) | **253K** | 1.19 | 21.8 / 50.8 µs | 298K | 6.1 µs | 118 µs | 63 µs | 85 ms | 491 ms |
| Postgres 17 (vacuumed) | client/server SQL | 39–105K³ | 3.81 | 49 / 78 µs | 66K | 57 µs | 180 µs | 121 µs | 171 ms | 241 ms |
| Redis 8.10 | in-memory server | 128K | 2.72 RAM | 187 / 288 µs | 22K | 93 µs | 213 µs | 204 µs | n/a | n/a |
| DuckDB | embedded columnar | 137–155K | 1.27 | 185 / 308 µs | 12K | 3.2 ms | 22 ms | 32 ms | 9.1 ms | 8.6 ms |
| TiDB 8.5 (1 PD/1 KV/1 DB) | distributed SQL | 7.0K | 1.54 | 568 / 771 µs | 7.5K | 838 µs | 2.0 ms | 1.6 ms | ~150 ms⁴ | ~600 ms⁴ |
| **ClickHouse 26.11** | columnar server | 35K⁵ | **0.47** | 3.3 / 17.5 ms | 1.4K | 1.2 ms | 6.1 ms | 8.7 ms | **5.3 ms** | **5.4 ms** |

Notes:
1. redb multimap values cannot be seeked; a composite key would fix it — not measured.
2. Rust harness payload bytes were constant (LZ4-compressible) — Rust engine sizes
   are not comparable with the Go column; timings are unaffected.
3. Postgres ingest varied 39K→105K between two runs (laptop variance). Unvacuumed
   right after load: p99 1 ms, p99.9 22 ms (autovacuum/checkpoint interference).
4. TiKV coprocessor cache served repeated identical scans from memory (reported
   p50 < 1 ms); figures shown are the cold first-run times.
5. ClickHouse ingest was measured with 10K-row inserts into 4 tables; its bulk
   path with large batches is much faster — not its best case.
- QMDB on macOS runs without io_uring (Linux-only); its Linux numbers should be better.
- One run per engine on a laptop: treat differences under ~1.5× as noise. Orders
  of magnitude are reliable.

## What the numbers say

1. **The network hop is the latency floor.** Every in-process store answers a
   point lookup in 2–12 µs. Every client/server store (Postgres, Redis, TiDB,
   ClickHouse) costs 50 µs–3 ms — Redis, fully in RAM, is still 40× slower than
   MDBX because of the round trip. Ultra-low latency requires the data to be
   in the serving process.
2. **No engine wins both access patterns.** Row/KV stores win lookups and
   newest-first lists by 100–1000×; columnar stores win scans and aggregates by
   20–150×. The design needs two tiers.
3. **Ingest separates B+trees from LSMs.** mmap B+trees (MDBX, LMDB, redb) and
   SQLite read fastest but ingest 9–35K tx/s with random hash keys. LSMs
   (RocksDB, Fjall, Pebble) ingest 190–250K tx/s with reads only 2–3× slower.
   A high-TPS chain (evm-node targets ≫ 35K tx/s) rules out B+trees as the
   primary write path.
4. **Fjall ≈ RocksDB on reads, with 2× the parallel lookup throughput** in this
   run; RocksDB is the far more proven engine (Meta, Solana, TiKV) and has Go
   and Rust bindings.
5. **QMDB is the fastest point store (1.9 µs, 314K tx/s ingest) but has no
   ordered iteration.** It fits Merkle-authenticated state, not explorer indexes.
6. **ClickHouse** is the clear analytics/bulk-scan tier (5 ms scans and
   aggregates, 8× smaller than Postgres). DuckDB is close on scans but
   serialises concurrent queries — not a server tier.
7. **Postgres** is fine (≈50 µs) for small mutable app data and user-defined
   indexer entities behind GraphQL, but not for the hot explorer path.
8. **TiDB** only makes sense if a single write node can't keep up; it is
   100–300× slower per request here.
