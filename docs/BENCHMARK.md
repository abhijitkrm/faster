# Benchmark: faster-sync vs ClickHouse

Plan step 1 was to check the chunk design against ClickHouse (the analytics
tier of the Faster stack) before building on it. Both engines get the same
rows and answer the same queries over HTTP; the client fetches the complete
result (faster-sync pages with `next_block`, ClickHouse returns one
response) as Arrow.

**Machine:** Apple M1 Pro laptop (8 performance cores), data in the OS page
cache, client and servers on the same machine.
**ClickHouse:** 26.11, two MergeTree layouts of the same rows: `by block`
(`ORDER BY (block_number, log_index)` with `bloom_filter` skip indexes on
address and topics, the analogue of our design) and `by address`
(`ORDER BY (address, block_number, log_index)`, its best case for contract
queries). Output `ArrowStream` (LZ4 by default).
**faster-sync:** `format=arrow` (LZ4), default settings. Times are medians
of 5 runs of the whole result. Columns returned: every log column except
`extra`.

## Synthetic chain: 20,000 blocks, 3M transactions, 9M logs

`fsgen` data: 150 txs/block, 3 Transfer logs/tx, 200K contracts with a
log-uniform popularity skew, 2M accounts.

| Query | Rows | faster-sync | CH by block | CH by address |
|---|---:|---:|---:|---:|
| rare contract, all history | 12 | **3.4 ms** | 9.9 ms | 4.3 ms |
| busiest contract, all history | 510,799 | **49 ms** | 230 ms | 65 ms |
| Transfers to one wallet (topic2) | 9 | **3.4 ms** | 9.2 ms | 9.7 ms |
| every Transfer (10 pages of 1M rows) | 8,999,400 | **893 ms** | 942 ms | 1358 ms |

First page of the 9M-row query: 102 ms.

## Ethereum mainnet: 4,963 blocks, 4.54M logs

Blocks 26,247,674 – 26,252,637 ingested from a public RPC.

| Query | Rows | faster-sync | CH by block | CH by address |
|---|---:|---:|---:|---:|
| rare contract (`0x0000…17c6…fecd`) | 14 | **3.6 ms** | 10.6 ms | 5.5 ms |
| USDT, all logs (3 pages) | 529,869 | **66 ms** | 233 ms | 70 ms |
| Transfers with topic2 = `0x…01` | 14 | 32.5 ms | 34.1 ms | **6.3 ms** |
| every Transfer (14 pages of 200K) | 2,683,819 | 760 ms | 544 ms | **523 ms** |

Where ClickHouse wins:

- **topic2 = `0x…01`**: that value appears in 2,010 logs of other events,
  spread over 865 of 1,563 filter sub-groups, so the filters cannot prune
  and the query decodes topic0 and topic2 for 3.5M rows; ClickHouse's full
  column scan is faster. Planned fix: evaluate predicates column by column,
  rarest first (later columns are decoded only for surviving rows).
- **every Transfer**: 14 round trips of 200K rows (the server's
  `QUERY_MAX_ROWS`) against one ClickHouse response. Larger pages close
  most of the gap (the synthetic run uses 1M-row pages).

## What made the difference

Measured step by step on the synthetic set (engine time, in-process):

| Change | rare | busy | wallet | all 9M |
|---|---:|---:|---:|---:|
| zstd, 32K-row groups, row-group work units, late materialization | 24 ms | 319 ms | 19 ms | 288 ms |
| LZ4 instead of zstd | 22 ms | 282 ms | 19 ms | 271 ms |
| logs clustered by address inside each row group + page index | 7 ms | 91 ms | 19 ms | 988 ms |
| memory-mapped chunk files (no per-page open/close) | 4.8 ms | 43 ms | 11 ms | 363 ms |
| filters per 4,096-row sub-group (page aligned) | 5.0 ms | 45 ms | 5.7 ms | 356 ms |
| prune work units before scheduling | 2.2 ms | 42 ms | 2.9 ms | 364 ms |

Before those, work units were whole chunks: a page that filled its budget
early threw away the rest of the chunks scanned in parallel (the 9M-row
query took 2.9 s over HTTP). Over HTTP, streaming the Arrow response (each work unit encodes its own
frames on its worker thread) took the 9M-row query from 1.8 s to 0.9 s.

Storage for the synthetic set: chunks 1.3 GB (Parquet, LZ4) vs ClickHouse
446 MB (by block) / 636 MB (by address); filters 36 MB in memory.

## Reproduce

```bash
cargo build --release
fsgen ./bench-data 20000 150 3                       # build the store
DATA_DIR=./bench-data LISTEN=127.0.0.1:3200 QUERY_MAX_ROWS=1000000 faster-sync   # serve-only (no RPC_URL)
fsq ./bench-data '{"from_block":0,"logs":[{"address":["0xc000000000000000000000000000000000000000"]}],"join":"nothing"}'
```

Load the same rows into ClickHouse with
`INSERT INTO t FORMAT Parquet` from `chunks/*/logs.parquet`.

## v0.2.0

**Unique-value columns without dictionaries.** Reading a dictionary-encoded
column decodes its whole dictionary first; for hashes, signatures and call
data the dictionary is as large as the data and that dominated point
lookups. Turning dictionaries off for those columns (and capping pages at
64 KB) changed, in-process on the synthetic set:

| | v0.1 | v0.2 |
|---|---:|---:|
| tx by hash, all fields | 1.9 ms | 0.70 ms |
| tx + its logs + header (receipt) | 4.3 ms | 1.8 ms |
| full block (151 txs) | 1.5 ms | 0.35 ms |
| busiest contract (511K logs) | 42 ms | 18 ms |
| rare contract | 2.3 ms | 1.3 ms |
| one wallet's Transfers | 1.9 ms | 1.1 ms |

Chunks are 7% smaller. Through faster-rpc on Ethereum mainnet data (same
blocks, both layouts served side by side, cold, median of 20):
`eth_getTransactionByHash` 7.1 → 2.4 ms, `eth_getTransactionReceipt`
13.0 → 3.8 ms, `eth_getBlockReceipts` 25 → 18 ms.

**Rarest field first.** For selections with several fields, the field
whose filters pass in the fewest sub-groups is checked alone first; the
other columns are decoded only for rows that survive it. Mainnet Transfers
with topic2 = `0x…01`: 18.5 → 14.6 ms (ClickHouse by address: 6.3 ms; the
rest is decoding the 32-byte topic column for 2.2M candidate rows).
Single-field selections are unchanged.
