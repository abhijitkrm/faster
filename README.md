# faster-sync

Bulk historical data API for EVM chains: ask for exactly the blocks,
transactions, receipts, logs and call traces you need across any block range
and get them back in pages, as JSON or Arrow. Built so an indexer catches up
on months of history in seconds instead of millions of `eth_getLogs` calls.

Part of the Faster stack: [faster](https://github.com/abhijitkrm/faster) (explorer) ·
faster-sync · [faster-rpc](https://github.com/abhijitkrm/faster-rpc) ·
[faster-index](https://github.com/abhijitkrm/faster-index). Clean-room
implementation.

| | faster-sync | ClickHouse (best layout) |
|---|---:|---:|
| one rare contract, all history | **3.4 ms** | 4.3 ms |
| busiest contract, 511K logs | **49 ms** | 65 ms |
| one wallet's incoming Transfers | **3.4 ms** | 9.2 ms |
| 9M logs, streamed in 10 pages | **0.89 s** | 0.94 s |

Synthetic 12M-row chain, laptop; method, mainnet numbers and where
ClickHouse still wins: [docs/BENCHMARK.md](docs/BENCHMARK.md).

## How it works

- **Immutable chunks.** Finalized history is cut into chunks of block
  ranges (~2M rows each). A chunk is one Parquet file per table (blocks,
  transactions + receipts, logs, traces) plus an index: per 4,096-row
  sub-group, the block range and binary fuse filters (~9 bits per value)
  over addresses, topics, senders, recipients and function selectors.
- **Clustered logs.** Inside each row group, logs are sorted by contract
  address and the file carries page-level min/max, so a contract query reads
  only the pages holding that contract, even for the busiest contracts.
- **Pruned, parallel, streamed.** A query checks the filters, skips every
  sub-group that cannot match, decodes the filter columns first and the rest
  only for matching rows, runs row-group-sized work units in parallel, and
  streams each unit's Arrow frames as soon as it is done.
- **Hot tail in RocksDB.** Blocks newer than the finalized head live in a
  RocksDB hot tier (and in memory) with parent-hash checks; reorgs roll them
  back. Finalized hot blocks are moved into chunks, and small chunks are
  compacted. Tx-hash and block-hash indexes cover all history.
- **Exact node objects.** Every column the node sends is kept, plus any
  chain-specific keys in an `extra` column, so `?shape=rpc` rebuilds the
  node's own JSON-RPC objects (faster-rpc uses this).

## Quick start

```bash
docker build -t faster-sync .
docker run -d -p 3100:3100 -v fsync-data:/data \
  -e RPC_URL=https://your-node -e START_BLOCK=20000000 faster-sync

curl -s localhost:3100/height
curl -s localhost:3100/query -H 'content-type: application/json' -d '{
  "from_block": 20000000,
  "logs": [{"address": ["0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"],
            "topics": [["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"]]}],
  "fields": {"log": ["block_number", "log_index", "topic1", "topic2", "data"]}
}'
```

The node needs `eth_getBlockByNumber` and `eth_getBlockReceipts` (falls
back to per-transaction receipts); `TRACES=true` also needs
`debug_traceBlockByNumber` with `callTracer`. Public endpoints work with
`RPC_RPS` and `RPC_BATCH` set to their limits.

Without `RPC_URL` the binary serves an existing data directory read-only
(replicas, benchmarks).

**Running reth?** [`reth/`](reth) builds `faster-sync-reth`: a reth node with
faster-sync inside it as an execution extension. Blocks and receipts go from
reth's database straight into the store, with no JSON-RPC in between, and
reorgs arrive as reth sees them.

## Clients

- Rust: [`clients/rust`](clients/rust) (`faster-sync-client`): JSON or Arrow
  pages, `stream()` that follows the tip and reports reorgs.
- TypeScript: [`clients/ts`](clients/ts) (`faster-sync-client`): same model,
  no dependencies; used by faster-index.

## Configuration

| Variable | Default | |
|---|---|---|
| `RPC_URL` | — | node JSON-RPC (unset = serve-only) |
| `DATA_DIR` | `./data` | chunks, hot tier, metadata |
| `START_BLOCK` | `0` | first block to ingest (fixed at first start) |
| `TRACES` | `false` | fetch `callTracer` traces |
| `RPC_BATCH` / `RPC_RPS` / `RPC_TIMEOUT_MS` | 100 / 0 (off) / 30000 | calls per HTTP request, requests per second, timeout |
| `FETCH_WINDOW` / `FETCH_CONCURRENCY` | 20 / 8 | blocks per fetch task, tasks in flight |
| `FINALITY_DEPTH` / `USE_FINALIZED_TAG` | 64 / true | what counts as final (node's `finalized` tag when it has one) |
| `CHUNK_ROWS` / `CHUNK_MAX_BLOCKS` | 2,000,000 / 250,000 | chunk size during backfill and compaction |
| `HOT_MAX_ROWS` / `HOT_MAX_AGE_SECS` | 500,000 / 3600 | when finalized hot blocks are chunked |
| `ROW_GROUP_ROWS` / `COMPRESSION` | 32768 / `lz4` | Parquet layout (`lz4`, `zstd<level>`, `none`) |
| `COMPACT_EVERY_SECS` | 300 | compaction interval (0 = off) |
| `LISTEN` / `ADMIN_LISTEN` | `0.0.0.0:3100` / `127.0.0.1:9101` | API / metrics |
| `QUERY_MAX_ROWS` / `QUERY_MAX_MS` | 200,000 / 5000 | page budget caps |
| `QUERY_PARALLELISM` / `QUERY_CONCURRENCY` | cores / 32 | work units per query / queries at once |
| `API_KEYS` | — | comma-separated bearer keys (required when set) |
| `RATE_LIMIT_RPS` / `RATE_LIMIT_BURST` | 0 (off) / 50 | per key, else per IP |
| `TRUST_PROXY` / `CORS_ORIGINS` | false / — | client IP from `X-Forwarded-For`; allowed origins |
| `READY_MAX_LAG` | 10 | `/ready` threshold in blocks |
| `LOG_FORMAT` | text | `json` for structured logs |

## Documentation

- [docs/API.md](docs/API.md): query model, responses, Arrow framing, errors
- [docs/BENCHMARK.md](docs/BENCHMARK.md): method and results vs ClickHouse

## Development

```bash
cargo test --release --workspace      # unit, e2e (fake node: backfill, reorg, restart, compaction), API, client
fakechain 8545 1000 500 &             # deterministic fake node, a block every 500 ms
RPC_URL=http://127.0.0.1:8545 cargo run --release
```
