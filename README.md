# faster-sync

Bulk historical data API for EVM chains: ask for exactly the blocks, transactions,
receipts, logs and traces you need across any block range, get them back as columnar
data in pages. Built so an indexer can catch up on months of history in seconds instead
of millions of `eth_getLogs` calls.

Part of the Faster stack: [faster](https://github.com/abhijitkrm/faster) (explorer) ·
faster-sync · [faster-rpc](https://github.com/abhijitkrm/faster-rpc) ·
[faster-index](https://github.com/abhijitkrm/faster-index).

Status: design. Clean-room implementation, no code from other projects.

## Query model

```jsonc
POST /query
{
  "from_block": 0,
  "to_block": null,                       // null = up to the archive head
  "logs": [{ "address": ["0x…"], "topics": [["0xddf252ad…"], [], ["0x…"]] }],
  "transactions": [{ "from": ["0x…"], "to": [], "sighash": ["0xa9059cbb"], "status": 1 }],
  "traces": [{ "to": ["0x…"], "call_type": ["call"] }],
  "join": "logs_with_transactions",        // also pull the tx (and block) behind each match
  "fields": { "log": ["block_number","log_index","address","topics","data"], "transaction": ["hash","from","to","value"] },
  "max_rows": 100000
}
```

Response: an Arrow IPC stream (JSON for debugging) with one table per selected kind, plus
`next_block` to continue from. Each request has a time and size budget, so the server
returns what it scanned and the client continues.

## Storage design

- History is cut into immutable chunks of block ranges. Each chunk stores blocks,
  transactions, receipts, logs and traces as compressed columns (Arrow / Parquet).
- Every chunk has small filters (bloom / xor) over addresses, topics 0–3, from, to and
  function selectors. A query skips every chunk that cannot match, so "one contract across
  all history" reads only the chunks that contain it.
- Immutable chunks replicate and cache trivially (and can move to object storage when old).
- Only finalized blocks enter chunks; the newest blocks are served from the hot tier
  (RocksDB, shared design with the faster explorer), so reorgs never touch chunk files.

## Plan

1. Benchmark the chunk design (in-process, DataFusion over Parquet with per-chunk filters)
   against ClickHouse for the three query shapes that matter: a rare contract across all
   history, a busy contract, and one event signature across all contracts. Measure rows/s
   to the client and time to first page.
2. Ingest: parallel backfill + live tail from JSON-RPC (reusing faster's fetcher), later a
   direct feed from evm-node.
3. Query engine + HTTP API with budgets and `next_block` paging.
4. Clients: Rust first, then TypeScript (used by faster-index), then Python.
5. Ops: metrics, chunk compaction, retention / object storage, replicas.
