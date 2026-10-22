# faster: hot store, indexer and explorer API

`services/faster` is one Rust process that follows an EVM chain over JSON-RPC,
keeps an embedded RocksDB store, and serves the explorer REST API and live
WebSocket feed from it. It replaces `fasterindexer` + `api-go` + Postgres +
Redis on the explorer path. Data never crosses a network hop to answer a
request (see `STORAGE_BENCHMARK.md` for why).

## Key layout

All integers are big-endian so keys sort numerically. `^x` is the bitwise
complement, which turns an ascending key order into newest-first.

| Column family | Key | Value |
|---|---|---|
| `meta` | `head`, `counters`, `version` | head (number, hash), totals, schema version |
| `blocks` | number u64 | block header record |
| `block_hash` | hash 32 | number |
| `txs` | number u64 ‖ index u32 | transaction + receipt fields |
| `tx_hash` | hash 32 | number ‖ index |
| `logs` | number u64 ‖ block log index u32 | log (tx index, address, topics, data) |
| `addr_tx` | address 20 ‖ ^number ‖ ^index | role bits (from / to / created) |
| `addr` | address 20 | tx count, first/last seen, contract flag, creating tx |
| `undo` | number u64 | address and counter state before the block |

Records are bincode; hashes and addresses are raw bytes; 256-bit values are
minimal big-endian bytes. `block_hash`, `tx_hash` and `addr` have bloom
filters. LZ4 compression everywhere.

Access paths:

- tx by hash: 2 point reads. Block by number: 1 read + 1 range scan for its txs.
- address history: one prefix scan of `addr_tx` (already newest-first) + a
  multi-get of the txs. No sort, no count query: `addr.tx_count` is the total.
- latest blocks / txs: reverse iteration from the end of `blocks` / `txs`.

## Write path

1. The indexer reads the node head, plans a window of blocks and fetches them
   in JSON-RPC batches (`FETCH_BATCH` blocks per request, `FETCH_CONCURRENCY`
   requests in flight). Each block is fetched with `eth_getBlockByNumber(n,
   true)` + `eth_getBlockReceipts(n)` (falls back to per-tx receipts if the
   node lacks that method).
2. A block is only accepted whole: a null block, a missing receipt, or a
   receipt for another tx is "not ready" and is retried with backoff (100 ms
   to 5 s). Nothing partial is ever written.
3. Blocks are committed in order, up to `COMMIT_BLOCKS` per commit, on a
   committer that runs alongside fetching. Each commit has two phases:
   - data rows (blocks, hashes, txs, logs, address history) are written as
     several batches from parallel threads (RocksDB inserts each batch into
     its memtable on its own thread), without fsync. Rewriting the same block
     produces identical rows, so these writes are idempotent;
   - then one fsynced batch with address info, counters, undo records and the
     new head. The head key *is* the checkpoint.

   A crash between the phases can leave rows above the head (or, after a
   later reorg, rows of an orphaned block). Reads never look above the head,
   bound a block's txs by its `tx_count`, and check that hash-index hits and
   address-history hits really match, so such rows are never served and are
   overwritten when the block is indexed again.
   Blocks more than `REORG_DEPTH` below the node's tip get no undo record:
   they can never be rolled back, and skipping them makes catch-up cheaper.
4. Address `tx_count` counts distinct transactions: the `addr_tx` key is
   unique per (address, tx), so a self-transfer counts once.

## Reorgs

- Every block's `parent_hash` must equal the stored head hash. A mismatch
  means the chain reorganised.
- While idle at the tip, the indexer also compares its head hash with the
  node's, catching a replaced head with nothing built on it yet.
- To resolve, it walks back: while the stored head differs from the node's
  canonical block at that height, it rolls the head back. Rollback deletes the
  block's keys and restores address/counter state from the undo record, in one
  atomic write.
- Undo records are kept for `REORG_DEPTH` blocks (default 256). A deeper reorg
  stops the process with an error (resync needed) instead of serving a mixed
  chain. `CONFIRMATIONS=n` makes the indexer stay n blocks behind the tip.
- After a reorg, every WebSocket client gets a fresh `init` snapshot.

## API

Same routes and JSON field names as the old api-go, so the UI is unchanged:
`/health`, `/ready` (503 until within `READY_MAX_LAG` blocks of the node),
`/stats`, `/api/blocks`, `/api/blocks/range/{start}/{end}`,
`/api/blocks/{number|hash}`, `/api/transactions`,
`/api/transactions/{hash}`, `/api/transactions/address/{addr}`,
`/api/addresses/{addr}?page&limit`, `/api/addresses/search/by-pattern`,
WebSocket on `/`. `/api/clob/*` is proxied to `CLOB_API_URL` (api-go) until
the CLOB API moves into this process.

Field changes from api-go: `gas_price` is the price actually paid
(`effectiveGasPrice` when the node reports it); new fields
`effective_gas_price`, `transaction_index`, `logs_count`, `base_fee_per_gas`;
block transactions are ordered by index (api-go sorted by nonce); invalid
input returns 400 instead of silently reading block 0; `/api/addresses`
(global ranking) returns 404 because there is no ranking index yet.

WebSocket: each client is served by one task that owns the socket (no
concurrent writes). The server pings every 20 s and drops clients that are
silent for 60 s, block a write for 10 s, or fall more than 256 messages behind;
the UI reconnects and receives `init`. Per commit at most 50 `block` and 20
`tx` messages are sent, plus one `stats`. `stats` is computed once per commit
(and every 5 s) and shared by all clients and `/stats`.

## Configuration

| Env | Default | |
|---|---|---|
| `RPC_URL` | `http://localhost:8545` | node JSON-RPC |
| `DATA_DIR` | `./data` | store lives in `$DATA_DIR/hot` |
| `PORT` | 3000 | |
| `START_BLOCK` | 0 | first block when the store is empty |
| `CONFIRMATIONS` | 0 | blocks to stay behind the tip |
| `REORG_DEPTH` | 256 | undo history kept |
| `FETCH_BATCH` | 10 | blocks per JSON-RPC batch |
| `FETCH_CONCURRENCY` | 16 | batches in flight |
| `FETCH_WINDOW` | 2000 | blocks planned per round |
| `COMMIT_BLOCKS` | 500 | max blocks per atomic write |
| `POLL_MS` | 250 | head poll interval when caught up |
| `CACHE_MB` | 512 | RocksDB block cache |
| `RPC_TIMEOUT_MS` | 15000 | per JSON-RPC request |
| `READY_MAX_LAG` | 10 | `/ready` threshold |
| `CLOB_API_URL` | unset | api-go base URL for `/api/clob/*` |
| `LOG_LEVEL` / `RUST_LOG` / `LOG_FORMAT` | info / – / json | `LOG_FORMAT=text` for humans |

## Measurements (2026-10-22, M1 Pro laptop)

Indexer ceiling, synthetic node (`target/release/fakenode`, 500 txs/block,
20% with two Transfer logs, ~200K skewed addresses), fresh store:

| Change | tx/s |
|---|---|
| P0 baseline (single batch, decode on the commit task) | 122K |
| fetch + decode in spawned tasks | 145K |
| commits pipelined with fetching | 154K |
| varint records (size −23%, speed unchanged) | 154K |
| data rows written from parallel threads, state batch last | 176–187K |
| state batch built during data writes, no undo below REORG_DEPTH | 236K |
| 1/1/2 writer layout, 16 fetches in flight (3 runs) | 240–281K |
| same, 3M txs (6,000 blocks) | 253K sustained, 1.2 GB |

Finer sharding (3 writers per table) was slower: 218K. At this point faster
and the synthetic node share the 8 cores; faster uses ~2.3 cores.

### P0 run against anvil

Chain: 2,202 blocks, 1,100,001 txs (500 per block, 20% contract calls emitting
a log), 1,350 addresses, one address in 621,827 txs.

- Fresh index: 50.8 s = 43 blocks/s = 21.7K tx/s. The node was the limit:
  fetching the same blocks + receipts with no processing ran at 48 blocks/s
  (857 MB of JSON per 1,000 blocks). faster used 8.9 s of CPU in total for
  the run. Store size 473 MB.
- `kill -9` during catch-up, restart: resumed from the last committed head,
  counters exact.
- anvil `anvil_reorg 5`: detected while idle, 5 blocks rolled back in ~2 ms,
  re-indexed; all block hashes match the node afterwards.
- API, one connection (server time, mean): tx detail 0.04 ms; address page
  (25 txs, hot address, incl. two node calls for balance/code) 0.25 ms; block
  with 500 txs (267 KB JSON) 1.5 ms.
- API, 32 connections (`ab -k`): `/stats` 91K req/s, tx detail 96K req/s,
  latest 50 txs 30K req/s, hot address page 1 19K req/s, block with 500 txs
  3.8K req/s, hot address page 400 (offset 10,000) 3.7K req/s. No failures.
