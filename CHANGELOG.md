# Changelog

All notable changes to this project are documented here.  
Format: **[date] — type: description**

---

## [2026-08-27]

### Added
- **Fasterindexer (`services/fasterindexer`)** — new Go-based indexer that replaces both the Node `block-listener` and `indexer`:
  - Single binary polls `eth_blockNumber`, fetches full blocks + receipts in parallel, and writes directly to Postgres
  - Uses `pgx.CopyFrom` into unlogged staging tables, then `INSERT ... ON CONFLICT` merge into main tables
  - Publishes `blocks:stream` and `transactions:stream` XADDs so the API real-time WebSocket feed keeps working
  - Persists `fasterindexer:lastBlock` in Redis to resume across restarts
  - Health endpoint on `http://localhost:3102/health`
- **TypeScript frontend** — all `src/*.jsx` files converted to `.tsx`, `tsconfig.json` added, build verified
- **Benchmark suite** — `main_test.go` with `BenchmarkNormalize` and `BenchmarkHexToDec` for performance regression testing

### Improved
- **Latest Blocks / Latest Transactions panels** now render the same number of rows (`ROWS = 20` in `Home.jsx`)
- **Transaction log display** in `TxDetail.jsx` now parses stored log JSON, highlights known event signatures (`Transfer`, `Approval`), and decodes the raw data hex
- **Contract address topic display** in logs — indexed address topics are rendered as clickable address links

### Removed
- **Prometheus** removed from `docker-compose.yml` to simplify the stack and avoid a crash loop on non-existent Redis/Postgres exporters

### Fixed
- **Indexer `log_index` null constraint error** — the Node indexer was using `log.index` (undefined) instead of `log.logIndex`, causing every transaction batch to fail and the `transactions` table to stay empty

---

## [2026-03-08]

### Fixed
- **Indexer stream position not persisted** — after every Docker rebuild the indexer reset `lastBlockId` to `'0-0'` and replayed the entire Redis stream from the beginning (~225 k entries). On a ~400 ms block chain the stream grows fast; full re-scan took ~37 minutes, making the DB appear permanently stuck.
  - **Fix**: `lastBlockId` and `lastTxId` are now saved to Redis keys `indexer:lastBlockId` / `indexer:lastTxId` after every committed batch and loaded back on startup — restarts resume in milliseconds.
  - One-time pre-seed of both keys to the correct stream positions was applied before rebuild so the existing backlog was skipped immediately.
  - Verified: DB block height advanced from 1 665 218 → 1 678 881 in under 30 seconds after rebuild.

### Added
- **Full bytecode display on contract pages** — `BytecodeBlock` component in `Address.jsx` renders contract bytecode as 32-byte rows with hex offset counters, alternating byte highlighting, scrollable pre (max 320 px), and a one-click copy button.
- **Stats heartbeat in API** — `setInterval(5000)` in `api/src/index.js` broadcasts a `stats` WebSocket message unconditionally every 5 s so clients always get fresh metrics even during low-block-activity periods.
- **REST fallback poll in WSContext** — if the WebSocket stats message hasn't arrived, the frontend polls `GET /stats` every 5 s via `setInterval` as a fallback, ensuring the metrics bar never goes blank.

---

## [2026-03-07]

### Added
- **Contract-aware address page** — address page now auto-detects whether an address is a contract via `eth_getCode` RPC call:
  - **EOA**: blue "EOA" badge, First Seen stat card, normal transaction history
  - **Contract**: purple "Contract" badge, Bytecode Size stat card (in bytes), "Contract Info" panel with creator address link, deploy transaction link, and deploy block link + timestamp
  - Transaction direction now also shows **CREATE** (purple) and **SELF** (yellow) badges in addition to IN/OUT
  - "Transaction History" renamed to "Contract Interactions" for contracts
  - `to_address = null` rows show "Contract Create" instead of a broken link
- **`eth_getCode` in API** — `addresses.js` now calls `eth_getCode` in parallel with balance fetch to determine `is_contract` and `bytecode_size` from live RPC; overrides stale DB flag
- **Deploy info query** — API finds the earliest transaction directed at a contract address to surface creator + deploy tx + deploy block

### Fixed
- **Block list out-of-order display** — blocks arriving out of order over WebSocket (due to parallel 50-block batch fetching) were naively prepended, causing e.g. block 115 to appear above 112. WSContext now deduplicates by hash and sorts descending by block number on every `block` event and on `init`.
- **Block list ordering on init** — initial payload from the API was not sorted; now sorted descending by `number` before storing in state.

### Added (earlier today)
- **Search bar** — global search in the header auto-detects input type and redirects:
  - `0x` + 40 hex chars → `/address/:address`
  - `0x` + 64 hex chars → probes tx hash first, falls back to block hash
  - Pure integer → `/blocks/:number`
  - Shows live type badge (Address / Tx+Block hash / Block #) as you type
  - Spinner during API probe, inline error on not-found
- **Smooth metric counters** — header stats bar now animates instead of jumping:
  - Block Height & Total Blocks use `useCountUp` (steps ±1 per tick at ~25 steps/sec, snaps if gap > 50)
  - TPS & Avg Block Time use `useLerp` (exponential ease, 10% per frame)


---

## [2026-03-06]

### Added
- **Transaction detail page** (`/tx/:hash`) — overview panel with hash, block link, timestamp + age, from/to address links, value (ETH), gas limit/used + %, gas price (Gwei), nonce, expandable input data, status banner (SUCCESS / FAILED / PENDING), and decoded logs section (topics, data per log).
- **`/tx/:hash` route** registered in `App.jsx`.
- **Tx hash links** — hash cells in `TransactionList.jsx` and `Home.jsx` now link to `/tx/:hash`.
- **Block number links** in `TransactionList.jsx` now link to `/blocks/:number` (were pointing to `/blocks`).

### Fixed
- **Logs missing `topics` field** — `transactions.js` route now includes `topics` in the logs SELECT query.

---

## [2026-03-05]

### Added
- **Block detail page** (`/blocks/:number`) — overview panel (hash, parent hash link, timestamp, miner, tx count, gas used/limit bar), transactions table, Prev/Next navigation.
- **`/blocks/:number` route** registered in `App.jsx`.
- **Block number links** everywhere (Home, BlockList, TransactionList) now navigate to the detail page.
- **Address page overhaul** — live ETH balance via `eth_getBalance` RPC call, IN/OUT direction badge, age from `block_timestamp`, copy-to-clipboard button, pagination (25/page).
- **Light mode** — full `[data-theme="light"]` CSS override set; ☀/☾ toggle button in header persisted to `localStorage`.
- **`RPC_URL` env var** added to API service in `docker-compose.yml` to support live balance lookups.

### Fixed
- **`varchar = bigint` SQL error** in `blocks.js` — `WHERE number = $1 OR hash = $1` split into two separate queries based on whether the param starts with `0x`.
- **Blocks page stale data** — `/blocks` page switched from 5 s REST poll to `useWS()` + 1 s ticker.
- **Age column layout shift** — time span given `display:inline-block; min-width:74px` to prevent width jumping.

### Improved
- `WSContext` `MAX_BLOCKS` widened from 20 → 50.
- API `blocks.js` query returns richer tx fields: `gas`, `gas_price`, `gas_used`, `status`, `nonce`, `input_data`, `block_timestamp`.
- API `addresses.js` returns richer tx fields including `block_timestamp`, with pagination (`?page=&limit=`).

---

## [2026-03-04]

### Added
- **README** fully rewritten for public GitHub: removed hardcoded IPs/paths, added clone + setup instructions, environment variable documentation.
- **`.env.example`** with placeholder values for all required environment variables.

### Fixed
- **TPS calculation** — changed from 101-block average to `latest_block.transactions_count ÷ avg_block_time(last 20 blocks)` for a more responsive instantaneous reading. `avg_block_time` metric continues to use a 101-block window.

---

## [2026-03-03]

### Fixed
- **Block listener fetching 0 transactions** — rewrote listener to use raw `fetch()` JSON-RPC instead of ethers.js. Now calls `eth_getBlockByNumber(hex, true)` and `eth_getBlockReceipts(hex)` in parallel per block.

### Improved
- Block listener processes up to 50 blocks in parallel on catch-up.
- Redis checkpoint key `listener:lastBlock` tracks progress across restarts.
