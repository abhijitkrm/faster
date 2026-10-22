# CLOB history archive

Long-term, queryable history of every CLOB order, cancel/amend and fill, built
for billions to trillions of rows.

```
node (CLOB_ARCHIVE=1)          services/clobarchiver            ClickHouse (clob.*)          services/faster         frontend
per-block spool  ──JSON-RPC──▶  N fetchers, in-order batches ──▶ base tables + MV copies ──▶ /api/clob/*  ──────▶ /clob/... pages
clob_archiveLatest              checkpoint after insert          ReplacingMergeTree           (503 if CH unset)     block page section
clob_getBlockArchive            gaps, repair, retries, metrics                                                      order-hash search
```

## Data contract

The node side is documented in evm-node `docs/clob-archive-rpc.md`:
`clob_archiveLatest` → `{enabled, oldest, latest}`; `clob_getBlockArchive [height]` →
`{height, time_ns, orders[], cancels[], fills[]}` or `null` when the spool does not hold the
height. The archiver relies on these properties:

- integers are JSON numbers in uint64/int64 range (decoded straight into Go integers);
- `hash` / `trader` / `maker` / `taker` are `0x` hex (32 / 20 bytes);
- order `status` ∈ open | partial | filled | rejected; cancel `kind` ∈ cancel | amend,
  `status` ∈ cancelled | amended | rejected (anything else stops loading loudly);
- a non-null archive for height *h* has `height == h`, and heights `<= latest` never change;
- block time is non-decreasing in height (used to prune repair deletes to recent partitions);
- `fills[].seq` is unique within the block; the position of an order/cancel in its array is
  its identity (`idx`) — a duplicate submission of the same signed hash is its own item.

## ClickHouse schema (`database/clickhouse/01-clob.sql`)

| table | sort key | partition | serves |
|---|---|---|---|
| `orders` | (height, idx) | day | block contents, point read after a hash lookup |
| `orders_by_trader` (MV) | (trader, height, idx) | month | trader's orders newest first |
| `cancels` | (height, idx) | day | block contents |
| `cancels_by_trader` (MV) | (trader, height, idx) | month | trader's cancels/amends |
| `fills` | (pair_id, height, seq) | day | pair trade tape newest first; block fills (time pinned) |
| `fills_by_trader` (MV, 2 rows per fill) | (trader, height, seq, role) | month | trader fills as maker or taker |
| `fills_by_order` (MV) | (pair_id, order_id, height, seq, role) | month | fills of one order |
| `item_hash` (MVs) | (hash, height, kind, idx) | month | order / cancel / amend by hash |
| `blocks` | height | — | per-block counts, block time, stats |
| `archive_progress` | id | — | loader checkpoint (`argMax(height, updated_at)`) |
| `gaps` | (from_height, to_height) | — | heights the node did not hold |

All data tables are `ReplacingMergeTree` keyed by the row's chain identity, so a re-loaded
height collapses on merge. The archiver inserts only the four base tables; materialized views
derive the re-sorted copies server side. Projections were rejected: on 24.8 a projection of a
ReplacingMergeTree keeps duplicates after the parent part is deduplicated, and lightweight
DELETE is refused on tables with projections.

**Idempotency.** On start, and before retrying a failed insert, the archiver deletes every
row above its checkpoint from every table (`ALTER TABLE … DELETE WHERE height > cp AND
block_time >= t(cp)`, only when such rows exist), then reloads from `cp + 1`. A crash between
insert and checkpoint, a partially applied batch or a deliberate rewind (insert a lower
height into `clob.archive_progress`) therefore never duplicates rows. API list queries also
apply `LIMIT 1 BY <identity>` as a cheap backstop; `/api/clob/stats` counts come from part
metadata and would include not-yet-merged duplicates if rows were ever inserted around the
archiver.

## clobarchiver

Loop: read checkpoint → repair → `clob_archiveLatest` → fetch `cp+1 .. latest` with
`FETCH_WORKERS` concurrent fetchers inside a bounded window → take results strictly in
height order → flush a batch when it reaches `BATCH_MAX_ROWS` rows or `BATCH_MAX_HEIGHTS`
heights (or after `BATCH_MAX_WAIT_MS` when caught up) as one INSERT per table → advance the
checkpoint. `null` heights are recorded in `clob.gaps` as coalesced ranges (`missing`);
heights below the node's `oldest` become one `unavailable` range without being fetched. RPC
and ClickHouse errors are retried with exponential backoff (200 ms → 10 s). SIGINT/SIGTERM
flush the already-ordered batch and exit. Run one archiver per database.

| env | default | |
|---|---|---|
| `NODE_RPC` | `http://127.0.0.1:8545` | node JSON-RPC URL |
| `RPC_TIMEOUT_MS` | `30000` | per request |
| `CLICKHOUSE_ADDR` | `127.0.0.1:9000` | native protocol |
| `CLICKHOUSE_DB` / `CLICKHOUSE_USER` / `CLICKHOUSE_PASSWORD` | `clob` / `default` / empty | |
| `CLICKHOUSE_DSN` | — | `clickhouse://user:pass@host:9000/clob`, overrides the above |
| `FETCH_WORKERS` | `8` | concurrent `clob_getBlockArchive` calls |
| `FETCH_WINDOW` | `4 × workers` | max heights fetched ahead of the inserter |
| `BATCH_MAX_ROWS` | `100000` | rows sent per batch (blocks + orders + cancels + fills) |
| `BATCH_MAX_HEIGHTS` | `1000` | heights per batch |
| `BATCH_MAX_WAIT_MS` | `2000` | flush a partial batch after this when caught up |
| `MAX_RANGE` | `100000` | heights per `clob_archiveLatest` refresh |
| `POLL_MS` | `1000` | poll interval at the tip |
| `START_HEIGHT` | — | first height when no checkpoint exists |
| `METRICS_ADDR` | `:9102` | `/metrics` (Prometheus text), `/health` |
| `LOG_LEVEL` | `info` | `debug` logs every flush |

A `progress` log line every 10 s reports heights/s, rows/s, checkpoint, node latest and lag.

## API

Served by `services/faster` (`src/clob.rs`, ported from the former api-go with identical
routes and JSON; verified response-for-response on the shared fixtures). Enabled by
`CLICKHOUSE_URL` (HTTP interface, e.g. `http://clickhouse:8123`) with `CLICKHOUSE_DB`
(default `clob`), `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`.

Without ClickHouse configured every `/api/clob/*` endpoint answers `503 {"error": …}`.
JSON conventions: heights, `pair_id`, `idx`, `seq` are numbers; every other chain integer
(prices, amounts, filled, qty, order/client IDs, nanosecond timestamps, row counts) is a
decimal string; hashes/addresses are lowercase `0x` hex; `block_time` is RFC 3339 UTC plus
`block_time_ns`. Lists return `{"items": [...], "next_before": "<height>-<pos>" | null}`,
newest first; pass `next_before` as `?before=` for the next page (`?before=<height>` means
strictly below that height). `limit` defaults to 50 and is capped at 500.

| endpoint | response |
|---|---|
| `GET /api/clob/orders/{hash}` | `{hash, kind: order\|cancel\|amend, order?, cancel?, fills: [trader fill], occurrences: [{kind, height, idx, status}]}` |
| `GET /api/clob/traders/{addr}/orders` | list of orders |
| `GET /api/clob/traders/{addr}/fills` | list of trader fills `{height, seq, role, trader, pair_id, order_id, counterparty, counter_order_id, is_buy, price, qty, …}` |
| `GET /api/clob/traders/{addr}/cancels` | list of cancels/amends |
| `GET /api/clob/pairs/{pairId}/trades` | list of fills `{height, seq, pair_id, maker, taker, taker_is_buy, maker_order_id, taker_order_id, price, qty, …}` |
| `GET /api/clob/blocks/{height}?limit=` | `{height, block_time, counts, orders, cancels, fills, truncated}`; 404 with `gap` / `not_loaded_yet` / `latest_loaded_height` |
| `GET /api/clob/stats` | `{latest_loaded_height, latest_block_time, checkpoint_updated_at, blocks, orders, cancels, fills, gap_ranges, gap_heights}` |

Order fields: `kind, height, idx, block_time, block_time_ns, hash, trader, pair_id,
client_order_id, order_id, is_buy, is_market, price, amount, timestamp_ns, status, filled,
reason`. Cancel fields: `kind, height, idx, block_time, block_time_ns, hash, trader, pair_id,
client_id, target_order_id, target_client, price, amount, status, reason`.

## UI

`/clob/order/:hash`, `/clob/trader/:addr` (tabs orders / fills / cancels), `/clob/pair/:id`,
and a CLOB section on `/blocks/:number` that loads when scrolled into view. Searching a
32-byte hash that is neither a tx nor a block tries the order-hash endpoint. Address pages
link to the trader's CLOB history.

## Run locally

```bash
# ClickHouse (applies database/clickhouse/*.sql on first start; ports bound to 127.0.0.1)
docker compose up -d clickhouse

# archiver against a node with CLOB_ARCHIVE=1 ...
cd services/clobarchiver
NODE_RPC=http://127.0.0.1:8545 CLICKHOUSE_ADDR=127.0.0.1:9000 \
CLICKHOUSE_USER=clob CLICKHOUSE_PASSWORD=clob go run .

# ... or against synthetic data (heights 1..400, gaps at 150-152 and 301)
go run ./cmd/fakenode -addr 127.0.0.1:18545 -latest 400 -gaps 150,151,152,301 &
NODE_RPC=http://127.0.0.1:18545 CLICKHOUSE_USER=clob CLICKHOUSE_PASSWORD=clob go run .

# API with the CLOB endpoints
cd ../faster
RPC_URL=http://127.0.0.1:8545 CLICKHOUSE_URL=http://127.0.0.1:8123 \
CLICKHOUSE_USER=clob CLICKHOUSE_PASSWORD=clob cargo run --release
curl localhost:3000/api/clob/stats

# Everything in compose (archiver reads NODE_RPC, default host.docker.internal:8545)
docker compose up -d
```

Tests: `go test ./...` in `services/clobarchiver` and `cargo test` in `services/faster`;
add `CLOB_CH_TEST=1` (with ClickHouse up; `CLICKHOUSE_URL`, `CLICKHOUSE_USER`,
`CLICKHOUSE_PASSWORD`) to run the integration tests, which create and drop their own
databases.
