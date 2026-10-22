-- CLOB history archive (billions to trillions of rows over time).
--
-- Loaded by services/clobarchiver from the node's per-block archive
-- (JSON-RPC clob_getBlockArchive, see evm-node docs/clob-archive-rpc.md) and
-- served by services/api-go under /api/clob/*.
--
-- Write path: the loader inserts heights strictly in order, many heights per
-- INSERT (one part per table per batch, never per-row inserts), then advances
-- clob.archive_progress. Data arrives in height order, so parts of the
-- (height, ...)-sorted tables do not overlap and merges are cheap.
--
-- Idempotency: every data table is a ReplacingMergeTree whose sort key is the
-- row's identity inside the chain ((height, idx) or (height, seq) plus the
-- access-path prefix), so a re-loaded height collapses into the same rows on
-- merge. On top of that the loader deletes rows above its checkpoint before
-- (re)loading (crash between INSERT and checkpoint, or an operator rewinding
-- the checkpoint), so normal reads never see duplicates and the hot queries
-- run without FINAL; the API adds a cheap LIMIT 1 BY on the few rows it
-- returns as a backstop.
--
-- Partitioning: data tables by day of block time (cheap TTL / drop of old
-- days, partition pruning for block lookups, which always know the block
-- time). The hash lookup table by month (lookups by hash cannot prune, so
-- fewer, larger partitions keep the per-lookup index probes low).
--
-- Positions: idx = index of the item in the block's orders (or cancels)
-- array, seq = index of the fill in the block. Together with height they are
-- unique, so even a duplicate submission of the same signed order (same hash,
-- e.g. rejected as a duplicate) is kept as its own row.

CREATE DATABASE IF NOT EXISTS clob;

-- One row per executed placement.
-- Main order (height, idx): block contents and the point read after a hash
-- lookup touch one granule. Projection by_trader serves "a trader's orders,
-- newest first" (a projection rather than a second table so the two copies are
-- written atomically by the same INSERT).
CREATE TABLE IF NOT EXISTS clob.orders
(
    height          UInt64 CODEC(Delta, ZSTD(1)),
    idx             UInt32 CODEC(Delta, ZSTD(1)),
    block_time      DateTime64(9, 'UTC') CODEC(Delta, ZSTD(1)),
    hash            FixedString(32) CODEC(ZSTD(1)),
    trader          FixedString(20) CODEC(ZSTD(1)),
    pair_id         UInt32 CODEC(ZSTD(1)),
    client_order_id UInt64 CODEC(ZSTD(1)),
    order_id        UInt64 CODEC(Delta, ZSTD(1)),
    is_buy          Bool,
    is_market       Bool,
    price           UInt64 CODEC(ZSTD(1)),
    amount          UInt64 CODEC(ZSTD(1)),
    timestamp_ns    Int64 CODEC(Delta, ZSTD(1)),
    status          Enum8('open' = 1, 'partial' = 2, 'filled' = 3, 'rejected' = 4, 'cancelled' = 5, 'amended' = 6),
    filled          UInt64 CODEC(ZSTD(1)),
    reason          LowCardinality(String),
    PROJECTION by_trader (SELECT * ORDER BY trader, height, idx)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMMDD(block_time)
ORDER BY (height, idx)
SETTINGS index_granularity = 8192,
         deduplicate_merge_projection_mode = 'rebuild';

-- Cancels and amends (kind). An amend carries its new price/amount; a cancel
-- has them 0. Same layout and projection as orders.
CREATE TABLE IF NOT EXISTS clob.cancels
(
    height          UInt64 CODEC(Delta, ZSTD(1)),
    idx             UInt32 CODEC(Delta, ZSTD(1)),
    block_time      DateTime64(9, 'UTC') CODEC(Delta, ZSTD(1)),
    kind            Enum8('cancel' = 1, 'amend' = 2),
    hash            FixedString(32) CODEC(ZSTD(1)),
    trader          FixedString(20) CODEC(ZSTD(1)),
    pair_id         UInt32 CODEC(ZSTD(1)),
    client_id       UInt64 CODEC(ZSTD(1)),
    target_order_id UInt64 CODEC(ZSTD(1)),
    target_client   UInt64 CODEC(ZSTD(1)),
    price           UInt64 CODEC(ZSTD(1)),
    amount          UInt64 CODEC(ZSTD(1)),
    status          Enum8('open' = 1, 'partial' = 2, 'filled' = 3, 'rejected' = 4, 'cancelled' = 5, 'amended' = 6),
    reason          LowCardinality(String),
    PROJECTION by_trader (SELECT * ORDER BY trader, height, idx)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMMDD(block_time)
ORDER BY (height, idx)
SETTINGS index_granularity = 8192,
         deduplicate_merge_projection_mode = 'rebuild';

-- Fills (trades), in execution order. Main order (pair_id, height, seq) is the
-- pair trade tape, newest first (read in reverse key order). A block's fills
-- are read with the block's time pinned (one partition), then the per-pair
-- ranges of that partition.
CREATE TABLE IF NOT EXISTS clob.fills
(
    height         UInt64 CODEC(Delta, ZSTD(1)),
    seq            UInt32 CODEC(Delta, ZSTD(1)),
    block_time     DateTime64(9, 'UTC') CODEC(Delta, ZSTD(1)),
    pair_id        UInt32,
    maker          FixedString(20) CODEC(ZSTD(1)),
    taker          FixedString(20) CODEC(ZSTD(1)),
    taker_is_buy   Bool,
    maker_order_id UInt64 CODEC(ZSTD(1)),
    taker_order_id UInt64 CODEC(ZSTD(1)),
    price          UInt64 CODEC(ZSTD(1)),
    qty            UInt64 CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMMDD(block_time)
ORDER BY (pair_id, height, seq)
SETTINGS index_granularity = 8192;

-- Fills from each participant's side: two rows per fill (role maker and role
-- taker; a self-trade yields both rows for the same trader). A separate table
-- rather than two projections on clob.fills because "fills where I am maker OR
-- taker, newest first" is then one sorted range read instead of a UNION of two
-- projection reads merged and re-sorted. order_id is the participant's own
-- order in the fill; projection by_order serves "fills of this order" for the
-- order page without scanning the trader's whole history.
CREATE TABLE IF NOT EXISTS clob.fills_by_trader
(
    trader        FixedString(20) CODEC(ZSTD(1)),
    height        UInt64 CODEC(Delta, ZSTD(1)),
    seq           UInt32 CODEC(Delta, ZSTD(1)),
    role          Enum8('maker' = 0, 'taker' = 1),
    block_time    DateTime64(9, 'UTC') CODEC(Delta, ZSTD(1)),
    pair_id       UInt32,
    order_id      UInt64 CODEC(ZSTD(1)),
    counterparty  FixedString(20) CODEC(ZSTD(1)),
    counter_order UInt64 CODEC(ZSTD(1)),
    is_buy        Bool,          -- the participant's side
    price         UInt64 CODEC(ZSTD(1)),
    qty           UInt64 CODEC(ZSTD(1)),
    PROJECTION by_order (SELECT * ORDER BY pair_id, order_id, height, seq, role)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMMDD(block_time)
ORDER BY (trader, height, seq, role)
SETTINGS index_granularity = 8192,
         deduplicate_merge_projection_mode = 'rebuild';

-- Point lookup by item hash (orders, cancels and amends): narrow rows sorted
-- by hash pointing at the item's (block_time, height, idx), so the follow-up
-- read of clob.orders / clob.cancels prunes to one partition and one granule.
CREATE TABLE IF NOT EXISTS clob.item_hash
(
    hash       FixedString(32),
    height     UInt64,
    idx        UInt32,
    kind       Enum8('order' = 0, 'cancel' = 1, 'amend' = 2),
    block_time DateTime64(9, 'UTC'),
    trader     FixedString(20),
    pair_id    UInt32
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(block_time)
ORDER BY (hash, height, kind, idx)
SETTINGS index_granularity = 4096;

-- Per-block summary: one row for every loaded height (also empty ones), so
-- "loaded, nothing there" differs from "not loaded". Gives the block's time
-- for partition pruning, the block page counts, and stats.
CREATE TABLE IF NOT EXISTS clob.blocks
(
    height     UInt64 CODEC(Delta, ZSTD(1)),
    block_time DateTime64(9, 'UTC') CODEC(Delta, ZSTD(1)),
    orders     UInt32,
    cancels    UInt32,
    fills      UInt32,
    rejected   UInt32
)
ENGINE = ReplacingMergeTree
ORDER BY height;

-- Loader checkpoint: the highest height whose rows are fully inserted. Append
-- only; the current value is argMax(height, updated_at) (no FINAL needed). To
-- rewind, insert a lower height: the loader deletes rows above it and reloads.
CREATE TABLE IF NOT EXISTS clob.archive_progress
(
    id         UInt8,
    height     UInt64,
    updated_at DateTime64(6, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY id;

-- Heights the node did not hold, as inclusive ranges. reason 'missing': the
-- node returned null for a height inside [oldest, latest] (crash gap), and
-- 'unavailable': below the node's oldest retained height when the loader got
-- there (pruned, or archiving enabled later).
CREATE TABLE IF NOT EXISTS clob.gaps
(
    from_height UInt64,
    to_height   UInt64,
    reason      Enum8('missing' = 1, 'unavailable' = 2),
    detected_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(detected_at)
ORDER BY (from_height, to_height);
