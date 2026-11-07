-- CLOB history archive (billions to trillions of rows over time).
--
-- Loaded by services/clobarchiver from the node's per-block archive
-- (JSON-RPC clob_getBlockArchive, see evm-node docs/clob-archive-rpc.md) and
-- served by services/api-go under /api/clob/*.
--
-- Write path: the loader inserts heights strictly in order, many heights per
-- INSERT (one part per table per batch, never per-row inserts) into the four
-- base tables (orders, cancels, fills, blocks), then advances
-- clob.archive_progress. Materialized views copy each inserted block,
-- server side, into the re-sorted access-path tables (*_by_trader,
-- fills_by_order, item_hash), so the loader ships every row once.
--
-- Why tables + materialized views and not PROJECTIONs: on 24.8 a projection of
-- a ReplacingMergeTree is not deduplicated when the parent part is (verified:
-- OPTIMIZE FINAL dedups the parent rows but the projection keeps both copies),
-- and lightweight DELETE is refused on tables with projections. Separate
-- ReplacingMergeTree tables each deduplicate on merge on their own key.
--
-- Idempotency: every data table is a ReplacingMergeTree whose sort key ends in
-- the row's identity inside the chain ((height, idx) or (height, seq[, role])),
-- so a re-loaded height collapses into the same rows on merge. On top of that
-- the loader deletes rows above its checkpoint from every table before
-- (re)loading (crash between INSERT and checkpoint, failed partial insert, or
-- an operator rewinding the checkpoint), so normal reads never see duplicates
-- and hot queries run without FINAL; the API adds a cheap LIMIT 1 BY on the
-- few rows it returns as a backstop.
--
-- Partitioning: the base tables (orders, cancels, fills) by day of block
-- time (cheap TTL / drop of old days, partition pruning for block lookups,
-- which always know the block time). The access-path tables (*_by_trader,
-- fills_by_order, item_hash) are read by trader / order / hash, never by
-- time, so they cannot prune: they use month partitions, so a lookup probes
-- ~30x fewer partitions (and parts) than with day partitions.
--
-- Positions: idx = index of the item in the block's orders (or cancels)
-- array, seq = index of the fill in the block. Together with height they are
-- unique, so even a duplicate submission of the same signed order (same hash,
-- e.g. rejected as a duplicate) is kept as its own row.

CREATE DATABASE IF NOT EXISTS clob;

-- One row per executed placement, in block order: block contents and the
-- point read after a hash lookup touch one granule of one partition.
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
    reason          LowCardinality(String)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMMDD(block_time)
ORDER BY (height, idx);

-- "A trader's orders, newest first": same rows sorted by trader.
CREATE TABLE IF NOT EXISTS clob.orders_by_trader AS clob.orders
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(block_time)
ORDER BY (trader, height, idx);

CREATE MATERIALIZED VIEW IF NOT EXISTS clob.orders_by_trader_mv TO clob.orders_by_trader
AS SELECT * FROM clob.orders;

-- Cancels and amends (kind). An amend carries its new price/amount, a cancel
-- has them 0.
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
    reason          LowCardinality(String)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMMDD(block_time)
ORDER BY (height, idx);

CREATE TABLE IF NOT EXISTS clob.cancels_by_trader AS clob.cancels
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(block_time)
ORDER BY (trader, height, idx);

CREATE MATERIALIZED VIEW IF NOT EXISTS clob.cancels_by_trader_mv TO clob.cancels_by_trader
AS SELECT * FROM clob.cancels;

-- Fills (trades), in execution order. Sort key (pair_id, height, seq) is the
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
ORDER BY (pair_id, height, seq);

-- Fills from each participant's side: two rows per fill (role maker and role
-- taker; a self-trade yields both rows for the same trader), so "fills where
-- I am maker OR taker, newest first" is one sorted range read instead of a
-- UNION of two reads merged and re-sorted. order_id is the participant's own
-- order in the fill; is_buy the participant's side.
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
    is_buy        Bool,
    price         UInt64 CODEC(ZSTD(1)),
    qty           UInt64 CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(block_time)
ORDER BY (trader, height, seq, role);

CREATE MATERIALIZED VIEW IF NOT EXISTS clob.fills_by_trader_mv TO clob.fills_by_trader
AS SELECT
    if(r = 0, maker, taker)                         AS trader,
    height,
    seq,
    CAST(toInt8(r), 'Enum8(\'maker\' = 0, \'taker\' = 1)') AS role,
    block_time,
    pair_id,
    if(r = 0, maker_order_id, taker_order_id)       AS order_id,
    if(r = 0, taker, maker)                         AS counterparty,
    if(r = 0, taker_order_id, maker_order_id)       AS counter_order,
    if(r = 0, NOT taker_is_buy, taker_is_buy)       AS is_buy,
    price,
    qty
FROM clob.fills
ARRAY JOIN [0, 1] AS r;

-- "Fills of this order" for the order page, without scanning the trader's
-- whole history: the same rows keyed by (pair, order).
CREATE TABLE IF NOT EXISTS clob.fills_by_order AS clob.fills_by_trader
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(block_time)
ORDER BY (pair_id, order_id, height, seq, role);

CREATE MATERIALIZED VIEW IF NOT EXISTS clob.fills_by_order_mv TO clob.fills_by_order
AS SELECT * FROM clob.fills_by_trader;

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

CREATE MATERIALIZED VIEW IF NOT EXISTS clob.item_hash_orders_mv TO clob.item_hash
AS SELECT hash, height, idx, CAST('order', 'Enum8(\'order\' = 0, \'cancel\' = 1, \'amend\' = 2)') AS kind,
          block_time, trader, pair_id
FROM clob.orders;

CREATE MATERIALIZED VIEW IF NOT EXISTS clob.item_hash_cancels_mv TO clob.item_hash
AS SELECT hash, height, idx, CAST(toString(kind), 'Enum8(\'order\' = 0, \'cancel\' = 1, \'amend\' = 2)') AS kind,
          block_time, trader, pair_id
FROM clob.cancels;

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
-- only, the current value is argMax(height, updated_at) (no FINAL needed). To
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
