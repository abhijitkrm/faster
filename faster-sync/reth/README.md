# faster-sync-reth

A [reth](https://github.com/paradigmxyz/reth) node with faster-sync built in.

An execution extension (ExEx) receives every chain reth commits, reorgs or
reverts and applies it to a faster-sync store; the faster-sync HTTP API is
served from the same process. Blocks and receipts are read through reth's own
`eth` API handlers in-process, straight from its database, so the stored
objects are exactly what reth's JSON-RPC returns. There is no HTTP, no
polling and no rate limit between the node and the store, and reorgs arrive
as reth sees them.

```bash
cargo build --release            # in this directory (pulls reth v2.8.0)

# any reth command line; faster-sync settings come from the environment
FSYNC_DATA_DIR=/data/fsync LISTEN=0.0.0.0:3100 \
  ./target/release/faster-sync-reth node --chain mainnet --datadir /data/reth --http

# local dev chain (1 s blocks)
FSYNC_DATA_DIR=./fsync ./target/release/faster-sync-reth node --dev --dev.block-time 1s --http
```

On start it catches up from reth's database (from `START_BLOCK`, or where
the store left off), then follows reth's chain notifications from its own
head; reth replays anything it missed while stopped. Finality comes from
reth's finalized block (or `FINALITY_DEPTH` below the tip when reth has
none, as in dev mode).

| Variable | Default | |
|---|---|---|
| `FSYNC_DATA_DIR` | `./fsync-data` | faster-sync store |
| `START_BLOCK` | 0 | first block to store (fixed at first start) |
| `FINALITY_DEPTH` | 64 | used when reth reports no finalized block |
| `FETCH_CONCURRENCY` | 16 | blocks read in parallel during catch-up |
| `CHUNK_ROWS`, `HOT_MAX_ROWS`, `COMPACT_EVERY_SECS`, `LISTEN`, `API_KEYS`, … | | as for `faster-sync` |

Call traces are not stored by this source yet (the JSON-RPC source with
`TRACES=true` stores them).
