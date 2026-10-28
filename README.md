# Faster

Home of the Faster stack: a real-time EVM block explorer plus a data layer
that serves chain history from pre-built indexes instead of a node. This repo
is the entry point. The code lives in the four repos below.

## Repos

| Repo | Language | Latest | What it is |
|---|---|---|---|
| [faster](https://github.com/abhijitkrm/faster) | Rust + web UI | [v1.0.0](https://github.com/abhijitkrm/faster/releases/tag/v1.0.0) | Real-time block explorer: indexer, REST API and WebSocket feed in one process over an embedded RocksDB store |
| [faster-sync](https://github.com/abhijitkrm/faster-sync) | Rust | [v0.3.0](https://github.com/abhijitkrm/faster-sync/releases/tag/v0.3.0) | Bulk historical data API: filtered blocks, txs, receipts, logs and traces across any block range, streamed as JSON or Arrow pages. Includes `faster-sync-reth`, a reth execution extension |
| [faster-rpc](https://github.com/abhijitkrm/faster-rpc) | Rust | [v0.2.0](https://github.com/abhijitkrm/faster-rpc/releases/tag/v0.2.0) | Read-only Ethereum JSON-RPC served from faster-sync: archive-speed reads, uncapped `eth_getLogs`, WebSocket subscriptions; state methods forwarded upstream |
| [faster-index](https://github.com/abhijitkrm/faster-index) | TypeScript | [v0.1.0](https://github.com/abhijitkrm/faster-index/releases/tag/v0.1.0) | Indexing framework: config + event handlers → entities → GraphQL, synced from faster-sync, reorg-safe, Postgres or embedded PGlite |

## How they fit together

```
                  ┌──────────────────────────┐
  EVM node ──RPC──▶       faster-sync        │◀── reth node (faster-sync-reth ExEx,
  (any client)    │ Parquet chunks + fuse    │     in-process, no RPC hop)
                  │ filters, RocksDB hot tail│
                  └──────┬────────────┬──────┘
                 Arrow / JSON pages   │
                         │            │
              ┌──────────▼───┐   ┌────▼──────────┐
              │ faster-index │   │  faster-rpc   │── state calls ──▶ upstream node
              │ handlers →   │   │ eth_* JSON-RPC│
              │ GraphQL      │   │ + WebSocket   │
              └──────────────┘   └───────────────┘

  EVM node ──RPC──▶ faster (explorer: indexer + API + live UI)
```

- **faster-sync** ingests from any JSON-RPC node (or from inside reth via the
  ExEx), cuts finalized history into immutable Parquet chunks with per-sub-group
  binary fuse filters, keeps the unfinalized tail in RocksDB and rolls it back on
  reorgs.
- **faster-rpc** answers the standard block / transaction / receipt / log
  methods from faster-sync and forwards everything else to an upstream node.
- **faster-index** pages events out of faster-sync, runs your handlers, and
  serves the entities over GraphQL, following the head and handling reorgs.
- **faster** is the standalone explorer and runs directly against a node.

## Highlights

| | |
|---|---|
| faster-sync vs ClickHouse (synthetic 12M rows) | rare contract 3.4 ms vs 4.3 ms · busiest contract 49 ms vs 65 ms · wallet Transfers 3.4 ms vs 9.2 ms · 9M logs streamed 0.89 s vs 0.94 s |
| faster-sync on Ethereum mainnet | Transfers filtered on a common topic value: 5–7 ms |
| faster-rpc conformance vs reth (mainnet) | 550/550 and 221/221 identical responses; 389/389 through the reth ExEx under injected reorgs |
| faster-index on mainnet | 320,366 USDT Transfers into 176,535 accounts in 16 s, balances exact |
| faster explorer | ~250K tx/s ingest (synthetic), tx lookup 0.04 ms, ~90K req/s per instance |

Method and full numbers live in each repo: faster-sync
[docs/BENCHMARK.md](https://github.com/abhijitkrm/faster-sync/blob/main/docs/BENCHMARK.md),
faster-rpc [docs/CONFORMANCE.md](https://github.com/abhijitkrm/faster-rpc/blob/main/docs/CONFORMANCE.md),
faster [docs/STORE.md](https://github.com/abhijitkrm/faster/blob/main/docs/STORE.md).

## Quick start (data stack against a public node)

```sh
# 1. faster-sync: ingest and serve on :3100
git clone https://github.com/abhijitkrm/faster-sync && cd faster-sync
# recent start block and a request-rate cap for a public endpoint
RPC_URL=https://ethereum-rpc.publicnode.com START_BLOCK=23000000 RPC_RPS=6 \
  cargo run --release

# 2. faster-rpc: JSON-RPC on :8545, backed by faster-sync
git clone https://github.com/abhijitkrm/faster-rpc && cd faster-rpc
SYNC_URL=http://localhost:3100 UPSTREAM_URL=https://ethereum-rpc.publicnode.com cargo run --release

# 3. faster-index: scaffold an indexer, GraphQL on :8080
npx faster-index init my-indexer && cd my-indexer && npm install
SYNC_URL=http://localhost:3100 START_BLOCK=23000000 npx faster-index dev
```

See each repo's README for every setting, Docker images and the API reference.

## Roadmap

- Run the full stack on a server next to a synced reth node: soak test and
  reth-ExEx backfill benchmark.
- Put the faster explorer on top of faster-sync and faster-rpc.
- Production hardening: dashboards and alerts, chunk backups to object storage,
  registry images.
- Product gaps: faster-index loaders and multichain ordering, faster-rpc trace
  methods, traces from the reth source.
