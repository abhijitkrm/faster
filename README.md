# Faster

Monorepo for the Faster stack: a real-time EVM block explorer plus a data
layer that serves chain history from pre-built indexes instead of a node.

## Layout

| Directory | Language | Last release | What it is |
|---|---|---|---|
| [`faster/`](faster) | Rust + web UI | v1.0.0 | Real-time block explorer: indexer, REST API and WebSocket feed in one process over an embedded RocksDB store |
| [`faster-sync/`](faster-sync) | Rust | v0.3.0 | Bulk historical data API: filtered blocks, txs, receipts, logs and traces across any block range, streamed as JSON or Arrow pages. [`faster-sync/reth/`](faster-sync/reth) is `faster-sync-reth`, a reth execution extension |
| [`faster-rpc/`](faster-rpc) | Rust | v0.2.0 | Read-only Ethereum JSON-RPC served from faster-sync: archive-speed reads, uncapped `eth_getLogs`, WebSocket subscriptions; state methods forwarded upstream |
| [`faster-index/`](faster-index) | TypeScript | v0.1.0 | Indexing framework: config + event handlers → entities → GraphQL, synced from faster-sync, reorg-safe, Postgres or embedded PGlite |

Each directory is a self-contained project (its own Cargo workspace or npm
package, Dockerfile, README, CHANGELOG); build and test from inside it. The
directories were imported with `git subtree` from the original repos with their
full commit history, so `git log -- faster-sync/` shows the project's whole past:

| Directory | Imported from | Release history |
|---|---|---|
| `faster/` | [abhijitkrm/faster](https://github.com/abhijitkrm/faster) | [releases](https://github.com/abhijitkrm/faster/releases) |
| `faster-sync/` | [abhijitkrm/faster-sync](https://github.com/abhijitkrm/faster-sync) | [releases](https://github.com/abhijitkrm/faster-sync/releases) |
| `faster-rpc/` | [abhijitkrm/faster-rpc](https://github.com/abhijitkrm/faster-rpc) | [releases](https://github.com/abhijitkrm/faster-rpc/releases) |
| `faster-index/` | [abhijitkrm/faster-index](https://github.com/abhijitkrm/faster-index) | [releases](https://github.com/abhijitkrm/faster-index/releases) |

## Run it

Everything runs from the repo root with Docker Compose, through
[`fasterctl`](fasterctl) or plain `docker compose`. Pick the parts you need:

| Profile | Services | Ports |
|---|---|---|
| `explorer` | `faster` (indexer + API), `frontend` (web UI, nginx) | UI + API on 4000 |
| `data` | `faster-sync`, `faster-rpc` | history API 3100, JSON-RPC 8545 |
| `index` | `faster-sync`, `faster-index`, `postgres` | GraphQL 8080 |
| `clob` | `clickhouse`, `clobarchiver` (CLOB archive for the explorer) | ClickHouse 8123 (localhost) |
| `all` | explorer + data + index | |
| `demo` | `fakechain`, a synthetic chain to point everything at | |

```sh
git clone https://github.com/abhijitkrm/faster-mono && cd faster-mono

./fasterctl up demo          # whole stack on a synthetic chain, no node needed

cp .env.example .env         # then set RPC_URL to your node
./fasterctl up all           # whole stack on a real chain
./fasterctl up explorer      # only the explorer
./fasterctl up data index    # faster-sync + faster-rpc + faster-index

./fasterctl status           # which endpoints answer
./fasterctl logs faster-sync
./fasterctl down             # stop (down -v also deletes the data)
```

The same with Compose directly: `docker compose --profile all up -d --build`
(add `--profile demo` for the synthetic chain).

All settings are in [`.env.example`](.env.example): the node (`RPC_URL`),
host ports, start blocks per service, request-rate cap for public endpoints
(`SYNC_RPC_RPS=6` suits free endpoints), API keys, and which indexer project to
run (`INDEX_PROJECT`, a directory inside `faster-index/`; the default is the
ERC-20 example). Each service's full settings are in its project README.

### Prebuilt images

Releases push every image to `ghcr.io/abhijitkrm/<service>:<tag>`. To run those
instead of building locally:

```sh
IMAGE_REGISTRY=ghcr.io/abhijitkrm IMAGE_TAG=v0.1.0 ./fasterctl up all
```

### Binaries

`./fasterctl build [dir]` builds native binaries into `./bin`: `faster`,
`faster-sync`, `fakechain`, `faster-rpc` and the `faster-index` CLI (Node 22).
`FASTER_RETH=1` adds `faster-sync-reth`, faster-sync inside a reth node.
Needs Rust (stable), clang/libclang (RocksDB) and Node 22. Every binary is
configured with environment variables, as listed in its project README; the
explorer's web UI is served by nginx, so run it from Docker or build
`faster/services/frontend` with `npm run build` and serve `dist/`.

Releases attach the same set as `faster-<tag>-<linux-x86_64|linux-arm64|macos-arm64>.tar.gz`.

## Releases and CI

A `vX.Y.Z` tag on `main` runs [`release.yml`](.github/workflows/release.yml):
binaries for three platforms on the GitHub release and all images on GHCR.
Each project keeps its own version in its manifest and CHANGELOG.

CI is one workflow per project, triggered only by changes under its directory
(`faster.yml`, `faster-sync.yml`, `faster-sync-reth.yml`, `faster-rpc.yml`,
`faster-index.yml`), plus [`stack.yml`](.github/workflows/stack.yml), which
brings the whole stack up with `./fasterctl up demo` and checks every service
end to end.

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

Method and full numbers:
[faster-sync/docs/BENCHMARK.md](faster-sync/docs/BENCHMARK.md),
[faster-rpc/docs/CONFORMANCE.md](faster-rpc/docs/CONFORMANCE.md),
[faster/docs/STORE.md](faster/docs/STORE.md).

## Roadmap

- Run the full stack on a server next to a synced reth node: soak test and
  reth-ExEx backfill benchmark.
- Put the faster explorer on top of faster-sync and faster-rpc.
- Production hardening: dashboards and alerts, chunk backups to object storage,
  registry images.
- Product gaps: faster-index loaders and multichain ordering, faster-rpc trace
  methods, traces from the reth source.
