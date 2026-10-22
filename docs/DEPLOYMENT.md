# Deployment and operations

## Topology

```
browser ──► frontend (nginx :8080, published as $HTTP_PORT, default 4000)
              ├─ /            UI (static build)
              └─ /api /stats /health /ready /ws ──► faster :3000 (not published)
                                                      ├─ RocksDB store (volume faster-data)
                                                      ├─ node JSON-RPC (RPC_URL)
                                                      ├─ ClickHouse :8123 (CLOB history, optional)
                                                      └─ admin :9100 (metrics, backups; compose network only)
clobarchiver ──► ClickHouse (optional)
```

nginx is the only public port. It overwrites `X-Forwarded-For` with the client
address and faster trusts that header (`TRUST_PROXY_HEADERS=true`), which is
safe only while faster itself is unreachable from outside. If you expose
faster directly, set `TRUST_PROXY_HEADERS=false`.

## Configuration (`.env`)

| Variable | Default | |
|---|---|---|
| `RPC_URL` | — | node JSON-RPC (required). Archive data is not needed; receipts are. |
| `HTTP_PORT` | 4000 | public port |
| `START_BLOCK` | 0 | first block for an empty store |
| `CONFIRMATIONS` | 0 | stay this many blocks behind the tip (0: follow the tip, roll back on reorgs) |
| `REORG_DEPTH` | 256 | deepest reorg handled; deeper stops the process (resync needed) |
| `FETCH_BATCH` / `FETCH_CONCURRENCY` | 10 / 16 | blocks per RPC batch / batches in flight; lower them for rate-limited providers |
| `CACHE_MB` | 512 | RocksDB block cache |
| `TRACE_CALLS` | false | index internal transactions (needs `debug_traceBlockByNumber`; archive node for history) |
| `RATE_LIMIT_RPS` / `RATE_LIMIT_BURST` | 100 / 200 | per client IP; 0 disables |
| `CORS_ORIGINS` | `*` | comma-separated allow-list for browsers on other origins |
| `FASTER_API_URL` | empty | UI → API base URL; empty = same origin (via nginx) |
| `CLICKHOUSE_USER` / `CLICKHOUSE_PASSWORD` | clob / clob | CLOB archive; change for production |
| `NODE_RPC` | host.docker.internal:8545 | evm-node with `CLOB_ARCHIVE=1`, for clobarchiver |
| `LOG_LEVEL` | info | |

All faster settings are listed in [STORE.md](STORE.md#configuration).

## Start, upgrade, stop

```bash
cp .env.example .env && $EDITOR .env
docker compose up -d --build
docker compose logs -f faster          # "progress" lines every 10 s: head, tip, lag, blocks/s
curl -s localhost:4000/ready           # 200 once caught up
```

Upgrades: `docker compose up -d --build faster`. faster stops on SIGTERM
(grace period 30 s); every commit is atomic, so a hard kill is also safe and
indexing resumes from the last committed block. A binary with a different
store schema version refuses to open the store and says so: wipe the
`faster-data` volume (or restore a backup of the matching version) and let it
resync.

## Capacity

- Disk: about 0.4 KB per transaction with a log, before compaction gains
  (1.1M txs ≈ 0.45 GB). Use SSD/NVMe.
- Memory: `CACHE_MB` plus RocksDB memtables (~64 MB per column family while
  writing) plus the process (~100 MB).
- CPU: catching up uses several cores (JSON decoding and parallel writes); at
  the tip it is mostly idle. Catch-up speed is usually bounded by the node:
  raise `FETCH_CONCURRENCY` if the node has headroom.

## Monitoring

Prometheus scrape target `faster:9100/metrics` (see `config/prometheus.yml`).
Useful series:

- `faster_lag_blocks` — alert if > 20 for 5 minutes
- `faster_rpc_requests_total{result="error"}` rate — node trouble
- `faster_reorgs_total`, `faster_rolled_back_blocks_total`
- `faster_commit_seconds` — store write latency
- `faster_http_seconds{route=...}` — API latency per route
- `faster_rate_limited_total`, `faster_ws_clients`, `faster_ws_dropped_slow_total`

Kubernetes-style probes: liveness `GET /health`, readiness `GET /ready`.

## Backups

The store can always be rebuilt from the chain, but a backup makes recovery
minutes instead of a resync:

```bash
docker compose exec faster curl -s -X POST localhost:9100/admin/checkpoint
# {"path":"/data/backups/checkpoint-1792678379","head":123456}
```

A checkpoint is a consistent RocksDB snapshot made of hard links (instant,
shares disk with the live store until compaction rewrites files). Copy it off
the volume, then delete it from `/data/backups`. To restore, stop faster, put
the checkpoint's `hot/` directory at `/data/hot` and start; indexing resumes
from the checkpoint's head.

## Scaling reads

One instance serves ~90K req/s of lookups. For more, run several faster
instances, each with its own volume and indexing from the same (or its own)
node, behind the nginx upstream; they converge to the same data. ClickHouse
(CLOB history) is shared.

## Security checklist

- Change `CLICKHOUSE_PASSWORD`; keep ClickHouse ports bound to localhost (as in compose).
- Terminate TLS in front of nginx (or add a TLS server block).
- Keep the admin port (9100) off public networks; it can create backups.
- Set `CORS_ORIGINS` if other sites call the API from browsers.
