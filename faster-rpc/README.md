# faster-rpc

Read-only Ethereum JSON-RPC served from [faster-sync](https://github.com/abhijitkrm/faster-sync)'s
indexed history instead of a node: the standard block, transaction, receipt
and log methods, answered from pre-built indexes, with `eth_getLogs` over
long ranges at index speed. State methods (`eth_call`, `eth_getBalance`, …)
and writes go to an upstream node.

Part of [Faster](https://github.com/abhijitkrm/faster): [explorer](../faster-explorer) · [faster-sync](../faster-sync) · faster-rpc · [faster-index](../faster-index). Clean-room implementation.

## What it serves

| Method | Source |
|---|---|
| `eth_chainId`, `net_version`, `web3_clientVersion`, `eth_blockNumber`, `eth_syncing` | faster-sync heights |
| `eth_getBlockByNumber`, `eth_getBlockByHash` (with or without transactions) | faster-sync |
| `eth_getBlockTransactionCountByNumber/Hash`, `eth_getUncleCountByBlockNumber/Hash` | faster-sync |
| `eth_getTransactionByHash`, `eth_getTransactionByBlockNumberAndIndex/HashAndIndex` | faster-sync (tx-hash index) |
| `eth_getTransactionReceipt`, `eth_getBlockReceipts` | faster-sync |
| `eth_getLogs` | faster-sync filters: long ranges are cheap |
| `eth_subscribe` `newHeads` / `logs` (WebSocket) | faster-sync tip, reorgs send `removed: true` logs |
| everything else (`eth_call`, `eth_getBalance`, `eth_estimateGas`, `eth_sendRawTransaction`, `debug_*`, …) | forwarded to `UPSTREAM_URL` |

Responses are the node's own objects: faster-sync stores every key the
node returned (including chain-specific ones such as L2 fee fields and
`blockTimestamp`), and faster-rpc adds only the positional keys
(`blockHash`, receipt `logs`). Blocks newer than the archive (it trails the
node by about a block), unknown transaction hashes (maybe pending) and log
ranges past the archive head are forwarded to the upstream node.

## Conformance

`conformance/conformance.py` calls a reference node and faster-rpc with the
same requests (blocks, full blocks, block receipts, transactions, receipts,
log filters by address / topic / block hash, unknown hashes, bad ranges) and
deep-compares the JSON.

| Reference | Result |
|---|---|
| anvil 1.x, token + ETH + internal-call traffic | 190 / 191 equal (anvil answers a reversed `eth_getLogs` range with `[]`; we answer `invalid block range params` as geth and reth do) |
| Ethereum mainnet, reth 2.8 (public endpoint) | 550 / 550 equal over two runs ([docs/CONFORMANCE.md](docs/CONFORMANCE.md)) |

```bash
python3 conformance/conformance.py --node https://reference-node --ours http://localhost:8545 \
  --from 20000000 --to 20001000 --samples 20 --rps 5
```

## Run

```bash
docker build -t faster-rpc .
docker run -d -p 8545:8545 -e SYNC_URL=http://faster-sync:3100 -e UPSTREAM_URL=https://your-node faster-rpc
cast block-number --rpc-url http://localhost:8545
```

| Variable | Default | |
|---|---|---|
| `SYNC_URL` / `SYNC_API_KEY` | — | faster-sync endpoint (required) and its API key |
| `UPSTREAM_URL` | — | node for state methods and blocks newer than the archive (unset: those answer "not supported" / `null`) |
| `MAX_LOGS` | 10000 | `eth_getLogs` fails with -32005 above this many results |
| `MAX_BLOCK_RANGE` | 0 (none) | `eth_getLogs` block range limit |
| `MAX_BATCH` | 100 | calls per batch request |
| `CACHE_BLOCKS` | 4096 | finalized blocks kept as ready responses |
| `CACHE_TXS` | 100000 | finalized transactions / receipts kept as ready responses |
| `BLOCK_TIMESTAMP` | false | add `blockTimestamp` to receipts and logs when the node does not |
| `API_KEYS` | — | comma-separated keys (`Authorization: Bearer` or `?key=` for WebSocket) |
| `RATE_LIMIT_RPS` / `RATE_LIMIT_BURST` | 0 (off) / 100 | per key, else per IP |
| `TRUST_PROXY` / `CORS_ORIGINS` | false / — | |
| `LISTEN` / `ADMIN_LISTEN` | `0.0.0.0:8545` / `127.0.0.1:9102` | JSON-RPC (HTTP + WebSocket on `/`) / metrics |
| `READY_MAX_LAG` | 10 | `/ready` threshold in blocks |

## Development

```bash
cargo test --release     # methods and subscriptions against a fake faster-sync
```
