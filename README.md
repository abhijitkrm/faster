# faster-rpc

Read-only Ethereum JSON-RPC served from a pre-indexed store instead of a node: standard
methods, archive-speed answers, and `eth_getLogs` over any block range without the usual
range caps.

Part of the Faster stack: [faster](https://github.com/abhijitkrm/faster) (explorer) ·
[faster-sync](https://github.com/abhijitkrm/faster-sync) · faster-rpc ·
[faster-index](https://github.com/abhijitkrm/faster-index).

Status: design. Clean-room implementation.

## Scope

| Method | Served from |
|---|---|
| `eth_chainId`, `eth_blockNumber` | hot tier head |
| `eth_getBlockByNumber`, `eth_getBlockByHash` (with / without txs) | hot tier (recent), faster-sync chunks (history) |
| `eth_getTransactionByHash`, `eth_getTransactionReceipt`, `eth_getBlockReceipts` | hash indexes + chunks |
| `eth_getLogs` | faster-sync chunk filters: long ranges are cheap |
| `eth_getTransactionCount`, `eth_getBalance`, `eth_call`, … (state) | proxied to an upstream node |

Writes (`eth_sendRawTransaction`) are forwarded upstream unchanged.

## Design

- Same process model as the faster explorer: data in-process, no database round trip.
- Recent blocks from the RocksDB hot tier (reorg-safe), finalized history from
  faster-sync's chunks.
- Responses byte-compatible with geth for the supported methods (conformance tests
  against a reference node).
- Batch requests, WebSocket subscriptions (`newHeads`, `logs`) from the live tail.
- Per-key rate limits and metrics.

## Plan

1. Method set on top of the faster hot tier (recent blocks) with a geth conformance suite.
2. History through faster-sync once its chunk store lands.
3. Upstream proxy for state methods; subscriptions.
4. Benchmarks against a node for `eth_getLogs` over long ranges.
