# Conformance

`conformance/conformance.py` sends the same requests to a reference node and
to faster-rpc and deep-compares the JSON results (hex strings compared
case-insensitively; nothing else is normalised). Per sampled block:
`eth_getBlockByNumber` (with and without transactions), `eth_getBlockByHash`,
`eth_getBlockTransactionCountByNumber`, `eth_getBlockReceipts`, and for
sampled transactions `eth_getTransactionByHash`,
`eth_getTransactionReceipt`, `eth_getTransactionByBlockNumberAndIndex`; then
`eth_getLogs` for the block, by address, by topic0, and by block hash with
address and a topic1 position; plus unknown hashes and a reversed range.

## Ethereum mainnet vs reth (2026-10-23)

Reference: `reth/v2.8.0` (public endpoint). faster-sync ingested blocks
26,249,778 – 26,252,8xx from that endpoint; faster-rpc on top of it with the
same node as upstream. Two runs, seeds 7 and 11, 15 blocks × 3 transactions:

```
275 calls: 275 equal, 0 different
275 calls: 275 equal, 0 different
```

Found and fixed on the way: reth (like geth) requires a log to have at least
as many topics as the filter has positions, even wildcard ones
(`"topics": [null, null]` skips logs with one topic).

## anvil

`anvil` with 1 s blocks and ERC-20, ETH and internal-call traffic, 15
blocks × 4 transactions: 190 of 191 equal. The difference: a reversed
`eth_getLogs` range, which anvil answers with `[]` and faster-rpc, geth and
reth answer with `invalid block range params`.

Found and fixed on the way: anvil adds `blockTimestamp` to transactions,
receipts (as a JSON number) and logs; faster-sync now keeps every key the
node returns verbatim instead of dropping derivable ones.

## reth dev chain through faster-sync-reth (v0.2.0)

faster-sync running inside a reth dev node as an execution extension,
faster-rpc on top, compared with that node's own JSON-RPC: 335 / 335 equal;
with reth's reorg injection (`--debug.reorg-frequency 8 --debug.reorg-depth
2`) on blocks that had settled: 389 / 389 equal.

## Latency v0.2.0 (mainnet data, cold, curl, median of 20)

| Call | v0.1 layout | v0.2 layout |
|---|---:|---:|
| `eth_getTransactionByHash` | 7.1 ms | 2.4 ms |
| `eth_getTransactionReceipt` | 13.0 ms | 3.8 ms |
| `eth_getBlockReceipts` | 25 ms | 18 ms |
| repeated finalized tx / receipt (cache) | | < 1 ms |

## Latency v0.1.0 (mainnet data, laptop, faster-rpc → faster-sync on localhost, Python client)

| Call | Median |
|---|---:|
| `eth_blockNumber` | 0.31 ms |
| `eth_getBlockByNumber` full, finalized (cached) | 2.4 ms |
| `eth_getTransactionByHash` | 5.2 ms |
| `eth_getTransactionReceipt` | 10.9 ms |
| `eth_getBlockReceipts` (209 receipts) | 20 ms |
| `eth_getLogs` all logs of one block (737) | 9.2 ms |
| `eth_getLogs` USDT Transfers to one address over 2,900 blocks (5,476) | 68 ms |
