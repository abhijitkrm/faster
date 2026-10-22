# API reference

Served by `services/faster`. In the compose deployment the API shares the UI's
origin (nginx proxies `/api/`, `/stats`, `/health`, `/ready` and `/ws`).

Conventions:

- Hashes and addresses: lower-case `0x` hex. Inputs are accepted in any case.
- Block numbers, gas, counts, nonces, indexes: JSON numbers.
- Wei amounts and prices (`value`, `gas_price`, `balance`, fee fields): decimal
  strings, since they exceed 2^53.
- Times: `timestamp` / `block_timestamp` are unix seconds; `first_seen` /
  `last_seen` are RFC 3339.
- Errors: `{"error": "..."}` with 400 (bad input), 404, 429 (rate limited,
  with `Retry-After`), 500, 503, 504 (request took over 15 s).
- Blocks and txs more than `REORG_DEPTH` below the head are served with
  `Cache-Control: public, max-age=86400, immutable`; newer data with `no-cache`.

## Blocks

| Endpoint | |
|---|---|
| `GET /api/blocks?limit=20` | newest blocks (limit ≤ 100) |
| `GET /api/blocks/range/{start}/{end}` | blocks in a range (≤ 1000), ascending |
| `GET /api/blocks/{number\|hash}` | one block with up to 500 transactions inline |
| `GET /api/blocks/{number\|hash}/transactions?offset=0&limit=100` | a block's transactions, in index order (limit ≤ 1000) |

Block fields: `number`, `hash`, `parent_hash`, `timestamp`, `miner`,
`gas_used`, `gas_limit`, `base_fee_per_gas`, `difficulty`,
`transactions_count`, `logs_count`, `state_root`, `transactions_root`,
`receipts_root`, `size`. The detail adds `transactions` and
`transactions_truncated` (more than 500 txs: page the rest).

## Transactions

| Endpoint | |
|---|---|
| `GET /api/transactions?limit=20` | newest transactions (limit ≤ 100) |
| `GET /api/transactions/{hash}` | one transaction with its `logs` and `block_hash` |
| `GET /api/transactions/address/{address}?limit=20&before=` | an address's transactions, newest first |
| `GET /api/transactions/pending/list` | always `[]` (no mempool view) |

Transaction fields: `hash`, `block_number`, `block_timestamp`,
`transaction_index`, `from_address`, `to_address` (null for creations),
`value`, `gas`, `gas_price` (price actually paid: the receipt's
`effectiveGasPrice` when the node reports it), `effective_gas_price`,
`max_fee_per_gas`, `max_priority_fee_per_gas`, `gas_used`, `nonce`,
`input_data`, `status` (1, 0, or null for pre-Byzantium), `contract_address`,
`logs_count`, `transaction_type`.

Logs: `log_index` (block-level), `address`, `topics`, `data`.

## Addresses

| Endpoint | |
|---|---|
| `GET /api/addresses?limit=20&offset=0` | addresses by transaction count, highest first |
| `GET /api/addresses/{address}?limit=25&before=` | summary + transactions, newest first |
| `GET /api/addresses/{address}?limit=25&page=N` | same, offset paging (cost grows with depth) |
| `GET /api/addresses/search/by-pattern?pattern=0xab12` | addresses starting with a hex prefix (≥ 3 hex chars) |

Address fields: `address`, `transaction_count` (distinct transactions it is
party to: sender, recipient or created contract), `first_seen`, `last_seen`,
`is_contract`, `bytecode`, `bytecode_size`, `deploy_info` (`hash`,
`block_number`, `block_timestamp`, `from_address` of the creating tx),
`balance` (live from the node, cached 2 s; null if the node is unavailable),
`transactions`, `total_transactions`, `page`, `limit`, `next_cursor`.

Paging with cursors: pass the response's `next_cursor` (`"<block>:<index>"`)
as `before=` to get the next page; `next_cursor` is null on the last page.
This costs the same at any depth (0.16 ms at 100K deep vs 13 ms with `page=`).

## Tokens

ERC-20, ERC-721 and ERC-1155 transfers are decoded from logs (`Transfer`,
`TransferSingle`, `TransferBatch`). Balances are derived from transfers, so
they are exact when indexing starts at block 0 (`START_BLOCK=0`). Name,
symbol and decimals are read from the contract once, when the token is first
seen.

| Endpoint | |
|---|---|
| `GET /api/tokens?limit=50&offset=0` | tokens by transfer count: `address, type, name, symbol, decimals, holders, transfers, first_block` |
| `GET /api/tokens/{address}` | one token |
| `GET /api/tokens/{address}/transfers?limit=25&before=` | its transfers, newest first |
| `GET /api/tokens/{address}/holders?limit=50&offset=0` | largest holders (`value`: balance, or NFT count for ERC-721) |
| `GET /api/addresses/{address}/token-transfers?limit=25&before=` | transfers in or out of an address |
| `GET /api/addresses/{address}/tokens` | tokens held (`value`, and `token_id` for ERC-1155) |

Transfer fields: `block_number`, `block_timestamp`, `transaction_hash`,
`log_index`, `batch_index` (position in an ERC-1155 batch), `token`,
`token_type`, `token_symbol`, `token_decimals`, `from_address`, `to_address`,
`token_id`, `value` (raw integer string), `cursor`. Lists return
`{"items": [...], "next_cursor": ...}`; pass `next_cursor` as `before=`.
`GET /api/transactions/{hash}` includes `token_transfers`.

## Search

`GET /api/search?q=` resolves a block number, transaction or block hash,
address (`type: "token"` if it is a token contract) or token name / symbol
prefix: `[{"type": "block" | "transaction" | "address" | "token", "value": ...}]`.

## Etherscan-compatible API

`GET /api?module=...&action=...` with Etherscan's envelope
(`{"status":"1","message":"OK","result":...}`, all values strings):

| module / action | parameters |
|---|---|
| `account` / `balance` | `address` |
| `account` / `balancemulti` | `address` (comma-separated, ≤ 20) |
| `account` / `txlist` | `address`, `startblock`, `endblock`, `page`, `offset`, `sort` (asc default) |
| `account` / `tokentx`, `tokennfttx`, `token1155tx` | `address` and/or `contractaddress`, same paging |
| `account` / `txlistinternal` | `address` or `txhash` (requires `TRACE_CALLS=true`) |
| `block` / `getblocknumberbytime` | `timestamp`, `closest` (before / after) |
| `transaction` / `gettxreceiptstatus` | `txhash` |
| `proxy` / `eth_blockNumber` | — |

`page × offset` is capped at 10,000 as on Etherscan.

## Stats and health

`GET /stats`:

```json
{"total_blocks":"2202","block_height":"2201","latest_block_timestamp":"1792675773",
 "total_transactions":"1100001","total_addresses":"1350","total_logs":"219560",
 "avg_block_time":"1.000","tps":"500.00"}
```

`avg_block_time` is over the last 100 blocks; `tps` is the latest block's
transactions divided by the average block time of the last 20 blocks.

- `GET /health` — `{"status":"ok","timestamp":...,"connectedClients":N}` (liveness)
- `GET /ready` — 200 when the store is within `READY_MAX_LAG` blocks of the
  node, else 503; body `{"ready","head","tip","lag"}`

## WebSocket

Connect to `/ws` (or `/`). The server sends JSON text frames:

| `type` | `data` | when |
|---|---|---|
| `init` | `{stats, blocks: [20 newest], txs: [20 newest]}` | on connect, and after a chain reorganisation |
| `block` | `{hash, number, timestamp, miner, gas_used, transactions_count}` | each committed block (≤ 50 per commit) |
| `tx` | `{hash, block_number, from_address, to_address, value, status}` | newest txs of a commit (≤ 20), oldest first |
| `stats` | as `/stats` | after each commit and every 5 s |

Clients may send `{"type":"ping"}` and get `{"type":"pong"}`. The server
pings every 20 s and closes connections that are silent for 60 s, block a
write for 10 s, or fall more than 256 messages behind; reconnecting yields a
fresh `init`.

## CLOB history

`/api/clob/*` (orders, fills, cancels by trader, pair tape, per-block lists,
order lookup by hash, archive stats) is documented in
[CLOB_ARCHIVE.md](CLOB_ARCHIVE.md#api). It needs `CLICKHOUSE_URL`; without it
these routes answer 503.
