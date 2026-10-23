# faster-sync HTTP API

All endpoints are on `LISTEN` (default `0.0.0.0:3100`). Metrics are on the
admin listener (`ADMIN_LISTEN`, default `127.0.0.1:9101`, path `/metrics`).

If `API_KEYS` is set, every request except `/health` and `/ready` needs
`Authorization: Bearer <key>`; rate limits then apply per key, otherwise per
client IP (`RATE_LIMIT_RPS`, `RATE_LIMIT_BURST`).

## `POST /query`

Body: a query (JSON). Response: rows of up to four tables plus paging
metadata.

```jsonc
{
  "from_block": 18000000,            // required
  "to_block": 18100000,              // exclusive; omit = up to the newest stored block
  "logs": [                          // log selections (OR)
    { "address": ["0xa0b8…eb48"],    // fields within a selection: AND; values within a field: OR
      "topics": [["0xddf2…b3ef"], [], ["0x0000…wallet"]] }   // per position; [] = any
  ],
  "transactions": [                  // transaction selections (OR)
    { "from": [], "to": [], "sighash": ["0xa9059cbb"], "contract_address": [], "hash": [], "status": 1 }
  ],
  "traces": [                        // call-trace selections (OR), needs TRACES=true
    { "from": [], "to": [], "sighash": [], "call_type": ["CALL", "DELEGATECALL"] }
  ],
  "blocks": [ { "hash": [], "miner": [] } ],
  "include_all_blocks": false,       // every block header in the range
  "join": "default",                 // "nothing" | "blocks" | "default" | "all"
  "fields": {                        // columns per table; ["*"] = all; omitted = defaults
    "block": ["number", "hash", "timestamp"],
    "log": ["block_number", "log_index", "address", "topic0", "data"]
  },
  "max_rows": 100000,                // page budget (capped by QUERY_MAX_ROWS)
  "max_ms": 2000,                    // time budget (capped by QUERY_MAX_MS)
  "parent_hash": "0x…"               // hash of block from_block-1 as you last saw it (reorg check)
}
```

An empty selection (`{}`) matches every row of its table.

### Joins

| `join` | Returned |
|---|---|
| `nothing` | only selected rows (headers only with `include_all_blocks`) |
| `blocks` | selected rows and the header of every block with a result row |
| `default` | selected rows, the transaction of every selected log and trace, the header of every block with a result row |
| `all` | `default`, plus every log and trace of every selected transaction |

### Paging

A page ends at a block boundary when the row or time budget is used.
`next_block` is where the next request starts; the range is done when
`next_block >= to_block` (or reaches `archive_height + 1`). Pages are
never empty because of filtering alone: a page that covers blocks without
matches still advances `next_block`.

### Response (JSON)

```jsonc
{
  "blocks": [ { "number": 18000001, "hash": "0x…", … } ],
  "transactions": [ … ], "logs": [ … ], "traces": [ … ],
  "next_block": 18004211,
  "archive_height": 18999999,        // newest stored block
  "chain_head": 19000064,            // node head
  "finalized_block": 18999935,
  "last_block": { "block_number": 18004210, "hash": "0x…" },  // only when that block can still be reorged
  "rows": 100000,
  "elapsed_ms": 41.7
}
```

Value encoding: 64-bit integers (block numbers, gas, nonce, indexes) as JSON
numbers; hashes, addresses and byte strings as `0x` hex; arbitrary-size
quantities (wei values) as `0x` hex quantities; list-valued fields
(`access_list`, `withdrawals`, …) as JSON.

`?shape=rpc` returns the node's own JSON-RPC objects instead (all fields,
camelCase, chain-specific extra fields included): blocks as from
`eth_getBlockByNumber` without transactions, transactions as
`{"tx": {…}, "receipt": {…}}` (receipt without logs), logs as from
`eth_getLogs`.

### Response (Arrow)

`?format=arrow` (or `Accept: …arrow…`) streams frames
`[tag u8][length u32 LE][bytes]`:

| tag | content |
|---|---|
| 0 / 1 / 2 / 3 | Arrow IPC stream of blocks / transactions / logs / traces (a table can span several frames; frames arrive in block order) |
| 254 | JSON `{"error": "…"}` (the query failed after streaming began) |
| 255 | JSON metadata as above; always the last frame |

Buffers are LZ4-compressed (`?compression=none` to turn off). The clients in
`clients/` decode this format.

### Errors

| Status | Meaning |
|---|---|
| 400 | malformed query, unknown field, bad hex, too many selection values |
| 401 | missing or unknown API key |
| 409 | `{"reorg": true, "block": N}`: block `N` (`from_block - 1`) with your `parent_hash` is no longer canonical |
| 429 | rate limited (`Retry-After`) |

## `GET /height`

```json
{"height": 18999999, "chain_head": 19000064, "finalized_block": 18999935,
 "first_block": 0, "chunked_to": 18999872, "chain_id": 1}
```

`?above=N&timeout_ms=T` waits until `height > N` (at most `T` ms, max 30 s):
the way to follow the tip without polling.

## `GET /status`

`/height` plus `chunks`, `chunk_rows`, `filter_bytes`, `hot_blocks`, `version`.

## `GET /health`, `GET /ready`

`/health` is 200 while the process runs. `/ready` is 200 once the store is
within `READY_MAX_LAG` blocks of the node.

## Columns

Every table has the columns below; `extra` / `receipt_extra` hold any other
keys the node returned for that object (L2 fee fields, deposit fields, …).

- **blocks**: number, hash, parent_hash, nonce, sha3_uncles, logs_bloom,
  transactions_root, state_root, receipts_root, miner, difficulty,
  total_difficulty, extra_data, size, gas_limit, gas_used, timestamp, uncles,
  base_fee_per_gas, mix_hash, withdrawals_root, withdrawals, blob_gas_used,
  excess_blob_gas, parent_beacon_block_root, requests_hash,
  transaction_count, extra
- **transactions** (transaction + receipt): block_number, transaction_index,
  hash, from, to, value, gas, gas_price, max_fee_per_gas,
  max_priority_fee_per_gas, max_fee_per_blob_gas, nonce, input, type,
  chain_id, v, r, s, y_parity, access_list, blob_versioned_hashes,
  authorization_list, sighash, extra, status, cumulative_gas_used, gas_used,
  effective_gas_price, contract_address, logs_bloom, root, blob_gas_used,
  blob_gas_price, receipt_extra
- **logs**: block_number, log_index, transaction_index, transaction_hash,
  address, topic0, topic1, topic2, topic3, data, extra
- **traces** (`callTracer`, one row per call frame, `trace_address` like
  `"1,0"`; the top frame has `""`): block_number, transaction_index,
  transaction_hash, trace_address, type, from, to, value, gas, gas_used,
  input, output, error, revert_reason, sighash
