# Changelog

## v0.3.0

- Logs store a 64-bit key per topic; topic filters compare keys first and the
  32-byte topics only for survivors. Mainnet Transfers filtered on a common
  topic2 value: 17.9 → 6.7 ms and 12.0 → 5.1 ms (ClickHouse sorted by
  address: 5.8 / 6.2 ms). `QUERY_KEYS=0` turns the key pass off.
- **Store format 2:** data dirs written by v0.1/v0.2 must be rebuilt; the
  binary refuses to open them with a message saying so.
- Logs table +9 bytes per row on mainnet data.

## v0.2.0

- **faster-sync-reth** ([`reth/`](reth)): a reth node with faster-sync as an
  execution extension. Blocks and receipts come from reth's own `eth` handlers
  in-process; reorgs and reverts arrive as reth notifications. Tested on a reth
  dev chain with reth's reorg injection (every 8 blocks): the store equals
  reth's canonical chain block for block, faster-rpc over it equals reth's
  JSON-RPC on 389 / 389 calls, and restarts resume without gaps. A failure in
  faster-sync never stops the node: it rewinds unfinalized blocks and catches
  up from reth's database.
- `Feed`: push-ingest API (append, revert, finalize) for sources that deliver
  blocks themselves.
- No dictionary encoding for unique-valued columns, 64 KB page cap: point
  lookups 2.5–3.5× faster, busiest-contract scans 2.3× faster
  ([docs/BENCHMARK.md](docs/BENCHMARK.md#v020)).
- Multi-field selections check their rarest field first (`QUERY_PRE=0` to
  disable).
- API startup moved into the library (`api::spawn_from_env`).

## v0.1.0

First release.
