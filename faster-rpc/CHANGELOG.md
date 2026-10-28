# Changelog

## v0.2.0

- Finalized transactions and receipts are cached as ready responses
  (`CACHE_TXS`, default 100,000): repeated lookups under 1 ms.
- With faster-sync v0.2.0 (no dictionaries on unique columns), cold lookups on
  Ethereum mainnet data: `eth_getTransactionByHash` 7.1 → 2.4 ms,
  `eth_getTransactionReceipt` 13.0 → 3.8 ms, `eth_getBlockReceipts` 25 → 18 ms.
- Conformance re-run: 221 / 221 equal to reth 2.8 on mainnet; 335 / 335 and
  389 / 389 (under injected reorgs) against a reth dev node through
  faster-sync-reth.

## v0.1.0

First release.
