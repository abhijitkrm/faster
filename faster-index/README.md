# faster-index

Indexing framework for EVM applications: describe the contracts and events
you care about, write handlers that turn them into your own entities, and get
a GraphQL API that stays in sync with the chain, including reorgs.

Part of the Faster stack: [faster](https://github.com/abhijitkrm/faster) (explorer) ·
[faster-sync](https://github.com/abhijitkrm/faster-sync) ·
[faster-rpc](https://github.com/abhijitkrm/faster-rpc) · faster-index.
Clean-room: its own design and code; other indexing frameworks were a
behavioural reference only.

**Ethereum mainnet:** every USDT `Transfer` over 3,163 blocks (320,366
events, 176,535 accounts) indexed in 16 s on a laptop with the embedded
database; every balance equals the sum of the raw logs and the balances sum
to zero.

## How it works

```yaml
# faster-index.yaml
name: usdt-balances
schema: schema.graphql
handlers: src/handlers.ts
contracts:
  - name: ERC20
    events:
      - Transfer(address indexed from, address indexed to, uint256 value)
networks:
  - id: 1
    sync_url: ${SYNC_URL:-http://localhost:3100}   # a faster-sync server
    start_block: 0
    contracts:
      - name: ERC20
        address: ["0xdac17f958d2ee523a2206206994597c13d831ec7"]
```

```graphql
# schema.graphql
type Account @entity {
  id: ID!
  balance: BigInt!
  transfers: [Transfer!]! @derivedFrom(field: "from")
}
type Transfer @entity {
  id: ID!
  from: Account!
  to: Account!
  value: BigInt!
  block: Int! @index
}
```

```ts
// src/handlers.ts — types come from `faster-index codegen`
import { ERC20 } from "../generated/index.ts";

ERC20.Transfer.handler(async ({ event, context }) => {
  const { from, to, value } = event.params;
  const a = await context.Account.getOrCreate(from, () => ({ id: from, balance: 0n }));
  context.Account.set({ ...a, balance: a.balance - value });
  const b = await context.Account.getOrCreate(to, () => ({ id: to, balance: 0n }));
  context.Account.set({ ...b, balance: b.balance + value });
  context.Transfer.set({ id: `${event.block.number}-${event.logIndex}`, from, to, value, block: event.block.number });
});
```

```graphql
{ accounts(first: 10, orderBy: balance, orderDirection: desc) { id balance transfers(first: 3) { to { id } value } }
  _meta { chains { chainId nextBlock } } }
```

- **History in bulk.** Events come from faster-sync in pages of tens of
  thousands of matching logs (only the configured addresses and event
  signatures are fetched), while the next page is already being fetched.
- **Handlers run in chain order** (block, log index), with an entity cache
  shared across pages; each page commits entities and the checkpoint in one
  transaction, so a crash resumes exactly where it stopped.
- **Reorgs.** For blocks that are not final yet, the state of every entity
  before its first change in a block is journaled. When faster-sync reports
  that the chain changed under us, faster-index finds the newest block that
  is still canonical, restores the journaled state after it and continues.
  The journal is pruned as blocks become final.
- **GraphQL** generated from the schema: `account(id)`, `accounts(first,
  skip, where, orderBy, orderDirection)` with `field`, `_not`, `_gt`, `_gte`,
  `_lt`, `_lte`, `_in`, `_not_in`, `_contains`, `_starts_with` filters,
  entity references, `@derivedFrom` lists, `_meta`. GraphiQL on `GET /graphql`.
- **Postgres** in production (`DATABASE_URL`); embedded Postgres (PGlite) for
  `dev` and tests, nothing to install.

## Use

```bash
npx faster-index init my-indexer && cd my-indexer && npm install
npx faster-index dev            # codegen, index into .faster-index/, GraphQL on :8080, reloads on save
DATABASE_URL=postgres://… npx faster-index start
```

| Command | |
|---|---|
| `init [dir]` | new project (the ERC-20 balances example) |
| `codegen` | `generated/index.ts`: entity interfaces, typed event params, handler registration |
| `dev` | codegen + index + GraphQL with embedded Postgres; restarts on handler/schema/config changes (schema changes re-index) |
| `start` | production: Postgres from `DATABASE_URL` (else embedded at `PGLITE_DIR`) |

Options: `-c <config>`, `--reset` (drop indexed data), `--port <n>`.

| Variable | |
|---|---|
| `DATABASE_URL` | Postgres connection string |
| `PG_POOL_MAX` | Postgres connections (default 8) |
| `GRAPHQL_PORT` | default 8080 (`/graphql`, `/healthz`, `/metrics`) |
| `${ANY}` in faster-index.yaml | replaced from the environment (`${VAR:-default}`) |

### Config reference

| Key | |
|---|---|
| `contracts[].events` | human-readable event signatures |
| `contracts[].abi` | or a JSON ABI file (its events) |
| `networks[].id` | chain id |
| `networks[].sync_url` / `sync_api_key` | faster-sync endpoint and key |
| `networks[].start_block` / `end_block` | range (no `end_block`: follow the chain) |
| `networks[].contracts[]` | `name`, `address` (one or a list), optional `start_block` |
| `transaction_fields` | extra transaction fields on `event.transaction` (e.g. `[from, to, value]`) |
| `page_rows` | logs per faster-sync page (default 50,000) |

### Event values

Addresses are lowercase hex strings; integers up to 48 bits are numbers,
wider ones `bigint`; `bytesN`/`bytes` are hex strings; indexed dynamic values
(string, bytes, arrays) are their topic hash, as in the log. `event.block` has
`number`, `hash`, `timestamp`; `event.transaction` has `hash`, `index` and
any `transaction_fields`.

### Entity types

`ID`, `String`, `Int`, `BigInt`, `BigDecimal`, `Float`, `Boolean`, `Bytes`,
`Json`, references to other entities (stored as their id), lists of scalars,
and `@derivedFrom(field: …)` lists. `@index` adds a database index.

## Docker

```bash
docker build -t my-indexer --build-arg PROJECT=examples/erc20 .
docker run -e DATABASE_URL=postgres://… -e SYNC_URL=http://faster-sync:3100 -p 8080:8080 my-indexer
```

## Development

```bash
npm install && npm test     # decoder vs viem, runtime + GraphQL, reorg rollback (fake faster-sync, PGlite)
TEST_DATABASE_URL=postgres://… npm test    # same against Postgres
```
