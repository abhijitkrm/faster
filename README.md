# faster-index

Indexing framework for EVM applications: describe the contracts and events you care
about, write handlers that turn them into your own entities, and get a GraphQL API that
stays in sync with the chain, including reorgs.

Part of the Faster stack: [faster](https://github.com/abhijitkrm/faster) (explorer) ·
[faster-sync](https://github.com/abhijitkrm/faster-sync) ·
[faster-rpc](https://github.com/abhijitkrm/faster-rpc) · faster-index.

Status: design. Clean-room: its own design and code. Other indexing frameworks are a
behavioural reference only; no code is copied from any of them.

## How it works

```yaml
# faster-index.yaml
name: dex
networks:
  - id: 7001
    start_block: 0
contracts:
  - name: Pool
    address: ["0x…"]
    events:
      - Swap(address indexed sender, int256 amount0, int256 amount1)
```

```ts
// handlers.ts
Pool.Swap.handler(async ({ event, context }) => {
  const pool = await context.Pool.getOrCreate(event.srcAddress);
  pool.volume0 += abs(event.params.amount0);
  pool.swaps += 1;
  context.Pool.set(pool);
});
```

- History is read from faster-sync in bulk (seconds, not hours), then the live tail.
- Entities are stored in Postgres with history for rollback; a reorg rewinds affected
  entities automatically.
- Generated GraphQL API with filtering, sorting and pagination; aggregations can be backed
  by ClickHouse.
- `faster-index init | codegen | dev | start`.

## Plan

1. Config schema, codegen of typed event handlers and entity types (TypeScript first).
2. Runtime: batch processing from faster-sync, entity store with rollback journal.
3. GraphQL server.
4. Dev workflow (`dev` with hot reload), deployment docs, examples.
