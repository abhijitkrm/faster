#!/usr/bin/env node
// faster-index init | codegen | dev | start

import { existsSync, mkdirSync, watch, writeFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { codegen } from "./codegen.js";
import { loadConfig, type Config } from "./config.js";
import { migrate, openPglite, openPostgres, type Db } from "./db.js";
import { serveGraphql } from "./graphql.js";
import { clearHandlers, consoleLogger, registeredKeys, runNetwork, type Progress } from "./runtime.js";
import { loadSchema } from "./schema.js";
import { EntityCache } from "./store.js";

const USAGE = `faster-index <command> [options]

  init [dir]        create a project (ERC-20 balances example)
  codegen           generate typed handlers and entities (generated/index.ts)
  dev               codegen + index with embedded Postgres + GraphQL, reload on change
  start             index into Postgres (DATABASE_URL) + GraphQL

options:
  -c, --config <file>   project file (default faster-index.yaml)
  --reset               drop indexed data and start over
  --port <n>            GraphQL port (default $GRAPHQL_PORT or 8080)
`;

function args(argv: string[]) {
  const o: { cmd?: string; pos: string[]; config: string; reset: boolean; port: number } = {
    pos: [],
    config: "faster-index.yaml",
    reset: false,
    port: Number(process.env.GRAPHQL_PORT ?? 8080),
  };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "-c" || a === "--config") o.config = argv[++i];
    else if (a === "--reset") o.reset = true;
    else if (a === "--port") o.port = Number(argv[++i]);
    else if (!o.cmd) o.cmd = a;
    else o.pos.push(a);
  }
  return o;
}

const progress = new Map<number, Progress>();
function metrics(): string {
  const lines = ["# TYPE findex_events_total counter", "# TYPE findex_next_block gauge", "# TYPE findex_reorgs_total counter"];
  for (const p of progress.values()) {
    lines.push(`findex_events_total{chain_id="${p.chainId}"} ${p.events}`);
    lines.push(`findex_next_block{chain_id="${p.chainId}"} ${p.nextBlock}`);
    lines.push(`findex_reorgs_total{chain_id="${p.chainId}"} ${p.reorgs}`);
    if (p.head !== null) lines.push(`findex_archive_height{chain_id="${p.chainId}"} ${p.head}`);
  }
  return lines.join("\n") + "\n";
}

async function loadHandlers(cfg: Config, bust: boolean) {
  clearHandlers();
  const url = pathToFileURL(cfg.handlers).href + (bust ? `?v=${Date.now()}` : "");
  await import(url);
  if (registeredKeys().length === 0) throw new Error(`${cfg.handlers} registered no handlers`);
}

async function index(cfg: Config, db: Db, signal: AbortSignal) {
  const entities = loadSchema(cfg.schema);
  let last = 0;
  // One cache for all networks: they write the same tables.
  const cache = new EntityCache();
  await cache.init(db, entities);
  await Promise.all(
    cfg.networks.map((net) =>
      runNetwork(cfg, net, db, entities, {
        signal,
        cache,
        onPage: (p) => {
          progress.set(p.chainId, { ...p });
          if (Date.now() - last > 5000) {
            last = Date.now();
            consoleLogger.info("progress", { chain_id: p.chainId, next_block: p.nextBlock, archive_height: p.head, events: p.events });
          }
        },
      }).then((p) => {
        progress.set(p.chainId, p);
        consoleLogger.info("network done", { chain_id: p.chainId, next_block: p.nextBlock, events: p.events });
      }),
    ),
  );
}

async function main() {
  const o = args(process.argv.slice(2));
  switch (o.cmd) {
    case "init":
      return init(resolve(o.pos[0] ?? "."));
    case "codegen": {
      const cfg = loadConfig(o.config);
      console.log(`wrote ${codegen(cfg)}`);
      return;
    }
    case "start": {
      const cfg = loadConfig(o.config);
      codegen(cfg);
      const db = process.env.DATABASE_URL ? await openPostgres(process.env.DATABASE_URL) : await openPglite(process.env.PGLITE_DIR ?? join(cfg.root, ".faster-index/pglite"));
      const entities = loadSchema(cfg.schema);
      await migrate(db, entities, o.reset);
      await loadHandlers(cfg, false);
      const srv = await serveGraphql(db, entities, { port: o.port, metrics });
      consoleLogger.info("graphql listening", { port: o.port });
      const ac = new AbortController();
      process.on("SIGTERM", () => ac.abort());
      process.on("SIGINT", () => ac.abort());
      await index(cfg, db, ac.signal);
      if (cfg.networks.every((n) => n.end_block !== undefined) && !ac.signal.aborted) {
        consoleLogger.info("all networks reached end_block; serving GraphQL until stopped");
        await new Promise((r) => ac.signal.addEventListener("abort", r));
      }
      srv.close();
      await db.close();
      return;
    }
    case "dev":
      return dev(o);
    default:
      process.stdout.write(USAGE);
      process.exitCode = o.cmd ? 1 : 0;
  }
}

async function dev(o: ReturnType<typeof args>) {
  let cfg = loadConfig(o.config);
  codegen(cfg);
  const db = await openPglite(join(cfg.root, ".faster-index/pglite"));
  let entities = loadSchema(cfg.schema);
  await migrate(db, entities, o.reset);
  let srv = await serveGraphql(db, entities, { port: o.port, metrics });
  consoleLogger.info("dev: GraphQL at http://localhost:" + o.port + "/graphql");
  let ac = new AbortController();
  let running: Promise<void> = Promise.resolve();
  const start = async (bust: boolean) => {
    await loadHandlers(cfg, bust);
    running = index(cfg, db, ac.signal).catch((e) => consoleLogger.error("indexing stopped", { error: String(e?.stack ?? e) }));
  };
  await start(false);
  let timer: NodeJS.Timeout | undefined;
  const reload = (what: string) => {
    clearTimeout(timer);
    timer = setTimeout(async () => {
      consoleLogger.info("dev: change detected, restarting", { file: what });
      ac.abort();
      await running;
      ac = new AbortController();
      try {
        cfg = loadConfig(o.config);
        codegen(cfg);
        const next = loadSchema(cfg.schema);
        const schemaChanged = JSON.stringify(next) !== JSON.stringify(entities);
        if (schemaChanged) {
          await migrate(db, next, true);
          entities = next;
          srv.close();
          srv = await serveGraphql(db, entities, { port: o.port, metrics });
        }
        await start(true);
      } catch (e) {
        consoleLogger.error("dev: reload failed (fix and save again)", { error: String((e as Error)?.message ?? e) });
      }
    }, 300);
  };
  for (const p of [cfg.handlers, cfg.schema, resolve(o.config)]) watch(p, () => reload(p));
  await new Promise(() => {});
}

function init(dir: string) {
  if (existsSync(join(dir, "faster-index.yaml"))) throw new Error(`${dir} already has a faster-index.yaml`);
  mkdirSync(join(dir, "src"), { recursive: true });
  writeFileSync(
    join(dir, "faster-index.yaml"),
    `name: erc20
schema: schema.graphql
handlers: src/handlers.ts
contracts:
  - name: ERC20
    events:
      - Transfer(address indexed from, address indexed to, uint256 value)
networks:
  - id: 1
    sync_url: \${SYNC_URL:-http://localhost:3100}
    start_block: 0
    contracts:
      - name: ERC20
        address: ["0xdac17f958d2ee523a2206206994597c13d831ec7"]
`,
  );
  writeFileSync(
    join(dir, "schema.graphql"),
    `type Account @entity {
  id: ID!
  balance: BigInt!
  transfersIn: Int!
  transfersOut: Int!
  transfers: [Transfer!]! @derivedFrom(field: "from")
}

type Transfer @entity {
  id: ID!
  from: Account!
  to: Account!
  value: BigInt!
  block: Int! @index
  timestamp: Int!
  txHash: Bytes!
}
`,
  );
  writeFileSync(
    join(dir, "src/handlers.ts"),
    `import { ERC20, type Account } from "../generated/index.ts";

const empty = (id: string): Account => ({ id, balance: 0n, transfersIn: 0, transfersOut: 0 });

ERC20.Transfer.handler(async ({ event, context }) => {
  const { from, to, value } = event.params;
  const a = await context.Account.getOrCreate(from.toLowerCase(), () => empty(from.toLowerCase()));
  context.Account.set({ ...a, balance: a.balance - value, transfersOut: a.transfersOut + 1 });
  const b = await context.Account.getOrCreate(to.toLowerCase(), () => empty(to.toLowerCase()));
  context.Account.set({ ...b, balance: b.balance + value, transfersIn: b.transfersIn + 1 });
  context.Transfer.set({
    id: \`\${event.block.number}-\${event.logIndex}\`,
    from: from.toLowerCase(),
    to: to.toLowerCase(),
    value,
    block: event.block.number,
    timestamp: event.block.timestamp,
    txHash: event.transaction.hash,
  });
});
`,
  );
  writeFileSync(
    join(dir, "package.json"),
    JSON.stringify(
      {
        name: "my-indexer",
        private: true,
        type: "module",
        scripts: { codegen: "faster-index codegen", dev: "faster-index dev", start: "faster-index start" },
        dependencies: { "faster-index": "^0.1.0" },
      },
      null,
      2,
    ) + "\n",
  );
  writeFileSync(join(dir, ".gitignore"), "node_modules/\ngenerated/\n.faster-index/\n");
  console.log(`created ${dir}: edit faster-index.yaml, then \`npm install && npx faster-index dev\``);
}

main().catch((e) => {
  consoleLogger.error("fatal", { error: String(e?.stack ?? e) });
  process.exit(1);
});
