// The indexing loop: pages of matching logs from faster-sync, decoded and
// passed to handlers in chain order; one database transaction per page
// (entities, journal, checkpoint); rollback and resume on reorgs.

import { contractEvents, decode, type EventDef } from "./abi.js";
import type { Config, NetworkConfig } from "./config.js";
import type { Db } from "./db.js";
import { columns, type Entity } from "./schema.js";
import { EntityCache, rollback, Store, type Row } from "./store.js";
import { FasterSync, ReorgError, type Page, type Query } from "./sync-client.js";

export interface Block {
  number: number;
  hash: string;
  timestamp: number;
}

export interface Transaction {
  hash: string;
  index: number;
  [field: string]: unknown;
}

export interface Event<P = Record<string, unknown>> {
  contract: string;
  name: string;
  params: P;
  srcAddress: string;
  logIndex: number;
  chainId: number;
  block: Block;
  transaction: Transaction;
}

export interface EntityOps<T = Row> {
  get(id: string): Promise<T | undefined>;
  getOrThrow(id: string): Promise<T>;
  getOrCreate(id: string, init: () => T): Promise<T>;
  set(entity: T): void;
  delete(id: string): void;
}

export interface Context {
  chainId: number;
  log: { info(...a: unknown[]): void; warn(...a: unknown[]): void; error(...a: unknown[]): void };
  [entity: string]: EntityOps<any> | unknown;
}

export type Handler = (args: { event: Event<any>; context: any }) => Promise<void> | void;

const registry = new Map<string, Handler[]>();

/** Registers a handler for `contract.event` (generated code calls this). */
export function register(contract: string, event: string, fn: Handler): void {
  const k = `${contract}.${event}`;
  registry.set(k, [...(registry.get(k) ?? []), fn]);
}

export function clearHandlers(): void {
  registry.clear();
}

export function registeredKeys(): string[] {
  return [...registry.keys()];
}

export interface Logger {
  info(msg: string, fields?: Record<string, unknown>): void;
  warn(msg: string, fields?: Record<string, unknown>): void;
  error(msg: string, fields?: Record<string, unknown>): void;
}

export const consoleLogger: Logger = {
  info: (m, f) => console.log(JSON.stringify({ level: "info", msg: m, ...f }, bigintJson)),
  warn: (m, f) => console.warn(JSON.stringify({ level: "warn", msg: m, ...f }, bigintJson)),
  error: (m, f) => console.error(JSON.stringify({ level: "error", msg: m, ...f }, bigintJson)),
};

function bigintJson(_: string, v: unknown) {
  return typeof v === "bigint" ? v.toString() : v;
}

export interface Progress {
  chainId: number;
  nextBlock: number;
  events: number;
  head: number | null;
  reorgs: number;
}

export interface RunOptions {
  signal?: AbortSignal;
  log?: Logger;
  /** Called after each committed page. */
  onPage?: (p: Progress) => void;
  /** Entity cache shared by networks writing the same tables. */
  cache?: EntityCache;
}

const LOG_FIELDS = ["block_number", "log_index", "transaction_index", "transaction_hash", "address", "topic0", "topic1", "topic2", "topic3", "data"];

/** Indexes one network until `end_block` (or forever). */
export async function runNetwork(cfg: Config, net: NetworkConfig, db: Db, entities: Entity[], opts: RunOptions = {}): Promise<Progress> {
  const log = opts.log ?? consoleLogger;
  const client = new FasterSync({ url: net.sync_url, apiKey: net.sync_api_key });
  const defs = contractEvents(cfg).filter((d) => registry.has(`${d.contract}.${d.name}`));
  if (defs.length === 0) throw new Error("no handlers registered for any configured event");
  const byAddr = new Map<string, { contract: string; start: number }>();
  for (const c of net.contracts) for (const a of c.address) byAddr.set(a, { contract: c.name, start: c.start_block ?? net.start_block });
  const byTopic = new Map<string, EventDef[]>();
  for (const d of defs) byTopic.set(`${d.contract}:${d.topic0}`, [...(byTopic.get(`${d.contract}:${d.topic0}`) ?? []), d]);

  const selections = net.contracts
    .map((c) => ({
      address: c.address,
      topics: [[...new Set(defs.filter((d) => d.contract === c.name).map((d) => d.topic0))]],
    }))
    .filter((s) => s.topics[0].length > 0);
  const txFields = cfg.transaction_fields;
  const base: Query = {
    from_block: 0,
    logs: selections,
    join: txFields.length ? "default" : "blocks",
    fields: {
      block: ["number", "hash", "timestamp"],
      log: LOG_FIELDS,
      transaction: ["block_number", "transaction_index", ...txFields.filter((f) => f !== "transaction_index" && f !== "block_number")],
    },
    max_rows: cfg.page_rows,
  };
  if (net.end_block !== undefined) base.to_block = net.end_block + 1;

  const progress: Progress = { chainId: net.id, nextBlock: 0, events: 0, head: null, reorgs: 0 };
  const cache = opts.cache ?? new EntityCache();
  if (!opts.cache) await cache.init(db, entities);
  const firstBlock = Math.min(net.start_block, ...net.contracts.map((c) => c.start_block ?? net.start_block));

  while (!opts.signal?.aborted) {
    const cp = (await db.query(`SELECT next_block, last_hash FROM _fi_checkpoint WHERE chain_id = $1`, [net.id])).rows[0];
    const from = cp ? Number(cp.next_block) : firstBlock;
    const parent: string | undefined = cp?.last_hash ?? undefined;
    progress.nextBlock = from;
    if (base.to_block !== undefined && from >= base.to_block) return progress;
    log.info("indexing", { chain_id: net.id, from_block: from, contracts: net.contracts.length, events: defs.length });
    try {
      const pages = prefetch(client.stream({ ...base, from_block: from, parent_hash: parent }, { follow: net.end_block === undefined, signal: opts.signal }));
      for await (const page of pages) {
        await processPage(page, net, db, entities, byAddr, byTopic, progress, log, cache);
        opts.onPage?.(progress);
        if (opts.signal?.aborted) break;
      }
      return progress;
    } catch (e) {
      if (!(e instanceof ReorgError)) throw e;
      progress.reorgs++;
      const fork = await findFork(db, client, net.id, e.block);
      const undone = await rollback(db, entities, net.id, fork);
      await cache.init(db, entities);
      log.warn("reorg: rolled back", { chain_id: net.id, fork_block: fork, journal_entries: undone });
    }
  }
  return progress;
}

/** Iterates `it` while already fetching the next item (overlaps network and processing). */
async function* prefetch<T>(it: AsyncGenerator<T>): AsyncGenerator<T> {
  let next = it.next();
  try {
    while (true) {
      const r = await next;
      if (r.done) return;
      next = it.next();
      // Avoid an unhandled rejection if the consumer stops first.
      next.catch(() => {});
      yield r.value;
    }
  } finally {
    await it.return(undefined).catch(() => {});
  }
}

/** Newest block we processed that is still canonical. */
async function findFork(db: Db, client: FasterSync, chainId: number, at: number): Promise<number> {
  const ours = (await db.query(`SELECT number, hash FROM _fi_blocks WHERE chain_id = $1 AND number <= $2 ORDER BY number DESC`, [chainId, at])).rows;
  if (ours.length === 0) return at - 1;
  const lo = Number(ours[ours.length - 1].number);
  const canon = await client.blockHashes(lo, at + 1);
  for (const r of ours) {
    if (canon.get(Number(r.number)) === r.hash) return Number(r.number);
  }
  return lo - 1;
}

async function processPage(
  page: Page,
  net: NetworkConfig,
  db: Db,
  entities: Entity[],
  byAddr: Map<string, { contract: string; start: number }>,
  byTopic: Map<string, EventDef[]>,
  progress: Progress,
  log: Logger,
  cache: EntityCache,
): Promise<void> {
  const finalized = page.finalized_block;
  const blocks = new Map<number, Block>();
  for (const b of page.blocks) blocks.set(b.number as number, { number: b.number as number, hash: b.hash as string, timestamp: b.timestamp as number });
  const txs = new Map<string, Row>();
  for (const t of page.transactions) txs.set(`${t.block_number}:${t.transaction_index}`, t);

  const store = new Store(db, entities, net.id, cache);
  let block = 0;
  let journal = false;
  const ctx: Context = {
    chainId: net.id,
    log: {
      info: (...a) => log.info(a.map(String).join(" "), { chain_id: net.id, block }),
      warn: (...a) => log.warn(a.map(String).join(" "), { chain_id: net.id, block }),
      error: (...a) => log.error(a.map(String).join(" "), { chain_id: net.id, block }),
    },
  };
  for (const e of entities) {
    const name = e.name;
    const ops: EntityOps = {
      get: (id) => store.get(name, id),
      async getOrThrow(id) {
        const v = await store.get(name, id);
        if (!v) throw new Error(`${name} ${id} not found`);
        return v;
      },
      async getOrCreate(id, init) {
        const v = await store.get(name, id);
        if (v) return v;
        const n = init();
        store.set(name, n, block, journal);
        return n;
      },
      set: (row) => store.set(name, row, block, journal),
      delete: (id) => store.delete(name, id, block, journal),
    };
    ctx[name] = ops;
  }

  for (const l of page.logs) {
    const addr = (l.address as string).toLowerCase();
    const c = byAddr.get(addr);
    if (!c) continue;
    const bn = l.block_number as number;
    if (bn < c.start) continue;
    const topics = [l.topic0, l.topic1, l.topic2, l.topic3].filter((t) => t !== null && t !== undefined) as string[];
    const defs = byTopic.get(`${c.contract}:${(topics[0] ?? "").toLowerCase()}`);
    if (!defs) continue;
    for (const d of defs) {
      const params = decode(d, topics, l.data as string);
      if (!params) continue;
      block = bn;
      journal = bn > finalized;
      const tx = txs.get(`${bn}:${l.transaction_index}`) ?? {};
      const event: Event = {
        contract: d.contract,
        name: d.name,
        params,
        srcAddress: addr,
        logIndex: l.log_index as number,
        chainId: net.id,
        block: blocks.get(bn) ?? { number: bn, hash: "", timestamp: 0 },
        transaction: { ...tx, hash: l.transaction_hash as string, index: l.transaction_index as number },
      };
      for (const h of registry.get(`${d.contract}.${d.name}`) ?? []) {
        try {
          await h({ event, context: ctx });
        } catch (err) {
          log.error("handler failed", { chain_id: net.id, contract: d.contract, event: d.name, block: bn, log_index: event.logIndex, error: String((err as Error)?.stack ?? err) });
          throw err;
        }
      }
      progress.events++;
    }
  }

  const next = page.next_block;
  const lastHash = page.last_block && page.last_block.block_number === next - 1 ? page.last_block.hash : null;
  await store.commit(async (t) => {
    await t.query(
      `INSERT INTO _fi_checkpoint (chain_id, next_block, last_hash) VALUES ($1, $2, $3) ON CONFLICT (chain_id) DO UPDATE SET next_block = $2, last_hash = $3`,
      [net.id, next, lastHash],
    );
    const recent = [...blocks.values()].filter((b) => b.number > finalized);
    if (lastHash) recent.push({ number: next - 1, hash: lastHash, timestamp: 0 });
    for (const b of recent) {
      await t.query(`INSERT INTO _fi_blocks (chain_id, number, hash) VALUES ($1, $2, $3) ON CONFLICT (chain_id, number) DO UPDATE SET hash = $3`, [
        net.id,
        b.number,
        b.hash,
      ]);
    }
    await t.query(`DELETE FROM _fi_history WHERE chain_id = $1 AND block <= $2`, [net.id, finalized]);
    await t.query(`DELETE FROM _fi_blocks WHERE chain_id = $1 AND number < $2`, [net.id, finalized]);
  });
  progress.nextBlock = next;
  progress.head = page.archive_height;
}

/** Column names of an entity (for codegen and GraphQL). */
export function entityColumns(e: Entity): string[] {
  return columns(e).map((f) => f.name);
}
