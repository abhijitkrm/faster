// Runtime against a fake faster-sync: paging, entities, GraphQL, and a
// reorg while following the tip (entities must equal a recomputation on
// the new chain).
import { test } from "node:test";
import assert from "node:assert/strict";
import { createServer } from "node:http";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { encodeAbiParameters, keccak256, toHex } from "viem";
import { clearHandlers, loadConfig, migrate, openPglite, openPostgres, register, runNetwork, parseSchema, buildSchema } from "../dist/index.js";

const openDb = () => (process.env.TEST_DATABASE_URL ? openPostgres(process.env.TEST_DATABASE_URL) : openPglite());
import { graphql } from "graphql";

const TRANSFER = keccak256(toHex("Transfer(address,address,uint256)"));
const TOKEN = "0x00000000000000000000000000000000000000c0";
const pad = (a) => "0x" + "0".repeat(24) + a.slice(2);
const acct = (i) => "0x" + (i + 1).toString(16).padStart(40, "0");

/** Mutable chain: block n has 3 transfers among 5 accounts, depending on salt. */
class Chain {
  constructor(height) { this.height = height; this.salt = new Map(); }
  saltOf(n) { let s = 0; for (const [from, v] of this.salt) if (n >= from) s = v; return s; }
  hash(n) { return keccak256(toHex(`b${n}:${this.saltOf(n)}`)); }
  logs(n) {
    const s = this.saltOf(n);
    return [0, 1, 2].map((k) => {
      const f = acct((n * 3 + k + s) % 5), t = acct((n * 7 + k + 2 * s) % 5);
      return { block_number: n, log_index: k, transaction_index: k, transaction_hash: keccak256(toHex(`t${n}:${k}:${s}`)), address: TOKEN,
        topic0: TRANSFER, topic1: pad(f), topic2: pad(t), topic3: null, data: encodeAbiParameters([{ type: "uint256" }], [BigInt(n * 10 + k + s)]) };
    });
  }
  reorg(from) { this.salt.set(from, (this.salt.size + 1) * 13); }
}

function serve(chain) {
  const srv = createServer(async (req, res) => {
    let body = ""; for await (const c of req) body += c;
    const url = new URL(req.url, "http://x");
    const send = (s, v) => res.writeHead(s, { "content-type": "application/json" }).end(JSON.stringify(v));
    if (url.pathname === "/height") {
      const above = url.searchParams.get("above");
      if (above !== null && chain.height <= Number(above)) await new Promise((r) => setTimeout(r, 50));
      return send(200, { height: chain.height, chain_head: chain.height, finalized_block: chain.height - 5, chain_id: 1 });
    }
    const q = JSON.parse(body);
    const from = q.from_block, to = Math.min(q.to_block ?? chain.height + 1, chain.height + 1);
    if (q.parent_hash && from > 0 && q.parent_hash !== chain.hash(from - 1)) return send(409, { error: "reorg", reorg: true, block: from - 1 });
    const end = Math.min(to, from + 7); // small pages
    const logs = [], blocks = [];
    for (let n = from; n < end; n++) {
      const ls = q.logs?.length ? chain.logs(n) : [];
      logs.push(...ls);
      if (q.include_all_blocks || ls.length) blocks.push({ number: n, hash: chain.hash(n), timestamp: 1000 + n });
    }
    const last = end - 1;
    send(200, { blocks, transactions: [], logs, traces: [], next_block: end, archive_height: chain.height, chain_head: chain.height,
      finalized_block: chain.height - 5, last_block: last > chain.height - 5 && end > from ? { block_number: last, hash: chain.hash(last) } : null, rows: logs.length, elapsed_ms: 0 });
  });
  return new Promise((ok) => srv.listen(0, "127.0.0.1", () => ok(srv)));
}

const SCHEMA = `type Account @entity { id: ID! balance: BigInt! n: Int! }
type Transfer @entity { id: ID! from: Account! to: Account! value: BigInt! block: Int! }`;

function project(url, end) {
  const dir = mkdtempSync(join(tmpdir(), "fidx-"));
  writeFileSync(join(dir, "schema.graphql"), SCHEMA);
  writeFileSync(join(dir, "faster-index.yaml"), `name: t
contracts:
  - name: Token
    events: ["Transfer(address indexed from, address indexed to, uint256 value)"]
networks:
  - id: 1
    sync_url: ${url}
    start_block: 0
${end === undefined ? "" : `    end_block: ${end}\n`}    contracts:
      - name: Token
        address: ["${TOKEN}"]
`);
  return loadConfig(join(dir, "faster-index.yaml"));
}

function handlers() {
  clearHandlers();
  register("Token", "Transfer", async ({ event, context }) => {
    const { from, to, value } = event.params;
    const a = await context.Account.getOrCreate(from, () => ({ id: from, balance: 0n, n: 0 }));
    context.Account.set({ ...a, balance: a.balance - value, n: a.n + 1 });
    const b = await context.Account.getOrCreate(to, () => ({ id: to, balance: 0n, n: 0 }));
    context.Account.set({ ...b, balance: b.balance + value, n: b.n + 1 });
    context.Transfer.set({ id: `${event.block.number}-${event.logIndex}`, from, to, value, block: event.block.number });
  });
}

function expected(chain) {
  const bal = new Map();
  for (let n = 0; n <= chain.height; n++) for (const l of chain.logs(n)) {
    const f = "0x" + l.topic1.slice(26), t = "0x" + l.topic2.slice(26), v = BigInt(l.data);
    bal.set(f, (bal.get(f) ?? 0n) - v); bal.set(t, (bal.get(t) ?? 0n) + v);
  }
  return bal;
}

async function balances(db) {
  const rows = (await db.query(`SELECT id, balance FROM "Account"`)).rows;
  return new Map(rows.map((r) => [r.id, BigInt(r.balance)]));
}

test("index to end_block, query GraphQL", async () => {
  const chain = new Chain(40);
  const srv = await serve(chain);
  const cfg = project(`http://127.0.0.1:${srv.address().port}`, 40);
  const db = await openDb();
  const entities = parseSchema(SCHEMA);
  await migrate(db, entities, true);
  handlers();
  const p = await runNetwork(cfg, cfg.networks[0], db, entities, { log: { info() {}, warn() {}, error() {} } });
  assert.equal(p.nextBlock, 41);
  assert.equal(p.events, 41 * 3);
  assert.deepEqual(await balances(db), expected(chain));
  const schema = buildSchema(db, entities);
  const r = await graphql({ schema, source: `{ transfers(first: 2, where: { block_gte: 39 }, orderBy: block, orderDirection: desc) { id block from { id balance } } _meta { chains { nextBlock } } }` });
  assert.equal(r.errors, undefined, JSON.stringify(r.errors));
  assert.equal(r.data.transfers.length, 2);
  assert.equal(r.data.transfers[0].block, 40);
  assert.equal(r.data._meta.chains[0].nextBlock, 41);
  srv.close();
  await db.close();
});

test("reorg while following the tip rolls entities back", async () => {
  const chain = new Chain(30);
  const srv = await serve(chain);
  const cfg = project(`http://127.0.0.1:${srv.address().port}`);
  const db = await openDb();
  const entities = parseSchema(SCHEMA);
  await migrate(db, entities, true);
  handlers();
  const ac = new AbortController();
  let next = 0;
  let reorgs = 0;
  const run = runNetwork(cfg, cfg.networks[0], db, entities, { signal: ac.signal, log: { info() {}, warn() {}, error() {} }, onPage: (p) => { next = p.nextBlock; reorgs = p.reorgs; } });
  const until = async (f) => { for (let i = 0; i < 400 && !f(); i++) await new Promise((r) => setTimeout(r, 25)); assert.ok(f(), "timed out"); };
  await until(() => next === 31);
  assert.deepEqual(await balances(db), expected(chain));
  // Replace blocks 28..30, then grow to 33.
  chain.reorg(28);
  chain.height = 33;
  await until(() => next === 34);
  const want = expected(chain);
  assert.deepEqual(await balances(db), want);
  assert.equal(reorgs, 1, "the reorg was detected and rolled back");
  const hist = (await db.query(`SELECT count(*)::int AS n FROM _fi_history WHERE block <= 28`)).rows[0].n;
  assert.equal(hist, 0, "journal pruned up to the finalized block");
  const tr = (await db.query(`SELECT count(*)::int AS n FROM "Transfer"`)).rows[0].n;
  assert.equal(tr, 34 * 3, "no transfers left from the dropped fork");
  ac.abort();
  await run;
  srv.close();
  await db.close();
});
