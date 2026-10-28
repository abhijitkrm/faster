// Unit tests with a fake fetch: paging, tip following, reorg and retries.
import { test } from "node:test";
import assert from "node:assert/strict";
import { FasterSync, ReorgError } from "../dist/index.js";

function fakeFetch(handler) {
  const calls = [];
  const f = async (url, init) => {
    const body = init.body ? JSON.parse(init.body) : undefined;
    calls.push({ url, body });
    const [status, json] = await handler(url, body, calls.length);
    return new Response(JSON.stringify(json), { status, headers: { "content-type": "application/json" } });
  };
  return { f, calls };
}

const page = (next, extra = {}) => ({
  blocks: [], transactions: [], logs: [{ block_number: next - 1 }], traces: [],
  next_block: next, archive_height: 99, chain_head: 99, finalized_block: 90, last_block: null, rows: 1, elapsed_ms: 1, ...extra,
});

test("stream pages until to_block", async () => {
  const { f, calls } = fakeFetch((url, body) => [200, page(Math.min(body.from_block + 10, 30))]);
  const c = new FasterSync({ url: "http://x/", fetch: f });
  const pages = [];
  for await (const p of c.stream({ from_block: 0, to_block: 30 })) pages.push(p.next_block);
  assert.deepEqual(pages, [10, 20, 30]);
  assert.equal(calls[0].url, "http://x/query");
});

test("follow passes parent_hash and surfaces reorgs", async () => {
  let queries = 0;
  const { f, calls } = fakeFetch((url) => {
    if (url.includes("/height")) return [200, { height: 100 }];
    queries++;
    if (queries === 1) return [200, page(100, { last_block: { block_number: 99, hash: "0xaa" } })];
    if (queries === 2) return [200, page(100, { rows: 0, logs: [] })];
    return [409, { error: "reorg", reorg: true, block: 99 }];
  });
  const c = new FasterSync({ url: "http://x", fetch: f });
  const seen = [];
  await assert.rejects(async () => {
    for await (const p of c.stream({ from_block: 50 }, { follow: true })) seen.push(p.next_block);
  }, (e) => e instanceof ReorgError && e.block === 99);
  assert.deepEqual(seen, [100]);
  const qs = calls.filter((c) => c.url.endsWith("/query"));
  assert.equal(qs[1].body.parent_hash, "0xaa");
  assert.equal(qs[2].body.parent_hash, "0xaa");
  assert.ok(calls.some((c) => c.url.includes("/height?above=99")), "waited at the tip");
});

test("retries 429 then succeeds; API key header", async () => {
  let headers;
  const f = async (url, init) => {
    headers = init.headers;
    f.n = (f.n ?? 0) + 1;
    if (f.n < 3) return new Response("{\"error\":\"rate\"}", { status: 429 });
    return new Response(JSON.stringify(page(5)), { status: 200 });
  };
  const c = new FasterSync({ url: "http://x", fetch: f, apiKey: "k" });
  const p = await c.query({ from_block: 0 });
  assert.equal(p.next_block, 5);
  assert.equal(f.n, 3);
  assert.equal(headers.authorization, "Bearer k");
});
