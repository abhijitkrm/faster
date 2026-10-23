// Vendored from faster-sync/clients/ts (same author); keep in sync.
// Client for the faster-sync HTTP API.
//
// `query` runs one request and returns one page. `stream` follows
// `next_block` page by page and, with `follow`, keeps waiting for new
// blocks at the tip; it passes the hash of the last block it saw as
// `parent_hash`, so a reorg surfaces as a `ReorgError` instead of silently
// mixing two forks.

export type Hex = string;

export interface LogSelection {
  address?: Hex[];
  /** topics[i]: accepted values at position i; empty or missing = any. */
  topics?: Hex[][];
}

export interface TransactionSelection {
  from?: Hex[];
  to?: Hex[];
  sighash?: Hex[];
  contract_address?: Hex[];
  hash?: Hex[];
  status?: number;
}

export interface TraceSelection {
  from?: Hex[];
  to?: Hex[];
  sighash?: Hex[];
  call_type?: string[];
}

export interface BlockSelection {
  hash?: Hex[];
  miner?: Hex[];
}

export interface Fields {
  block?: string[];
  transaction?: string[];
  log?: string[];
  trace?: string[];
}

export interface Query {
  from_block: number;
  /** Exclusive. Missing = up to the newest stored block. */
  to_block?: number;
  logs?: LogSelection[];
  transactions?: TransactionSelection[];
  traces?: TraceSelection[];
  blocks?: BlockSelection[];
  include_all_blocks?: boolean;
  join?: "nothing" | "blocks" | "default" | "all";
  fields?: Fields;
  max_rows?: number;
  max_ms?: number;
  parent_hash?: Hex;
}

export type Row = Record<string, unknown>;

export interface Page {
  blocks: Row[];
  transactions: Row[];
  logs: Row[];
  traces: Row[];
  /** Where the next page starts. */
  next_block: number;
  archive_height: number | null;
  chain_head: number;
  finalized_block: number;
  /** The last block covered, when it can still be reorged. */
  last_block: { block_number: number; hash: Hex } | null;
  rows: number;
  elapsed_ms: number;
}

export interface Height {
  height: number | null;
  chain_head: number;
  finalized_block: number;
  first_block: number | null;
  chunked_to: number | null;
  chain_id: number;
}

export class FasterSyncError extends Error {
  constructor(
    message: string,
    readonly status: number,
  ) {
    super(message);
    this.name = "FasterSyncError";
  }
}

/** The block before the requested range is no longer canonical. */
export class ReorgError extends FasterSyncError {
  constructor(
    message: string,
    readonly block: number,
  ) {
    super(message, 409);
    this.name = "ReorgError";
  }
}

export interface ClientOptions {
  url: string;
  apiKey?: string;
  timeoutMs?: number;
  /** Retries for 429 / 5xx / network errors (with backoff). */
  retries?: number;
  fetch?: typeof fetch;
}

export interface StreamOptions {
  /** Keep waiting for new blocks once caught up (ignored with to_block). */
  follow?: boolean;
  /** Long-poll wait per /height call at the tip. */
  pollTimeoutMs?: number;
  signal?: AbortSignal;
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

export class FasterSync {
  private readonly url: string;
  private readonly f: typeof fetch;

  constructor(private readonly opts: ClientOptions) {
    this.url = opts.url.replace(/\/+$/, "");
    this.f = opts.fetch ?? fetch;
  }

  private headers(): Record<string, string> {
    const h: Record<string, string> = { "content-type": "application/json" };
    if (this.opts.apiKey) h.authorization = `Bearer ${this.opts.apiKey}`;
    return h;
  }

  private async request(path: string, init: RequestInit): Promise<unknown> {
    const retries = this.opts.retries ?? 5;
    let delay = 200;
    for (let attempt = 0; ; attempt++) {
      let res: Response;
      try {
        res = await this.f(this.url + path, {
          ...init,
          headers: this.headers(),
          signal: AbortSignal.timeout(this.opts.timeoutMs ?? 60_000),
        });
      } catch (e) {
        if (attempt >= retries) throw e;
        await sleep(delay);
        delay = Math.min(delay * 2, 10_000);
        continue;
      }
      if (res.ok) return res.json();
      const body = await res.text();
      let msg = body;
      let parsed: { error?: string; reorg?: boolean; block?: number } = {};
      try {
        parsed = JSON.parse(body);
        msg = parsed.error ?? body;
      } catch {
        /* not JSON */
      }
      if (res.status === 409 && parsed.reorg) throw new ReorgError(msg, parsed.block ?? 0);
      if ((res.status === 429 || res.status >= 500) && attempt < retries) {
        await sleep(delay);
        delay = Math.min(delay * 2, 10_000);
        continue;
      }
      throw new FasterSyncError(msg, res.status);
    }
  }

  /** One page of results. */
  async query(q: Query): Promise<Page> {
    return (await this.request("/query", { method: "POST", body: JSON.stringify(q) })) as Page;
  }

  /** Current heights; with `above`, waits (up to timeoutMs) until height > above. */
  async height(above?: number, timeoutMs = 10_000): Promise<Height> {
    const qs = above === undefined ? "" : `?above=${above}&timeout_ms=${timeoutMs}`;
    return (await this.request("/height" + qs, { method: "GET" })) as Height;
  }

  /** Hashes of blocks [from, to) as stored now (for finding a fork point). */
  async blockHashes(from: number, to: number): Promise<Map<number, Hex>> {
    const out = new Map<number, Hex>();
    let next = from;
    while (next < to) {
      const p = await this.query({
        from_block: next,
        to_block: to,
        include_all_blocks: true,
        fields: { block: ["number", "hash"] },
      });
      for (const b of p.blocks) out.set(b.number as number, b.hash as Hex);
      if (p.next_block <= next) break;
      next = p.next_block;
    }
    return out;
  }

  /**
   * Pages from `q.from_block` until `q.to_block`, or forever with `follow`.
   * Throws ReorgError when a block it already returned stops being canonical.
   */
  async *stream(q: Query, opts: StreamOptions = {}): AsyncGenerator<Page> {
    let from = q.from_block;
    let parent: Hex | undefined = q.parent_hash;
    const end = q.to_block;
    while (!opts.signal?.aborted) {
      if (end !== undefined && from >= end) return;
      const page = await this.query({ ...q, from_block: from, parent_hash: from > 0 ? parent : undefined });
      if (page.next_block > from || page.rows > 0) yield page;
      if (page.last_block && page.last_block.block_number === page.next_block - 1) {
        parent = page.last_block.hash;
      } else if (page.next_block > from) {
        // Final blocks: no reorg check needed for the next request.
        parent = undefined;
      }
      if (page.next_block > from) {
        from = page.next_block;
        continue;
      }
      // Caught up.
      if (end !== undefined && !opts.follow) return;
      if (!opts.follow) return;
      await this.height(from - 1, opts.pollTimeoutMs ?? 10_000);
    }
  }
}
