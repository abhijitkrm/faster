// Database access (Postgres via `pg`, or embedded PGlite for dev and tests)
// and the tables faster-index manages.

import { createHash } from "node:crypto";
import { columns, sqlType, type Entity } from "./schema.js";

export interface Db {
  query(sql: string, params?: unknown[]): Promise<{ rows: any[] }>;
  tx<T>(fn: (db: Db) => Promise<T>): Promise<T>;
  close(): Promise<void>;
}

export async function openPostgres(url: string): Promise<Db> {
  const pg = (await import("pg")).default;
  // numeric (1700) and int8 (20) as strings; converted per field type.
  const pool = new pg.Pool({ connectionString: url, max: Number(process.env.PG_POOL_MAX ?? 8) });
  const wrap = (c: { query: (s: string, p?: unknown[]) => Promise<{ rows: any[] }> }): Db => ({
    query: (s, p) => c.query(s, p),
    tx: () => Promise.reject(new Error("nested transaction")),
    close: async () => {},
  });
  return {
    query: (s, p) => pool.query(s, p as any[]),
    async tx(fn) {
      const c = await pool.connect();
      try {
        await c.query("BEGIN");
        const r = await fn(wrap(c));
        await c.query("COMMIT");
        return r;
      } catch (e) {
        await c.query("ROLLBACK").catch(() => {});
        throw e;
      } finally {
        c.release();
      }
    },
    close: () => pool.end(),
  };
}

export async function openPglite(dir?: string): Promise<Db> {
  const { PGlite } = await import("@electric-sql/pglite");
  if (dir) (await import("node:fs")).mkdirSync(dir, { recursive: true });
  const pg = dir ? new PGlite(dir) : new PGlite();
  await pg.waitReady;
  const wrap = (c: { query: (s: string, p?: unknown[]) => Promise<{ rows: any[] }> }): Db => ({
    query: (s, p) => c.query(s, p),
    tx: () => Promise.reject(new Error("nested transaction")),
    close: async () => {},
  });
  return {
    query: (s, p) => pg.query(s, p),
    tx: (fn) => pg.transaction((t) => fn(wrap(t))),
    close: () => pg.close(),
  };
}

export const q = (name: string) => `"${name.replace(/"/g, '""')}"`;

export function schemaHash(entities: Entity[]): string {
  const shape = entities.map((e) => ({ n: e.name, f: columns(e).map((f) => [f.name, sqlType(f), f.required]) }));
  return createHash("sha256").update(JSON.stringify(shape)).digest("hex").slice(0, 16);
}

/**
 * Creates the entity tables and faster-index's own tables. With `reset`,
 * drops everything first. Refuses to start on data written by a different
 * schema (the caller asks for a reset).
 */
export async function migrate(db: Db, entities: Entity[], reset = false): Promise<void> {
  if (reset) {
    for (const e of entities) await db.query(`DROP TABLE IF EXISTS ${q(e.name)}`);
    for (const t of ["_fi_meta", "_fi_checkpoint", "_fi_blocks", "_fi_history"]) await db.query(`DROP TABLE IF EXISTS ${t}`);
  }
  await db.query(`CREATE TABLE IF NOT EXISTS _fi_meta (key text PRIMARY KEY, value text NOT NULL)`);
  const hash = schemaHash(entities);
  const have = (await db.query(`SELECT value FROM _fi_meta WHERE key = 'schema'`)).rows[0]?.value;
  if (have && have !== hash) {
    throw new Error("schema.graphql changed since this database was indexed: run with --reset to re-index");
  }
  await db.tx(async (t) => {
    for (const e of entities) {
      const cols = columns(e).map((f) => `${q(f.name)} ${sqlType(f)}${f.name === "id" ? " PRIMARY KEY" : ""}`);
      await t.query(`CREATE TABLE IF NOT EXISTS ${q(e.name)} (${cols.join(", ")})`);
      for (const f of columns(e)) {
        if (f.index || f.entity) {
          await t.query(`CREATE INDEX IF NOT EXISTS ${q(`${e.name}_${f.name}_idx`)} ON ${q(e.name)} (${q(f.name)})`);
        }
      }
    }
    await t.query(`CREATE TABLE IF NOT EXISTS _fi_checkpoint (chain_id bigint PRIMARY KEY, next_block bigint NOT NULL, last_hash text)`);
    await t.query(`CREATE TABLE IF NOT EXISTS _fi_blocks (chain_id bigint, number bigint, hash text NOT NULL, PRIMARY KEY (chain_id, number))`);
    await t.query(
      `CREATE TABLE IF NOT EXISTS _fi_history (seq bigserial PRIMARY KEY, chain_id bigint NOT NULL, block bigint NOT NULL, entity text NOT NULL, entity_id text NOT NULL, prev jsonb)`,
    );
    await t.query(`CREATE INDEX IF NOT EXISTS _fi_history_block ON _fi_history (chain_id, block)`);
    await t.query(`INSERT INTO _fi_meta (key, value) VALUES ('schema', $1) ON CONFLICT (key) DO UPDATE SET value = $1`, [hash]);
  });
}
