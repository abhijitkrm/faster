// Entity store used by handlers: reads go through a per-batch cache to the
// database; writes are collected and committed with the checkpoint in one
// transaction. For blocks that can still be reorged, the state of each
// entity before its first change in a block is journaled in _fi_history,
// so a rollback restores it exactly.

import { columns, type Entity, type Field } from "./schema.js";
import { q, type Db } from "./db.js";

export type Row = Record<string, unknown>;

interface Slot {
  value: Row | null;
  dirty: boolean;
}

/** JSON with bigints as strings (journal, json columns). */
export function toJson(v: unknown): string {
  return JSON.stringify(v, (_, x) => (typeof x === "bigint" ? x.toString() : x));
}

export function fromSql(f: Field, v: unknown): unknown {
  if (v === null || v === undefined) return null;
  if (f.list || f.type === "Json") return typeof v === "string" ? JSON.parse(v) : v;
  if (f.entity) return v;
  switch (f.type) {
    case "BigInt":
      return BigInt(typeof v === "number" ? Math.trunc(v) : (v as string));
    case "BigDecimal":
      return String(v);
    case "Int":
    case "Float":
      return Number(v);
    default:
      return v;
  }
}

export function toSql(f: Field, v: unknown): unknown {
  if (v === null || v === undefined) return null;
  if (f.list || f.type === "Json") return toJson(v);
  if (typeof v === "bigint") return v.toString();
  return v;
}

/**
 * Entities across batches. faster-index is the only writer of its tables, so
 * committed values stay valid. While a table is `complete` (it was empty
 * when indexing started and nothing was evicted), a miss means the entity
 * does not exist and no query is needed.
 */
export class EntityCache {
  private m = new Map<string, Map<string, Row | null>>();
  private complete = new Set<string>();
  private size = 0;

  constructor(readonly limit = 1_000_000) {}

  /** Marks tables that are empty in the database as completely cached. */
  async init(db: Db, entities: Entity[]): Promise<void> {
    this.clear();
    for (const e of entities) {
      const r = (await db.query(`SELECT 1 FROM ${q(e.name)} LIMIT 1`)).rows;
      if (r.length === 0) this.complete.add(e.name);
    }
  }

  /** undefined: unknown (ask the database); null: known absent. */
  get(entity: string, id: string): Row | null | undefined {
    const v = this.m.get(entity)?.get(id);
    if (v !== undefined) return v;
    return this.complete.has(entity) ? null : undefined;
  }

  put(entity: string, id: string, v: Row | null): void {
    let t = this.m.get(entity);
    if (!t) this.m.set(entity, (t = new Map()));
    if (!t.has(id)) this.size++;
    t.set(id, v);
    if (this.size > this.limit) {
      // Over budget: drop everything; tables are no longer complete.
      this.m.clear();
      this.complete.clear();
      this.size = 0;
    }
  }

  clear(): void {
    this.m.clear();
    this.complete.clear();
    this.size = 0;
  }
}

export class Store {
  private cache = new Map<string, Map<string, Slot>>();
  /** entity → id → block of the newest journaled first-touch. */
  private touched = new Map<string, Map<string, number>>();
  /** Journal entries: prev === undefined means "load from the database". */
  private journal: { block: number; entity: string; id: string; prev: Row | null | undefined }[] = [];
  readonly entities: Map<string, Entity>;

  constructor(
    private db: Db,
    entities: Entity[],
    private chainId: number,
    private shared?: EntityCache,
  ) {
    this.entities = new Map(entities.map((e) => [e.name, e]));
  }

  private slots(entity: string): Map<string, Slot> {
    let m = this.cache.get(entity);
    if (!m) this.cache.set(entity, (m = new Map()));
    return m;
  }

  private entity(name: string): Entity {
    const e = this.entities.get(name);
    if (!e) throw new Error(`unknown entity ${name}`);
    return e;
  }

  async get(entity: string, id: string): Promise<Row | undefined> {
    const s = this.slots(entity).get(id);
    if (s) return s.value ? { ...s.value } : undefined;
    const e = this.entity(entity);
    const c = this.shared?.get(entity, id);
    if (c !== undefined) {
      this.slots(entity).set(id, { value: c, dirty: false });
      return c ? { ...c } : undefined;
    }
    const r = (await this.db.query(`SELECT * FROM ${q(entity)} WHERE id = $1`, [id])).rows[0];
    const value = r ? this.decode(e, r) : null;
    this.slots(entity).set(id, { value, dirty: false });
    return value ? { ...value } : undefined;
  }

  private decode(e: Entity, r: Row): Row {
    const out: Row = {};
    for (const f of columns(e)) out[f.name] = fromSql(f, r[f.name]);
    return out;
  }

  /** Journals the state before the first change of (entity, id) in `block`. */
  private touch(entity: string, id: string, block: number, journal: boolean) {
    if (!journal) return;
    let m = this.touched.get(entity);
    if (!m) this.touched.set(entity, (m = new Map()));
    if (m.get(id) === block) return;
    m.set(id, block);
    const s = this.slots(entity).get(id);
    let prev: Row | null | undefined = s ? (s.value ? { ...s.value } : null) : undefined;
    if (prev === undefined) {
      const c = this.shared?.get(entity, id);
      if (c !== undefined) prev = c ? { ...c } : null;
    }
    this.journal.push({ block, entity, id, prev });
  }

  set(entity: string, row: Row, block: number, journal: boolean): void {
    const e = this.entity(entity);
    if (typeof row.id !== "string" || !row.id) throw new Error(`${entity}.set: id must be a non-empty string`);
    for (const f of columns(e)) {
      if (f.required && (row[f.name] === undefined || row[f.name] === null)) {
        throw new Error(`${entity}.set(${row.id}): required field ${f.name} is missing`);
      }
    }
    this.touch(entity, row.id, block, journal);
    const value: Row = {};
    for (const f of columns(e)) value[f.name] = row[f.name] ?? null;
    this.slots(entity).set(row.id, { value, dirty: true });
  }

  delete(entity: string, id: string, block: number, journal: boolean): void {
    this.entity(entity);
    this.touch(entity, id, block, journal);
    this.slots(entity).set(id, { value: null, dirty: true });
  }

  pending(): number {
    let n = 0;
    for (const m of this.cache.values()) for (const s of m.values()) if (s.dirty) n++;
    return n;
  }

  /** Writes all changes plus `extra` statements in one transaction, then clears the cache. */
  async commit(extra: (t: Db) => Promise<void>): Promise<void> {
    await this.db.tx(async (t) => {
      // Journal first: entries with unknown prev read the committed state.
      for (const j of this.journal) {
        let prev = j.prev;
        if (prev === undefined) {
          const r = (await t.query(`SELECT * FROM ${q(j.entity)} WHERE id = $1`, [j.id])).rows[0];
          prev = r ? this.decode(this.entity(j.entity), r) : null;
        }
        await t.query(`INSERT INTO _fi_history (chain_id, block, entity, entity_id, prev) VALUES ($1, $2, $3, $4, $5)`, [
          this.chainId,
          j.block,
          j.entity,
          j.id,
          prev === null ? null : toJson(prev),
        ]);
      }
      for (const [name, slots] of this.cache) {
        const e = this.entity(name);
        const cols = columns(e);
        const ups: Row[] = [];
        const dels: string[] = [];
        for (const [id, s] of slots) {
          if (!s.dirty) continue;
          if (s.value) ups.push(s.value);
          else dels.push(id);
        }
        if (dels.length) await t.query(`DELETE FROM ${q(name)} WHERE id = ANY($1)`, [dels]);
        for (let i = 0; i < ups.length; i += 500) {
          const chunk = ups.slice(i, i + 500);
          const params: unknown[] = [];
          const values = chunk.map((r) => {
            const ph = cols.map((f) => {
              params.push(toSql(f, r[f.name]));
              return `$${params.length}`;
            });
            return `(${ph.join(", ")})`;
          });
          const set = cols.filter((f) => f.name !== "id").map((f) => `${q(f.name)} = EXCLUDED.${q(f.name)}`);
          await t.query(
            `INSERT INTO ${q(name)} (${cols.map((f) => q(f.name)).join(", ")}) VALUES ${values.join(", ")} ` +
              (set.length ? `ON CONFLICT (id) DO UPDATE SET ${set.join(", ")}` : `ON CONFLICT (id) DO NOTHING`),
            params,
          );
        }
      }
      await extra(t);
    });
    if (this.shared) {
      for (const [name, slots] of this.cache) for (const [id, s] of slots) this.shared.put(name, id, s.value);
    }
    this.cache.clear();
    this.touched.clear();
    this.journal = [];
  }

  /** Discards uncommitted changes. */
  reset(): void {
    this.cache.clear();
    this.touched.clear();
    this.journal = [];
  }
}

/**
 * Undoes every change of `chainId` made in blocks after `fork` and returns
 * the number of journal entries applied.
 */
export async function rollback(db: Db, entities: Entity[], chainId: number, fork: number): Promise<number> {
  const byName = new Map(entities.map((e) => [e.name, e]));
  return db.tx(async (t) => {
    const rows = (
      await t.query(`SELECT entity, entity_id, prev FROM _fi_history WHERE chain_id = $1 AND block > $2 ORDER BY seq DESC`, [chainId, fork])
    ).rows;
    for (const r of rows) {
      const e = byName.get(r.entity);
      if (!e) continue;
      const prev = typeof r.prev === "string" ? JSON.parse(r.prev) : r.prev;
      if (prev === null) {
        await t.query(`DELETE FROM ${q(e.name)} WHERE id = $1`, [r.entity_id]);
        continue;
      }
      const cols = columns(e);
      const vals = cols.map((f) => {
        const v = prev[f.name];
        if (v === null || v === undefined) return null;
        return f.list || f.type === "Json" ? JSON.stringify(v) : v;
      });
      const set = cols.filter((f) => f.name !== "id").map((f) => `${q(f.name)} = EXCLUDED.${q(f.name)}`);
      await t.query(
        `INSERT INTO ${q(e.name)} (${cols.map((f) => q(f.name)).join(", ")}) VALUES (${cols.map((_, i) => `$${i + 1}`).join(", ")}) ` +
          (set.length ? `ON CONFLICT (id) DO UPDATE SET ${set.join(", ")}` : `ON CONFLICT (id) DO NOTHING`),
        vals,
      );
    }
    await t.query(`DELETE FROM _fi_history WHERE chain_id = $1 AND block > $2`, [chainId, fork]);
    await t.query(`DELETE FROM _fi_blocks WHERE chain_id = $1 AND number > $2`, [chainId, fork]);
    const last = (await t.query(`SELECT hash FROM _fi_blocks WHERE chain_id = $1 AND number = $2`, [chainId, fork])).rows[0];
    await t.query(
      `INSERT INTO _fi_checkpoint (chain_id, next_block, last_hash) VALUES ($1, $2, $3) ON CONFLICT (chain_id) DO UPDATE SET next_block = $2, last_hash = $3`,
      [chainId, fork + 1, last?.hash ?? null],
    );
    return rows.length;
  });
}
