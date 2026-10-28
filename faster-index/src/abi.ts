// Event ABIs: parsing, topic0, decoding.

import { readFileSync } from "node:fs";
import { decodeAbiParameters, parseAbiItem, toEventSelector, type AbiEvent, type AbiParameter } from "viem";
import type { Config } from "./config.js";

export interface EventDef {
  contract: string;
  name: string;
  abi: AbiEvent;
  topic0: string;
  /** Number of topics (topic0 + indexed params) a log of this event has. */
  topics: number;
  indexed: AbiParameter[];
  data: AbiParameter[];
  /** Every data param is a static type (decoded word by word, no viem). */
  staticData: boolean;
}

const STATIC = /^(address|bool|u?int\d*|bytes([1-9]|[12]\d|3[0-2]))$/;

export function contractEvents(cfg: Config): EventDef[] {
  const out: EventDef[] = [];
  for (const c of cfg.contracts) {
    const items: AbiEvent[] = [];
    for (const sig of c.events) {
      const s = sig.trim().startsWith("event ") ? sig.trim() : `event ${sig.trim()}`;
      const item = parseAbiItem(s) as AbiEvent;
      if (item.type !== "event") throw new Error(`${c.name}: not an event: ${sig}`);
      items.push(item);
    }
    if (c.abi) {
      const j = JSON.parse(readFileSync(c.abi, "utf8"));
      const abi = Array.isArray(j) ? j : j.abi;
      for (const it of abi) if (it.type === "event") items.push(it as AbiEvent);
    }
    const seen = new Set<string>();
    for (const abi of items) {
      if (seen.has(abi.name)) throw new Error(`${c.name}: event ${abi.name} defined twice (overloads are not supported)`);
      seen.add(abi.name);
      const indexed = abi.inputs.filter((i) => i.indexed);
      const data = abi.inputs.filter((i) => !i.indexed);
      out.push({
        contract: c.name,
        name: abi.name,
        abi,
        topic0: toEventSelector(abi).toLowerCase(),
        topics: 1 + indexed.length,
        indexed,
        data,
        staticData: data.every((p) => STATIC.test(p.type)),
      });
    }
  }
  return out;
}

/** One 32-byte word (64 hex chars, no 0x) as a value of static type `t`. */
function word(t: string, w: string): unknown {
  if (t === "address") return "0x" + w.slice(24);
  if (t === "bool") return /[1-9a-f]/.test(w);
  if (t.startsWith("bytes")) return "0x" + w.slice(0, Number(t.slice(5)) * 2);
  const bits = Number(t.replace(/^u?int/, "") || 256);
  let v = BigInt("0x" + w);
  if (t.startsWith("int") && v >> 255n === 1n) v -= 1n << 256n;
  return bits <= 48 ? Number(v) : v;
}

/**
 * Decoded params of a log, or null if it does not decode as `ev`.
 * Addresses are lowercase hex; integers up to 48 bits are numbers, wider
 * ones bigints; indexed dynamic values (string, bytes, arrays) are their
 * topic hash, as in the log.
 */
export function decode(ev: EventDef, topics: string[], data: string): Record<string, unknown> | null {
  if (topics.length !== ev.topics) return null;
  const out: Record<string, unknown> = {};
  const key = (p: AbiParameter, i: number) => p.name || `_${i}`;
  const pos = new Map(ev.abi.inputs.map((p, i) => [p, i]));
  for (let i = 0; i < ev.indexed.length; i++) {
    const p = ev.indexed[i];
    const w = topics[i + 1].slice(2).toLowerCase();
    out[key(p, pos.get(p)!)] = STATIC.test(p.type) ? word(p.type, w) : topics[i + 1];
  }
  const hex = data.startsWith("0x") ? data.slice(2) : data;
  if (ev.staticData) {
    if (hex.length < ev.data.length * 64) return null;
    for (let i = 0; i < ev.data.length; i++) {
      const p = ev.data[i];
      out[key(p, pos.get(p)!)] = word(p.type, hex.slice(i * 64, i * 64 + 64).toLowerCase());
    }
    return out;
  }
  try {
    const vals = decodeAbiParameters(ev.data, ("0x" + hex) as `0x${string}`);
    ev.data.forEach((p, i) => {
      const v = vals[i];
      out[key(p, pos.get(p)!)] = p.type === "address" && typeof v === "string" ? v.toLowerCase() : v;
    });
    return out;
  } catch {
    return null;
  }
}
