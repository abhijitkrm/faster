// Project configuration (faster-index.yaml).

import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { parse } from "yaml";

export interface ContractConfig {
  name: string;
  /** Human-readable event signatures, e.g. "Transfer(address indexed from, address indexed to, uint256 value)". */
  events: string[];
  /** Or a JSON ABI file (relative to the config file). */
  abi?: string;
}

export interface NetworkContract {
  name: string;
  address: string[];
  start_block?: number;
}

export interface NetworkConfig {
  id: number;
  sync_url: string;
  sync_api_key?: string;
  start_block: number;
  end_block?: number;
  contracts: NetworkContract[];
}

export interface Config {
  name: string;
  schema: string;
  handlers: string;
  contracts: ContractConfig[];
  networks: NetworkConfig[];
  /** Extra transaction fields on events (needs a join in faster-sync). */
  transaction_fields: string[];
  /** Rows per faster-sync page. */
  page_rows: number;
  root: string;
}

/** Numbers may come from ${ENV} substitution as strings. */
function num(v: unknown, d: number): number {
  if (v === undefined || v === null || v === "") return d;
  const n = Number(v);
  if (!Number.isSafeInteger(n) || n < 0) fail(`bad block number ${v}`);
  return n;
}

function fail(msg: string): never {
  throw new Error(`faster-index.yaml: ${msg}`);
}

/** Replaces ${VAR} and ${VAR:-default} with environment values. */
function expandEnv(s: string): string {
  return s.replace(/\$\{([A-Z0-9_]+)(?::-([^}]*))?\}/g, (_, k, d) => process.env[k] ?? d ?? "");
}

export function loadConfig(path: string): Config {
  const raw = parse(expandEnv(readFileSync(path, "utf8"))) ?? {};
  const root = dirname(resolve(path));
  if (typeof raw.name !== "string") fail("name is required");
  const contracts: ContractConfig[] = (raw.contracts ?? []).map((c: any, i: number) => {
    if (typeof c?.name !== "string") fail(`contracts[${i}].name is required`);
    if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(c.name)) fail(`contracts[${i}].name must be an identifier`);
    const events: string[] = c.events ?? [];
    if (!Array.isArray(events)) fail(`contracts[${i}].events must be a list`);
    if (events.length === 0 && !c.abi) fail(`contracts[${i}] needs events or abi`);
    return { name: c.name, events, abi: c.abi ? resolve(root, c.abi) : undefined };
  });
  const names = new Set(contracts.map((c) => c.name));
  const networks: NetworkConfig[] = (raw.networks ?? []).map((n: any, i: number) => {
    if (typeof n?.id !== "number") fail(`networks[${i}].id (chain id) is required`);
    if (typeof n?.sync_url !== "string" || !n.sync_url) fail(`networks[${i}].sync_url is required`);
    const cs: NetworkContract[] = (n.contracts ?? []).map((c: any, j: number) => {
      if (!names.has(c?.name)) fail(`networks[${i}].contracts[${j}]: unknown contract ${c?.name}`);
      const address = typeof c.address === "string" ? [c.address] : c.address;
      if (!Array.isArray(address) || address.length === 0) fail(`networks[${i}].contracts[${j}].address is required`);
      for (const a of address) if (!/^0x[0-9a-fA-F]{40}$/.test(a)) fail(`bad address ${a}`);
      return { name: c.name, address: address.map((a: string) => a.toLowerCase()), start_block: c.start_block === undefined ? undefined : num(c.start_block, 0) };
    });
    return {
      id: n.id,
      sync_url: n.sync_url,
      sync_api_key: n.sync_api_key || undefined,
      start_block: num(n.start_block, 0),
      end_block: n.end_block === undefined || n.end_block === "" ? undefined : num(n.end_block, 0),
      contracts: cs,
    };
  });
  if (networks.length === 0) fail("at least one network is required");
  return {
    name: raw.name,
    schema: resolve(root, raw.schema ?? "schema.graphql"),
    handlers: resolve(root, raw.handlers ?? "src/handlers.ts"),
    contracts,
    networks,
    transaction_fields: raw.transaction_fields ?? [],
    page_rows: raw.page_rows ?? 50_000,
    root,
  };
}
