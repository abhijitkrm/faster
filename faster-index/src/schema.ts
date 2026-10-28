// Entity schema (schema.graphql): `type X @entity { … }`.

import { readFileSync } from "node:fs";
import { Kind, parse, type FieldDefinitionNode, type TypeNode } from "graphql";

export type Scalar = "ID" | "String" | "Int" | "BigInt" | "BigDecimal" | "Float" | "Boolean" | "Bytes" | "Json";
export const SCALARS: Scalar[] = ["ID", "String", "Int", "BigInt", "BigDecimal", "Float", "Boolean", "Bytes", "Json"];

export interface Field {
  name: string;
  /** Scalar name or entity name. */
  type: string;
  required: boolean;
  list: boolean;
  /** Set for references to another entity (stored as its id). */
  entity?: string;
  /** @derivedFrom(field: "x"): virtual list of entities pointing here. */
  derivedFrom?: string;
  /** @index: create a database index on this column. */
  index: boolean;
}

export interface Entity {
  name: string;
  fields: Field[];
}

function unwrap(t: TypeNode): { name: string; required: boolean; list: boolean } {
  let required = false;
  let list = false;
  if (t.kind === Kind.NON_NULL_TYPE) {
    required = true;
    t = t.type;
  }
  if (t.kind === Kind.LIST_TYPE) {
    list = true;
    t = t.type;
    if (t.kind === Kind.NON_NULL_TYPE) t = t.type;
  }
  if (t.kind !== Kind.NAMED_TYPE) throw new Error("nested lists are not supported");
  return { name: t.name.value, required, list };
}

export function parseSchema(src: string): Entity[] {
  const doc = parse(src);
  const entities: Entity[] = [];
  for (const d of doc.definitions) {
    if (d.kind !== Kind.OBJECT_TYPE_DEFINITION) continue;
    if (!d.directives?.some((x) => x.name.value === "entity")) continue;
    const fields = (d.fields ?? []).map((f: FieldDefinitionNode) => {
      const t = unwrap(f.type);
      const derived = f.directives?.find((x) => x.name.value === "derivedFrom");
      const arg = derived?.arguments?.find((a) => a.name.value === "field");
      return {
        name: f.name.value,
        type: t.name,
        required: t.required,
        list: t.list,
        derivedFrom: arg && arg.value.kind === Kind.STRING ? arg.value.value : undefined,
        index: !!f.directives?.some((x) => x.name.value === "index"),
      } as Field;
    });
    entities.push({ name: d.name.value, fields });
  }
  const names = new Set(entities.map((e) => e.name));
  for (const e of entities) {
    const id = e.fields.find((f) => f.name === "id");
    if (!id || id.type !== "ID" || !id.required || id.list) throw new Error(`${e.name}: needs "id: ID!"`);
    for (const f of e.fields) {
      if (names.has(f.type)) {
        f.entity = f.type;
        if (f.list && !f.derivedFrom) throw new Error(`${e.name}.${f.name}: entity lists need @derivedFrom(field: …)`);
        if (f.derivedFrom) {
          const target = entities.find((x) => x.name === f.type)!;
          const back = target.fields.find((x) => x.name === f.derivedFrom);
          if (!back || back.type !== e.name) throw new Error(`${e.name}.${f.name}: ${f.type}.${f.derivedFrom} must reference ${e.name}`);
        }
      } else if (!SCALARS.includes(f.type as Scalar)) {
        throw new Error(`${e.name}.${f.name}: unknown type ${f.type}`);
      } else if (f.derivedFrom) {
        throw new Error(`${e.name}.${f.name}: @derivedFrom needs an entity type`);
      }
    }
  }
  if (entities.length === 0) throw new Error("schema.graphql: no `@entity` types");
  return entities;
}

export function loadSchema(path: string): Entity[] {
  return parseSchema(readFileSync(path, "utf8"));
}

/** Stored columns (derived fields are virtual). */
export function columns(e: Entity): Field[] {
  return e.fields.filter((f) => !f.derivedFrom);
}

export function sqlType(f: Field): string {
  if (f.list) return "jsonb";
  if (f.entity) return "text";
  switch (f.type as Scalar) {
    case "ID":
    case "String":
    case "Bytes":
      return "text";
    case "Int":
      return "integer";
    case "BigInt":
    case "BigDecimal":
      return "numeric";
    case "Float":
      return "double precision";
    case "Boolean":
      return "boolean";
    case "Json":
      return "jsonb";
  }
  return "text";
}

/** TypeScript type of a field as handlers see it. */
export function tsType(f: Field): string {
  let t: string;
  if (f.entity) t = "string";
  else
    switch (f.type as Scalar) {
      case "Int":
      case "Float":
        t = "number";
        break;
      case "BigInt":
        t = "bigint";
        break;
      case "Boolean":
        t = "boolean";
        break;
      case "Json":
        t = "unknown";
        break;
      default:
        t = "string";
    }
  if (f.list) t = `${t}[]`;
  return t;
}
