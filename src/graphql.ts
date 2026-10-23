// GraphQL API generated from schema.graphql.
//
//   account(id: ID!): Account
//   accounts(first: 100, skip: 0, where: Account_filter, orderBy: Account_orderBy, orderDirection: asc): [Account!]!
//   _meta { chains { chainId nextBlock } }
//
// Filters follow the familiar `field`, `field_not`, `field_gt`, `field_gte`,
// `field_lt`, `field_lte`, `field_in`, `field_not_in`, `field_contains`,
// `field_starts_with` form.

import { createServer, type Server } from "node:http";
import {
  GraphQLBoolean,
  GraphQLEnumType,
  GraphQLFloat,
  GraphQLID,
  GraphQLInputObjectType,
  GraphQLInt,
  GraphQLList,
  GraphQLNonNull,
  GraphQLObjectType,
  GraphQLScalarType,
  GraphQLSchema,
  GraphQLString,
  graphql,
  type GraphQLFieldConfigMap,
  type GraphQLInputFieldConfigMap,
  type GraphQLOutputType,
} from "graphql";
import { q, type Db } from "./db.js";
import { columns, type Entity, type Field } from "./schema.js";
import { fromSql, toSql } from "./store.js";

const BigIntScalar = new GraphQLScalarType({
  name: "BigInt",
  serialize: (v) => String(v),
  parseValue: (v) => String(v),
  parseLiteral: (ast) => ("value" in ast ? String(ast.value) : null),
});
const BigDecimalScalar = new GraphQLScalarType({ name: "BigDecimal", serialize: (v) => String(v), parseValue: (v) => String(v), parseLiteral: (ast) => ("value" in ast ? String(ast.value) : null) });
const JsonScalar = new GraphQLScalarType({ name: "JSON", serialize: (v) => v, parseValue: (v) => v });
const BytesScalar = new GraphQLScalarType({ name: "Bytes", serialize: (v) => String(v), parseValue: (v) => String(v).toLowerCase(), parseLiteral: (ast) => ("value" in ast ? String(ast.value).toLowerCase() : null) });

function scalar(f: Field) {
  switch (f.type) {
    case "ID":
      return GraphQLID;
    case "Int":
      return GraphQLInt;
    case "Float":
      return GraphQLFloat;
    case "Boolean":
      return GraphQLBoolean;
    case "BigInt":
      return BigIntScalar;
    case "BigDecimal":
      return BigDecimalScalar;
    case "Bytes":
      return BytesScalar;
    case "Json":
      return JsonScalar;
    default:
      return GraphQLString;
  }
}

export const lowerFirst = (s: string) => s[0].toLowerCase() + s.slice(1);
export const plural = (s: string) => (s.endsWith("s") ? `${s}es` : `${s}s`);

const OPS: Record<string, string> = { "": "=", _not: "<>", _gt: ">", _gte: ">=", _lt: "<", _lte: "<=" };
const MAX_FIRST = 1000;

export function buildSchema(db: Db, entities: Entity[]): GraphQLSchema {
  const types = new Map<string, GraphQLObjectType>();
  const byName = new Map(entities.map((e) => [e.name, e]));
  const OrderDirection = new GraphQLEnumType({ name: "OrderDirection", values: { asc: { value: "asc" }, desc: { value: "desc" } } });

  const filters = new Map<string, GraphQLInputObjectType>();
  const orderBys = new Map<string, GraphQLEnumType>();
  for (const e of entities) {
    const fields: GraphQLInputFieldConfigMap = {};
    const order: Record<string, { value: string }> = {};
    for (const f of columns(e)) {
      if (f.list || f.type === "Json") continue;
      const t = f.entity ? GraphQLString : scalar(f);
      for (const s of Object.keys(OPS)) fields[f.name + s] = { type: t };
      fields[`${f.name}_in`] = { type: new GraphQLList(new GraphQLNonNull(t)) };
      fields[`${f.name}_not_in`] = { type: new GraphQLList(new GraphQLNonNull(t)) };
      if (f.type === "String" || f.type === "Bytes" || f.type === "ID") {
        fields[`${f.name}_contains`] = { type: GraphQLString };
        fields[`${f.name}_starts_with`] = { type: GraphQLString };
      }
      order[f.name] = { value: f.name };
    }
    filters.set(e.name, new GraphQLInputObjectType({ name: `${e.name}_filter`, fields }));
    orderBys.set(e.name, new GraphQLEnumType({ name: `${e.name}_orderBy`, values: order }));
  }

  const decode = (e: Entity, r: Record<string, unknown>) => {
    const o: Record<string, unknown> = {};
    for (const f of columns(e)) o[f.name] = fromSql(f, r[f.name]);
    return o;
  };

  async function list(e: Entity, args: any, extra?: { col: string; value: unknown }) {
    const where: string[] = [];
    const params: unknown[] = [];
    const cols = new Map(columns(e).map((f) => [f.name, f]));
    const param = (f: Field, v: unknown) => {
      params.push(toSql(f, f.type === "BigInt" && typeof v === "string" ? BigInt(v) : v));
      return f.type === "BigInt" || f.type === "BigDecimal" ? `$${params.length}::numeric` : `$${params.length}`;
    };
    if (extra) {
      params.push(extra.value);
      where.push(`${q(extra.col)} = $${params.length}`);
    }
    for (const [k, v] of Object.entries(args.where ?? {})) {
      if (v === undefined) continue;
      const m = k.match(/^(.*?)(_not_in|_in|_not|_gte|_gt|_lte|_lt|_contains|_starts_with)?$/)!;
      const f = cols.get(m[1]);
      if (!f) continue;
      const suffix = m[2] ?? "";
      const c = q(f.name);
      if (v === null) {
        where.push(suffix === "_not" ? `${c} IS NOT NULL` : `${c} IS NULL`);
      } else if (suffix === "_in" || suffix === "_not_in") {
        const ph = (v as unknown[]).map((x) => param(f, x));
        where.push(ph.length ? `${c} ${suffix === "_in" ? "IN" : "NOT IN"} (${ph.join(", ")})` : suffix === "_in" ? "false" : "true");
      } else if (suffix === "_contains") {
        params.push(`%${String(v).replace(/[%_\\]/g, (x) => "\\" + x)}%`);
        where.push(`${c} LIKE $${params.length}`);
      } else if (suffix === "_starts_with") {
        params.push(`${String(v).replace(/[%_\\]/g, (x) => "\\" + x)}%`);
        where.push(`${c} LIKE $${params.length}`);
      } else {
        where.push(`${c} ${OPS[suffix]} ${param(f, v)}`);
      }
    }
    const first = Math.min(Math.max(args.first ?? 100, 0), MAX_FIRST);
    const skip = Math.max(args.skip ?? 0, 0);
    const order = args.orderBy ? `${q(args.orderBy)} ${args.orderDirection === "desc" ? "DESC" : "ASC"}, id ASC` : "id ASC";
    const sql = `SELECT * FROM ${q(e.name)}${where.length ? ` WHERE ${where.join(" AND ")}` : ""} ORDER BY ${order} LIMIT ${first} OFFSET ${skip}`;
    return (await db.query(sql, params)).rows.map((r) => decode(e, r));
  }

  async function one(e: Entity, id: string) {
    const r = (await db.query(`SELECT * FROM ${q(e.name)} WHERE id = $1`, [id])).rows[0];
    return r ? decode(e, r) : null;
  }

  for (const e of entities) {
    types.set(
      e.name,
      new GraphQLObjectType({
        name: e.name,
        fields: () => {
          const fields: GraphQLFieldConfigMap<any, any> = {};
          for (const f of e.fields) {
            if (f.derivedFrom) {
              const target = byName.get(f.type)!;
              fields[f.name] = {
                type: new GraphQLNonNull(new GraphQLList(new GraphQLNonNull(types.get(f.type)!))),
                args: {
                  first: { type: GraphQLInt },
                  skip: { type: GraphQLInt },
                  where: { type: filters.get(f.type)! },
                  orderBy: { type: orderBys.get(f.type)! },
                  orderDirection: { type: OrderDirection },
                },
                resolve: (parent, args) => list(target, args, { col: f.derivedFrom!, value: parent.id }),
              };
            } else if (f.entity) {
              const target = byName.get(f.entity)!;
              let t: GraphQLOutputType = types.get(f.entity)!;
              if (f.required) t = new GraphQLNonNull(t);
              fields[f.name] = { type: t, resolve: (parent) => (parent[f.name] ? one(target, parent[f.name]) : null) };
            } else {
              let t: GraphQLOutputType = scalar(f);
              if (f.list) t = new GraphQLList(new GraphQLNonNull(t));
              if (f.required) t = new GraphQLNonNull(t);
              fields[f.name] = { type: t };
            }
          }
          return fields;
        },
      }),
    );
  }

  const Chain = new GraphQLObjectType({
    name: "_Chain_",
    fields: { chainId: { type: new GraphQLNonNull(GraphQLInt) }, nextBlock: { type: new GraphQLNonNull(GraphQLInt) } },
  });
  const Meta = new GraphQLObjectType({ name: "_Meta_", fields: { chains: { type: new GraphQLNonNull(new GraphQLList(new GraphQLNonNull(Chain))) } } });

  const query: GraphQLFieldConfigMap<any, any> = {
    _meta: {
      type: new GraphQLNonNull(Meta),
      resolve: async () => ({
        chains: (await db.query(`SELECT chain_id, next_block FROM _fi_checkpoint ORDER BY chain_id`)).rows.map((r) => ({
          chainId: Number(r.chain_id),
          nextBlock: Number(r.next_block),
        })),
      }),
    },
  };
  for (const e of entities) {
    const t = types.get(e.name)!;
    query[lowerFirst(e.name)] = { type: t, args: { id: { type: new GraphQLNonNull(GraphQLID) } }, resolve: (_, a) => one(e, a.id) };
    query[lowerFirst(plural(e.name))] = {
      type: new GraphQLNonNull(new GraphQLList(new GraphQLNonNull(t))),
      args: {
        first: { type: GraphQLInt },
        skip: { type: GraphQLInt },
        where: { type: filters.get(e.name)! },
        orderBy: { type: orderBys.get(e.name)! },
        orderDirection: { type: OrderDirection },
      },
      resolve: (_, a) => list(e, a),
    };
  }
  return new GraphQLSchema({ query: new GraphQLObjectType({ name: "Query", fields: query }) });
}

const PLAYGROUND = `<!doctype html><html><head><meta charset="utf-8"><title>faster-index</title>
<link rel="stylesheet" href="https://unpkg.com/graphiql@3.7.1/graphiql.min.css"></head>
<body style="margin:0"><div id="g" style="height:100vh"></div>
<script src="https://unpkg.com/react@18.3.1/umd/react.production.min.js"></script>
<script src="https://unpkg.com/react-dom@18.3.1/umd/react-dom.production.min.js"></script>
<script src="https://unpkg.com/graphiql@3.7.1/graphiql.min.js"></script>
<script>ReactDOM.createRoot(document.getElementById('g')).render(React.createElement(GraphiQL,{fetcher:GraphiQL.createFetcher({url:location.pathname})}))</script>
</body></html>`;

export interface ServeOptions {
  port: number;
  host?: string;
  /** Extra text for GET /metrics. */
  metrics?: () => string;
  maxBody?: number;
}

export function serveGraphql(db: Db, entities: Entity[], o: ServeOptions): Promise<Server> {
  const schema = buildSchema(db, entities);
  const server = createServer(async (req, res) => {
    const path = (req.url ?? "/").split("?")[0];
    if (path === "/healthz") return void res.writeHead(200).end("ok");
    if (path === "/metrics") return void res.writeHead(200, { "content-type": "text/plain" }).end(o.metrics?.() ?? "");
    if (path !== "/graphql" && path !== "/") return void res.writeHead(404).end();
    if (req.method === "GET") return void res.writeHead(200, { "content-type": "text/html" }).end(PLAYGROUND);
    if (req.method !== "POST") return void res.writeHead(405).end();
    let body = "";
    for await (const c of req) {
      body += c;
      if (body.length > (o.maxBody ?? 1 << 20)) return void res.writeHead(413).end();
    }
    let p: { query?: string; variables?: Record<string, unknown>; operationName?: string };
    try {
      p = JSON.parse(body);
    } catch {
      return void res.writeHead(400, { "content-type": "application/json" }).end(JSON.stringify({ errors: [{ message: "invalid JSON" }] }));
    }
    const r = await graphql({ schema, source: p.query ?? "", variableValues: p.variables, operationName: p.operationName });
    res.writeHead(200, { "content-type": "application/json" }).end(JSON.stringify(r));
  });
  return new Promise((ok) => server.listen(o.port, o.host ?? "0.0.0.0", () => ok(server)));
}
