#!/usr/bin/env python3
"""Compares faster-rpc with a reference node, call by call.

usage: conformance.py --node URL --ours URL --from N --to M [--samples K] [--txs T] [--rps R]

For sampled blocks in [from, to] it calls block, transaction, receipt and
log methods on both endpoints and deep-compares the results. Exit status 1
if anything differs. Keys listed in --ignore are dropped from node objects
before comparing (for example blockTimestamp when faster-rpc runs without
BLOCK_TIMESTAMP)."""
import argparse, json, random, sys, time, urllib.request

ap = argparse.ArgumentParser()
ap.add_argument("--node", required=True)
ap.add_argument("--ours", required=True)
ap.add_argument("--from", dest="frm", type=int, required=True)
ap.add_argument("--to", type=int, required=True)
ap.add_argument("--samples", type=int, default=10)
ap.add_argument("--txs", type=int, default=3)
ap.add_argument("--rps", type=float, default=0, help="max node requests/s (public endpoints)")
ap.add_argument("--ignore", default="", help="comma-separated keys to ignore")
ap.add_argument("--seed", type=int, default=7)
a = ap.parse_args()
IGNORE = set(k for k in a.ignore.split(",") if k)
random.seed(a.seed)
last = [0.0]

def call(url, method, params, node=False):
    if node and a.rps:
        wait = last[0] + 1 / a.rps - time.time()
        if wait > 0: time.sleep(wait)
        last[0] = time.time()
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode()
    for attempt in range(8):
        try:
            req = urllib.request.Request(url, data=body, headers={"content-type": "application/json", "user-agent": "faster-rpc-conformance"})
            with urllib.request.urlopen(req, timeout=60) as r:
                v = json.load(r)
            if node and "error" in v and "rate" in json.dumps(v["error"]).lower():
                time.sleep(2 ** attempt * 0.5); continue
            return v.get("result"), v.get("error")
        except urllib.error.HTTPError as e:
            if e.code == 429: time.sleep(2 ** attempt * 0.5); continue
            raise
    raise RuntimeError("node keeps rate limiting")

def norm(v):
    if isinstance(v, dict):
        return {k: norm(x) for k, x in v.items() if k not in IGNORE}
    if isinstance(v, list):
        return [norm(x) for x in v]
    if isinstance(v, str) and v.startswith("0x"):
        return v.lower()
    return v

def diff(x, y, path="$"):
    if type(x) != type(y):
        return f"{path}: {json.dumps(x)[:120]} != {json.dumps(y)[:120]}"
    if isinstance(x, dict):
        for k in sorted(set(x) | set(y)):
            if k not in x: return f"{path}.{k}: missing in node, ours={json.dumps(y[k])[:80]}"
            if k not in y: return f"{path}.{k}: missing in ours, node={json.dumps(x[k])[:80]}"
            d = diff(x[k], y[k], f"{path}.{k}")
            if d: return d
        return None
    if isinstance(x, list):
        if len(x) != len(y): return f"{path}: length {len(x)} != {len(y)}"
        for i, (p, q) in enumerate(zip(x, y)):
            d = diff(p, q, f"{path}[{i}]")
            if d: return d
        return None
    return None if x == y else f"{path}: {json.dumps(x)[:120]} != {json.dumps(y)[:120]}"

stats = {"calls": 0, "ok": 0, "fail": 0}
fails = []

def check(method, params):
    n, ne = call(a.node, method, params, node=True)
    o, oe = call(a.ours, method, params)
    stats["calls"] += 1
    if ne is not None:
        # Both should reject the same bad input; message texts differ.
        ok = oe is not None
        d = None if ok else f"node error {ne}, ours result {json.dumps(o)[:100]}"
    else:
        d = f"ours error {oe}" if oe is not None else diff(norm(n), norm(o))
    if d:
        stats["fail"] += 1
        fails.append(f"{method}{json.dumps(params)[:100]}: {d}")
    else:
        stats["ok"] += 1
    return n

check("eth_chainId", [])
check("net_version", [])
blocks = sorted(random.sample(range(a.frm, a.to + 1), min(a.samples, a.to - a.frm + 1)))
for b in blocks:
    h = hex(b)
    check("eth_getBlockByNumber", [h, False])
    blk = check("eth_getBlockByNumber", [h, True])
    if not blk: continue
    check("eth_getBlockByHash", [blk["hash"], True])
    check("eth_getBlockTransactionCountByNumber", [h])
    check("eth_getBlockReceipts", [h])
    txs = blk["transactions"]
    for t in random.sample(txs, min(a.txs, len(txs))):
        check("eth_getTransactionByHash", [t["hash"]])
        check("eth_getTransactionReceipt", [t["hash"]])
        check("eth_getTransactionByBlockNumberAndIndex", [h, t["transactionIndex"]])
    logs = check("eth_getLogs", [{"fromBlock": h, "toBlock": h}]) or []
    if logs:
        l = random.choice(logs)
        check("eth_getLogs", [{"fromBlock": h, "toBlock": h, "address": l["address"]}])
        check("eth_getLogs", [{"fromBlock": h, "toBlock": h, "topics": [l["topics"][0] if l["topics"] else None]}])
        check("eth_getLogs", [{"blockHash": blk["hash"], "address": [l["address"]], "topics": [None, l["topics"][1:2] or None]}])
# Unknowns and bad input.
check("eth_getTransactionByHash", ["0x" + "ab" * 32])
check("eth_getBlockByHash", ["0x" + "cd" * 32, False])
check("eth_getLogs", [{"fromBlock": hex(a.to), "toBlock": hex(a.frm)}])

print(f"{stats['calls']} calls: {stats['ok']} equal, {stats['fail']} different")
for f in fails[:40]:
    print("  DIFF", f)
sys.exit(1 if fails else 0)
