// The fast decoder agrees with viem (addresses compared lowercase).
import { test } from "node:test";
import assert from "node:assert/strict";
import { decodeEventLog, encodeAbiParameters, encodeEventTopics, parseAbiItem } from "viem";
import { decode } from "../dist/abi.js";

function def(sig) {
  const abi = parseAbiItem(`event ${sig}`);
  const indexed = abi.inputs.filter((i) => i.indexed);
  const data = abi.inputs.filter((i) => !i.indexed);
  const STATIC = /^(address|bool|u?int\d*|bytes([1-9]|[12]\d|3[0-2]))$/;
  return { contract: "C", name: abi.name, abi, topic0: "", topics: 1 + indexed.length, indexed, data, staticData: data.every((p) => STATIC.test(p.type)) };
}

const lower = (o) => JSON.parse(JSON.stringify(o, (_, v) => (typeof v === "bigint" ? `n${v}` : typeof v === "string" ? v.toLowerCase() : v)));

test("static and dynamic events match viem", () => {
  const cases = [
    ["Transfer(address indexed from, address indexed to, uint256 value)", { from: "0x00000000000000000000000000000000000000aa", to: "0x1111111111111111111111111111111111111111", value: 12345678901234567890n }],
    ["Swap(address indexed sender, int256 amount0, int256 amount1, uint160 sqrtPriceX96, uint128 liquidity, int24 tick)", { sender: "0x2222222222222222222222222222222222222222", amount0: -5n, amount1: 10n ** 30n, sqrtPriceX96: 2n ** 150n, liquidity: 77n, tick: -887272 }],
    ["Flag(bool indexed on, bytes4 sel, uint8 small)", { on: true, sel: "0xa9059cbb", small: 255 }],
    ["Named(string name, bytes blob, uint256[] ids)", { name: "faster", blob: "0x0102", ids: [1n, 2n, 3n] }],
  ];
  for (const [sig, args] of cases) {
    const d = def(sig);
    const topics = encodeEventTopics({ abi: [d.abi], eventName: d.name, args });
    const data = encodeAbiParameters(d.data, d.data.map((p) => args[p.name]));
    const ours = decode(d, topics, data);
    const viem = decodeEventLog({ abi: [d.abi], topics, data }).args;
    assert.deepEqual(lower(ours), lower(viem), sig);
  }
});

test("wrong topic count or short data does not decode", () => {
  const d = def("Transfer(address indexed from, address indexed to, uint256 value)");
  assert.equal(decode(d, ["0x" + "00".repeat(32)], "0x"), null);
  assert.equal(decode(d, ["0x" + "00".repeat(32), "0x" + "00".repeat(32), "0x" + "00".repeat(32)], "0x"), null);
});
