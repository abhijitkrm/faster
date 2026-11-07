/** Wei (decimal string) as ETH: up to `max` decimals, no trailing zeros. */
export function fmtEth(wei, max = 6) {
  try {
    const v = BigInt(wei ?? 0);
    if (v === 0n) return '0';
    const n = Number(v) / 1e18;
    if (n < 10 ** -max) return '< ' + (10 ** -max).toFixed(max);
    return n.toLocaleString('en-US', { maximumFractionDigits: max });
  } catch { return '—'; }
}

/** When block `b` was produced, in unix ms. Timestamps are whole seconds,
 *  so for blocks faster received in real time (within 5 s of the
 *  timestamp) its own millisecond receive time is the better clock. */
export function blockTimeMs(b) {
  const ts = Number(b.timestamp) * 1000;
  return b.seen_ms && Math.abs(b.seen_ms - ts) < 5000 ? b.seen_ms : ts;
}
