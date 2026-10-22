/** Wei (decimal string) as ETH: up to `max` decimals, no trailing zeros. */
export function fmtEth(wei, max = 6) {
  try {
    const v = BigInt(wei ?? 0);
    if (v === 0n) return '0';
    const n = Number(v) / 1e18;
    return n.toLocaleString('en-US', { maximumFractionDigits: n < 1e-6 ? 18 : max });
  } catch { return '—'; }
}
