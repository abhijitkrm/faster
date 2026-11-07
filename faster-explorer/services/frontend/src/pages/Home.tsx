import { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import { useWS } from '../context/WSContext';
import { fmtEth, blockTimeMs } from '../format';

const short = (h) => (h ? h.slice(0, 6) + '…' + h.slice(-4) : '—');
const fmtInt = (n) => (n == null ? '—' : Number(n).toLocaleString('en-US'));
const ago = (b, now) => {
  if (!b || !b.timestamp) return '—';
  const s = Math.max(0, (now - blockTimeMs(b)) / 1000);
  if (s < 60) return s.toFixed(s < 10 ? 1 : 0) + ' s';
  if (s < 3600) return Math.floor(s / 60) + ' min';
  return Math.floor(s / 3600) + ' h';
};
// Common 4-byte selectors; anything else shows the selector itself.
const SELECTORS = {
  '0xa9059cbb': 'transfer', '0x23b872dd': 'transferFrom', '0x095ea7b3': 'approve', '0x40c10f19': 'mint',
  '0x42842e0e': 'safeTransferFrom', '0xf242432a': 'safeTransferFrom', '0x38ed1739': 'swap', '0x7ff36ab5': 'swap',
  '0x18cbafe5': 'swap', '0x414bf389': 'swap', '0x5ae401dc': 'multicall', '0xac9650d8': 'multicall',
  '0xd0e30db0': 'deposit', '0x2e1a7d4d': 'withdraw', '0x3593564c': 'execute',
};
const method = (m) => (m === '' ? 'send' : m === 'create' ? 'create' : SELECTORS[m] || m || '—');

/** Re-render every `ms` so ages stay live. */
function useNow(ms = 200) {
  const [now, setNow] = useState(Date.now());
  useEffect(() => { const id = setInterval(() => setNow(Date.now()), ms); return () => clearInterval(id); }, [ms]);
  return now;
}

/** Block-to-block intervals (s), oldest first: faster's own receive times
 *  when known (ms precision), else block timestamps. */
function intervals(blocks) {
  const asc = blocks.slice().reverse();
  const out = [];
  for (let i = 1; i < asc.length; i++) {
    const a = asc[i - 1], b = asc[i];
    if (b.number !== a.number + 1) continue;
    const d = a.seen_ms && b.seen_ms && b.seen_ms !== a.seen_ms ? (b.seen_ms - a.seen_ms) / 1000 : Number(b.timestamp) - Number(a.timestamp);
    if (d >= 0) out.push(d);
  }
  return out;
}

function Trace({ blocks }) {
  const xs = intervals(blocks);
  if (xs.length < 3) return null;
  const sorted = xs.slice().sort((a, b) => a - b);
  const p50 = sorted[Math.floor(sorted.length / 2)];
  const max = Math.max(...xs, p50 * 2, 0.001);
  const pts = xs.map((v, i) => `${(i / (xs.length - 1)) * 1000},${76 - (v / max) * 68}`).join(' ');
  const mid = 76 - (p50 / max) * 68;
  return (
    <section className="trace" aria-label="Block interval trace">
      <div className="trace-head">
        <span>Block interval, last {xs.length + 1} blocks</span>
        <span className="mono">min {sorted[0].toFixed(2)} s · p50 {p50.toFixed(2)} s · max {sorted[sorted.length - 1].toFixed(2)} s</span>
      </div>
      <svg viewBox="0 0 1000 80" preserveAspectRatio="none" aria-hidden="true">
        <line x1="0" y1={mid} x2="1000" y2={mid} stroke="var(--border)" strokeDasharray="4 6" vectorEffect="non-scaling-stroke" />
        <polyline points={pts} fill="none" stroke="var(--accent)" strokeWidth="1.6" vectorEffect="non-scaling-stroke" />
      </svg>
    </section>
  );
}

export default function Home() {
  const { stats, blocks, txs, newHashes } = useWS();
  const now = useNow();
  const head = blocks[0];
  const indexMs = head && head.seen_ms && head.indexed_ms >= head.seen_ms ? head.indexed_ms - head.seen_ms : null;
  const gasPct = (b) => (b.gas_limit > 0 ? Math.min(100, (Number(b.gas_used) / Number(b.gas_limit)) * 100) : 0);

  const metrics = [
    { label: 'Throughput', value: stats ? Number(stats.tps).toLocaleString('en-US', { maximumFractionDigits: 0 }) : '—', unit: 'tx/s', note: 'latest block ÷ block time' },
    { label: 'Block time', value: stats ? Number(stats.avg_block_time).toFixed(2) : '—', unit: 's', note: 'average of 100 blocks' },
    { label: 'Index lag', value: stats?.lag_blocks ?? '—', unit: 'blocks', note: stats?.lag_blocks === 0 ? 'head = node tip' : 'behind the node' },
    { label: 'Indexing', value: stats?.index_latency_ms == null ? '—' : stats.index_latency_ms < 1 ? '< 1' : stats.index_latency_ms, unit: 'ms', note: 'received → searchable, p50' },
  ];

  return (
    <div>
      <section className="hero" aria-label="Chain head">
        <div style={{ flex: '1 1 520px', minWidth: 0 }}>
          <div className="eyebrow">Latest block</div>
          {head
            ? <Link to={`/blocks/${head.number}`} className="hero-num" style={{ display: 'block', color: 'var(--text)' }}>{fmtInt(head.number)}</Link>
            : <div className="hero-num" style={{ color: 'var(--text-3)' }}>—</div>}
          {head && (
            <div className="hero-meta">
              <span>arrived <b>{ago(head, now)}</b> ago</span>
              {indexMs != null && <span>indexed <span className="fast">{indexMs < 1 ? '< 1 ms' : `+${indexMs} ms`}</span> after arrival</span>}
              <span>{fmtInt(head.transactions_count)} txs{head.gas_limit > 0 ? ` · ${gasPct(head).toFixed(0)}% gas` : ''}</span>
            </div>
          )}
        </div>
        <div className="metric-grid" style={{ flex: '1 1 560px' }}>
          {metrics.map(m => (
            <div className="metric-card" key={m.label}>
              <div className="metric-label">{m.label}</div>
              <div className="metric-value">{m.value}<span className="unit">{m.unit}</span></div>
              <div className="metric-sub">{m.note}</div>
            </div>
          ))}
        </div>
      </section>

      <Trace blocks={blocks} />

      <section aria-label="Block tape">
        <div className="page-head" style={{ justifyContent: 'space-between' }}>
          <h2 className="page-title" style={{ margin: 0 }}>Block tape</h2>
          <span className="page-count">newest left · bar = gas used</span>
        </div>
        <div className="tape">
          {blocks.slice(0, 12).map((b, i) => (
            <Link key={b.hash} to={`/blocks/${b.number}`} className={'tape-tile' + (i === 0 ? ' newest' : '')}>
              <span className="n">#{fmtInt(b.number)}</span>
              <span className="s">{fmtInt(b.transactions_count)} txs · {ago(b, now)}</span>
              <span className="bar"><span style={{ width: `${gasPct(b)}%` }} /></span>
            </Link>
          ))}
          {!blocks.length && <div className="empty">Waiting for blocks…</div>}
        </div>
      </section>

      <div className="home-grid">
        <section className="panel" aria-label="Latest blocks">
          <div className="panel-head">
            <h2 className="panel-title" style={{ margin: 0 }}>Latest blocks</h2>
            <Link to="/blocks" className="panel-link">All blocks →</Link>
          </div>
          <table className="dt">
            <thead><tr><th>Block</th><th className="hide-sm">Proposer</th><th style={{ textAlign: 'right' }}>Txs</th><th style={{ textAlign: 'right' }}>Age</th></tr></thead>
            <tbody>
              {blocks.slice(0, 10).map(b => (
                <tr key={b.hash} className={newHashes.has(b.hash) ? 'is-new' : ''}>
                  <td><Link to={`/blocks/${b.number}`} className="num-link">{fmtInt(b.number)}</Link></td>
                  <td className="hide-sm"><Link to={`/address/${b.miner}`} className="addr-link" style={{ color: 'var(--text-2)' }}>{short(b.miner)}</Link></td>
                  <td className="mono" style={{ textAlign: 'right' }}>{fmtInt(b.transactions_count)}</td>
                  <td className="mono" style={{ textAlign: 'right', color: 'var(--text-2)' }}>{ago(b, now)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </section>

        <section className="panel" aria-label="Latest transactions">
          <div className="panel-head">
            <h2 className="panel-title" style={{ margin: 0 }}>Latest transactions</h2>
            <Link to="/transactions" className="panel-link">All transactions →</Link>
          </div>
          <table className="dt">
            <thead><tr><th>Hash</th><th className="hide-sm">From → To</th><th className="hide-sm">Method</th><th style={{ textAlign: 'right' }}>Value</th></tr></thead>
            <tbody>
              {txs.slice(0, 10).map(t => (
                <tr key={t.hash}>
                  <td>
                    <span aria-label={t.status === 0 ? 'failed' : 'succeeded'} style={{ display: 'inline-block', width: 6, height: 6, borderRadius: '50%', marginRight: 8, background: t.status === 0 ? 'var(--red)' : 'var(--accent)' }} />
                    <Link to={`/tx/${t.hash}`} className="hash-link">{short(t.hash)}</Link>
                  </td>
                  <td className="mono hide-sm" style={{ color: 'var(--text-2)' }}>
                    <Link to={`/address/${t.from_address}`} style={{ color: 'inherit' }}>{short(t.from_address)}</Link> → {t.to_address ? <Link to={`/address/${t.to_address}`} style={{ color: 'inherit' }}>{short(t.to_address)}</Link> : 'new contract'}
                  </td>
                  <td className="hide-sm"><span className="chip">{method(t.method)}</span></td>
                  <td className="mono" style={{ textAlign: 'right' }}>{fmtEth(t.value)} ETH</td>
                </tr>
              ))}
            </tbody>
          </table>
        </section>
      </div>
    </div>
  );
}
