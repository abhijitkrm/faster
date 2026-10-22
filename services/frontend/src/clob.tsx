// Shared helpers for the CLOB archive pages (api-go /api/clob/*).
// Prices, amounts, quantities and IDs arrive as decimal strings (uint64 can
// exceed 2^53), so they are formatted with BigInt, never Number.
import { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';

export const API = import.meta.env.VITE_API_URL || 'http://localhost:4000';

export const shortHash = (h) => (h ? h.slice(0, 10) + '…' + h.slice(-6) : '');
export const shortAddr = (a) => (a ? a.slice(0, 6) + '…' + a.slice(-4) : '—');

export const fmtBig = (s) => {
  if (s == null || s === '') return '—';
  try { return BigInt(s).toLocaleString(); } catch { return String(s); }
};

export const fmtHeight = (h) => (h == null ? '—' : Number(h).toLocaleString());

export const fmtTime = (iso) => {
  if (!iso) return '—';
  const d = new Date(iso);
  return isNaN(d.getTime()) ? iso : d.toLocaleString();
};

export const timeAgo = (iso) => {
  if (!iso) return '—';
  const s = Math.floor((Date.now() - new Date(iso).getTime()) / 1000);
  if (s < 1) return 'just now';
  if (s < 60) return s + 's ago';
  if (s < 3600) return Math.floor(s / 60) + 'm ago';
  if (s < 86400) return Math.floor(s / 3600) + 'h ago';
  return Math.floor(s / 86400) + 'd ago';
};

// fetchJSON returns { status, body } and never throws on HTTP errors.
export async function fetchJSON(path) {
  const res = await fetch(`${API}${path}`);
  let body = null;
  try { body = await res.json(); } catch { /* empty */ }
  return { status: res.status, body };
}

const STATUS_CLASS = {
  filled: 'badge-ok',
  amended: 'badge-ok',
  cancelled: 'badge-pend',
  open: 'badge-pend',
  partial: 'badge-pend',
  rejected: 'badge-fail',
};

export function StatusBadge({ status }) {
  return <span className={'badge ' + (STATUS_CLASS[status] || 'badge-pend')}>{status}</span>;
}

export function SideBadge({ isBuy }) {
  return (
    <span style={{ color: isBuy ? 'var(--green)' : 'var(--red)', fontWeight: 700, fontSize: 10, letterSpacing: '.08em' }}>
      {isBuy ? 'BUY' : 'SELL'}
    </span>
  );
}

export function TraderLink({ addr, full = false }) {
  if (!addr) return <span>—</span>;
  return (
    <Link to={`/clob/trader/${addr}`} className="addr-link" title={addr}
      style={full ? { fontFamily: 'monospace', fontSize: 11 } : undefined}>
      {full ? addr : <span className="addr-cell">{shortAddr(addr)}</span>}
    </Link>
  );
}

export function HashLink({ hash, full = false }) {
  if (!hash) return <span>—</span>;
  return (
    <Link to={`/clob/order/${hash}`} className="hash-link" title={hash}
      style={full ? { fontFamily: 'monospace', fontSize: 11 } : undefined}>
      {full ? hash : <span className="hash-cell">{shortHash(hash)}</span>}
    </Link>
  );
}

export function BlockLink({ height }) {
  return <Link to={`/blocks/${height}`} className="num-link">#{fmtHeight(height)}</Link>;
}

export function PairLink({ id }) {
  return <Link to={`/clob/pair/${id}`} className="num-link">#{id}</Link>;
}

export const Row = ({ label, children }) => (
  <div style={{
    display: 'grid', gridTemplateColumns: '200px 1fr',
    padding: '10px 20px', borderBottom: '1px solid var(--border)',
    fontSize: 12, alignItems: 'start',
  }}>
    <span style={{ color: 'var(--text-3)', fontWeight: 600, letterSpacing: '.06em',
                   fontSize: 10, textTransform: 'uppercase', paddingTop: 2 }}>{label}</span>
    <span style={{ color: 'var(--text-2)', wordBreak: 'break-all' }}>{children}</span>
  </div>
);

export const btnStyle = {
  background: 'none',
  border: '1px solid var(--border-hi)',
  borderRadius: 6,
  color: 'var(--text-2)',
  cursor: 'pointer',
  fontSize: 10,
  fontWeight: 700,
  letterSpacing: '.1em',
  padding: '4px 12px',
  textTransform: 'uppercase',
};

// useCursorList loads a newest-first list endpoint page by page
// ({ items, next_before }); "more" appends the next page.
export function useCursorList(path, limit = 50) {
  const [items, setItems] = useState(null);
  const [next, setNext] = useState(null);
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);

  const load = async (before, append) => {
    setLoading(true);
    setError('');
    try {
      const sep = path.includes('?') ? '&' : '?';
      const q = `${path}${sep}limit=${limit}` + (before ? `&before=${encodeURIComponent(before)}` : '');
      const { status, body } = await fetchJSON(q);
      if (status !== 200) {
        setError(body?.error || `HTTP ${status}`);
        if (!append) setItems([]);
        return;
      }
      setItems((prev) => (append && prev ? prev.concat(body.items) : body.items));
      setNext(body.next_before);
    } catch {
      setError('Network error');
      if (!append) setItems([]);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { setItems(null); setNext(null); load(null, false); /* eslint-disable-next-line */ }, [path]);

  return { items, error, loading, hasMore: !!next, more: () => next && load(next, true) };
}

export function MoreButton({ list }) {
  if (!list.hasMore) return null;
  return (
    <div style={{ padding: 14, textAlign: 'center' }}>
      <button style={btnStyle} disabled={list.loading} onClick={list.more}>
        {list.loading ? 'Loading…' : 'Load more'}
      </button>
    </div>
  );
}

export function ErrorNote({ msg }) {
  if (!msg) return null;
  return <div className="empty" style={{ color: 'var(--red)' }}>{msg}</div>;
}

// ── Tables ──────────────────────────────────────────────────────────────────

export function OrdersTable({ orders, showTrader = true, showBlock = true }) {
  if (!orders || orders.length === 0) return <div className="empty">No orders</div>;
  return (
    <div style={{ overflowX: 'auto' }}>
      <table className="dt">
        <thead><tr>
          <th>Hash</th>
          {showBlock && <th>Block</th>}
          {showTrader && <th>Trader</th>}
          <th>Pair</th>
          <th>Side</th>
          <th style={{ textAlign: 'right' }}>Price</th>
          <th style={{ textAlign: 'right' }}>Amount</th>
          <th style={{ textAlign: 'right' }}>Filled</th>
          <th style={{ textAlign: 'right' }}>Status</th>
        </tr></thead>
        <tbody>
          {orders.map((o) => (
            <tr key={`${o.height}-${o.idx}`}>
              <td style={{ fontSize: 11 }}><HashLink hash={o.hash} /></td>
              {showBlock && <td><BlockLink height={o.height} /></td>}
              {showTrader && <td><TraderLink addr={o.trader} /></td>}
              <td><PairLink id={o.pair_id} /></td>
              <td><SideBadge isBuy={o.is_buy} />{o.is_market && <span style={{ color: 'var(--text-3)', fontSize: 10, marginLeft: 6 }}>MKT</span>}</td>
              <td style={{ textAlign: 'right' }}>{fmtBig(o.price)}</td>
              <td style={{ textAlign: 'right' }}>{fmtBig(o.amount)}</td>
              <td style={{ textAlign: 'right', color: 'var(--text-2)' }}>{fmtBig(o.filled)}</td>
              <td style={{ textAlign: 'right' }} title={o.reason || undefined}><StatusBadge status={o.status} /></td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export function CancelsTable({ cancels, showTrader = true, showBlock = true }) {
  if (!cancels || cancels.length === 0) return <div className="empty">No cancels or amends</div>;
  return (
    <div style={{ overflowX: 'auto' }}>
      <table className="dt">
        <thead><tr>
          <th>Hash</th>
          <th>Kind</th>
          {showBlock && <th>Block</th>}
          {showTrader && <th>Trader</th>}
          <th>Pair</th>
          <th>Target</th>
          <th style={{ textAlign: 'right' }}>New price</th>
          <th style={{ textAlign: 'right' }}>New amount</th>
          <th style={{ textAlign: 'right' }}>Status</th>
        </tr></thead>
        <tbody>
          {cancels.map((c) => (
            <tr key={`${c.height}-${c.idx}`}>
              <td style={{ fontSize: 11 }}><HashLink hash={c.hash} /></td>
              <td style={{ textTransform: 'uppercase', fontSize: 10, fontWeight: 700, color: 'var(--text-2)' }}>{c.kind}</td>
              {showBlock && <td><BlockLink height={c.height} /></td>}
              {showTrader && <td><TraderLink addr={c.trader} /></td>}
              <td><PairLink id={c.pair_id} /></td>
              <td style={{ color: 'var(--text-2)' }}>
                {c.target_order_id !== '0' ? `order ${fmtBig(c.target_order_id)}` : `client ${fmtBig(c.target_client)}`}
              </td>
              <td style={{ textAlign: 'right' }}>{c.kind === 'amend' ? fmtBig(c.price) : '—'}</td>
              <td style={{ textAlign: 'right' }}>{c.kind === 'amend' ? fmtBig(c.amount) : '—'}</td>
              <td style={{ textAlign: 'right' }} title={c.reason || undefined}><StatusBadge status={c.status} /></td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

// Fills as matches (maker/taker columns): block page and pair tape.
export function FillsTable({ fills, showBlock = true, showPair = true }) {
  if (!fills || fills.length === 0) return <div className="empty">No fills</div>;
  return (
    <div style={{ overflowX: 'auto' }}>
      <table className="dt">
        <thead><tr>
          {showBlock && <th>Block</th>}
          <th>Seq</th>
          {showPair && <th>Pair</th>}
          <th>Taker side</th>
          <th>Maker</th>
          <th>Taker</th>
          <th style={{ textAlign: 'right' }}>Price</th>
          <th style={{ textAlign: 'right' }}>Qty</th>
          {showBlock && <th style={{ textAlign: 'right' }}>Age</th>}
        </tr></thead>
        <tbody>
          {fills.map((f) => (
            <tr key={`${f.height}-${f.seq}`}>
              {showBlock && <td><BlockLink height={f.height} /></td>}
              <td style={{ color: 'var(--text-3)' }}>{f.seq}</td>
              {showPair && <td><PairLink id={f.pair_id} /></td>}
              <td><SideBadge isBuy={f.taker_is_buy} /></td>
              <td><TraderLink addr={f.maker} /></td>
              <td><TraderLink addr={f.taker} /></td>
              <td style={{ textAlign: 'right' }}>{fmtBig(f.price)}</td>
              <td style={{ textAlign: 'right' }}>{fmtBig(f.qty)}</td>
              {showBlock && <td style={{ textAlign: 'right', color: 'var(--text-3)' }}>{timeAgo(f.block_time)}</td>}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

// Fills from one participant's side (trader page, order page).
export function TraderFillsTable({ fills, showPair = true }) {
  if (!fills || fills.length === 0) return <div className="empty">No fills</div>;
  return (
    <div style={{ overflowX: 'auto' }}>
      <table className="dt">
        <thead><tr>
          <th>Block</th>
          <th>Seq</th>
          {showPair && <th>Pair</th>}
          <th>Role</th>
          <th>Side</th>
          <th>Counterparty</th>
          <th style={{ textAlign: 'right' }}>Price</th>
          <th style={{ textAlign: 'right' }}>Qty</th>
          <th style={{ textAlign: 'right' }}>Age</th>
        </tr></thead>
        <tbody>
          {fills.map((f) => (
            <tr key={`${f.height}-${f.seq}-${f.role}`}>
              <td><BlockLink height={f.height} /></td>
              <td style={{ color: 'var(--text-3)' }}>{f.seq}</td>
              {showPair && <td><PairLink id={f.pair_id} /></td>}
              <td style={{ textTransform: 'uppercase', fontSize: 10, fontWeight: 700, color: f.role === 'maker' ? 'var(--blue)' : 'var(--purple)' }}>{f.role}</td>
              <td><SideBadge isBuy={f.is_buy} /></td>
              <td><TraderLink addr={f.counterparty} /></td>
              <td style={{ textAlign: 'right' }}>{fmtBig(f.price)}</td>
              <td style={{ textAlign: 'right' }}>{fmtBig(f.qty)}</td>
              <td style={{ textAlign: 'right', color: 'var(--text-3)' }}>{timeAgo(f.block_time)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
