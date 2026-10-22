// Shared helpers for token pages. Amounts arrive as decimal strings.
import { Link } from 'react-router-dom';

export const shortAddr = (a) => (a ? a.slice(0, 6) + '…' + a.slice(-4) : '—');

/** Formats a raw integer amount with `decimals` (null: plain integer). */
export function fmtAmount(raw, decimals) {
  if (raw == null) return '—';
  try {
    const v = BigInt(raw);
    if (decimals == null || decimals === 0) return v.toLocaleString();
    const d = BigInt(10) ** BigInt(decimals);
    const whole = v / d;
    let frac = (v % d).toString().padStart(decimals, '0').replace(/0+$/, '').slice(0, 6);
    return whole.toLocaleString() + (frac ? '.' + frac : '');
  } catch { return String(raw); }
}

export const tokenLabel = (t) => t.symbol || t.name || shortAddr(t.address || t.token);

export function TransfersTable({ items, showToken = true }) {
  if (!items) return <div className="empty">Loading…</div>;
  if (!items.length) return <div className="empty">No token transfers</div>;
  return (
    <table className="dt">
      <thead><tr>
        <th>Tx</th><th>Block</th><th>From</th><th>To</th>
        {showToken && <th>Token</th>}
        <th style={{ textAlign: 'right' }}>Amount / ID</th>
      </tr></thead>
      <tbody>
        {items.map(t => (
          <tr key={t.cursor}>
            <td style={{ fontSize: 11 }}>{t.transaction_hash ? <Link to={`/tx/${t.transaction_hash}`} className="hash-link">{t.transaction_hash.slice(0, 10)}…</Link> : '—'}</td>
            <td><Link to={`/blocks/${t.block_number}`} className="num-link">#{Number(t.block_number).toLocaleString()}</Link></td>
            <td><Link to={`/address/${t.from_address}`} className="addr-link"><span className="addr-cell">{shortAddr(t.from_address)}</span></Link></td>
            <td><Link to={`/address/${t.to_address}`} className="addr-link"><span className="addr-cell">{shortAddr(t.to_address)}</span></Link></td>
            {showToken && <td><Link to={`/token/${t.token}`} className="addr-link">{t.token_symbol || shortAddr(t.token)}</Link> <span style={{ color: 'var(--text-3)', fontSize: 10 }}>{t.token_type}</span></td>}
            <td style={{ textAlign: 'right' }}>
              {t.token_type === 'ERC-721' ? `#${t.token_id}` : fmtAmount(t.value, t.token_decimals)}
              {t.token_type === 'ERC-1155' && <span style={{ color: 'var(--text-3)', fontSize: 10 }}> (id {t.token_id})</span>}
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}
