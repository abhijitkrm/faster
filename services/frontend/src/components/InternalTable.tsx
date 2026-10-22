import { Link } from 'react-router-dom';
import { shortAddr } from '../tokens';

const fmtEth = (wei) => { try { return (Number(BigInt(wei)) / 1e18).toFixed(6); } catch { return '0'; } };

// Internal calls (from the node's call tracer; TRACE_CALLS=true).
export function InternalTable({ items, showTx = false }) {
  if (!items) return <div className="empty">Loading…</div>;
  if (!items.length) return <div className="empty">No internal transactions</div>;
  return (
    <table className="dt">
      <thead><tr>
        {showTx && <th>Tx</th>}{showTx && <th>Block</th>}
        <th>Type</th><th>From</th><th>To</th>
        <th style={{ textAlign: 'right' }}>Value (ETH)</th>
        <th style={{ textAlign: 'right' }}>Gas Used</th>
      </tr></thead>
      <tbody>
        {items.map(c => (
          <tr key={(c.cursor || '') + c.transaction_index + ':' + c.trace_index}>
            {showTx && <td style={{ fontSize: 11 }}><Link to={`/tx/${c.transaction_hash}`} className="hash-link">{c.transaction_hash.slice(0, 10)}…</Link></td>}
            {showTx && <td><Link to={`/blocks/${c.block_number}`} className="num-link">#{Number(c.block_number).toLocaleString()}</Link></td>}
            <td style={{ fontSize: 11, color: c.error ? 'var(--red)' : 'var(--text-2)' }}>
              {'↳'.repeat(Math.max(0, c.depth - 1))} {c.type}{c.error ? ` (${c.error})` : ''}
            </td>
            <td><Link to={`/address/${c.from_address}`} className="addr-link"><span className="addr-cell">{shortAddr(c.from_address)}</span></Link></td>
            <td>{c.to_address ? <Link to={`/address/${c.to_address}`} className="addr-link"><span className="addr-cell">{shortAddr(c.to_address)}</span></Link> : '—'}</td>
            <td style={{ textAlign: 'right' }}>{fmtEth(c.value)}</td>
            <td style={{ textAlign: 'right', color: 'var(--text-3)' }}>{Number(c.gas_used).toLocaleString()}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}
