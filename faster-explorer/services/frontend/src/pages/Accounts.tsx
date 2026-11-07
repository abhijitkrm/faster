import { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import { API } from '../config';

const PAGE = 50;

// Addresses ranked by transaction count (/api/addresses).
export default function Accounts() {
  const [rows, setRows] = useState(null);
  const [offset, setOffset] = useState(0);

  useEffect(() => {
    setRows(null);
    fetch(`${API}/api/addresses?limit=${PAGE}&offset=${offset}`)
      .then(r => r.json()).then(setRows).catch(() => setRows([]));
  }, [offset]);

  return (
    <div>
      <div className="page-head">
        <span className="page-title">Top Accounts</span>
        <span className="page-count">by transaction count</span>
      </div>
      <div className="panel">
        {rows == null ? <div className="empty">Loading…</div> : rows.length === 0 ? <div className="empty">No accounts</div> : (
          <table className="dt">
            <thead><tr>
              <th>Rank</th><th>Address</th><th>Type</th>
              <th style={{ textAlign: 'right' }}>Txns</th>
              <th style={{ textAlign: 'right' }}>Last Seen</th>
            </tr></thead>
            <tbody>
              {rows.map((r, i) => (
                <tr key={r.address}>
                  <td style={{ color: 'var(--text-3)' }}>{offset + i + 1}</td>
                  <td><Link to={`/address/${r.address}`} className="addr-link"><span className="addr-cell">{r.address}</span></Link></td>
                  <td style={{ color: 'var(--text-3)', fontSize: 12 }}>{r.is_contract ? 'Contract' : 'EOA'}</td>
                  <td style={{ textAlign: 'right' }}>{Number(r.transaction_count).toLocaleString()}</td>
                  <td style={{ textAlign: 'right', color: 'var(--text-3)', fontSize: 12 }}>{new Date(r.last_seen).toLocaleString()}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
      <div style={{ display: 'flex', gap: 8, justifyContent: 'center', marginTop: 12 }}>
        <button className="nav-link" disabled={offset === 0} onClick={() => setOffset(Math.max(0, offset - PAGE))}>← Prev</button>
        <button className="nav-link" disabled={!rows || rows.length < PAGE} onClick={() => setOffset(offset + PAGE)}>Next →</button>
      </div>
    </div>
  );
}
