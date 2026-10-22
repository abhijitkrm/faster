import { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import { API } from '../config';
import { shortAddr } from '../tokens';

const PAGE = 50;

// Tokens ranked by number of transfers (/api/tokens).
export default function Tokens() {
  const [rows, setRows] = useState(null);
  const [offset, setOffset] = useState(0);

  useEffect(() => {
    setRows(null);
    fetch(`${API}/api/tokens?limit=${PAGE}&offset=${offset}`).then(r => r.json()).then(setRows).catch(() => setRows([]));
  }, [offset]);

  return (
    <div>
      <div className="page-head">
        <span className="page-title">Tokens</span>
        <span className="page-count">by transfers</span>
      </div>
      <div className="panel">
        {rows == null ? <div className="empty">Loading…</div> : !rows.length ? <div className="empty">No tokens yet</div> : (
          <table className="dt">
            <thead><tr>
              <th>#</th><th>Token</th><th>Type</th>
              <th style={{ textAlign: 'right' }}>Holders</th>
              <th style={{ textAlign: 'right' }}>Transfers</th>
            </tr></thead>
            <tbody>
              {rows.map((t, i) => (
                <tr key={t.address}>
                  <td style={{ color: 'var(--text-3)' }}>{offset + i + 1}</td>
                  <td>
                    <Link to={`/token/${t.address}`} className="addr-link">{t.name || shortAddr(t.address)}</Link>
                    {t.symbol && <span style={{ color: 'var(--text-3)', marginLeft: 6 }}>({t.symbol})</span>}
                  </td>
                  <td style={{ color: 'var(--text-3)', fontSize: 11 }}>{t.type}</td>
                  <td style={{ textAlign: 'right' }}>{Number(t.holders).toLocaleString()}</td>
                  <td style={{ textAlign: 'right' }}>{Number(t.transfers).toLocaleString()}</td>
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
