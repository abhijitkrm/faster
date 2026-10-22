import { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import { API } from '../config';
import { fmtAmount, shortAddr } from '../tokens';
import CursorTransfers from './CursorTransfers';

// Token holdings and token transfers of an address.
export default function AddressTokens({ address }) {
  const [tab, setTab] = useState('transfers');
  const [held, setHeld] = useState(null);

  useEffect(() => { setHeld(null); setTab('transfers'); }, [address]);
  useEffect(() => {
    if (tab === 'held' && held == null) {
      fetch(`${API}/api/addresses/${address}/tokens`).then(r => r.json()).then(setHeld).catch(() => setHeld([]));
    }
  }, [tab, held, address]);

  return (
    <div style={{ marginTop: 18 }}>
      <div style={{ display: 'flex', gap: 8, marginBottom: 10 }}>
        <button className={'nav-link' + (tab === 'transfers' ? ' active' : '')} onClick={() => setTab('transfers')}>Token Transfers</button>
        <button className={'nav-link' + (tab === 'held' ? ' active' : '')} onClick={() => setTab('held')}>Tokens Held</button>
      </div>
      <div className="panel">
        {tab === 'transfers' ? <CursorTransfers path={`/api/addresses/${address}/token-transfers`} /> : (
          held == null ? <div className="empty">Loading…</div> : !held.length ? <div className="empty">No tokens held</div> : (
            <table className="dt">
              <thead><tr><th>Token</th><th>Type</th><th style={{ textAlign: 'right' }}>Balance</th></tr></thead>
              <tbody>
                {held.map(t => (
                  <tr key={t.address + (t.token_id ?? '')}>
                    <td><Link to={`/token/${t.address}`} className="addr-link">{t.name || shortAddr(t.address)}</Link>{t.symbol && <span style={{ color: 'var(--text-3)' }}> ({t.symbol})</span>}</td>
                    <td style={{ color: 'var(--text-3)', fontSize: 11 }}>{t.type}{t.token_id != null && ` id ${t.token_id}`}</td>
                    <td style={{ textAlign: 'right' }}>{t.type === 'ERC-20' ? fmtAmount(t.value, t.decimals) : t.value}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )
        )}
      </div>
    </div>
  );
}
