import { useEffect, useState } from 'react';
import { Link, useParams } from 'react-router-dom';
import { API } from '../config';
import { fmtAmount } from '../tokens';
import CursorTransfers from '../components/CursorTransfers';

export default function Token() {
  const { address } = useParams();
  const [t, setT] = useState(null);
  const [missing, setMissing] = useState(false);
  const [tab, setTab] = useState('transfers');
  const [holders, setHolders] = useState(null);

  useEffect(() => {
    setT(null); setMissing(false); setHolders(null); setTab('transfers');
    fetch(`${API}/api/tokens/${address}`)
      .then(r => (r.ok ? r.json() : Promise.reject()))
      .then(setT)
      .catch(() => setMissing(true));
  }, [address]);

  useEffect(() => {
    if (tab === 'holders' && holders == null) {
      fetch(`${API}/api/tokens/${address}/holders?limit=100`).then(r => r.json()).then(setHolders).catch(() => setHolders([]));
    }
  }, [tab, address, holders]);

  if (missing) return <div className="empty" style={{ paddingTop: 60 }}>No token at this address. <Link to={`/address/${address}`}>View address</Link></div>;
  if (!t) return <div className="empty">Loading token…</div>;

  return (
    <div>
      <div className="page-head">
        <span className="page-title">{t.name || 'Token'}</span>
        {t.symbol && <span className="page-count">{t.symbol}</span>}
        <span className="page-count">{t.type}</span>
      </div>
      <div className="panel" style={{ marginBottom: 18, padding: '12px 20px', display: 'flex', gap: 32, flexWrap: 'wrap', fontSize: 12 }}>
        <div><div style={{ color: 'var(--text-3)', fontSize: 12 }}>CONTRACT</div><Link to={`/address/${t.address}`} className="addr-link" style={{ fontFamily: 'var(--mono)' }}>{t.address}</Link></div>
        <div><div style={{ color: 'var(--text-3)', fontSize: 12 }}>HOLDERS</div>{Number(t.holders).toLocaleString()}</div>
        <div><div style={{ color: 'var(--text-3)', fontSize: 12 }}>TRANSFERS</div>{Number(t.transfers).toLocaleString()}</div>
        <div><div style={{ color: 'var(--text-3)', fontSize: 12 }}>DECIMALS</div>{t.decimals ?? '—'}</div>
        <div><div style={{ color: 'var(--text-3)', fontSize: 12 }}>FIRST BLOCK</div><Link to={`/blocks/${t.first_block}`} className="num-link">#{Number(t.first_block).toLocaleString()}</Link></div>
      </div>
      <div style={{ display: 'flex', gap: 8, marginBottom: 10 }}>
        <button className={'nav-link' + (tab === 'transfers' ? ' active' : '')} onClick={() => setTab('transfers')}>Transfers</button>
        <button className={'nav-link' + (tab === 'holders' ? ' active' : '')} onClick={() => setTab('holders')}>Holders</button>
      </div>
      <div className="panel">
        {tab === 'transfers' ? <CursorTransfers path={`/api/tokens/${address}/transfers`} showToken={false} /> : (
          holders == null ? <div className="empty">Loading…</div> : !holders.length ? <div className="empty">No holders</div> : (
            <table className="dt">
              <thead><tr><th>Rank</th><th>Holder</th><th style={{ textAlign: 'right' }}>{t.type === 'ERC-721' ? 'NFTs' : 'Balance'}</th></tr></thead>
              <tbody>
                {holders.map((h, i) => (
                  <tr key={h.address}>
                    <td style={{ color: 'var(--text-3)' }}>{i + 1}</td>
                    <td><Link to={`/address/${h.address}`} className="addr-link"><span className="addr-cell">{h.address}</span></Link></td>
                    <td style={{ textAlign: 'right' }}>{t.type === 'ERC-721' ? h.value : fmtAmount(h.value, t.decimals)}</td>
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
