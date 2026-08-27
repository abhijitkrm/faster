import { useEffect, useState } from 'react';
import { useParams, Link, useNavigate } from 'react-router-dom';

const API = import.meta.env.VITE_API_URL || 'http://localhost:4000';

const fmtNum  = (n) => (n == null ? '—' : Number(n).toLocaleString());
const fmtVal  = (wei) => { try { return (Number(BigInt(wei)) / 1e18).toFixed(4); } catch { return '0.0000'; } };
const shortHash = (h) => h ? h.slice(0, 10) + '…' + h.slice(-6) : '';
const shortAddr = (a) => a ? a.slice(0, 6)  + '…' + a.slice(-4)  : 'Contract';

const timeAgo = (blockTs) => {
  if (!blockTs) return '—';
  const s = Math.floor(Date.now() / 1000) - parseInt(blockTs);
  if (s < 1)     return 'just now';
  if (s < 60)    return s + 's ago';
  if (s < 3600)  return Math.floor(s / 60) + 'm ago';
  if (s < 86400) return Math.floor(s / 3600) + 'h ago';
  return Math.floor(s / 86400) + 'd ago';
};

const fmtTs = (ts) => {
  if (!ts) return '—';
  return new Date(parseInt(ts) * 1000).toLocaleString();
};

const Row = ({ label, children }) => (
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

export default function BlockDetail() {
  const { number } = useParams();
  const navigate   = useNavigate();
  const [block, setBlock] = useState(null);
  const [notFound, setNotFound] = useState(false);

  useEffect(() => {
    setBlock(null); setNotFound(false);
    fetch(`${API}/api/blocks/${number}`)
      .then(r => { if (r.status === 404) { setNotFound(true); return null; } return r.json(); })
      .then(d => { if (d) setBlock(d); })
      .catch(() => setNotFound(true));
  }, [number]);

  if (notFound) return (
    <div className="empty" style={{ paddingTop: 60 }}>
      Block {number} not found — may not be indexed yet.
      <div style={{ marginTop: 16 }}>
        <Link to="/blocks" className="nav-link" style={{ fontSize: 11 }}>← Back to blocks</Link>
      </div>
    </div>
  );

  if (!block) return <div className="empty">Loading block…</div>;

  const gasUsedPct = block.gas_limit > 0
    ? ((block.gas_used / block.gas_limit) * 100).toFixed(1)
    : '0';

  return (
    <div>
      {/* Header */}
      <div className="page-head">
        <span className="page-title">Block</span>
        <span className="metric-value blue" style={{ fontSize: 20 }}>
          #{Number(block.number).toLocaleString()}
        </span>
        <div style={{ marginLeft: 'auto', display: 'flex', gap: 8 }}>
          <button onClick={() => navigate(`/blocks/${block.number - 1}`)}
            style={navBtnStyle}>← Prev</button>
          <button onClick={() => navigate(`/blocks/${block.number + 1}`)}
            style={navBtnStyle}>Next →</button>
        </div>
      </div>

      {/* Overview panel */}
      <div className="panel" style={{ marginBottom: 18 }}>
        <div className="panel-head">
          <span className="panel-title">Overview</span>
          <span style={{ fontSize: 10, color: 'var(--text-3)' }}>{timeAgo(block.timestamp)}</span>
        </div>

        <Row label="Block Hash">
          <span style={{ fontFamily: 'monospace', fontSize: 11 }}>{block.hash}</span>
        </Row>
        <Row label="Parent Hash">
          {block.parent_hash
            ? <Link to={`/blocks/${Number(block.number) - 1}`} className="num-link"
                style={{ fontFamily: 'monospace', fontSize: 11 }}>{block.parent_hash}</Link>
            : '—'}
        </Row>
        <Row label="Timestamp">
          {fmtTs(block.timestamp)}{' '}
          <span style={{ color: 'var(--text-3)', marginLeft: 8 }}>({timeAgo(block.timestamp)})</span>
        </Row>
        <Row label="Miner">
          <Link to={`/address/${block.miner}`} className="addr-link" style={{ fontFamily: 'monospace', fontSize: 11 }}>
            {block.miner}
          </Link>
        </Row>
        <Row label="Transactions">
          <span style={{ color: block.transactions_count > 0 ? 'var(--green)' : 'var(--text-3)' }}>
            {block.transactions_count} transaction{block.transactions_count !== 1 ? 's' : ''}
          </span>
        </Row>
        <Row label="Gas Used">
          <span>{fmtNum(block.gas_used)}</span>
          <span style={{ color: 'var(--text-3)', marginLeft: 10, fontSize: 11 }}>
            ({gasUsedPct}% of {fmtNum(block.gas_limit)})
          </span>
          {/* Gas usage bar */}
          <div style={{ marginTop: 6, height: 4, width: 200, background: 'var(--surface-2)',
                        borderRadius: 2, overflow: 'hidden' }}>
            <div style={{ height: '100%', borderRadius: 2,
                          width: `${Math.min(100, gasUsedPct)}%`,
                          background: gasUsedPct > 80 ? 'var(--red)' : gasUsedPct > 50 ? 'var(--yellow)' : 'var(--green)' }} />
          </div>
        </Row>
        {block.state_root && (
          <Row label="State Root">
            <span style={{ fontFamily: 'monospace', fontSize: 10, color: 'var(--text-3)' }}>{block.state_root}</span>
          </Row>
        )}
      </div>

      {/* Transactions */}
      <div className="page-head">
        <span className="page-title">Transactions</span>
        <span className="page-count">{(block.transactions || []).length} in this block</span>
      </div>
      <div className="panel">
        {(!block.transactions || block.transactions.length === 0)
          ? <div className="empty">No transactions in this block</div>
          : (
            <table className="dt">
              <thead><tr>
                <th>Hash</th>
                <th>From</th>
                <th>To</th>
                <th style={{ textAlign: 'right' }}>Value (ETH)</th>
                <th style={{ textAlign: 'right' }}>Gas Used</th>
                <th style={{ textAlign: 'right' }}>Status</th>
              </tr></thead>
              <tbody>
                {block.transactions.map(tx => (
                  <tr key={tx.hash}>
                    <td style={{ color: 'var(--text-2)', fontSize: 11 }}>
                      <span className="hash-cell">{shortHash(tx.hash)}</span>
                    </td>
                    <td>
                      <Link to={`/address/${tx.from_address}`} className="addr-link">
                        <span className="addr-cell">{shortAddr(tx.from_address)}</span>
                      </Link>
                    </td>
                    <td>
                      <Link to={`/address/${tx.to_address}`} className="addr-link">
                        <span className="addr-cell">{shortAddr(tx.to_address)}</span>
                      </Link>
                    </td>
                    <td style={{ textAlign: 'right', color: tx.value === '0' ? 'var(--text-3)' : 'var(--text)' }}>
                      {fmtVal(tx.value)}
                    </td>
                    <td style={{ textAlign: 'right', color: 'var(--text-2)' }}>
                      {tx.gas_used ? fmtNum(tx.gas_used) : '—'}
                    </td>
                    <td style={{ textAlign: 'right' }}>
                      {tx.status === 1   && <span className="badge badge-ok">OK</span>}
                      {tx.status === 0   && <span className="badge badge-fail">Fail</span>}
                      {tx.status == null && <span className="badge badge-pend">—</span>}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )
        }
      </div>
    </div>
  );
}

const navBtnStyle = {
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
