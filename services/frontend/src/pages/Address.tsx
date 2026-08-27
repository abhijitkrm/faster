import { useEffect, useState, useCallback } from 'react';
import { useParams, Link } from 'react-router-dom';

const API = import.meta.env.VITE_API_URL || 'http://localhost:4000';

const fmtNum   = (n) => (n == null ? '—' : Number(n).toLocaleString());
const fmtEth   = (wei) => { try { return (Number(BigInt(wei)) / 1e18).toFixed(6); } catch { return '0.000000'; } };
const fmtVal   = (wei) => { try { return (Number(BigInt(wei)) / 1e18).toFixed(4);  } catch { return '0.0000';   } };
const shortHash = (h) => h ? h.slice(0, 10) + '…' + h.slice(-6) : '';
const shortAddr = (a) => a ? a.slice(0, 6)  + '…' + a.slice(-4)  : '—';

const timeAgo = (ts) => {
  if (!ts) return '—';
  const s = Math.floor(Date.now() / 1000) - parseInt(ts);
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

function TypeBadge({ isContract }) {
  return isContract ? (
    <span style={{
      fontSize: 9, fontWeight: 700, letterSpacing: '.14em', textTransform: 'uppercase',
      padding: '3px 9px', borderRadius: 4,
      background: 'rgba(163,113,247,.14)', color: 'var(--purple)',
      border: '1px solid rgba(163,113,247,.3)',
    }}>Contract</span>
  ) : (
    <span style={{
      fontSize: 9, fontWeight: 700, letterSpacing: '.14em', textTransform: 'uppercase',
      padding: '3px 9px', borderRadius: 4,
      background: 'rgba(88,166,255,.1)', color: 'var(--blue)',
      border: '1px solid rgba(88,166,255,.25)',
    }}>EOA</span>
  );
}

function Row({ label, children }) {
  return (
    <div style={{
      display: 'grid', gridTemplateColumns: '190px 1fr',
      padding: '10px 20px', borderBottom: '1px solid var(--border)',
      fontSize: 12, alignItems: 'start',
    }}>
      <span style={{ color: 'var(--text-3)', fontWeight: 600, fontSize: 10,
                     letterSpacing: '.06em', textTransform: 'uppercase', paddingTop: 2 }}>{label}</span>
      <span style={{ color: 'var(--text-2)', wordBreak: 'break-all' }}>{children}</span>
    </div>
  );
}

function BytecodeBlock({ bytecode }) {
  const [copied, setCopied] = useState(false);
  const copy = () => {
    navigator.clipboard.writeText(bytecode).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    });
  };
  // Format as rows of 32 bytes (64 hex chars) for readability
  const hex = bytecode.startsWith('0x') ? bytecode.slice(2) : bytecode;
  const rows = [];
  for (let i = 0; i < hex.length; i += 64) rows.push(hex.slice(i, i + 64));

  return (
    <div style={{ width: '100%' }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 6 }}>
        <button onClick={copy} style={{
          background: 'none', border: '1px solid var(--border-hi)', borderRadius: 5,
          color: copied ? 'var(--green)' : 'var(--text-3)', cursor: 'pointer',
          fontSize: 9, fontWeight: 700, letterSpacing: '.1em', padding: '2px 8px',
          textTransform: 'uppercase', transition: 'all .15s',
        }}>{copied ? '✓ Copied' : 'Copy'}</button>
        <span style={{ fontSize: 10, color: 'var(--text-3)' }}>{hex.length / 2} bytes · {rows.length} rows</span>
      </div>
      <pre style={{
        fontFamily: 'monospace', fontSize: 10.5, lineHeight: 1.7,
        color: 'var(--text-3)', background: 'var(--surface-2)',
        border: '1px solid var(--border)', borderRadius: 6,
        padding: '12px 14px', margin: 0,
        maxHeight: 320, overflowY: 'auto',
        wordBreak: 'break-all', whiteSpace: 'pre-wrap',
      }}>
        {rows.map((row, i) => (
          <span key={i}>
            <span style={{ color: 'var(--text-3)', opacity: 0.4, marginRight: 14, userSelect: 'none' }}>
              {String(i * 32).padStart(6, '0')}
            </span>
            {/* Highlight every byte pair alternately for readability */}
            {Array.from({ length: row.length / 2 }, (_, b) => (
              <span key={b} style={{ color: b % 2 === 0 ? 'var(--text-2)' : 'var(--text-3)' }}>
                {row.slice(b * 2, b * 2 + 2)}
              </span>
            ))}
            {'\n'}
          </span>
        ))}
      </pre>
    </div>
  );
}

export default function Address() {
  const { address } = useParams();
  const [data,   setData]   = useState(null);
  const [page,   setPage]   = useState(1);
  const [copied, setCopied] = useState(false);

  const load = useCallback((p) => {
    fetch(`${API}/api/addresses/${address}?page=${p}&limit=25`)
      .then(r => r.json()).then(d => { setData(d); setPage(p); }).catch(() => {});
  }, [address]);

  useEffect(() => { load(1); }, [load]);

  const copy = () => {
    navigator.clipboard.writeText(address).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    });
  };

  if (!data) return <div className="empty">Loading address…</div>;

  const isContract  = !!data.is_contract;
  const totalPages  = Math.ceil((data.total_transactions || 0) / 25);
  const deployInfo  = data.deploy_info;

  return (
    <div>
      {/* ── Header bar ── */}
      <div style={{
        display: 'flex', alignItems: 'center', gap: 10, marginBottom: 16,
        background: 'var(--surface)', border: '1px solid var(--border)',
        borderRadius: 10, padding: '12px 18px',
      }}>
        <TypeBadge isContract={isContract} />
        <span style={{ fontFamily: 'monospace', fontSize: 12, color: 'var(--text-2)',
                       wordBreak: 'break-all', flex: 1 }}>{address}</span>
        <button onClick={copy} style={{
          background: 'none', border: '1px solid var(--border-hi)', borderRadius: 6,
          color: copied ? 'var(--green)' : 'var(--text-2)', cursor: 'pointer',
          fontSize: 10, fontWeight: 700, letterSpacing: '.1em', padding: '4px 10px',
          textTransform: 'uppercase', transition: 'all .15s', whiteSpace: 'nowrap',
        }}>{copied ? '✓ Copied' : 'Copy'}</button>
      </div>

      {/* ── Stats cards ── */}
      <div className="addr-card" style={{ marginBottom: 18 }}>
        <div className="addr-stat">
          <div className="metric-label">Balance</div>
          <div className="metric-value green" style={{ fontSize: 18 }}>{fmtEth(data.balance)}</div>
          <div className="metric-sub">ETH</div>
        </div>
        <div className="addr-stat">
          <div className="metric-label">Transactions</div>
          <div className="metric-value blue" style={{ fontSize: 18 }}>{fmtNum(data.total_transactions)}</div>
          <div className="metric-sub">Total indexed</div>
        </div>
        {isContract ? (
          <div className="addr-stat">
            <div className="metric-label">Bytecode Size</div>
            <div className="metric-value" style={{ fontSize: 18, color: 'var(--purple)' }}>
              {data.bytecode_size ? fmtNum(data.bytecode_size) : '—'}
            </div>
            <div className="metric-sub">bytes</div>
          </div>
        ) : (
          <div className="addr-stat">
            <div className="metric-label">First Seen</div>
            <div style={{ fontSize: 11, marginTop: 6, color: 'var(--text-2)' }}>
              {data.first_seen ? new Date(data.first_seen).toLocaleString() : '—'}
            </div>
          </div>
        )}
        <div className="addr-stat">
          <div className="metric-label">Last Active</div>
          <div style={{ fontSize: 11, marginTop: 6, color: 'var(--text-2)' }}>
            {data.last_seen ? new Date(data.last_seen).toLocaleString() : '—'}
          </div>
        </div>
      </div>

      {/* ── Contract info panel (contracts only) ── */}
      {isContract && (
        <div style={{ marginBottom: 18 }}>
          <div className="page-head">
            <span className="page-title">Contract Info</span>
          </div>
          <div className="panel">
            <Row label="Type">
              <span style={{ color: 'var(--purple)', fontWeight: 600 }}>Smart Contract</span>
            </Row>
            <Row label="Bytecode Size">
              {data.bytecode_size ? `${fmtNum(data.bytecode_size)} bytes` : '—'}
            </Row>
            {data.bytecode && (
              <Row label="Bytecode">
                <BytecodeBlock bytecode={data.bytecode} />
              </Row>
            )}
            {deployInfo ? (
              <>
                <Row label="Creator">
                  <Link to={`/address/${deployInfo.from_address}`} className="addr-link"
                    style={{ fontFamily: 'monospace', fontSize: 11 }}>
                    {deployInfo.from_address}
                  </Link>
                </Row>
                <Row label="Deploy Transaction">
                  <Link to={`/tx/${deployInfo.hash}`} className="hash-link"
                    style={{ fontFamily: 'monospace', fontSize: 11 }}>
                    {deployInfo.hash}
                  </Link>
                </Row>
                <Row label="Deploy Block">
                  <Link to={`/blocks/${deployInfo.block_number}`} className="num-link">
                    #{fmtNum(deployInfo.block_number)}
                  </Link>
                  {deployInfo.block_timestamp && (
                    <span style={{ color: 'var(--text-3)', marginLeft: 10, fontSize: 11 }}>
                      {fmtTs(deployInfo.block_timestamp)} ({timeAgo(deployInfo.block_timestamp)})
                    </span>
                  )}
                </Row>
              </>
            ) : (
              <Row label="Deployment">
                <span style={{ color: 'var(--text-3)' }}>Not found in indexed transactions</span>
              </Row>
            )}
          </div>
        </div>
      )}

      {/* ── Transaction history ── */}
      <div className="page-head">
        <span className="page-title">
          {isContract ? 'Contract Interactions' : 'Transaction History'}
        </span>
        <span className="page-count">
          {data.total_transactions ? `${fmtNum(data.total_transactions)} total` : 'no transactions'}
        </span>
      </div>
      <div className="panel">
        {(!data.transactions || data.transactions.length === 0)
          ? <div className="empty">No transactions found</div>
          : (
            <>
              <table className="dt">
                <thead><tr>
                  <th>Hash</th>
                  <th>Block</th>
                  <th style={{ textAlign: 'center' }}>Dir</th>
                  <th>From</th>
                  <th>To</th>
                  <th style={{ textAlign: 'right' }}>Value (ETH)</th>
                  <th style={{ textAlign: 'right' }}>Status</th>
                  <th style={{ textAlign: 'right' }}>Age</th>
                </tr></thead>
                <tbody>
                  {data.transactions.map(tx => {
                    const isOut  = tx.from_address?.toLowerCase() === address.toLowerCase();
                    const isSelf = tx.from_address?.toLowerCase() === tx.to_address?.toLowerCase();
                    const isCreate = !tx.to_address;
                    return (
                      <tr key={tx.hash}>
                        <td style={{ fontSize: 11 }}>
                          <Link to={`/tx/${tx.hash}`} className="hash-link">
                            <span className="hash-cell">{shortHash(tx.hash)}</span>
                          </Link>
                        </td>
                        <td>
                          <Link to={`/blocks/${tx.block_number}`} className="num-link">
                            #{Number(tx.block_number).toLocaleString()}
                          </Link>
                        </td>
                        <td style={{ textAlign: 'center' }}>
                          {isCreate ? (
                            <span style={{
                              display:'inline-block', fontSize:9, fontWeight:700,
                              letterSpacing:'.08em', padding:'2px 7px', borderRadius:3,
                              background:'rgba(163,113,247,.12)', color:'var(--purple)',
                              border:'1px solid rgba(163,113,247,.3)',
                            }}>CREATE</span>
                          ) : isSelf ? (
                            <span style={{
                              display:'inline-block', fontSize:9, fontWeight:700,
                              letterSpacing:'.08em', padding:'2px 7px', borderRadius:3,
                              background:'rgba(255,179,71,.1)', color:'var(--yellow)',
                              border:'1px solid rgba(255,179,71,.25)',
                            }}>SELF</span>
                          ) : isOut ? (
                            <span style={{
                              display:'inline-block', fontSize:9, fontWeight:700,
                              letterSpacing:'.08em', padding:'2px 7px', borderRadius:3,
                              background:'rgba(255,87,87,.1)', color:'var(--red)',
                              border:'1px solid rgba(255,87,87,.25)',
                            }}>OUT</span>
                          ) : (
                            <span style={{
                              display:'inline-block', fontSize:9, fontWeight:700,
                              letterSpacing:'.08em', padding:'2px 7px', borderRadius:3,
                              background:'rgba(0,255,136,.1)', color:'var(--green)',
                              border:'1px solid rgba(0,255,136,.25)',
                            }}>IN</span>
                          )}
                        </td>
                        <td>
                          <Link to={`/address/${tx.from_address}`} className="addr-link">
                            <span className="addr-cell">{shortAddr(tx.from_address)}</span>
                          </Link>
                        </td>
                        <td>
                          {tx.to_address ? (
                            <Link to={`/address/${tx.to_address}`} className="addr-link">
                              <span className="addr-cell">{shortAddr(tx.to_address)}</span>
                            </Link>
                          ) : (
                            <span style={{ color: 'var(--text-3)', fontSize: 10 }}>Contract Create</span>
                          )}
                        </td>
                        <td style={{ textAlign: 'right', color: tx.value === '0' ? 'var(--text-3)' : 'var(--text)' }}>
                          {fmtVal(tx.value)}
                        </td>
                        <td style={{ textAlign: 'right' }}>
                          {tx.status === 1   && <span className="badge badge-ok">OK</span>}
                          {tx.status === 0   && <span className="badge badge-fail">Fail</span>}
                          {tx.status == null && <span className="badge badge-pend">—</span>}
                        </td>
                        <td style={{ textAlign: 'right', color: 'var(--text-3)' }}>
                          <span style={{ display: 'inline-block', minWidth: 74, textAlign: 'right' }}>
                            {timeAgo(tx.block_timestamp)}
                          </span>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>

              {totalPages > 1 && (
                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center',
                              gap: 8, padding: '14px 20px', borderTop: '1px solid var(--border)' }}>
                  <button onClick={() => load(page - 1)} disabled={page <= 1}
                    style={pagerStyle(page <= 1)}>← Prev</button>
                  <span style={{ fontSize: 10, color: 'var(--text-3)', letterSpacing: '.1em',
                                 textTransform: 'uppercase' }}>
                    Page {page} / {totalPages}
                  </span>
                  <button onClick={() => load(page + 1)} disabled={page >= totalPages}
                    style={pagerStyle(page >= totalPages)}>Next →</button>
                </div>
              )}
            </>
          )
        }
      </div>
    </div>
  );
}

const pagerStyle = (disabled) => ({
  background: 'none',
  border: '1px solid var(--border-hi)',
  borderRadius: 6,
  color: disabled ? 'var(--text-3)' : 'var(--text-2)',
  cursor: disabled ? 'not-allowed' : 'pointer',
  fontSize: 10,
  fontWeight: 700,
  letterSpacing: '.1em',
  padding: '4px 12px',
  textTransform: 'uppercase',
  opacity: disabled ? 0.4 : 1,
});
