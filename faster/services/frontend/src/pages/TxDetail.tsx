import { fmtEth } from '../format';
import { useEffect, useState } from 'react';
import { useParams, Link } from 'react-router-dom';

import { API } from '../config';
import { useWS } from '../context/WSContext';
import { TransfersTable } from '../tokens';
import { InternalTable } from '../components/InternalTable';

const fmtNum   = (n) => (n == null ? '—' : Number(n).toLocaleString());
const fmtGwei  = (wei) => { try { return (Number(BigInt(wei)) / 1e9).toFixed(2) + ' Gwei'; } catch { return '—'; } };
const shortAddr = (a) => a ? a.slice(0, 6) + '…' + a.slice(-4) : 'Contract';

const EVENT_NAMES = {
  '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef': 'Transfer',
  '0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925': 'Approval',
};

const parseTopicAddress = (t) => {
  if (t.length === 66 && t.startsWith('0x000000000000000000000000')) {
    return '0x' + t.slice(26);
  }
  return null;
};

const LogEntry = ({ log }) => {
  let parsed = null;
  try { parsed = JSON.parse(log.data); } catch {}

  const address = log.address || parsed?.address;
  const topics = (parsed?.topics || log.topics || []);
  const data = parsed?.data ?? log.data;
  const eventSig = topics[0]?.toLowerCase() || '';
  const eventName = log.decoded?.name || EVENT_NAMES[eventSig];

  const topicLabel = (idx) => {
    if (idx === 0) return 'signature';
    if (eventName === 'Transfer') return idx === 1 ? 'from' : idx === 2 ? 'to' : `topic[${idx}]`;
    if (eventName === 'Approval') return idx === 1 ? 'owner' : idx === 2 ? 'spender' : `topic[${idx}]`;
    return `topic[${idx}]`;
  };

  const decodedAmount = () => {
    if (eventName !== 'Transfer' || !data || data.length < 66) return null;
    try { return BigInt(data).toString(); } catch { return null; }
  };

  return (
    <div>
      <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 8 }}>
        <span style={{ fontSize: 12, fontWeight: 700, letterSpacing: '.1em',
                       color: 'var(--text-3)', background: 'var(--surface-2)',
                       padding: '2px 8px', borderRadius: 4 }}>LOG #{log.log_index}</span>
        {eventName && (
          <span style={{ fontSize: 12, fontWeight: 700, color: 'var(--purple)', letterSpacing: '.05em' }}>
            {eventName.toUpperCase()}
          </span>
        )}
        <Link to={`/address/${address}`} className="addr-link" style={{ fontFamily: 'var(--mono)', fontSize: 12 }}>
          {shortAddr(address)} ({address})
        </Link>
      </div>

      {topics.length > 0 && !log.decoded && (
        <div style={{ marginBottom: 8 }}>
          {topics.map((t, ti) => {
            const addr = parseTopicAddress(t);
            return (
              <div key={ti} style={{ fontSize: 12, fontFamily: 'var(--mono)', marginBottom: 2 }}>
                <span style={{ color: 'var(--text-3)', marginRight: 8 }}>{topicLabel(ti)}</span>
                {addr
                  ? <Link to={`/address/${addr}`} className="addr-link" style={{ fontSize: 12 }}>
                      {shortAddr(addr)} ({addr})
                    </Link>
                  : <span style={{ color: ti === 0 ? 'var(--purple)' : 'var(--text-2)', wordBreak: 'break-all' }}>{t}</span>
                }
              </div>
            );
          })}
        </div>
      )}

      {log.decoded && (
        <div style={{ fontSize: 12, fontFamily: 'var(--mono)', marginBottom: 6, color: 'var(--text-2)' }}>
          <div style={{ color: 'var(--purple)', marginBottom: 2 }}>{log.decoded.signature}</div>
          {log.decoded.params.map((p, i) => (
            <div key={i}><span style={{ color: 'var(--text-3)', marginRight: 8 }}>{p.name || `arg${i}`}{p.indexed ? ' (indexed)' : ''}</span>
              {typeof p.value === 'string' ? p.value : JSON.stringify(p.value)}</div>
          ))}
        </div>
      )}
      {data && data !== '0x' && (
        <div style={{ fontSize: 12, color: 'var(--text-3)', wordBreak: 'break-all', marginBottom: 4 }}>
          <span style={{ color: 'var(--text-3)', marginRight: 8 }}>data</span>
          <span style={{ fontFamily: 'var(--mono)', color: 'var(--text-2)' }}>{data}</span>
          {decodedAmount() && (
            <span style={{ marginLeft: 10, color: 'var(--green)' }}>
              ({decodedAmount()} units)
            </span>
          )}
        </div>
      )}
    </div>
  );
};

const fmtTs = (ts) => {
  if (!ts) return '—';
  return new Date(parseInt(ts) * 1000).toLocaleString();
};

const timeAgo = (ts) => {
  if (!ts) return '';
  const s = Math.floor(Date.now() / 1000) - parseInt(ts);
  if (s < 1)     return 'just now';
  if (s < 60)    return s + 's ago';
  if (s < 3600)  return Math.floor(s / 60) + 'm ago';
  if (s < 86400) return Math.floor(s / 3600) + 'h ago';
  return Math.floor(s / 86400) + 'd ago';
};

const Row = ({ label, children }) => (
  <div className="kv"><span>{label}</span><span>{children}</span></div>
);

function CopyButton({ text }) {
  const [done, setDone] = useState(false);
  return (
    <button type="button" aria-label="Copy hash" className="nav-link" style={{ minHeight: 32, padding: '0 10px' }}
      onClick={() => navigator.clipboard.writeText(text).then(() => { setDone(true); setTimeout(() => setDone(false), 1200); })}>
      {done ? 'Copied' : 'Copy'}
    </button>
  );
}

/** Where the time went, measured by faster: the node producing the block,
 *  faster receiving and indexing it, and the server building this page. */
function Timeline({ tx }) {
  const seen = tx.block_seen_ms, idx = tx.block_indexed_ms, sealedMs = tx.block_timestamp * 1000;
  if (!seen || !idx || idx < seen) return null;
  const arrive = Math.max(0, seen - sealedMs);   // block timestamps are whole seconds: approximate
  const index = idx - seen;
  const page = (tx.served_us || 0) / 1000;
  const total = arrive + index + page || 1;
  const fmt = (ms) => (ms >= 1000 ? (ms / 1000).toFixed(2) + ' s' : ms >= 1 ? Math.round(ms) + ' ms' : ms.toFixed(2) + ' ms');
  const legs = [
    { label: 'Reached faster', value: '≈ ' + fmt(arrive), note: 'after the block timestamp', color: '#3A414B', w: arrive },
    { label: 'Indexed', value: index < 1 ? '< 1 ms' : '+' + fmt(index), note: 'decoded and committed', color: 'var(--accent)', w: index, fast: true },
    { label: 'This page', value: fmt(page), note: 'server time to build it', color: 'var(--text)', w: page },
  ];
  return (
    <section className="panel" style={{ padding: '18px 20px', marginBottom: 24 }} aria-label="Timeline">
      <div className="trace-head"><span>From the chain to this page</span><span className="mono">≈ {fmt(total)}</span></div>
      <div className="timeline-bar" aria-hidden="true">
        {legs.map(l => <span key={l.label} style={{ flex: `${Math.max(l.w, total * 0.01)} 0 0`, background: l.color }} />)}
      </div>
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))', gap: 16, marginTop: 16 }}>
        {legs.map(l => (
          <div key={l.label}>
            <div className="metric-label">{l.label}</div>
            <div className="mono" style={{ fontSize: 18, marginTop: 4, color: l.fast ? 'var(--accent)' : 'var(--text)' }}>{l.value}</div>
            <div className="metric-sub" style={{ marginTop: 2 }}>{l.note}</div>
          </div>
        ))}
      </div>
    </section>
  );
}

export default function TxDetail() {
  const { hash } = useParams();
  const { stats } = useWS();
  const [tx, setTx] = useState(null);
  const [notFound, setNotFound] = useState(false);

  useEffect(() => {
    setTx(null); setNotFound(false);
    fetch(`${API}/api/transactions/${hash}`)
      .then(r => { if (r.status === 404) { setNotFound(true); return null; } return r.json(); })
      .then(d => { if (d) setTx(d); })
      .catch(() => setNotFound(true));
  }, [hash]);

  if (notFound) return (
    <div className="empty" style={{ paddingTop: 60 }}>
      Transaction not found.
      <div style={{ marginTop: 16 }}>
        <Link to="/transactions" className="nav-link" style={{ fontSize: 12 }}>← Back to transactions</Link>
      </div>
    </div>
  );

  if (!tx) return <div className="empty">Loading transaction…</div>;

  const isSuccess = tx.status === 1;
  const isFail    = tx.status === 0;

  return (
    <div>
      {/* Header */}
      <div style={{ marginBottom: 24 }}>
        <div className="eyebrow">Transaction</div>
        <div style={{ display: 'flex', flexWrap: 'wrap', alignItems: 'center', gap: 12, marginTop: 10 }}>
          <h1 className="mono" style={{ margin: 0, fontSize: 20, fontWeight: 500, wordBreak: 'break-all' }}>{tx.hash}</h1>
          <CopyButton text={tx.hash} />
          <span className={'badge ' + (isSuccess ? 'badge-ok' : isFail ? 'badge-fail' : 'badge-pend')}>
            {isSuccess ? 'Success' : isFail ? 'Failed' : 'Pending'}
          </span>
        </div>
        <div className="mono" style={{ marginTop: 10, fontSize: 13, color: 'var(--text-2)' }}>
          Block <Link to={`/blocks/${tx.block_number}`} className="num-link">{fmtNum(tx.block_number)}</Link>
          {stats?.block_height && ` · ${fmtNum(Number(stats.block_height) - tx.block_number + 1)} confirmations`}
          {tx.block_timestamp && ` · ${timeAgo(tx.block_timestamp)}`}
        </div>
      </div>

      <Timeline tx={tx} />

      {/* Overview */}
      <div className="panel" style={{ marginBottom: 18 }}>
        <div className="panel-head"><h2 className="panel-title" style={{ margin: 0 }}>Overview</h2></div>

        <Row label="Transaction Hash">
          <span style={{ fontFamily: 'var(--mono)', fontSize: 12 }}>{tx.hash}</span>
        </Row>
        <Row label="From">
          <Link to={`/address/${tx.from_address}`} className="addr-link"
            style={{ fontFamily: 'var(--mono)', fontSize: 12 }}>
            {tx.from_address}
          </Link>
        </Row>
        <Row label="To">
          {tx.to_address
            ? <Link to={`/address/${tx.to_address}`} className="addr-link"
                style={{ fontFamily: 'var(--mono)', fontSize: 12 }}>
                {tx.to_address}
              </Link>
            : <span style={{ color: 'var(--text-3)' }}>Contract Creation
                {tx.contract_address && (
                  <> → <Link to={`/address/${tx.contract_address}`} className="addr-link"
                    style={{ fontFamily: 'var(--mono)', fontSize: 12 }}>{tx.contract_address}</Link></>
                )}
              </span>
          }
        </Row>
        <Row label="Value">
          <span style={{ color: tx.value === '0' ? 'var(--text-3)' : 'var(--text)' }}>
            {fmtEth(tx.value)} ETH
          </span>
        </Row>
        <Row label="Gas Price">{fmtGwei(tx.gas_price)}</Row>
        <Row label="Gas Limit / Used">
          {fmtNum(tx.gas)} / {tx.gas_used ? fmtNum(tx.gas_used) : '—'}
          {tx.gas_used && tx.gas > 0 && (
            <span style={{ color: 'var(--text-3)', marginLeft: 10, fontSize: 12 }}>
              ({((tx.gas_used / tx.gas) * 100).toFixed(1)}%)
            </span>
          )}
        </Row>
        <Row label="Nonce">{tx.nonce ?? '—'}</Row>
        {tx.input_data && tx.input_data !== '0x' && (
          <Row label="Input Data">
            <details>
              <summary style={{ cursor: 'pointer', color: 'var(--blue)', fontSize: 12 }}>
                {Math.ceil((tx.input_data.length - 2) / 2)} bytes
              </summary>
              <pre style={{ marginTop: 8, fontSize: 12, color: 'var(--text-3)',
                            background: 'var(--surface-2)', padding: 10,
                            borderRadius: 6, overflowX: 'auto', whiteSpace: 'pre-wrap',
                            wordBreak: 'break-all' }}>
                {tx.input_data}
              </pre>
            </details>
          </Row>
        )}
      </div>

      {/* Decoded call (verified contracts) */}
      {tx.decoded_input && (
        <div className="panel" style={{ marginBottom: 18 }}>
          <div className="panel-head"><h2 className="panel-title" style={{ margin: 0 }}>Call</h2>
            <span style={{ fontSize: 12, color: 'var(--accent)', fontFamily: 'var(--mono)', wordBreak: 'break-all' }}>{tx.decoded_input.signature}</span></div>
          <table className="dt"><tbody>
            {tx.decoded_input.params.map((p, i) => (
              <tr key={i}><td style={{ color: 'var(--text-3)', width: 160 }}>{p.name || `arg${i}`}</td>
                <td style={{ color: 'var(--text-3)', width: 120 }}>{p.type}</td>
                <td style={{ fontFamily: 'var(--mono)', fontSize: 12, wordBreak: 'break-all' }}>{typeof p.value === 'string' ? p.value : JSON.stringify(p.value)}</td></tr>
            ))}
          </tbody></table>
        </div>
      )}

      {/* Internal calls */}
      {tx.internal_transactions && tx.internal_transactions.length > 0 && (
        <>
          <div className="page-head">
            <span className="page-title">Internal Transactions</span>
            <span className="page-count">{tx.internal_transactions.length}</span>
          </div>
          <div className="panel" style={{ marginBottom: 18 }}>
            <InternalTable items={tx.internal_transactions} />
          </div>
        </>
      )}

      {/* Token transfers */}
      {tx.token_transfers && tx.token_transfers.length > 0 && (
        <>
          <div className="page-head">
            <span className="page-title">Token Transfers</span>
            <span className="page-count">{tx.token_transfers.length}</span>
          </div>
          <div className="panel" style={{ marginBottom: 18 }}>
            <TransfersTable items={tx.token_transfers} compact />
          </div>
        </>
      )}

      {/* Logs */}
      {tx.logs && tx.logs.length > 0 && (
        <>
          <div className="page-head">
            <span className="page-title">Logs</span>
            <span className="page-count">{tx.logs.length} event{tx.logs.length !== 1 ? 's' : ''}</span>
          </div>
          <div className="panel">
            {tx.logs.map((log, i) => (
              <div key={log.log_index} style={{
                padding: '14px 20px',
                borderBottom: i < tx.logs.length - 1 ? '1px solid var(--border)' : 'none',
              }}>
                <LogEntry log={log} />
              </div>
            ))}
          </div>
        </>
      )}
    </div>
  );
}
