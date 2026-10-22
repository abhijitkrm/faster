import { useEffect, useState } from 'react';
import { useParams, Link } from 'react-router-dom';
import {
  fetchJSON, fmtBig, fmtTime, timeAgo, Row, StatusBadge, SideBadge,
  TraderLink, BlockLink, PairLink, TraderFillsTable,
} from '../clob';

// /clob/order/:hash — an order, cancel or amend by its EIP-712 hash.
export default function ClobOrder() {
  const { hash } = useParams();
  const [data, setData] = useState(null);
  const [error, setError] = useState('');

  useEffect(() => {
    setData(null); setError('');
    fetchJSON(`/api/clob/orders/${hash}`)
      .then(({ status, body }) => {
        if (status === 200) setData(body);
        else if (status === 404) setError('Not found in the CLOB archive — it may not be loaded yet.');
        else setError(body?.error || `HTTP ${status}`);
      })
      .catch(() => setError('Network error'));
  }, [hash]);

  if (error) return (
    <div className="empty" style={{ paddingTop: 60 }}>
      {error}
      <div style={{ marginTop: 10, fontFamily: 'monospace', fontSize: 11 }}>{hash}</div>
    </div>
  );
  if (!data) return <div className="empty">Loading order…</div>;

  const o = data.order;
  const c = data.cancel;
  const it = o || c;
  const title = data.kind === 'order' ? 'CLOB Order' : data.kind === 'amend' ? 'CLOB Amend' : 'CLOB Cancel';

  return (
    <div>
      <div className="page-head">
        <span className="page-title">{title}</span>
        <StatusBadge status={it.status} />
      </div>

      <div className="panel" style={{ marginBottom: 18 }}>
        <div className="panel-head">
          <span className="panel-title">Overview</span>
          <span style={{ fontSize: 10, color: 'var(--text-3)' }}>{timeAgo(it.block_time)}</span>
        </div>
        <Row label="Hash"><span style={{ fontFamily: 'monospace', fontSize: 11 }}>{data.hash}</span></Row>
        <Row label="Status">
          <StatusBadge status={it.status} />
          {it.reason && <span style={{ marginLeft: 10, color: 'var(--red)' }}>{it.reason}</span>}
        </Row>
        <Row label="Block">
          <BlockLink height={it.height} />
          <span style={{ color: 'var(--text-3)', marginLeft: 10 }}>position {it.idx} · {fmtTime(it.block_time)}</span>
        </Row>
        <Row label="Trader"><TraderLink addr={it.trader} full /></Row>
        <Row label="Pair"><PairLink id={it.pair_id} /></Row>
        {o && (
          <>
            <Row label="Side"><SideBadge isBuy={o.is_buy} /> <span style={{ marginLeft: 8, color: 'var(--text-3)' }}>{o.is_market ? 'market' : 'limit'}</span></Row>
            <Row label="Price">{fmtBig(o.price)}</Row>
            <Row label="Amount">{fmtBig(o.amount)}</Row>
            <Row label="Filled">{fmtBig(o.filled)}</Row>
            <Row label="Order ID">{o.order_id === '0' ? <span style={{ color: 'var(--text-3)' }}>none (rejected before assignment)</span> : fmtBig(o.order_id)}</Row>
            <Row label="Client Order ID">{fmtBig(o.client_order_id)}</Row>
            <Row label="Signed at">{o.timestamp_ns === '0' ? '—' : fmtTime(new Date(Number(BigInt(o.timestamp_ns) / 1000000n)).toISOString())}</Row>
          </>
        )}
        {c && (
          <>
            <Row label="Kind"><span style={{ textTransform: 'uppercase', fontWeight: 700 }}>{c.kind}</span></Row>
            <Row label="Target">
              {c.target_order_id !== '0' ? `order ID ${fmtBig(c.target_order_id)}` : `client order ID ${fmtBig(c.target_client)}`}
            </Row>
            {c.kind === 'amend' && <Row label="New price">{fmtBig(c.price)}</Row>}
            {c.kind === 'amend' && <Row label="New amount">{fmtBig(c.amount)}</Row>}
            <Row label="Client ID">{fmtBig(c.client_id)}</Row>
          </>
        )}
      </div>

      {data.occurrences && data.occurrences.length > 1 && (
        <div className="panel" style={{ marginBottom: 18 }}>
          <div className="panel-head"><span className="panel-title">All executions of this hash</span></div>
          <table className="dt">
            <thead><tr><th>Kind</th><th>Block</th><th>Position</th><th style={{ textAlign: 'right' }}>Status</th></tr></thead>
            <tbody>
              {data.occurrences.map((x) => (
                <tr key={`${x.kind}-${x.height}-${x.idx}`}>
                  <td style={{ textTransform: 'uppercase', fontSize: 10, fontWeight: 700 }}>{x.kind}</td>
                  <td><BlockLink height={x.height} /></td>
                  <td>{x.idx}</td>
                  <td style={{ textAlign: 'right' }}><StatusBadge status={x.status} /></td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {o && (
        <>
          <div className="page-head">
            <span className="page-title">Fills</span>
            <span className="page-count">{data.fills.length} fill{data.fills.length !== 1 ? 's' : ''}</span>
          </div>
          <div className="panel">
            <TraderFillsTable fills={data.fills} showPair={false} />
          </div>
        </>
      )}
      <div style={{ marginTop: 16 }}>
        <Link to={`/clob/trader/${it.trader}`} className="panel-link">Trader CLOB history →</Link>
      </div>
    </div>
  );
}
