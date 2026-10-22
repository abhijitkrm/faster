import { useState } from 'react';
import { useParams, Link } from 'react-router-dom';
import {
  useCursorList, MoreButton, ErrorNote, OrdersTable, CancelsTable, TraderFillsTable, btnStyle,
} from '../clob';

const TABS = [
  { id: 'orders', label: 'Orders' },
  { id: 'fills', label: 'Fills' },
  { id: 'cancels', label: 'Cancels / Amends' },
];

function TabBody({ addr, tab }) {
  const list = useCursorList(`/api/clob/traders/${addr}/${tab}`, 50);
  if (list.items == null) return <div className="empty">Loading…</div>;
  return (
    <>
      <ErrorNote msg={list.error} />
      {!list.error && tab === 'orders' && <OrdersTable orders={list.items} showTrader={false} />}
      {!list.error && tab === 'fills' && <TraderFillsTable fills={list.items} />}
      {!list.error && tab === 'cancels' && <CancelsTable cancels={list.items} showTrader={false} />}
      <MoreButton list={list} />
    </>
  );
}

// /clob/trader/:addr — a trader's CLOB history, newest first.
export default function ClobTrader() {
  const { addr } = useParams();
  const [tab, setTab] = useState('orders');
  const valid = /^0x[0-9a-fA-F]{40}$/.test(addr || '');

  if (!valid) return <div className="empty" style={{ paddingTop: 60 }}>Not a valid address: {addr}</div>;

  return (
    <div>
      <div style={{
        display: 'flex', alignItems: 'center', gap: 10, marginBottom: 16,
        background: 'var(--surface)', border: '1px solid var(--border)',
        borderRadius: 10, padding: '12px 18px', flexWrap: 'wrap',
      }}>
        <span className="page-title">CLOB Trader</span>
        <span style={{ fontFamily: 'monospace', fontSize: 12, color: 'var(--text-2)', wordBreak: 'break-all', flex: 1 }}>
          {addr.toLowerCase()}
        </span>
        <Link to={`/address/${addr}`} className="panel-link">Address page →</Link>
      </div>

      <div style={{ display: 'flex', gap: 8, marginBottom: 14 }}>
        {TABS.map((t) => (
          <button key={t.id} onClick={() => setTab(t.id)}
            style={{
              ...btnStyle,
              color: tab === t.id ? 'var(--blue)' : 'var(--text-3)',
              borderColor: tab === t.id ? 'var(--blue)' : 'var(--border-hi)',
            }}>
            {t.label}
          </button>
        ))}
      </div>

      <div className="panel">
        <TabBody key={tab + addr} addr={addr} tab={tab} />
      </div>
    </div>
  );
}
