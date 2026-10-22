import { useEffect, useRef, useState } from 'react';
import { fetchJSON, OrdersTable, CancelsTable, FillsTable } from '../clob';

// CLOB section of the block page. Loads /api/clob/blocks/:height only when the
// section scrolls into view, so plain block browsing never hits ClickHouse.
export default function ClobBlockSection({ height }) {
  const ref = useRef(null);
  const [visible, setVisible] = useState(false);
  const [state, setState] = useState({ status: 'idle', data: null, msg: '' });

  useEffect(() => { setVisible(false); setState({ status: 'idle', data: null, msg: '' }); }, [height]);

  useEffect(() => {
    if (visible || !ref.current) return;
    if (typeof IntersectionObserver === 'undefined') { setVisible(true); return; }
    const io = new IntersectionObserver((entries) => {
      if (entries.some((e) => e.isIntersecting)) setVisible(true);
    }, { rootMargin: '200px' });
    io.observe(ref.current);
    return () => io.disconnect();
  }, [visible, height]);

  useEffect(() => {
    if (!visible) return;
    let cancelled = false;
    setState({ status: 'loading', data: null, msg: '' });
    fetchJSON(`/api/clob/blocks/${height}?limit=500`)
      .then(({ status, body }) => {
        if (cancelled) return;
        if (status === 200) setState({ status: 'ok', data: body, msg: '' });
        else if (status === 404) {
          const msg = body?.gap
            ? `The node did not hold this height (${body.gap}).`
            : body?.not_loaded_yet
              ? `Not loaded yet (archive at #${Number(body.latest_loaded_height).toLocaleString()}).`
              : 'No CLOB record for this block.';
          setState({ status: 'none', data: null, msg });
        } else setState({ status: 'error', data: null, msg: body?.error || `HTTP ${status}` });
      })
      .catch(() => !cancelled && setState({ status: 'error', data: null, msg: 'Network error' }));
    return () => { cancelled = true; };
  }, [visible, height]);

  const d = state.data;
  return (
    <div ref={ref} style={{ marginTop: 24 }}>
      <div className="page-head">
        <span className="page-title">CLOB</span>
        {d && (
          <span className="page-count">
            {d.counts.orders} order{d.counts.orders !== 1 ? 's' : ''} · {d.counts.cancels} cancel/amend
            · {d.counts.fills} fill{d.counts.fills !== 1 ? 's' : ''}
            {d.counts.rejected > 0 && ` · ${d.counts.rejected} rejected`}
            {d.truncated && ' · showing the first 500 of each'}
          </span>
        )}
      </div>
      {state.status === 'idle' || state.status === 'loading'
        ? <div className="panel"><div className="empty">Loading CLOB activity…</div></div>
        : state.status !== 'ok'
          ? <div className="panel"><div className="empty" style={state.status === 'error' ? { color: 'var(--text-3)' } : undefined}>{state.msg}</div></div>
          : (d.counts.orders + d.counts.cancels + d.counts.fills === 0)
            ? <div className="panel"><div className="empty">No CLOB activity in this block</div></div>
            : (
              <>
                {d.orders.length > 0 && (
                  <div className="panel" style={{ marginBottom: 14 }}>
                    <div className="panel-head"><span className="panel-title">Orders</span></div>
                    <OrdersTable orders={d.orders} showBlock={false} />
                  </div>
                )}
                {d.cancels.length > 0 && (
                  <div className="panel" style={{ marginBottom: 14 }}>
                    <div className="panel-head"><span className="panel-title">Cancels / Amends</span></div>
                    <CancelsTable cancels={d.cancels} showBlock={false} />
                  </div>
                )}
                {d.fills.length > 0 && (
                  <div className="panel">
                    <div className="panel-head"><span className="panel-title">Fills</span></div>
                    <FillsTable fills={d.fills} showBlock={false} />
                  </div>
                )}
              </>
            )}
    </div>
  );
}
