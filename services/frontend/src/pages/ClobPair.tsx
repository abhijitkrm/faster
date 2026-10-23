import { useParams } from 'react-router-dom';
import { useCursorList, MoreButton, ErrorNote, FillsTable } from '../clob';

// /clob/pair/:id — the pair's trade tape, newest first.
export default function ClobPair() {
  const { id } = useParams();
  const valid = /^\d{1,10}$/.test(id || '');
  const list = useCursorList(`/api/clob/pairs/${valid ? id : '0'}/trades`, 100);

  if (!valid) return <div className="empty" style={{ paddingTop: 60 }}>Not a valid pair id: {id}</div>;

  return (
    <div>
      <div className="page-head">
        <span className="page-title">Pair</span>
        <span className="metric-value blue" style={{ fontSize: 20 }}>#{id}</span>
        <span className="page-count">Trade tape · newest first</span>
      </div>
      <div className="panel">
        {list.items == null
          ? <div className="empty">Loading trades…</div>
          : (
            <>
              <ErrorNote msg={list.error} />
              {!list.error && <FillsTable fills={list.items} showPair={false} />}
              <MoreButton list={list} />
            </>
          )}
      </div>
    </div>
  );
}
