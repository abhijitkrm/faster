import { useEffect, useState } from 'react';
import { API } from '../config';
import { TransfersTable } from '../tokens';

// Token transfers list with keyset paging ("Older" follows next_cursor).
export default function CursorTransfers({ path, showToken = true }) {
  const [items, setItems] = useState(null);
  const [next, setNext] = useState(null);
  const [loading, setLoading] = useState(false);

  const load = (before) => {
    setLoading(true);
    fetch(`${API}${path}?limit=25${before ? `&before=${before}` : ''}`)
      .then(r => r.json())
      .then(d => { setItems(prev => (before && prev ? [...prev, ...d.items] : d.items)); setNext(d.next_cursor); })
      .catch(() => setItems(prev => prev ?? []))
      .finally(() => setLoading(false));
  };

  useEffect(() => { setItems(null); setNext(null); load(null); }, [path]);

  return (
    <div>
      <TransfersTable items={items} showToken={showToken} />
      {next && (
        <div style={{ textAlign: 'center', padding: 10 }}>
          <button className="nav-link" disabled={loading} onClick={() => load(next)}>{loading ? 'Loading…' : 'Older →'}</button>
        </div>
      )}
    </div>
  );
}
