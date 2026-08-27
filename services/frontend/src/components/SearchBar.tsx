import { useState, useRef, useCallback } from 'react';
import { useNavigate } from 'react-router-dom';

const API = import.meta.env.VITE_API_URL || 'http://localhost:4000';

// ── Type detection ────────────────────────────────────────────────────────────
const RE_TX_OR_BLOCK_HASH = /^0x[0-9a-fA-F]{64}$/i;   // 32-byte hash (tx or block)
const RE_ADDRESS           = /^0x[0-9a-fA-F]{40}$/i;   // 20-byte address
const RE_BLOCK_NUM         = /^\d+$/;                   // pure decimal → block number

function detect(raw) {
  const v = raw.trim();
  if (!v) return { type: null, value: v };
  if (RE_ADDRESS.test(v))           return { type: 'address', value: v };
  if (RE_TX_OR_BLOCK_HASH.test(v))  return { type: 'hash32',  value: v };   // ambiguous
  if (RE_BLOCK_NUM.test(v))         return { type: 'block',   value: v };
  return { type: 'unknown', value: v };
}

const TYPE_LABELS = {
  address: { label: 'Address',   color: 'var(--blue)' },
  hash32:  { label: 'Tx / Block hash', color: 'var(--purple)' },
  block:   { label: 'Block #',   color: 'var(--green)' },
  unknown: { label: '?',         color: 'var(--text-3)' },
};

export default function SearchBar() {
  const navigate  = useNavigate();
  const [query,   setQuery]   = useState('');
  const [status,  setStatus]  = useState('idle'); // idle | loading | error
  const [errMsg,  setErrMsg]  = useState('');
  const inputRef  = useRef(null);

  const { type, value } = detect(query);

  const go = useCallback(async (e) => {
    e?.preventDefault();
    const v = query.trim();
    if (!v) return;

    const resolved = detect(v);

    if (resolved.type === 'address') {
      navigate(`/address/${resolved.value}`);
      setQuery('');
      return;
    }

    if (resolved.type === 'block') {
      navigate(`/blocks/${resolved.value}`);
      setQuery('');
      return;
    }

    if (resolved.type === 'hash32') {
      // Could be a tx hash or a block hash — probe tx first, fallback to block
      setStatus('loading');
      setErrMsg('');
      try {
        const res = await fetch(`${API}/api/transactions/${resolved.value}`);
        if (res.ok) {
          navigate(`/tx/${resolved.value}`);
          setQuery('');
          setStatus('idle');
          return;
        }
        // Try block hash
        const res2 = await fetch(`${API}/api/blocks/${resolved.value}`);
        if (res2.ok) {
          const blk = await res2.json();
          navigate(`/blocks/${blk.number ?? resolved.value}`);
          setQuery('');
          setStatus('idle');
          return;
        }
        setStatus('error');
        setErrMsg('Not found');
      } catch {
        setStatus('error');
        setErrMsg('Network error');
      }
      return;
    }

    setStatus('error');
    setErrMsg('Enter a block number, tx hash, or address');
  }, [query, navigate]);

  const typeInfo = type ? TYPE_LABELS[type] : null;

  return (
    <form onSubmit={go} className="search-form" autoComplete="off">
      <div className="search-wrap">
        <span className="search-icon">
          {status === 'loading'
            ? <span className="search-spinner" />
            : <svg width="13" height="13" viewBox="0 0 24 24" fill="none"
                stroke="currentColor" strokeWidth="2.5" strokeLinecap="round">
                <circle cx="11" cy="11" r="7"/><line x1="16.5" y1="16.5" x2="21" y2="21"/>
              </svg>
          }
        </span>
        <input
          ref={inputRef}
          className="search-input"
          value={query}
          onChange={e => { setQuery(e.target.value); setStatus('idle'); setErrMsg(''); }}
          placeholder="Search block / tx hash / address…"
          spellCheck={false}
        />
        {typeInfo && query.trim() && (
          <span className="search-type-badge" style={{ color: typeInfo.color }}>
            {typeInfo.label}
          </span>
        )}
        {query && (
          <button type="button" className="search-clear" onClick={() => { setQuery(''); setStatus('idle'); inputRef.current?.focus(); }}>
            ✕
          </button>
        )}
      </div>
      {status === 'error' && (
        <div className="search-error">{errMsg}</div>
      )}
    </form>
  );
}
