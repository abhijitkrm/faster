import { useState, useRef, useCallback } from 'react';
import { useNavigate } from 'react-router-dom';

import { API } from '../config';

// ── Type detection ────────────────────────────────────────────────────────────
const RE_TX_OR_BLOCK_HASH = /^0x[0-9a-fA-F]{64}$/i;   // 32-byte hash (tx, block or CLOB order)
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
  hash32:  { label: 'Tx / Block / Order hash', color: 'var(--purple)' },
  block:   { label: 'Block #',   color: 'var(--green)' },
  unknown: { label: 'Token',     color: 'var(--text-3)' },
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

    // The server resolves what a query names (block, tx, token, address).
    const lookup = async (q) => {
      const r = await fetch(`${API}/api/search?q=${encodeURIComponent(q)}`);
      return r.ok ? r.json() : [];
    };
    const open = (hit) => {
      const to = hit.type === 'transaction' ? `/tx/${hit.value}`
        : hit.type === 'block' ? `/blocks/${hit.value}`
        : hit.type === 'token' ? `/token/${hit.value}`
        : `/address/${hit.value}`;
      navigate(to);
      setQuery('');
      setStatus('idle');
    };

    if (resolved.type === 'address') {
      try {
        const hits = await lookup(resolved.value);
        if (hits[0]?.type === 'token') { open(hits[0]); return; }
      } catch {}
      navigate(`/address/${resolved.value}`);
      setQuery('');
      return;
    }

    if (resolved.type === 'unknown' && v.length >= 2) {
      setStatus('loading');
      try {
        const hits = await lookup(v);
        if (hits.length) { open(hits[0]); return; }
        setStatus('error');
        setErrMsg('No token with that name or symbol');
      } catch {
        setStatus('error');
        setErrMsg('Network error');
      }
      return;
    }

    if (resolved.type === 'block') {
      navigate(`/blocks/${resolved.value}`);
      setQuery('');
      return;
    }

    if (resolved.type === 'hash32') {
      // A tx or block hash (one request), else a CLOB order hash
      setStatus('loading');
      setErrMsg('');
      try {
        const hits = await lookup(resolved.value);
        if (hits.length) { open(hits[0]); return; }
        // Try a CLOB order / cancel / amend hash
        const res3 = await fetch(`${API}/api/clob/orders/${resolved.value.toLowerCase()}`);
        if (res3.ok) {
          navigate(`/clob/order/${resolved.value.toLowerCase()}`);
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
    setErrMsg('Enter a block number, hash, address or token name');
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
          placeholder="Search block / tx / address / token…"
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
