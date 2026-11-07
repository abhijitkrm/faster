import { createContext, useContext, useEffect, useRef, useState } from 'react';
import { API as API_URL, WS_URL } from '../config';

const MAX_BLOCKS = 50;
const MAX_TXS    = 20;

const WSCtx = createContext(null);

// Live feed from the faster service. Messages are queued and applied once
// per animation frame, so a burst (one commit can carry 50 blocks and 20
// txs) causes one render instead of dozens.
export function WSProvider({ children }) {
  const [wsOk,   setWsOk]   = useState(false);
  const [stats,  setStats]  = useState(null);
  const [blocks, setBlocks] = useState([]);
  const [txs,    setTxs]    = useState([]);
  // hashes added in the last 2 s — used to trigger the CSS flash animation
  const [newHashes, setNewHashes] = useState(new Set());
  const [features, setFeatures] = useState({ clob: false, verifier: false });
  const wsRef    = useRef(null);
  const queue    = useRef([]);
  const frame    = useRef(0);
  const alive    = useRef(true);
  const retry    = useRef(null);

  useEffect(() => {
    alive.current = true;
    connect();
    fetch(`${API_URL}/api/config`).then(r => r.json()).then(setFeatures).catch(() => {});
    // Stats come over the socket; poll only while it is down.
    const pollId = setInterval(async () => {
      if (wsRef.current?.readyState === WebSocket.OPEN) return;
      try {
        const r = await fetch(`${API_URL}/stats`);
        if (r.ok) setStats(await r.json());
      } catch {}
    }, 5000);
    return () => {
      alive.current = false;
      clearInterval(pollId);
      clearTimeout(retry.current);
      cancelAnimationFrame(frame.current);
      if (wsRef.current) wsRef.current.close();
    };
  }, []);

  function flush() {
    frame.current = 0;
    const msgs = queue.current;
    queue.current = [];
    let init = null, latestStats = null;
    const newBlocks = [], newTxs = [];
    for (const m of msgs) {
      if (m.type === 'init') { init = m.data; newBlocks.length = 0; newTxs.length = 0; }
      else if (m.type === 'stats') latestStats = m.data;
      else if (m.type === 'block') newBlocks.push({ ...m.data, _addedAt: Date.now() });
      else if (m.type === 'tx') newTxs.push({ ...m.data, _addedAt: Date.now() });
    }
    if (init) {
      setStats(init.stats);
      setBlocks((init.blocks ?? []).slice().sort((a, b) => Number(b.number) - Number(a.number)));
      setTxs(init.txs ?? []);
    }
    if (latestStats) setStats(latestStats);
    if (newBlocks.length) {
      setBlocks((prev) => {
        const map = new Map(prev.map(b => [b.hash, b]));
        for (const b of newBlocks) map.set(b.hash, b);
        return [...map.values()].sort((a, b) => Number(b.number) - Number(a.number)).slice(0, MAX_BLOCKS);
      });
      const hashes = newBlocks.map(b => b.hash);
      setNewHashes((prev) => new Set([...prev, ...hashes]));
      setTimeout(() => setNewHashes((prev) => {
        const next = new Set(prev);
        for (const h of hashes) next.delete(h);
        return next;
      }), 2000);
    }
    if (newTxs.length) {
      // the server sends txs oldest first; newest ends up on top
      setTxs((prev) => {
        const seen = new Set();
        return [...newTxs.reverse(), ...prev].filter(t => !seen.has(t.hash) && seen.add(t.hash)).slice(0, MAX_TXS);
      });
    }
  }

  function connect() {
    if (!alive.current) return;
    let ws;
    try { ws = new WebSocket(WS_URL); } catch { retry.current = setTimeout(connect, 3000); return; }
    wsRef.current = ws;
    ws.onopen = () => setWsOk(true);
    ws.onclose = () => {
      setWsOk(false);
      if (alive.current) retry.current = setTimeout(connect, 3000);
    };
    ws.onerror = () => ws.close();
    ws.onmessage = (e) => {
      try { queue.current.push(JSON.parse(e.data)); } catch { return; }
      if (!frame.current) frame.current = requestAnimationFrame(flush);
    };
  }

  return (
    <WSCtx.Provider value={{ wsOk, stats, blocks, txs, newHashes, features }}>
      {children}
    </WSCtx.Provider>
  );
}

export const useWS = () => useContext(WSCtx);
