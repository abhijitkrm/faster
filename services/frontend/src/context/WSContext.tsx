import { createContext, useContext, useEffect, useRef, useState } from 'react';

const WS_URL  = (import.meta.env.VITE_API_URL || 'http://localhost:4000').replace(/^http/, 'ws');
const API_URL  = import.meta.env.VITE_API_URL || 'http://localhost:4000';
const MAX_BLOCKS = 50;
const MAX_TXS    = 20;

const WSCtx = createContext(null);

export function WSProvider({ children }) {
  const [wsOk,   setWsOk]   = useState(false);
  const [stats,  setStats]  = useState(null);
  const [blocks, setBlocks] = useState([]);
  const [txs,    setTxs]    = useState([]);
  // hashes added in the last N ms — used to trigger CSS flash animation
  const [newHashes, setNewHashes] = useState(new Set());
  const wsRef = useRef(null);

  useEffect(() => {
    connect();

    // Fallback: poll /stats every 5 s so metrics never go stale
    // (covers the case where no new blocks arrive to trigger a WS stats push)
    const pollId = setInterval(async () => {
      try {
        const r = await fetch(`${API_URL}/stats`);
        if (r.ok) setStats(await r.json());
      } catch {}
    }, 5000);

    return () => {
      clearInterval(pollId);
      if (wsRef.current) wsRef.current.close();
    };
  }, []);

  function connect() {
    try {
      const ws = new WebSocket(WS_URL);
      wsRef.current = ws;

      ws.onopen = () => setWsOk(true);

      ws.onclose = () => {
        setWsOk(false);
        // auto-reconnect after 3 s
        setTimeout(connect, 3000);
      };

      ws.onerror = () => ws.close();

      ws.onmessage = (e) => {
        try {
          const msg = JSON.parse(e.data);
          switch (msg.type) {

            case 'init': {
              setStats(msg.data.stats);
              const initBlocks = (msg.data.blocks ?? [])
                .slice().sort((a, b) => Number(b.number) - Number(a.number));
              setBlocks(initBlocks);
              setTxs(msg.data.txs ?? []);
              break;
            }

            case 'stats': {
              setStats(msg.data);
              break;
            }

            case 'block': {
              // tag the arriving block with a client-side timestamp
              const blk = { ...msg.data, _addedAt: Date.now() };
              setBlocks((prev) => {
                // deduplicate then sort descending by block number
                const map = new Map(prev.map(b => [b.hash, b]));
                map.set(blk.hash, blk);
                return [...map.values()]
                  .sort((a, b) => Number(b.number) - Number(a.number))
                  .slice(0, MAX_BLOCKS);
              });

              // mark hash as "new" for the flash animation, auto-clear after 2 s
              setNewHashes((prev) => {
                const next = new Set(prev);
                next.add(blk.hash);
                return next;
              });
              setTimeout(() => {
                setNewHashes((prev) => {
                  const next = new Set(prev);
                  next.delete(blk.hash);
                  return next;
                });
              }, 2000);
              break;
            }

            case 'tx': {
              const tx = { ...msg.data, _addedAt: Date.now() };
              setTxs((prev) => [tx, ...prev].slice(0, MAX_TXS));
              break;
            }

            default:
              break;
          }
        } catch {}
      };
    } catch {}
  }

  return (
    <WSCtx.Provider value={{ wsOk, stats, blocks, txs, newHashes }}>
      {children}
    </WSCtx.Provider>
  );
}

export const useWS = () => useContext(WSCtx);
