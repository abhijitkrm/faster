import { useState, useEffect, useRef } from 'react';
import { Routes, Route, Link, useLocation } from 'react-router-dom';
import SearchBar from './components/SearchBar';
import Home from './pages/Home';
import Blocks from './pages/Blocks';
import BlockDetail from './pages/BlockDetail';
import TxDetail    from './pages/TxDetail';
import Transactions from './pages/Transactions';
import Address from './pages/Address';
import { useWS } from './context/WSContext';

const fmtNum = (n) => (n == null ? '—' : Number(n).toLocaleString());

// Smoothly counts integers up (or down) one step at a time.
// Snaps immediately if the gap exceeds `snapAt` to avoid falling behind.
function useCountUp(target, snapAt = 50) {
  const [display, setDisplay] = useState(target ?? 0);
  const s = useRef({ cur: target ?? 0, target: target ?? 0, raf: null, lastStep: 0 });

  useEffect(() => {
    if (target == null) return;
    s.current.target = Math.round(target);

    const animate = (now) => {
      const st = s.current;
      const diff = st.target - st.cur;
      if (diff === 0) return;

      // Too far behind — snap so we don't lag indefinitely
      if (Math.abs(diff) > snapAt) {
        st.cur = st.target;
        setDisplay(st.target);
        return;
      }

      // Throttle to ~25 steps / sec (one step every 40 ms)
      if (now - st.lastStep >= 40) {
        st.cur += Math.sign(diff);
        st.lastStep = now;
        setDisplay(st.cur);
      }

      if (st.cur !== st.target) {
        st.raf = requestAnimationFrame(animate);
      }
    };

    if (s.current.raf) cancelAnimationFrame(s.current.raf);
    s.current.raf = requestAnimationFrame(animate);
    return () => { if (s.current.raf) cancelAnimationFrame(s.current.raf); };
  }, [target, snapAt]);

  return display;
}

// Lerps a float value toward the target each frame — gives a smooth slide feel.
function useLerp(target, speed = 0.10) {
  const [display, setDisplay] = useState(target ?? 0);
  const s = useRef({ cur: parseFloat(target) || 0, raf: null });

  useEffect(() => {
    if (target == null) return;
    const tgt = parseFloat(target) || 0;

    const animate = () => {
      s.current.cur += (tgt - s.current.cur) * speed;
      if (Math.abs(s.current.cur - tgt) < 0.001) {
        s.current.cur = tgt;
        setDisplay(tgt);
        return;
      }
      setDisplay(s.current.cur);
      s.current.raf = requestAnimationFrame(animate);
    };

    if (s.current.raf) cancelAnimationFrame(s.current.raf);
    s.current.raf = requestAnimationFrame(animate);
    return () => { if (s.current.raf) cancelAnimationFrame(s.current.raf); };
  }, [target, speed]);

  return display;
}

export default function App() {
  const { wsOk, stats } = useWS();
  const location = useLocation();
  const [theme, setTheme] = useState(() => localStorage.getItem('theme') || 'dark');

  useEffect(() => {
    document.documentElement.dataset.theme = theme === 'light' ? 'light' : '';
    localStorage.setItem('theme', theme);
  }, [theme]);

  // Animated metric values
  const animHeight      = useCountUp(stats?.block_height ?? null);
  const animTotalBlocks = useCountUp(stats?.total_blocks  ?? null);
  const animTps         = useLerp(stats?.tps         ?? null);
  const animBlockTime   = useLerp(stats?.avg_block_time ?? null);

  const va = (p) => location.pathname === p ? ' active' : '';
  const toggleTheme = () => setTheme((t) => t === 'dark' ? 'light' : 'dark');

  return (
    <div className="app-wrapper">
      <header className="site-header">
        <div className="container">
          <div className="header-inner">
            <div style={{ display: 'flex', alignItems: 'center', gap: 16, flexShrink: 0 }}>
              <Link to="/" className="logo">
                <span className="logo-hex">&#x2B21;</span>
                EVM Explorer
              </Link>
              <span className="live-pill">
                <span className="live-dot" style={{ background: wsOk ? 'var(--accent)' : 'var(--text-3)' }} />
                {wsOk ? 'LIVE' : 'CONNECTING'}
              </span>
            </div>
            <SearchBar />
            <nav className="nav">
              <Link to="/"             className={'nav-link' + va('/')}>Home</Link>
              <Link to="/blocks"       className={'nav-link' + va('/blocks')}>Blocks</Link>
              <Link to="/transactions" className={'nav-link' + va('/transactions')}>Txns</Link>
              <button
                onClick={toggleTheme}
                title="Toggle light / dark"
                style={{
                  background:'none', border:'1px solid var(--border-hi)', borderRadius:6,
                  color:'var(--text-2)', cursor:'pointer', fontSize:13, lineHeight:1,
                  padding:'4px 9px', marginLeft:6, transition:'all .15s'
                }}
              >{theme === 'dark' ? '☀' : '☾'}</button>
            </nav>
          </div>
        </div>
      </header>

      {stats && (
        <div className="metrics-bar">
          <div className="container">
            <div className="metrics-grid">
              <div className="metric-card">
                <div className="metric-label">Block Height</div>
                <div className="metric-value blue">{fmtNum(animHeight)}</div>
                <div className="metric-sub">Latest indexed</div>
              </div>
              <div className="metric-card">
                <div className="metric-label">Avg Block Time</div>
                <div className="metric-value green">
                  {stats.avg_block_time ? animBlockTime.toFixed(3) + 's' : '—'}
                </div>
                <div className="metric-sub">Last 100 blocks</div>
              </div>
              <div className="metric-card">
                <div className="metric-label">TPS</div>
                <div className="metric-value purple">{animTps.toFixed(2)}</div>
                <div className="metric-sub">Transactions / sec</div>
              </div>
              <div className="metric-card">
                <div className="metric-label">Total Blocks</div>
                <div className="metric-value">{fmtNum(animTotalBlocks)}</div>
                <div className="metric-sub">Indexed</div>
              </div>
            </div>
          </div>
        </div>
      )}

      <main className="main-content">
        <div className="container">
          <Routes>
            <Route path="/"                    element={<Home />} />
            <Route path="/blocks"              element={<Blocks />} />
            <Route path="/blocks/:number"      element={<BlockDetail />} />
            <Route path="/transactions"        element={<Transactions />} />
            <Route path="/tx/:hash"            element={<TxDetail />} />
            <Route path="/address/:address"    element={<Address />} />
          </Routes>
        </div>
      </main>

      <footer className="site-footer">
        EVM Block Explorer &nbsp;&middot;&nbsp; Real-time indexed data
      </footer>
    </div>
  );
}

