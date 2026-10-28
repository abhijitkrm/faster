import { Routes, Route, Link, useLocation } from 'react-router-dom';
import SearchBar from './components/SearchBar';
import Home from './pages/Home';
import Blocks from './pages/Blocks';
import BlockDetail from './pages/BlockDetail';
import TxDetail    from './pages/TxDetail';
import Transactions from './pages/Transactions';
import Accounts from './pages/Accounts';
import Tokens from './pages/Tokens';
import Token from './pages/Token';
import Address from './pages/Address';
import ClobOrder from './pages/ClobOrder';
import ClobTrader from './pages/ClobTrader';
import ClobPair from './pages/ClobPair';
import { useWS } from './context/WSContext';

export function FasterMark({ size = 22 }) {
  return (
    <svg width={size} height={size} viewBox="0 0 22 22" aria-hidden="true">
      <path d="M3 4l7 7-7 7M11 4l7 7-7 7" fill="none" stroke="var(--accent)" strokeWidth="2.6" strokeLinecap="square" />
    </svg>
  );
}

const NAV = [
  ['/', 'Overview'],
  ['/blocks', 'Blocks'],
  ['/transactions', 'Transactions'],
  ['/tokens', 'Tokens'],
  ['/accounts', 'Accounts'],
];

export default function App() {
  const { wsOk } = useWS();
  const location = useLocation();
  const active = (p) => (p === '/' ? location.pathname === '/' : location.pathname.startsWith(p)) ? ' active' : '';

  return (
    <div className="app-wrapper">
      <header className="site-header">
        <div className="container">
          <div className="header-inner">
            <Link to="/" className="logo" aria-label="Faster home"><FasterMark />faster</Link>
            <nav className="nav" aria-label="Main">
              {NAV.map(([to, label]) => (
                <Link key={to} to={to} className={'nav-link' + active(to)}>{label}</Link>
              ))}
            </nav>
            <SearchBar />
            <span className="live-pill" role="status">
              <span className="live-dot" style={{ background: wsOk ? 'var(--accent)' : 'var(--text-3)' }} />
              {wsOk ? 'live' : 'connecting…'}
            </span>
          </div>
        </div>
      </header>

      <main className="main-content">
        <div className="container">
          <Routes>
            <Route path="/"                    element={<Home />} />
            <Route path="/blocks"              element={<Blocks />} />
            <Route path="/blocks/:number"      element={<BlockDetail />} />
            <Route path="/transactions"        element={<Transactions />} />
            <Route path="/tx/:hash"            element={<TxDetail />} />
            <Route path="/address/:address"    element={<Address />} />
            <Route path="/clob/order/:hash"    element={<ClobOrder />} />
            <Route path="/clob/trader/:addr"   element={<ClobTrader />} />
            <Route path="/clob/pair/:id"       element={<ClobPair />} />
            <Route path="/accounts"            element={<Accounts />} />
            <Route path="/tokens"              element={<Tokens />} />
            <Route path="/token/:address"      element={<Token />} />
            <Route path="*"                    element={<div className="empty" style={{ paddingTop: 60 }}>Page not found.</div>} />
          </Routes>
        </div>
      </main>

      <footer className="site-footer">
        <div className="container">
          <span>faster · served from memory</span>
          <span><a href="/api?module=proxy&action=eth_blockNumber">Etherscan-compatible API</a> · <a href="/ready">status</a></span>
        </div>
      </footer>
    </div>
  );
}

