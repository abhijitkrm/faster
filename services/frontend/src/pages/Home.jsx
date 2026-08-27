import { Link } from 'react-router-dom';
import { useWS } from '../context/WSContext';

// _addedAt is a client-side ms timestamp set by WSContext when a block arrives over WS.
// For those fresh blocks we can show sub-second precision.
// For historical blocks (init payload) we use the Unix second timestamp.
const timeAgo = (ts, addedAt) => {
  if (addedAt) {
    const ms = Date.now() - addedAt;
    if (ms < 1000) return ms + 'ms ago';
    const s = Math.floor(ms / 1000);
    if (s < 60)   return s + 's ago';
    if (s < 3600) return Math.floor(s / 60) + 'm ago';
    return Math.floor(s / 3600) + 'h ago';
  }
  const s = Math.floor(Date.now() / 1000) - parseInt(ts);
  if (s < 1)    return 'just now';
  if (s < 60)   return s + 's ago';
  if (s < 3600) return Math.floor(s / 60) + 'm ago';
  return Math.floor(s / 3600) + 'h ago';
};

const shortAddr = (a) => a ? a.slice(0, 6) + '\u2026' + a.slice(-4) : 'Contract';
const shortHash = (h) => h ? h.slice(0, 8) + '\u2026' + h.slice(-6) : '';
const fmtVal    = (v) => { try { return (Number(BigInt(v)) / 1e18).toFixed(4); } catch { return '0.0000'; } };

const ROWS = 20;

export default function Home() {
  const { blocks, txs, newHashes } = useWS();

  const latestBlocks = blocks.slice(0, ROWS);
  const latestTxs    = txs.slice(0, ROWS);

  return (
    <div className="home-grid">
      {/* Latest Blocks */}
      <div className="panel">
        <div className="panel-head">
          <span className="panel-title">Latest Blocks</span>
          <Link to="/blocks" className="panel-link">View all &rarr;</Link>
        </div>
        {latestBlocks.length === 0
          ? <div className="empty">No blocks yet&hellip;</div>
          : (
            <table className="dt">
              <thead><tr>
                <th>Block</th><th>Miner</th>
                <th style={{textAlign:'right'}}>Txns</th>
                <th style={{textAlign:'right'}}>Age</th>
              </tr></thead>
              <tbody>
                {latestBlocks.map(b => (
                  <tr key={b.hash} className={newHashes.has(b.hash) ? 'is-new' : ''}>
                    <td><Link to={`/blocks/${b.number}`} className="num-link">#{Number(b.number).toLocaleString()}</Link></td>
                    <td>
                      <Link to={'/address/' + b.miner} className="addr-link">
                        <span className="addr-cell">{shortAddr(b.miner)}</span>
                      </Link>
                    </td>
                    <td style={{textAlign:'right',color: b.transactions_count > 0 ? 'var(--green)' : 'var(--text-3)'}}>{b.transactions_count}</td>
                    <td style={{textAlign:'right',color:'var(--text-3)'}}><span style={{display:'inline-block',minWidth:'74px',textAlign:'right'}}>{timeAgo(b.timestamp, b._addedAt)}</span></td>
                  </tr>
                ))}
              </tbody>
            </table>
          )
        }
      </div>

      {/* Latest Transactions */}
      <div className="panel">
        <div className="panel-head">
          <span className="panel-title">Latest Transactions</span>
          <Link to="/transactions" className="panel-link">View all &rarr;</Link>
        </div>
        {latestTxs.length === 0
          ? <div className="empty">No transactions yet&hellip;</div>
          : (
            <table className="dt">
              <thead><tr>
                <th>Hash</th><th>From &rarr; To</th>
                <th style={{textAlign:'right'}}>Value</th>
                <th style={{textAlign:'right'}}>Status</th>
              </tr></thead>
              <tbody>
                {latestTxs.map(tx => (
                  <tr key={tx.hash}>
                    <td style={{fontSize:11}}>
                      <Link to={`/tx/${tx.hash}`} className="hash-link">
                        <span className="addr-cell">{shortHash(tx.hash)}</span>
                      </Link>
                    </td>
                    <td style={{fontSize:11}}>
                      <Link to={'/address/' + tx.from_address} className="addr-link">
                        <span className="addr-cell">{shortAddr(tx.from_address)}</span>
                      </Link>
                      <span style={{margin:'0 4px',color:'var(--text-3)'}}>&rarr;</span>
                      <Link to={'/address/' + tx.to_address} className="addr-link">
                        <span className="addr-cell">{shortAddr(tx.to_address)}</span>
                      </Link>
                    </td>
                    <td style={{textAlign:'right'}}>{fmtVal(tx.value)}</td>
                    <td style={{textAlign:'right'}}>
                      {tx.status === 1   && <span className="badge badge-ok">OK</span>}
                      {tx.status === 0   && <span className="badge badge-fail">Fail</span>}
                      {tx.status == null && <span className="badge badge-pend">Pend</span>}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )
        }
      </div>
    </div>
  );
}
