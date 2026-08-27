import { Link } from 'react-router-dom';

const shortHash = (h) => h ? h.slice(0,10) + '\u2026' + h.slice(-6) : '';
const shortAddr = (a) => a ? a.slice(0,6)  + '\u2026' + a.slice(-4)  : 'Contract';
const fmtVal    = (v) => { try { return (Number(BigInt(v)) / 1e18).toFixed(4); } catch { return '0.0000'; } };

export default function TransactionList({ transactions }) {
  if (!transactions || transactions.length === 0) return <div className="empty">No transactions</div>;
  return (
    <table className="dt">
      <thead><tr>
        <th>Hash</th><th>Block</th><th>From</th><th>To</th>
        <th style={{textAlign:'right'}}>Value</th>
        <th style={{textAlign:'right'}}>Status</th>
      </tr></thead>
      <tbody>
        {transactions.map(tx => (
          <tr key={tx.hash}>
            <td style={{fontSize:11}}><Link to={`/tx/${tx.hash}`} className="hash-link"><span className="hash-cell">{shortHash(tx.hash)}</span></Link></td>
            <td><Link to={`/blocks/${tx.block_number}`} className="num-link">#{Number(tx.block_number).toLocaleString()}</Link></td>
            <td>
              <Link to={'/address/' + tx.from_address} className="addr-link">
                <span className="addr-cell">{shortAddr(tx.from_address)}</span>
              </Link>
            </td>
            <td>
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
  );
}
