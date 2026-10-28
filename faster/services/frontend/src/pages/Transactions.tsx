import { useEffect, useState } from 'react';
import TransactionList from '../components/TransactionList';

import { API } from '../config';

export default function Transactions() {
  const [txs, setTxs] = useState([]);

  useEffect(() => {
    load();
    const iv = setInterval(load, 5000);
    return () => clearInterval(iv);
  }, []);

  const load = async () => {
    try { setTxs(await (await fetch(API + '/api/transactions?limit=50')).json()); } catch {}
  };

  return (
    <div>
      <div className="page-head">
        <span className="page-title">Transactions</span>
        <span className="page-count">{txs.length} shown</span>
      </div>
      <div className="panel">
        <TransactionList transactions={txs} />
      </div>
    </div>
  );
}
