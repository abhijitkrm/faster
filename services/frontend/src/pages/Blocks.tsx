import { useEffect, useState } from 'react';
import BlockList from '../components/BlockList';
import { useWS } from '../context/WSContext';

export default function Blocks() {
  const { blocks, newHashes } = useWS();
  // tick every second so ages stay live
  const [, setTick] = useState(0);
  useEffect(() => {
    const iv = setInterval(() => setTick((t) => t + 1), 1000);
    return () => clearInterval(iv);
  }, []);

  return (
    <div>
      <div className="page-head">
        <span className="page-title">Blocks</span>
        <span className="page-count">{blocks.length} shown</span>
      </div>
      <div className="panel">
        <BlockList blocks={blocks} newHashes={newHashes} />
      </div>
    </div>
  );
}
