import { useEffect, useState } from 'react';
import { API } from '../config';

// Verified source + ABI, or a verification form for unverified contracts.
export default function ContractPanel({ address, verified }) {
  const [c, setC] = useState(null);
  const [open, setOpen] = useState(false);
  const [form, setForm] = useState({ compiler_version: '', contract_name: '', standard_json: '' });
  const [status, setStatus] = useState(null);

  useEffect(() => {
    setC(null); setStatus(null);
    if (verified) fetch(`${API}/api/contracts/${address}`).then(r => r.json()).then(setC).catch(() => {});
  }, [address, verified]);

  const submit = async (e) => {
    e.preventDefault();
    setStatus({ busy: true });
    try {
      let sj;
      try { sj = JSON.parse(form.standard_json); } catch { setStatus({ error: 'Standard JSON input is not valid JSON' }); return; }
      const r = await fetch(`${API}/api/contracts/${address}/verify`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ ...form, standard_json: sj }),
      });
      const d = await r.json();
      if (d.status === 'verified') { setStatus({ ok: `Verified (${d.match_type} match)` }); setTimeout(() => window.location.reload(), 800); }
      else setStatus({ error: d.error || 'Verification failed' });
    } catch { setStatus({ error: 'Network error' }); }
  };

  const box = { width: '100%', background: 'var(--surface-2)', color: 'var(--text)', border: '1px solid var(--border)', borderRadius: 6, padding: 8, fontFamily: 'var(--mono)', fontSize: 12 };

  if (verified && c) {
    return (
      <div className="panel" style={{ marginBottom: 18 }}>
        <div className="panel-head">
          <span className="panel-title">Contract Source</span>
          <span style={{ fontSize: 12, color: 'var(--green)' }}>Verified ({c.match_type} match) · {c.name} · {c.compiler}</span>
          <button className="nav-link" style={{ marginLeft: 'auto' }} onClick={() => setOpen(!open)}>{open ? 'Hide' : 'Show'}</button>
        </div>
        {open && (
          <div style={{ padding: '10px 20px' }}>
            {Object.entries(c.sources || {}).map(([file, src]: [string, any]) => (
              <div key={file} style={{ marginBottom: 14 }}>
                <div style={{ fontSize: 12, color: 'var(--text-3)', marginBottom: 4 }}>{file}</div>
                <pre style={{ ...box, maxHeight: 400, overflow: 'auto', whiteSpace: 'pre' }}>{src}</pre>
              </div>
            ))}
            <div style={{ fontSize: 12, color: 'var(--text-3)', marginBottom: 4 }}>ABI</div>
            <pre style={{ ...box, maxHeight: 240, overflow: 'auto' }}>{JSON.stringify(c.abi, null, 1)}</pre>
          </div>
        )}
      </div>
    );
  }
  if (verified) return null;
  return (
    <div className="panel" style={{ marginBottom: 18 }}>
      <div className="panel-head">
        <span className="panel-title">Contract not verified</span>
        <button className="nav-link" style={{ marginLeft: 'auto' }} onClick={() => setOpen(!open)}>{open ? 'Cancel' : 'Verify & publish'}</button>
      </div>
      {open && (
        <form onSubmit={submit} style={{ padding: '10px 20px', display: 'grid', gap: 8 }}>
          <input style={box} placeholder="Compiler version, e.g. v0.8.24+commit.e11b9ed9" value={form.compiler_version}
            onChange={e => setForm({ ...form, compiler_version: e.target.value })} required />
          <input style={box} placeholder="Contract, e.g. src/Token.sol:Token" value={form.contract_name}
            onChange={e => setForm({ ...form, contract_name: e.target.value })} required />
          <textarea style={{ ...box, minHeight: 160 }} placeholder="Solidity standard JSON input" value={form.standard_json}
            onChange={e => setForm({ ...form, standard_json: e.target.value })} required />
          <div style={{ fontSize: 12, color: 'var(--text-3)' }}>
            Or from Foundry: <code>forge verify-contract --verifier blockscout --verifier-url {API}/api &lt;address&gt; &lt;path:Name&gt;</code>
          </div>
          <button className="nav-link" type="submit" disabled={status?.busy}>{status?.busy ? 'Compiling…' : 'Verify'}</button>
          {status?.error && <div style={{ color: 'var(--red)', fontSize: 12 }}>{status.error}</div>}
          {status?.ok && <div style={{ color: 'var(--green)', fontSize: 12 }}>{status.ok}</div>}
        </form>
      )}
    </div>
  );
}
