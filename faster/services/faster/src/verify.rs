//! Solidity contract verification and ABI decoding.
//!
//! Verification compiles the submitted standard-JSON input with the exact
//! solc release (official binaries from binaries.soliditylang.org, checked
//! against the published sha256 and cached under DATA_DIR/solc), then
//! compares the runtime bytecode with the code on chain:
//! - immutables (positions from `immutableReferences`) are masked,
//! - unlinked library placeholders take the on-chain bytes,
//! - the CBOR metadata tail is compared separately: equal = "full" match,
//!   different = "partial" match (same code, different metadata/comments).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use alloy_dyn_abi::{DynSolValue, EventExt, JsonAbiExt};
use alloy_json_abi::JsonAbi;
use anyhow::{anyhow, bail, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::Digest;
use tokio::sync::Semaphore;

use crate::types::*;

const MAX_INPUT: usize = 20 << 20;
const COMPILE_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ContractRecord {
    pub name: String,
    pub file: String,
    pub compiler: String,
    /// "full" or "partial"
    pub match_type: String,
    pub abi: String,
    pub sources: HashMap<String, String>,
    pub settings: Value,
    pub constructor_args: String,
    pub verified_at: u64,
}

pub struct Verifier {
    dir: PathBuf,
    http: reqwest::Client,
    slots: Semaphore,
    /// Parsed ABIs of verified contracts, for decoding.
    abis: Mutex<HashMap<Addr, Option<Arc<JsonAbi>>>>,
    /// Etherscan-style async jobs: guid -> status text.
    pub jobs: Mutex<HashMap<String, String>>,
}

fn platform() -> &'static str {
    match (std::env::consts::OS, std::env::consts::ARCH) {
        ("macos", _) => "macosx-amd64",
        ("linux", "aarch64") => "linux-arm64",
        _ => "linux-amd64",
    }
}

/// "v0.8.24+commit.e11b9ed9" or "0.8.24+commit.e11b9ed9" -> long version.
fn long_version(v: &str) -> Result<String> {
    let v = v.trim().trim_start_matches('v');
    if v.is_empty() || v.len() > 64 || !v.chars().all(|c| c.is_ascii_alphanumeric() || ".+-".contains(c)) {
        bail!("invalid compiler version");
    }
    Ok(v.to_string())
}

/// Strips the CBOR metadata tail (its length is in the last two bytes).
fn split_metadata(code: &[u8]) -> (&[u8], &[u8]) {
    if code.len() < 2 {
        return (code, &[]);
    }
    let n = u16::from_be_bytes([code[code.len() - 2], code[code.len() - 1]]) as usize + 2;
    if n > code.len() || n < 2 {
        return (code, &[]);
    }
    code.split_at(code.len() - n)
}

/// Hex with library placeholders to bytes; placeholder bytes come from
/// `actual` at the same position. None if `actual` is too short.
fn unlink(compiled_hex: &str, actual: &[u8]) -> Option<Vec<u8>> {
    let b = compiled_hex.as_bytes();
    if b.len() % 2 != 0 || actual.len() * 2 < b.len() {
        return None;
    }
    let mut out = Vec::with_capacity(b.len() / 2);
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'_' {
            let end = (i + 40).min(b.len());
            out.extend_from_slice(&actual[i / 2..end / 2]);
            i = end;
            continue;
        }
        out.push(u8::from_str_radix(std::str::from_utf8(&b[i..i + 2]).ok()?, 16).ok()?);
        i += 2;
    }
    Some(out)
}

/// Compares compiled creation code with a creation tx's input. The runtime
/// metadata tail (embedded in the creation code) may differ for a partial
/// match. Returns the match and the constructor arguments that follow.
pub fn compare_creation(compiled_hex: &str, runtime_hex: &str, input: &[u8]) -> Option<(&'static str, Vec<u8>)> {
    let compiled = unlink(compiled_hex, input)?;
    let args = input[compiled.len()..].to_vec();
    let actual = &input[..compiled.len()];
    if compiled == actual {
        return Some(("full", args));
    }
    let runtime = unlink(runtime_hex, &vec![0u8; runtime_hex.len() / 2]).unwrap_or_default();
    let (_, meta) = split_metadata(&runtime);
    if meta.len() > 2 {
        let body = &meta[..meta.len() - 2];
        if let Some(p) = compiled.windows(body.len()).position(|w| w == body) {
            let mut masked = actual.to_vec();
            masked[p..p + body.len()].copy_from_slice(body);
            if masked == compiled {
                return Some(("partial", args));
            }
        }
    }
    None
}

/// Compares compiled runtime code (hex, maybe with library placeholders)
/// with on-chain code. Returns "full", "partial" or None.
pub fn compare(compiled_hex: &str, immutables: &Value, onchain: &[u8]) -> Option<&'static str> {
    if compiled_hex.len() != onchain.len() * 2 {
        return None;
    }
    // Library placeholders "__$...$__" (40 hex chars) take the on-chain bytes.
    let mut compiled = Vec::with_capacity(onchain.len());
    let b = compiled_hex.as_bytes();
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'_' {
            let end = (i + 40).min(b.len());
            compiled.extend_from_slice(&onchain[i / 2..end / 2]);
            i = end;
            continue;
        }
        compiled.push(u8::from_str_radix(std::str::from_utf8(&b[i..i + 2]).ok()?, 16).ok()?);
        i += 2;
    }
    let mut chain = onchain.to_vec();
    if let Some(refs) = immutables.as_object() {
        for r in refs.values().filter_map(|v| v.as_array()).flatten() {
            let (Some(s), Some(l)) = (r["start"].as_u64(), r["length"].as_u64()) else { continue };
            let (s, l) = (s as usize, l as usize);
            if s + l <= chain.len() {
                chain[s..s + l].copy_from_slice(&compiled[s..s + l]);
            }
        }
    }
    if compiled == chain {
        return Some("full");
    }
    let (a, _) = split_metadata(&compiled);
    let (c, _) = split_metadata(&chain);
    (a == c && !a.is_empty()).then_some("partial")
}

impl Verifier {
    pub fn new(dir: PathBuf) -> Arc<Verifier> {
        Arc::new(Verifier {
            dir,
            http: reqwest::Client::builder().timeout(Duration::from_secs(300)).build().expect("http client"),
            slots: Semaphore::new(2),
            abis: Mutex::default(),
            jobs: Mutex::default(),
        })
    }

    /// Path to a verified solc binary for `version`, downloading it if needed.
    async fn solc(&self, version: &str) -> Result<PathBuf> {
        let plat = platform();
        let bin = self.dir.join(format!("solc-{plat}-v{version}"));
        if bin.exists() {
            return Ok(bin);
        }
        tokio::fs::create_dir_all(&self.dir).await?;
        let base = format!("https://binaries.soliditylang.org/{plat}");
        let list: Value = self.http.get(format!("{base}/list.json")).send().await?.json().await?;
        let build = list["builds"]
            .as_array()
            .and_then(|b| b.iter().find(|x| x["longVersion"].as_str() == Some(version)))
            .ok_or_else(|| anyhow!("unknown compiler version v{version}"))?;
        let path = build["path"].as_str().ok_or_else(|| anyhow!("bad compiler list"))?;
        let want = build["sha256"].as_str().unwrap_or("").trim_start_matches("0x").to_string();
        let bytes = self.http.get(format!("{base}/{path}")).send().await?.error_for_status()?.bytes().await?;
        let got = hex::encode(sha2::Sha256::digest(&bytes));
        if want.is_empty() || got != want {
            bail!("compiler checksum mismatch for v{version}");
        }
        let tmp = bin.with_extension("tmp");
        tokio::fs::write(&tmp, &bytes).await?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            tokio::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o755)).await?;
        }
        tokio::fs::rename(&tmp, &bin).await?;
        Ok(bin)
    }

    async fn compile(&self, solc: &Path, input: &Value) -> Result<Value> {
        let _slot = self.slots.acquire().await?;
        let mut cmd = tokio::process::Command::new(solc);
        cmd.arg("--standard-json")
            .env_clear()
            .current_dir(std::env::temp_dir())
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true);
        let mut child = cmd.spawn().context("start solc")?;
        let body = serde_json::to_vec(input)?;
        let mut stdin = child.stdin.take().unwrap();
        tokio::spawn(async move {
            use tokio::io::AsyncWriteExt;
            let _ = stdin.write_all(&body).await;
        });
        let out = tokio::time::timeout(COMPILE_TIMEOUT, child.wait_with_output())
            .await
            .map_err(|_| anyhow!("compilation timed out"))??;
        serde_json::from_slice(&out.stdout).context("solc output")
    }

    /// Verifies `addr` against standard-JSON input. `contract` is
    /// "path:Name" or "Name".
    /// `creation_input`: input of the tx that deployed the contract, when
    /// known (EOA deployments); the creation code must then match too, so
    /// constructor-only differences cannot verify.
    pub async fn verify(
        &self,
        onchain: &[u8],
        creation_input: Option<&[u8]>,
        compiler: &str,
        contract: &str,
        mut input: Value,
        constructor_args: &str,
    ) -> Result<ContractRecord> {
        if onchain.is_empty() {
            bail!("no contract code at this address");
        }
        let version = long_version(compiler)?;
        if serde_json::to_vec(&input)?.len() > MAX_INPUT {
            bail!("input too large");
        }
        if input["language"].as_str().unwrap_or("Solidity") != "Solidity" {
            bail!("only Solidity is supported");
        }
        input["settings"]["outputSelection"] = json!({"*": {"*": [
            "abi", "evm.bytecode.object", "evm.deployedBytecode.object", "evm.deployedBytecode.immutableReferences"
        ]}});
        let solc = self.solc(&version).await?;
        let out = self.compile(&solc, &input).await?;
        if let Some(errs) = out["errors"].as_array() {
            if let Some(e) = errs.iter().find(|e| e["severity"] == "error") {
                bail!(
                    "compilation failed: {}",
                    e["formattedMessage"].as_str().unwrap_or("error").chars().take(500).collect::<String>()
                );
            }
        }
        let (want_file, want_name) = match contract.rsplit_once(':') {
            Some((f, n)) => (Some(f), n),
            None => (None, contract),
        };
        let contracts = out["contracts"].as_object().ok_or_else(|| anyhow!("no contracts in output"))?;
        for (file, cs) in contracts {
            if want_file.is_some_and(|f| f != file) {
                continue;
            }
            let Some(c) = cs.get(want_name) else { continue };
            let code = c["evm"]["deployedBytecode"]["object"].as_str().unwrap_or("");
            let runtime = compare(code, &c["evm"]["deployedBytecode"]["immutableReferences"], onchain);
            let (m, ctor_args) = match (runtime, creation_input) {
                (None, _) => (None, constructor_args.trim_start_matches("0x").to_string()),
                (Some(r), Some(input)) => {
                    match compare_creation(c["evm"]["bytecode"]["object"].as_str().unwrap_or(""), code, input) {
                        // Both must agree on "full"; anything else is partial.
                        Some((cm, args)) => {
                            (Some(if r == "full" && cm == "full" { "full" } else { "partial" }), hex::encode(args))
                        }
                        None => bail!(
                            "runtime code matches but the creation code does not (constructor or initializer differs)"
                        ),
                    }
                }
                // Deployed by another contract: runtime-only match.
                (Some(_), None) => (Some("partial"), constructor_args.trim_start_matches("0x").to_string()),
            };
            if let Some(m) = m {
                let sources = input["sources"]
                    .as_object()
                    .map(|o| {
                        o.iter().map(|(k, v)| (k.clone(), v["content"].as_str().unwrap_or("").to_string())).collect()
                    })
                    .unwrap_or_default();
                let mut settings = input["settings"].clone();
                settings.as_object_mut().map(|o| o.remove("outputSelection"));
                return Ok(ContractRecord {
                    name: want_name.to_string(),
                    file: file.clone(),
                    compiler: format!("v{version}"),
                    match_type: m.to_string(),
                    abi: c["abi"].to_string(),
                    sources,
                    settings,
                    constructor_args: ctor_args,
                    verified_at: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_secs())
                        .unwrap_or(0),
                });
            }
            bail!("compiled bytecode of {file}:{want_name} does not match the code on chain");
        }
        bail!("contract {contract} not found in the compiler output")
    }

    // ---- decoding ----

    pub fn abi(&self, store: &crate::store::Store, a: &Addr) -> Option<Arc<JsonAbi>> {
        if let Some(v) = self.abis.lock().unwrap().get(a) {
            return v.clone();
        }
        let parsed =
            store.contract(a).ok().flatten().and_then(|r| serde_json::from_str::<JsonAbi>(&r.abi).ok()).map(Arc::new);
        let mut m = self.abis.lock().unwrap();
        if m.len() > 10_000 {
            m.clear();
        }
        m.insert(*a, parsed.clone());
        parsed
    }

    pub fn forget(&self, a: &Addr) {
        self.abis.lock().unwrap().remove(a);
    }
}

fn value_json(v: &DynSolValue) -> Value {
    match v {
        DynSolValue::Bool(b) => json!(b),
        DynSolValue::Int(i, _) => json!(i.to_string()),
        DynSolValue::Uint(u, _) => json!(u.to_string()),
        DynSolValue::Address(a) => json!(format!("{a:#x}")),
        DynSolValue::FixedBytes(w, n) => json!(hex0x(&w[..*n])),
        DynSolValue::Bytes(b) => json!(hex0x(b)),
        DynSolValue::String(s) => json!(s),
        DynSolValue::Array(xs) | DynSolValue::FixedArray(xs) | DynSolValue::Tuple(xs) => {
            Value::Array(xs.iter().map(value_json).collect())
        }
        DynSolValue::Function(f) => json!(hex0x(f.as_slice())),
    }
}

/// Decodes a call's input with the target's ABI.
pub fn decode_input(abi: &JsonAbi, input: &[u8]) -> Option<Value> {
    if input.len() < 4 {
        return None;
    }
    let f = abi.functions().find(|f| f.selector().as_slice() == &input[..4])?;
    let vals = f.abi_decode_input(&input[4..]).ok()?;
    let params: Vec<Value> = f
        .inputs
        .iter()
        .zip(&vals)
        .map(|(p, v)| json!({"name": p.name, "type": p.ty, "value": value_json(v)}))
        .collect();
    Some(json!({"name": f.name, "signature": f.signature(), "params": params}))
}

/// Decodes a log with the emitter's ABI.
pub fn decode_log(abi: &JsonAbi, topics: &[H256], data: &[u8]) -> Option<Value> {
    let t0 = topics.first()?;
    let ev = abi.events().find(|e| !e.anonymous && e.selector().as_slice() == t0)?;
    let topics: Vec<alloy_primitives::B256> = topics.iter().map(|t| alloy_primitives::B256::from(*t)).collect();
    let d = ev.decode_log_parts(topics, data).ok()?;
    let (mut ix, mut bx) = (d.indexed.into_iter(), d.body.into_iter());
    let params: Vec<Value> = ev
        .inputs
        .iter()
        .filter_map(|p| {
            let v = if p.indexed { ix.next()? } else { bx.next()? };
            Some(json!({"name": p.name, "type": p.ty, "indexed": p.indexed, "value": value_json(&v)}))
        })
        .collect();
    Some(json!({"name": ev.name, "signature": ev.signature(), "params": params}))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bytecode_comparison() {
        // code ‖ metadata(2 bytes) ‖ len(0x0002)
        let compiled = "6001600200aabb0002";
        let same = hex::decode("6001600200aabb0002").unwrap();
        let meta_diff = hex::decode("6001600200ccdd0002").unwrap();
        let code_diff = hex::decode("6001600300aabb0002").unwrap();
        assert_eq!(compare(compiled, &Value::Null, &same), Some("full"));
        assert_eq!(compare(compiled, &Value::Null, &meta_diff), Some("partial"));
        assert_eq!(compare(compiled, &Value::Null, &code_diff), None);
        // immutable at byte 1..2 is masked
        let imm = json!({"7": [{"start": 1, "length": 1}]});
        let with_imm = hex::decode("60ff600200aabb0002").unwrap();
        assert_eq!(compare(compiled, &imm, &with_imm), Some("full"));
        // library placeholder takes the on-chain bytes
        let lib = format!("73{}00", "__$aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa$__");
        let chain = hex::decode(format!("73{}00", "11".repeat(20))).unwrap();
        assert!(compare(&lib, &Value::Null, &chain).is_some());
        assert!(long_version("v0.8.24+commit.e11b9ed9").is_ok());
        assert!(long_version("../../etc").is_err());
    }

    #[test]
    fn decodes_with_abi() {
        let abi: JsonAbi = serde_json::from_str(
            r#"[{"type":"function","name":"transfer","inputs":[{"name":"to","type":"address"},{"name":"v","type":"uint256"}],"outputs":[],"stateMutability":"nonpayable"},
                {"type":"event","name":"Transfer","anonymous":false,"inputs":[{"name":"from","type":"address","indexed":true},{"name":"to","type":"address","indexed":true},{"name":"value","type":"uint256","indexed":false}]}]"#,
        )
        .unwrap();
        let mut input = hex::decode("a9059cbb").unwrap();
        input.extend([0u8; 12]);
        input.extend([0xab; 20]);
        input.extend({
            let mut w = [0u8; 32];
            w[31] = 7;
            w
        });
        let d = decode_input(&abi, &input).unwrap();
        assert_eq!(d["name"], "transfer");
        assert_eq!(d["params"][1]["value"], "7");
        let mut t1 = [0u8; 32];
        t1[12..].copy_from_slice(&[0xab; 20]);
        let topics = [crate::tokens::TRANSFER, t1, [0u8; 32]];
        let mut data = vec![0u8; 32];
        data[31] = 9;
        let e = decode_log(&abi, &topics, &data).unwrap();
        assert_eq!(e["name"], "Transfer");
        assert_eq!(e["params"][0]["value"], format!("0x{}", "ab".repeat(20)));
        assert_eq!(e["params"][2]["value"], "9");
    }
}
