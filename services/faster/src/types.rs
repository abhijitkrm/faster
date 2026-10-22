//! Records stored in the hot store. Fixed-size hashes and addresses are raw
//! bytes; 256-bit quantities are minimal big-endian byte strings so most
//! values (zero, small amounts) cost one or two bytes.

use ruint::aliases::U256;
use serde::{Deserialize, Serialize};

pub type H256 = [u8; 32];
pub type Addr = [u8; 20];

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockRec {
    pub number: u64,
    pub hash: H256,
    pub parent_hash: H256,
    pub timestamp: u64,
    pub miner: Addr,
    pub gas_used: u64,
    pub gas_limit: u64,
    pub base_fee: Option<Vec<u8>>,
    pub difficulty: Vec<u8>,
    pub state_root: H256,
    pub transactions_root: H256,
    pub receipts_root: H256,
    pub size: u64,
    pub tx_count: u32,
    pub log_count: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TxRec {
    pub hash: H256,
    pub block_number: u64,
    pub block_timestamp: u64,
    pub index: u32,
    pub from: Addr,
    pub to: Option<Addr>,
    pub value: Vec<u8>,
    pub gas: u64,
    /// Price from the transaction itself (legacy gasPrice, or the RPC's
    /// reported gasPrice for typed txs).
    pub gas_price: Vec<u8>,
    /// Price actually paid, from the receipt. Absent on old nodes.
    pub effective_gas_price: Option<Vec<u8>>,
    pub max_fee_per_gas: Option<Vec<u8>>,
    pub max_priority_fee_per_gas: Option<Vec<u8>>,
    pub nonce: u64,
    pub input: Vec<u8>,
    pub tx_type: u8,
    /// None for pre-Byzantium receipts, which carry a state root instead.
    pub status: Option<u8>,
    pub gas_used: u64,
    pub contract_address: Option<Addr>,
    /// Block-level index of this tx's first log; its logs are
    /// `first_log_index .. first_log_index + log_count`.
    pub first_log_index: u32,
    pub log_count: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogRec {
    pub tx_index: u32,
    pub address: Addr,
    pub topics: Vec<H256>,
    pub data: Vec<u8>,
}

/// A call made inside a transaction (from the node's call tracer).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InternalTx {
    pub tx_index: u32,
    /// Depth-first position among the tx's internal calls.
    pub trace_index: u32,
    /// First 8 bytes of the tx hash, to detect rows of an orphaned block.
    pub tx_hash8: [u8; 8],
    pub depth: u16,
    /// CALL, DELEGATECALL, STATICCALL, CALLCODE, CREATE, CREATE2, SELFDESTRUCT
    pub call_type: String,
    pub from: Addr,
    pub to: Option<Addr>,
    pub value: Vec<u8>,
    pub gas: u64,
    pub gas_used: u64,
    pub error: Option<String>,
}

/// One block with everything the store needs to index it.
#[derive(Clone, Debug)]
pub struct FullBlock {
    pub block: BlockRec,
    pub txs: Vec<TxRec>,
    pub logs: Vec<LogRec>,
    /// Empty unless call tracing is enabled.
    pub internals: Vec<InternalTx>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AddrInfo {
    /// Distinct transactions this address is party to (from, to or created).
    pub tx_count: u64,
    pub first_block: u64,
    pub first_ts: u64,
    pub last_block: u64,
    pub last_ts: u64,
    pub is_contract: bool,
    /// (block, tx index) of the creating transaction for contracts.
    pub creator: Option<(u64, u32)>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Counters {
    pub blocks: u64,
    pub txs: u64,
    pub logs: u64,
    pub addresses: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Head {
    pub number: u64,
    pub hash: H256,
}

pub fn hex0x(b: &[u8]) -> String {
    let mut s = String::with_capacity(2 + b.len() * 2);
    s.push_str("0x");
    s.push_str(&hex::encode(b));
    s
}

/// Minimal big-endian bytes to a decimal string ("0" for empty).
pub fn dec(b: &[u8]) -> String {
    if b.is_empty() {
        return "0".into();
    }
    match U256::try_from_be_slice(b) {
        Some(v) => v.to_string(),
        None => "0".into(),
    }
}

pub fn parse_hex_bytes(s: &str) -> Option<Vec<u8>> {
    let t = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")).unwrap_or(s);
    if t.len() % 2 == 1 {
        let mut p = String::with_capacity(t.len() + 1);
        p.push('0');
        p.push_str(t);
        return hex::decode(p).ok();
    }
    hex::decode(t).ok()
}

pub fn parse_fixed<const N: usize>(s: &str) -> Option<[u8; N]> {
    let t = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")).unwrap_or(s);
    if t.len() != N * 2 {
        return None;
    }
    let mut out = [0u8; N];
    hex::decode_to_slice(t, &mut out).ok()?;
    Some(out)
}

pub fn parse_u64(s: &str) -> Option<u64> {
    let t = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")).unwrap_or(s);
    if t.is_empty() {
        return None;
    }
    u64::from_str_radix(t, 16).ok()
}

/// Hex quantity of up to 256 bits to minimal big-endian bytes.
pub fn parse_quantity(s: &str) -> Option<Vec<u8>> {
    let mut b = parse_hex_bytes(s)?;
    if b.len() > 32 {
        return None;
    }
    let lead = b.iter().take_while(|&&x| x == 0).count();
    b.drain(..lead);
    Some(b)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quantities() {
        assert_eq!(parse_quantity("0x0").unwrap(), Vec::<u8>::new());
        assert_eq!(parse_quantity("0x1").unwrap(), vec![1]);
        assert_eq!(parse_quantity("0x0100").unwrap(), vec![1, 0]);
        assert_eq!(dec(&parse_quantity("0xde0b6b3a7640000").unwrap()), "1000000000000000000");
        assert_eq!(dec(&[]), "0");
        assert_eq!(parse_u64("0x1a"), Some(26));
        assert_eq!(parse_u64("0x"), None);
        assert!(parse_fixed::<20>("0x00000000000000000000000000000000000000ff").is_some());
        assert!(parse_fixed::<20>("0x00ff").is_none());
    }
}
