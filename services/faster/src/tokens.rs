//! Token transfer decoding from logs (ERC-20, ERC-721, ERC-1155) and token
//! metadata decoding (name / symbol / decimals return data).

use serde::{Deserialize, Serialize};

use crate::types::*;

pub const KIND_ERC20: u8 = 20;
pub const KIND_ERC721: u8 = 72;
pub const KIND_ERC1155: u8 = 115;

/// keccak("Transfer(address,address,uint256)") — ERC-20 (3 topics) and
/// ERC-721 (4 topics, tokenId indexed).
pub const TRANSFER: H256 = hex32("ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef");
/// keccak("TransferSingle(address,address,address,uint256,uint256)")
pub const TRANSFER_SINGLE: H256 = hex32("c3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62");
/// keccak("TransferBatch(address,address,address,uint256[],uint256[])")
pub const TRANSFER_BATCH: H256 = hex32("4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb");

const fn hex32(s: &str) -> H256 {
    let b = s.as_bytes();
    let mut out = [0u8; 32];
    let mut i = 0;
    while i < 32 {
        out[i] = (nib(b[2 * i]) << 4) | nib(b[2 * i + 1]);
        i += 1;
    }
    out
}

const fn nib(c: u8) -> u8 {
    match c {
        b'0'..=b'9' => c - b'0',
        b'a'..=b'f' => c - b'a' + 10,
        _ => panic!("bad hex"),
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Transfer {
    pub tx_index: u32,
    pub log_index: u32,
    /// Position inside an ERC-1155 TransferBatch (0 otherwise).
    pub sub: u16,
    pub token: Addr,
    pub kind: u8,
    pub from: Addr,
    pub to: Addr,
    /// ERC-721 / ERC-1155 token id (minimal big-endian).
    pub token_id: Option<Vec<u8>>,
    /// Amount (minimal big-endian); 1 for ERC-721.
    pub value: Vec<u8>,
}

fn topic_addr(t: &H256) -> Option<Addr> {
    // An address topic is 12 zero bytes + 20 address bytes.
    if t[..12].iter().any(|&b| b != 0) {
        return None;
    }
    Some(t[12..].try_into().unwrap())
}

fn trim(b: &[u8]) -> Vec<u8> {
    let lead = b.iter().take_while(|&&x| x == 0).count();
    b[lead..].to_vec()
}

fn word(data: &[u8], i: usize) -> Option<&[u8]> {
    data.get(i * 32..i * 32 + 32)
}

fn word_usize(w: &[u8]) -> Option<usize> {
    if w[..24].iter().any(|&b| b != 0) {
        return None;
    }
    usize::try_from(u64::from_be_bytes(w[24..].try_into().unwrap())).ok()
}

/// ABI `uint256[]` at byte offset `off` of `data`.
fn uint_array(data: &[u8], off: usize) -> Option<Vec<Vec<u8>>> {
    let n = word_usize(data.get(off..off + 32)?)?;
    if n > 10_000 {
        return None;
    }
    (0..n).map(|i| data.get(off + 32 + i * 32..off + 64 + i * 32).map(trim)).collect()
}

/// Token transfers in a block, in log order. Logs that look like transfers
/// but are malformed (wrong topic count or data size) are skipped.
pub fn decode_block(fb: &FullBlock) -> Vec<Transfer> {
    let mut out = Vec::new();
    for (li, l) in fb.logs.iter().enumerate() {
        let Some(t0) = l.topics.first() else { continue };
        let base = |kind, from, to, token_id, value, sub| Transfer {
            tx_index: l.tx_index,
            log_index: li as u32,
            sub,
            token: l.address,
            kind,
            from,
            to,
            token_id,
            value,
        };
        if *t0 == TRANSFER {
            match (l.topics.len(), l.data.len()) {
                (3, 32) => {
                    if let (Some(f), Some(t)) = (topic_addr(&l.topics[1]), topic_addr(&l.topics[2])) {
                        out.push(base(KIND_ERC20, f, t, None, trim(&l.data), 0));
                    }
                }
                (4, 0) => {
                    if let (Some(f), Some(t)) = (topic_addr(&l.topics[1]), topic_addr(&l.topics[2])) {
                        out.push(base(KIND_ERC721, f, t, Some(trim(&l.topics[3])), vec![1], 0));
                    }
                }
                _ => {}
            }
        } else if *t0 == TRANSFER_SINGLE && l.topics.len() == 4 && l.data.len() == 64 {
            if let (Some(f), Some(t)) = (topic_addr(&l.topics[2]), topic_addr(&l.topics[3])) {
                let id = trim(word(&l.data, 0).unwrap());
                out.push(base(KIND_ERC1155, f, t, Some(id), trim(word(&l.data, 1).unwrap()), 0));
            }
        } else if *t0 == TRANSFER_BATCH && l.topics.len() == 4 {
            let (Some(f), Some(t)) = (topic_addr(&l.topics[2]), topic_addr(&l.topics[3])) else { continue };
            let arrays = (|| {
                let ids = uint_array(&l.data, word_usize(word(&l.data, 0)?)?)?;
                let vals = uint_array(&l.data, word_usize(word(&l.data, 1)?)?)?;
                (ids.len() == vals.len()).then_some((ids, vals))
            })();
            if let Some((ids, vals)) = arrays {
                for (i, (id, v)) in ids.into_iter().zip(vals).enumerate().take(u16::MAX as usize) {
                    out.push(base(KIND_ERC1155, f, t, Some(id), v, i as u16));
                }
            }
        }
    }
    out
}

/// Decodes an `eth_call` result of `name()` / `symbol()`: ABI string, or a
/// bytes32 (older tokens such as MKR). None if neither.
pub fn decode_string(ret: &[u8]) -> Option<String> {
    let s = if ret.len() == 32 {
        let end = ret.iter().position(|&b| b == 0).unwrap_or(32);
        String::from_utf8(ret[..end].to_vec()).ok()?
    } else {
        let off = word_usize(word(ret, 0)?)?;
        let len = word_usize(ret.get(off..off + 32)?)?;
        String::from_utf8(ret.get(off + 32..off + 32 + len)?.to_vec()).ok()?
    };
    let s: String = s.chars().filter(|c| !c.is_control()).take(64).collect();
    let s = s.trim().to_string();
    (!s.is_empty()).then_some(s)
}

/// `decimals()` result; None unless it fits in a u8 (the standard type).
pub fn decode_decimals(ret: &[u8]) -> Option<u8> {
    let w = word(ret, 0)?;
    w[..31].iter().all(|&b| b == 0).then_some(w[31])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn topic(a: u8) -> H256 {
        let mut t = [0u8; 32];
        t[31] = a;
        t
    }

    fn u256(v: u64) -> Vec<u8> {
        let mut w = vec![0u8; 32];
        w[24..].copy_from_slice(&v.to_be_bytes());
        w
    }

    fn block(logs: Vec<LogRec>) -> FullBlock {
        FullBlock {
            block: BlockRec {
                number: 1,
                hash: [1; 32],
                parent_hash: [0; 32],
                timestamp: 0,
                miner: [0; 20],
                gas_used: 0,
                gas_limit: 0,
                base_fee: None,
                difficulty: vec![],
                state_root: [0; 32],
                transactions_root: [0; 32],
                receipts_root: [0; 32],
                size: 0,
                tx_count: 1,
                log_count: logs.len() as u32,
                seen_ms: 0,
                indexed_ms: 0,
            },
            txs: vec![],
            logs,
            internals: vec![],
        }
    }

    #[test]
    fn decodes_all_standards() {
        let tok = [7u8; 20];
        let log = |topics: Vec<H256>, data: Vec<u8>| LogRec { tx_index: 0, address: tok, topics, data };
        let mut batch = Vec::new();
        batch.extend(u256(64)); // ids offset
        batch.extend(u256(160)); // values offset
        batch.extend(u256(2));
        batch.extend(u256(10));
        batch.extend(u256(11));
        batch.extend(u256(2));
        batch.extend(u256(100));
        batch.extend(u256(200));
        let mut single = u256(5);
        single.extend(u256(3));
        let fb = block(vec![
            log(vec![TRANSFER, topic(1), topic(2)], u256(1000)),
            log(vec![TRANSFER, topic(0), topic(2), topic(42)], vec![]),
            log(vec![TRANSFER_SINGLE, topic(9), topic(1), topic(2)], single),
            log(vec![TRANSFER_BATCH, topic(9), topic(2), topic(3)], batch),
            log(vec![TRANSFER, topic(1)], u256(1)), // malformed: skipped
            log(vec![TRANSFER, [0xff; 32], topic(2)], u256(1)), // non-address topic: skipped
        ]);
        let t = decode_block(&fb);
        assert_eq!(t.len(), 5);
        assert_eq!((t[0].kind, t[0].value.clone(), t[0].from[19], t[0].to[19]), (KIND_ERC20, vec![0x03, 0xe8], 1, 2));
        assert_eq!(
            (t[1].kind, t[1].token_id.clone(), t[1].from, t[1].value.clone()),
            (KIND_ERC721, Some(vec![42]), [0; 20], vec![1])
        );
        assert_eq!((t[2].kind, t[2].token_id.clone(), t[2].value.clone()), (KIND_ERC1155, Some(vec![5]), vec![3]));
        assert_eq!((t[3].sub, t[3].token_id.clone(), t[3].value.clone()), (0, Some(vec![10]), vec![100]));
        assert_eq!(
            (t[4].sub, t[4].token_id.clone(), t[4].value.clone(), t[4].log_index),
            (1, Some(vec![11]), vec![200], 3)
        );
    }

    #[test]
    fn metadata() {
        let mut s = u256(32);
        s.extend(u256(4));
        let mut w = b"USDC".to_vec();
        w.resize(32, 0);
        s.extend(w.clone());
        assert_eq!(decode_string(&s).as_deref(), Some("USDC"));
        assert_eq!(decode_string(&w).as_deref(), Some("USDC")); // bytes32
        assert_eq!(decode_string(&[]), None);
        assert_eq!(decode_decimals(&u256(18)), Some(18));
        assert_eq!(decode_decimals(&u256(300)), None);
    }
}
