//! Membership filters over the values of a column in one row group.
//!
//! A binary fuse filter (about 9 bits per distinct value, ~0.4% false
//! positives) answers "might this row group contain value X?". Queries skip
//! every row group (and so every chunk) whose filters all say no.

use serde::{Deserialize, Serialize};
use xorf::{BinaryFuse8, Filter as _};

use crate::model::Table;

pub fn key(v: &[u8]) -> u64 {
    xxhash_rust::xxh3::xxh3_64(v)
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(from = "Repr", into = "Repr")]
pub enum Filter {
    /// No values (every row null).
    Empty,
    /// Few values: kept exactly.
    Exact(Vec<u64>),
    Fuse(BinaryFuse8),
}

/// On-disk form (xorf's own serde form uses `flatten`, which bincode
/// cannot encode).
#[derive(Serialize, Deserialize)]
enum Repr {
    Empty,
    Exact(Vec<u64>),
    Fuse { seed: u64, len: u32, mask: u32, count_len: u32, fp: Vec<u8> },
}

impl From<Filter> for Repr {
    fn from(f: Filter) -> Repr {
        match f {
            Filter::Empty => Repr::Empty,
            Filter::Exact(v) => Repr::Exact(v),
            Filter::Fuse(f) => Repr::Fuse {
                seed: f.descriptor.seed,
                len: f.descriptor.segment_length,
                mask: f.descriptor.segment_length_mask,
                count_len: f.descriptor.segment_count_length,
                fp: f.fingerprints.into_vec(),
            },
        }
    }
}

impl From<Repr> for Filter {
    fn from(r: Repr) -> Filter {
        match r {
            Repr::Empty => Filter::Empty,
            Repr::Exact(v) => Filter::Exact(v),
            Repr::Fuse { seed, len, mask, count_len, fp } => Filter::Fuse(BinaryFuse8 {
                descriptor: xorf::Descriptor {
                    seed,
                    segment_length: len,
                    segment_length_mask: mask,
                    segment_count_length: count_len,
                },
                fingerprints: fp.into_boxed_slice(),
            }),
        }
    }
}

const EXACT_MAX: usize = 64;

impl Filter {
    pub fn build(mut keys: Vec<u64>) -> Filter {
        keys.sort_unstable();
        keys.dedup();
        if keys.is_empty() {
            return Filter::Empty;
        }
        if keys.len() <= EXACT_MAX {
            return Filter::Exact(keys);
        }
        match BinaryFuse8::try_from(&keys) {
            Ok(f) => Filter::Fuse(f),
            // Construction can fail on pathological inputs; an exact list is
            // always correct.
            Err(_) => Filter::Exact(keys),
        }
    }

    pub fn contains(&self, k: u64) -> bool {
        match self {
            Filter::Empty => false,
            Filter::Exact(v) => v.binary_search(&k).is_ok(),
            Filter::Fuse(f) => f.contains(&k),
        }
    }

    pub fn any(&self, ks: &[u64]) -> bool {
        ks.iter().any(|k| self.contains(*k))
    }

    pub fn bytes(&self) -> usize {
        match self {
            Filter::Empty => 0,
            Filter::Exact(v) => v.len() * 8,
            Filter::Fuse(f) => f.fingerprints.len(),
        }
    }
}

/// Columns that get a filter, per table.
pub fn filtered_cols(t: Table) -> &'static [&'static str] {
    match t {
        Table::Blocks => &["miner"],
        Table::Transactions => &["from", "to", "sighash", "contract_address"],
        Table::Logs => &["address", "topic0", "topic1", "topic2", "topic3"],
        Table::Traces => &["from", "to", "sighash"],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_and_fuse() {
        let small = Filter::build(vec![key(b"a"), key(b"b"), key(b"a")]);
        assert!(small.contains(key(b"a")) && small.contains(key(b"b")) && !small.contains(key(b"c")));
        let many: Vec<u64> = (0..10_000u32).map(|i| key(&i.to_be_bytes())).collect();
        let f = Filter::build(many.clone());
        assert!(matches!(f, Filter::Fuse(_)));
        assert!(many.iter().all(|k| f.contains(*k)));
        let fp = (10_000..110_000u32).filter(|i| f.contains(key(&i.to_be_bytes()))).count();
        assert!(fp < 1_000, "false positives {fp}");
        assert!(!Filter::build(vec![]).contains(1));
        let back: Filter = bincode::deserialize(&bincode::serialize(&f).unwrap()).unwrap();
        assert!(many.iter().all(|k| back.contains(*k)));
    }
}
