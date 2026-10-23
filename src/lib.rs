//! faster-sync: bulk historical EVM data served from immutable Parquet
//! chunks (with per-row-group filters) and a RocksDB hot tail.

pub mod api;
pub mod chunk;
pub mod filter;
pub mod hot;
pub mod ingest;
pub mod metrics;
pub mod model;
pub mod query;
pub mod rpc;
pub mod store;
pub mod testnode;
