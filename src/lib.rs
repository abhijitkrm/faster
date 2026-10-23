//! faster-rpc: Ethereum JSON-RPC answered from faster-sync's indexed history
//! (blocks, transactions, receipts, logs) with state methods forwarded to an
//! upstream node.

pub mod methods;
pub mod server;
pub mod sync;
pub mod upstream;
pub mod ws;
