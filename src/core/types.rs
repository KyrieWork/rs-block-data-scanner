use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ScannerProgress {
    /// Chain name (e.g. "ethereum", "bsc", "solana")
    pub chain: String,

    /// Current scanned latest block number (or slot)
    pub current_block: u64,

    /// Current scanning task target block (e.g. from current_block to last_block)
    pub target_block: u64,

    /// Latest block number on the chain (can be used to calculate synchronization progress)
    pub network_latest_block: Option<u64>,

    /// Current task status:
    /// - "idle" -> Paused
    /// - "scanning" -> Scanning
    /// - "catching_up" -> Catching up
    /// - "reorg_detected" -> Handling reorg
    pub status: String,

    /// Last updated UTC time
    pub updated_at: DateTime<Utc>,

    /// Latest chain reorg information detected (e.g. block hash corresponding to error)
    pub reorg_block: Option<u64>,

    /// Final confirmed block (e.g. EVM finality 12 blocks)
    pub finalized_block: Option<u64>,

    /// Data version number, for future schema compatibility upgrade
    pub version: u32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BlockData {
    pub hash: String,
    pub block_data_json: String,
    pub block_receipts_json: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct EvmBlockWithdrawal {
    #[serde(rename = "index")]
    pub index: String,
    pub validator_index: String,
    pub address: String,
    pub amount: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct EvmBlockData {
    pub base_fee_per_gas: String,
    pub difficulty: String,
    pub extra_data: String,
    pub gas_limit: String,
    pub gas_used: String,
    pub hash: String,
    pub logs_bloom: String,
    pub miner: String,
    pub mix_hash: String,
    pub nonce: String,
    pub number: String,
    pub parent_hash: String,
    pub receipts_root: String,
    pub sha3_uncles: String,
    pub size: String,
    pub state_root: String,
    pub timestamp: String,
    pub total_difficulty: String,
    pub transactions: Vec<String>,
    pub transactions_root: String,
    pub uncles: Vec<String>,
    pub withdrawals: Vec<EvmBlockWithdrawal>,
    pub withdrawals_root: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct EvmLogEntry {
    pub address: String,
    pub topics: Vec<String>,
    pub data: String,
    pub block_number: String,
    pub transaction_hash: String,
    pub transaction_index: String,
    pub block_hash: String,
    pub log_index: String,
    pub removed: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct EvmBlockReceipts {
    pub block_hash: String,
    pub block_number: String,
    pub contract_address: Option<String>,
    pub cumulative_gas_used: String,
    pub effective_gas_price: String,
    pub from: String,
    pub gas_used: String,
    pub logs: Vec<EvmLogEntry>,
    pub logs_bloom: String,
    pub status: String,
    pub to: String,
    pub transaction_hash: String,
    pub transaction_index: String,
    #[serde(rename = "type")]
    pub type_field: String,
}
