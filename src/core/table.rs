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

    /// Minimum block number stored in database (for cleanup tracking)
    #[serde(default)]
    pub min_block: Option<u64>,

    /// Data version number, for future schema compatibility upgrade
    pub version: u32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BlockIndex {
    pub block_hash: String,
    pub parent_hash: String, // parent block hash, for rollback verification
    pub created_at: DateTime<Utc>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BlockIndexHistory {
    pub block_hash: String,
    pub parent_hash: String, // parent block hash, for rollback verification
    pub created_at: DateTime<Utc>,
    pub is_active: bool,
    pub version: u64,                       // use timestamp as version number
    pub replaced_at: Option<DateTime<Utc>>, // replaced at
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BlockData {
    pub hash: String,
    pub block_data_json: String,
    pub block_receipts_json: String,
    pub trace_logs_json: String,
}
