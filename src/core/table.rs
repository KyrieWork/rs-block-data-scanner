use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Scanner status enumeration for type-safe status management
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum ScannerStatus {
    /// Paused state
    Idle,
    /// Scanning in progress
    Scanning,
    /// Catching up (batch scanning)
    CatchingUp,
    /// Fully synced
    Synced,
    /// Reorg detected mode (single block scanning)
    ReorgDetected,
    /// Service error
    Error,
}

impl ScannerStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            ScannerStatus::Idle => "idle",
            ScannerStatus::Scanning => "scanning",
            ScannerStatus::CatchingUp => "catching_up",
            ScannerStatus::Synced => "synced",
            ScannerStatus::ReorgDetected => "reorg_detected",
            ScannerStatus::Error => "error",
        }
    }

    pub fn from_string(s: &str) -> Self {
        match s {
            "idle" => ScannerStatus::Idle,
            "scanning" => ScannerStatus::Scanning,
            "catching_up" => ScannerStatus::CatchingUp,
            "synced" => ScannerStatus::Synced,
            "reorg_detected" => ScannerStatus::ReorgDetected,
            "error" => ScannerStatus::Error,
            _ => ScannerStatus::Error,
        }
    }
}

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

    /// Current task status using enum for type safety
    pub status: ScannerStatus,

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

    /// Reorganization statistics
    #[serde(default)]
    pub reorg_count: u64,

    /// Consecutive successful scan count (used for reorg mode exit)
    #[serde(default)]
    pub consecutive_success_count: u64,

    /// Reorg mode start time (for timeout detection)
    #[serde(default)]
    pub reorg_start_time: Option<DateTime<Utc>>,
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
    pub block_number: u64,
    pub parent_hash: String,
    pub block_data_json: String,
    pub block_receipts_json: String,
    pub debug_trace_block_json: String,
}
