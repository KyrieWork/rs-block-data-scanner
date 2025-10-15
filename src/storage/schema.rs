// Key naming conventions and prefix constants
pub mod keys {
    pub const PROGRESS_PREFIX: &str = "progress";
    pub const BLOCK_INDEX_ACTIVE_PREFIX: &str = "block_index_active";
    pub const BLOCK_INDEX_HISTORY_PREFIX: &str = "block_index_history";
    pub const BLOCK_DATA_PREFIX: &str = "block_data";
    pub const BLOCK_RECEIPTS_PREFIX: &str = "block_receipts";
    pub const BLOCK_TRACE_LOGS_PREFIX: &str = "block_trace_logs";

    // example: ethereum:progress
    pub fn progress_key(chain: &str) -> String {
        format!("{}:{}", chain, PROGRESS_PREFIX)
    }

    // example: ethereum:block_index_active:1000000
    pub fn block_index_active_key(chain: &str, block_number: u64) -> String {
        format!("{}:{}:{}", chain, BLOCK_INDEX_ACTIVE_PREFIX, block_number)
    }

    // example: ethereum:block_index_history:1000000:1640995200
    pub fn block_index_history_key(chain: &str, block_number: u64, timestamp: i64) -> String {
        format!(
            "{}:{}:{}:{}",
            chain, BLOCK_INDEX_HISTORY_PREFIX, block_number, timestamp
        )
    }

    // example: ethereum:block_data:0x1234567890
    pub fn block_data_key(chain: &str, block_hash: &str) -> String {
        format!("{}:{}:{}", chain, BLOCK_DATA_PREFIX, block_hash)
    }

    // example: ethereum:block_receipts:0x1234567890
    pub fn block_receipts_key(chain: &str, block_hash: &str) -> String {
        format!("{}:{}:{}", chain, BLOCK_RECEIPTS_PREFIX, block_hash)
    }

    // example: ethereum:block_trace_logs:0x1234567890
    pub fn block_trace_logs_key(chain: &str, block_hash: &str) -> String {
        format!("{}:{}:{}", chain, BLOCK_TRACE_LOGS_PREFIX, block_hash)
    }
}

// // Column family definitions (if using Column Families)
// pub mod column_families {
//     pub const BLOCKS: &str = "blocks";
//     pub const TRANSACTIONS: &str = "transactions";
//     pub const PROGRESS: &str = "progress";
// }

// Data version management
pub const SCHEMA_VERSION: u32 = 1;
