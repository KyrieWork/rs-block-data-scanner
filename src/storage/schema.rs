// Key naming conventions and prefix constants
pub mod keys {
    pub const PROGRESS_PREFIX: &str = "progress";
    pub const BLOCK_DATA_PREFIX: &str = "block_data";
    pub const BLOCK_RECEIPTS_PREFIX: &str = "block_receipts";

    // example: ethereum:progress
    pub fn progress_key(chain: &str) -> String {
        format!("{}:{}", chain, PROGRESS_PREFIX)
    }

    // example: ethereum:block_data:1000000
    pub fn block_data_key(chain: &str, block_number: u64) -> String {
        format!("{}:{}:{}", chain, BLOCK_DATA_PREFIX, block_number)
    }

    // example: ethereum:block_receipts:1000000
    pub fn block_receipts_key(chain: &str, block_number: u64) -> String {
        format!("{}:{}:{}", chain, BLOCK_RECEIPTS_PREFIX, block_number)
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
