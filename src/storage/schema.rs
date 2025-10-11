// Key naming conventions and prefix constants
pub mod keys {
    /// Scanner progress key prefix: "progress:{chain_name}"
    pub const PROGRESS_PREFIX: &str = "progress";

    /// Block data key prefix: "block_data:{chain}:{block_number}"
    pub const BLOCK_DATA_PREFIX: &str = "block_data";

    /// Receipts key prefix: "receipts:{chain}:{tx_hash}"
    pub const BLOCK_RECEIPTS_PREFIX: &str = "block_receipts";

    // example: ethereum:progress
    pub fn progress_key(chain: &str) -> String {
        format!("{}:{}", chain, PROGRESS_PREFIX)
    }

    // example: ethereum:block_data:1000000:0x1234567890abcdef
    pub fn block_data_key(chain: &str, block_number: u64, block_hash: &str) -> String {
        format!(
            "{}:{}:{}:{}",
            chain, BLOCK_DATA_PREFIX, block_number, block_hash
        )
    }

    // example: ethereum:block_receipts:1000000:0x1234567890abcdef
    pub fn block_receipts_key(chain: &str, block_number: u64, block_hash: &str) -> String {
        format!(
            "{}:{}:{}:{}",
            chain, BLOCK_RECEIPTS_PREFIX, block_number, block_hash
        )
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
