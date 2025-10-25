use anyhow::Result;
use tracing::{debug, info};

use crate::{
    config::ScannerConfig,
    core::table::{BlockIndex, ScannerProgress},
    storage::{rocksdb::RocksDBStorage, schema::keys, traits::KVStorage},
};
use std::{collections::HashSet, sync::Arc};

/// Cleanup result containing keys to be deleted
#[derive(Debug, Clone, Default)]
pub struct CleanupResult {
    /// Keys to be deleted from database
    pub keys_to_delete: Vec<Vec<u8>>,
    /// Number of block hashes that will be cleaned
    pub block_hashes_count: usize,
    /// New min_block value to update in progress
    pub new_min_block: Option<u64>,
}

impl CleanupResult {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.keys_to_delete.is_empty()
    }

    pub fn total_keys(&self) -> usize {
        self.keys_to_delete.len()
    }
}

pub struct EvmCleaner {
    scanner_cfg: ScannerConfig,
    storage: Arc<RocksDBStorage>,
}

impl EvmCleaner {
    pub fn new(scanner_cfg: ScannerConfig, storage: Arc<RocksDBStorage>) -> Self {
        Self {
            scanner_cfg,
            storage,
        }
    }
    /// Check if cleanup should be performed and return keys to delete
    /// Returns empty result if cleanup is not needed
    pub fn get_cleanup_keys(&self) -> Result<CleanupResult> {
        // Check if cleanup is enabled
        if !self.scanner_cfg.cleanup_enabled {
            debug!("Cleanup is disabled, skipping");
            return Ok(CleanupResult::new());
        }

        let retention_blocks = match self.scanner_cfg.retention_blocks {
            Some(blocks) if blocks > 0 => blocks,
            _ => {
                debug!("No retention_blocks configured, skipping cleanup");
                return Ok(CleanupResult::new());
            }
        };

        // Read progress (includes min_block)
        let progress_key = keys::progress_key(&self.scanner_cfg.chain_name);
        let progress: ScannerProgress = match self.storage.read_json(&progress_key)? {
            Some(p) => p,
            None => {
                debug!("No progress found, skipping cleanup");
                return Ok(CleanupResult::new());
            }
        };

        let current_block = progress.current_block;

        // Check if we have enough blocks to clean
        if current_block <= retention_blocks {
            debug!(
                "Current block {} <= retention {}, no cleanup needed",
                current_block, retention_blocks
            );
            return Ok(CleanupResult::new());
        }

        let cleanup_threshold = current_block - retention_blocks;

        // Get min_block from progress (or use start_block as fallback)
        let min_stored_block = progress.min_block.unwrap_or(self.scanner_cfg.start_block);

        if cleanup_threshold <= min_stored_block {
            debug!(
                "Cleanup threshold {} <= min_stored_block {}, no cleanup needed",
                cleanup_threshold, min_stored_block
            );
            return Ok(CleanupResult::new());
        }

        info!(
            "🧹 Starting cleanup analysis: current={}, retention={}, threshold={}",
            current_block, retention_blocks, cleanup_threshold
        );

        // Get keys for expired blocks cleanup
        let mut result = self.get_expired_blocks_keys(min_stored_block, cleanup_threshold)?;
        result.new_min_block = Some(cleanup_threshold);

        // Optionally get keys for orphaned data cleanup
        if self.scanner_cfg.cleanup_orphaned_enabled {
            let orphaned_keys = self.get_orphaned_data_keys()?;
            result.keys_to_delete.extend(orphaned_keys);
        } else {
            debug!("Orphaned data cleanup is disabled, skipping");
        }

        if !result.is_empty() {
            info!(
                "✅ Cleanup analysis completed: {} keys to delete, {} block hashes, new min_block: {}",
                result.total_keys(),
                result.block_hashes_count,
                result.new_min_block.unwrap_or(0)
            );
        } else {
            debug!("No data found to clean in the specified range");
        }

        Ok(result)
    }

    /// Get keys for expired blocks cleanup by block number range
    /// Returns keys for both indexes and data that should be deleted
    fn get_expired_blocks_keys(&self, min_block: u64, max_block: u64) -> Result<CleanupResult> {
        let mut result = CleanupResult::new();
        let batch_size = self.scanner_cfg.cleanup_batch_size;

        info!(
            "🧹 Analyzing expired blocks {}-{}",
            min_block,
            max_block - 1
        );

        for block_num in (min_block..max_block).step_by(batch_size) {
            let batch_end = std::cmp::min(block_num + batch_size as u64, max_block);
            let batch_keys = self.get_block_batch_keys(block_num, batch_end)?;
            result.keys_to_delete.extend(batch_keys);
        }

        result.block_hashes_count = self.count_block_hashes_in_range(min_block, max_block)?;

        debug!(
            "🗑️ Found {} keys for expired blocks cleanup (indexes + data)",
            result.total_keys()
        );
        Ok(result)
    }

    /// Get keys for a batch of blocks: get hashes from indexes, prepare keys for deletion
    fn get_block_batch_keys(&self, start_block: u64, end_block: u64) -> Result<Vec<Vec<u8>>> {
        let mut keys_to_delete = Vec::new();
        let mut hashes_to_clean = Vec::new();

        // Step 1: Get hashes from active indexes for the block range
        for block_number in start_block..end_block {
            let active_key =
                keys::block_index_active_key(&self.scanner_cfg.chain_name, block_number);

            if let Some(index_json) = self.storage.read(&active_key)?
                && let Ok(index) = serde_json::from_str::<BlockIndex>(&index_json)
            {
                hashes_to_clean.push(index.block_hash.clone());
                keys_to_delete.push(active_key.into_bytes());
            }
        }

        // Step 2: Get hashes from history indexes for the block range
        let history_prefix = format!(
            "{}:{}:",
            self.scanner_cfg.chain_name,
            keys::BLOCK_INDEX_HISTORY_PREFIX
        );
        let history_results = self.storage.scan_prefix(&history_prefix, None)?;

        for (key, value) in history_results {
            if let Some(block_number) = self.extract_block_number_from_history_key(&key)
                && block_number >= start_block
                && block_number < end_block
                && let Ok(index) = serde_json::from_str::<BlockIndex>(&value)
            {
                hashes_to_clean.push(index.block_hash.clone());
                keys_to_delete.push(key.into_bytes());
            }
        }

        // Step 3: Prepare data keys for deletion based on collected hashes
        for hash in &hashes_to_clean {
            let block_data_key = keys::block_data_key(&self.scanner_cfg.chain_name, hash);
            let block_receipts_key = keys::block_receipts_key(&self.scanner_cfg.chain_name, hash);
            let block_debug_trace_key =
                keys::block_debug_trace_key(&self.scanner_cfg.chain_name, hash);
            keys_to_delete.push(block_data_key.into_bytes());
            keys_to_delete.push(block_receipts_key.into_bytes());
            keys_to_delete.push(block_debug_trace_key.into_bytes());
        }

        Ok(keys_to_delete)
    }

    /// Get keys for orphaned data (not referenced by any active index)
    fn get_orphaned_data_keys(&self) -> Result<Vec<Vec<u8>>> {
        // Build active hash set for quick lookup
        let active_hashes = self.build_active_hash_set()?;

        // Scan all block data
        let block_data_prefix = format!(
            "{}:{}:",
            self.scanner_cfg.chain_name,
            keys::BLOCK_DATA_PREFIX
        );
        let block_data_results = self.storage.scan_prefix(&block_data_prefix, None)?;
        let mut keys_to_delete = Vec::new();

        for (key, _) in block_data_results {
            // Extract block hash from key
            if let Some(block_hash) = self.extract_block_hash_from_data_key(&key) {
                // Check if block hash is referenced by active index
                if !active_hashes.contains(&block_hash) {
                    keys_to_delete.push(key.into_bytes());

                    // Also delete corresponding receipts
                    let receipts_key =
                        keys::block_receipts_key(&self.scanner_cfg.chain_name, &block_hash);
                    keys_to_delete.push(receipts_key.into_bytes());

                    // Also delete corresponding trace logs
                    let debug_trace_key =
                        keys::block_debug_trace_key(&self.scanner_cfg.chain_name, &block_hash);
                    keys_to_delete.push(debug_trace_key.into_bytes());
                }
            }
        }

        debug!(
            "🗑️ Found {} keys for orphaned block data cleanup",
            keys_to_delete.len()
        );
        Ok(keys_to_delete)
    }

    /// Build active hash set for quick lookup
    fn build_active_hash_set(&self) -> Result<HashSet<String>> {
        let mut active_hashes = HashSet::new();
        let active_prefix = format!(
            "{}:{}:",
            self.scanner_cfg.chain_name,
            keys::BLOCK_INDEX_ACTIVE_PREFIX
        );
        let results = self.storage.scan_prefix(&active_prefix, None)?;

        for (_, value) in results {
            if let Ok(index) = serde_json::from_str::<BlockIndex>(&value) {
                active_hashes.insert(index.block_hash);
            }
        }

        Ok(active_hashes)
    }

    /// Count unique block hashes in the specified range
    fn count_block_hashes_in_range(&self, min_block: u64, max_block: u64) -> Result<usize> {
        let mut unique_hashes = HashSet::new();

        // Count from active indexes
        for block_number in min_block..max_block {
            let active_key =
                keys::block_index_active_key(&self.scanner_cfg.chain_name, block_number);

            if let Some(index_json) = self.storage.read(&active_key)?
                && let Ok(index) = serde_json::from_str::<BlockIndex>(&index_json)
            {
                unique_hashes.insert(index.block_hash);
            }
        }

        // Count from history indexes
        let history_prefix = format!(
            "{}:{}:",
            self.scanner_cfg.chain_name,
            keys::BLOCK_INDEX_HISTORY_PREFIX
        );
        let history_results = self.storage.scan_prefix(&history_prefix, None)?;

        for (key, value) in history_results {
            if let Some(block_number) = self.extract_block_number_from_history_key(&key)
                && block_number >= min_block
                && block_number < max_block
                && let Ok(index) = serde_json::from_str::<BlockIndex>(&value)
            {
                unique_hashes.insert(index.block_hash);
            }
        }

        Ok(unique_hashes.len())
    }

    /// Extract block number from history key
    fn extract_block_number_from_history_key(&self, key: &str) -> Option<u64> {
        let parts: Vec<&str> = key.split(':').collect();
        if parts.len() >= 3 {
            parts[2].parse::<u64>().ok()
        } else {
            None
        }
    }

    /// Extract block hash from data key
    fn extract_block_hash_from_data_key(&self, key: &str) -> Option<String> {
        let parts: Vec<&str> = key.split(':').collect();
        if parts.len() >= 3 {
            Some(parts[2].to_string())
        } else {
            None
        }
    }
}
