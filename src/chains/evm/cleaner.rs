use anyhow::Result;
use async_trait::async_trait;
use tracing::{debug, info, warn};

use crate::{
    config::ScannerConfig,
    core::{
        cleaner::Cleaner,
        table::{BlockIndex, ScannerProgress},
    },
    storage::{rocksdb::RocksDBStorage, schema::keys, traits::KVStorage},
};
use std::collections::HashSet;

pub struct EvmCleaner {
    scanner_cfg: ScannerConfig,
    storage: RocksDBStorage,
}

impl EvmCleaner {
    pub fn new(scanner_cfg: ScannerConfig, storage: RocksDBStorage) -> Self {
        Self {
            scanner_cfg,
            storage,
        }
    }
}

#[async_trait]
impl Cleaner for EvmCleaner {
    async fn cleanup(&self) -> Result<usize> {
        // Check if cleanup is enabled
        if !self.scanner_cfg.cleanup_enabled {
            debug!("Cleanup is disabled, skipping");
            return Ok(0);
        }

        let retention_blocks = match self.scanner_cfg.retention_blocks {
            Some(blocks) if blocks > 0 => blocks,
            _ => {
                debug!("No retention_blocks configured, skipping cleanup");
                return Ok(0);
            }
        };

        // Read progress (includes min_block)
        let progress_key = keys::progress_key(&self.scanner_cfg.chain_name);
        let progress: ScannerProgress = match self.storage.read_json(&progress_key)? {
            Some(p) => p,
            None => {
                debug!("No progress found, skipping cleanup");
                return Ok(0);
            }
        };

        let current_block = progress.current_block;

        // Check if we have enough blocks to clean
        if current_block <= retention_blocks {
            debug!(
                "Current block {} <= retention {}, no cleanup needed",
                current_block, retention_blocks
            );
            return Ok(0);
        }

        let cleanup_threshold = current_block - retention_blocks;

        // Get min_block from progress (or use start_block as fallback)
        let min_stored_block = progress.min_block.unwrap_or(self.scanner_cfg.start_block);

        if cleanup_threshold <= min_stored_block {
            debug!(
                "Cleanup threshold {} <= min_stored_block {}, no cleanup needed",
                cleanup_threshold, min_stored_block
            );
            return Ok(0);
        }

        info!(
            "🧹 Starting cleanup: current={}, retention={}, threshold={}",
            current_block, retention_blocks, cleanup_threshold
        );

        // 1. Clean up expired blocks (indexes + data) by block number range
        let expired_cleaned = self
            .cleanup_expired_blocks(min_stored_block, cleanup_threshold)
            .await?;

        // 2. Clean up orphaned data (not referenced by any active index) - optional
        let orphaned_cleaned = if self.scanner_cfg.cleanup_orphaned_enabled {
            self.cleanup_orphaned_data().await?
        } else {
            debug!("Orphaned data cleanup is disabled, skipping");
            0
        };

        let total_cleaned = expired_cleaned + orphaned_cleaned;

        // Update min_block in progress
        if total_cleaned > 0 {
            let updated_progress = ScannerProgress {
                min_block: Some(cleanup_threshold),
                ..progress
            };
            self.storage.write_json(&progress_key, &updated_progress)?;

            info!(
                "✅ Cleanup completed: expired={}, orphaned={}, total={}, new min_block: {}",
                expired_cleaned, orphaned_cleaned, total_cleaned, cleanup_threshold
            );
        } else {
            debug!("No data found to clean in the specified range");
        }

        Ok(total_cleaned)
    }
}

impl EvmCleaner {
    /// Clean up expired blocks by block number range
    /// Logic: Expired block number => Retrieve hash from index table => Delete index => Delete data => Verify cleanup completed
    async fn cleanup_expired_blocks(&self, min_block: u64, max_block: u64) -> Result<usize> {
        let mut total_cleaned = 0;
        let batch_size = self.scanner_cfg.cleanup_batch_size;

        info!("🧹 Cleaning expired blocks {}-{}", min_block, max_block - 1);

        for block_num in (min_block..max_block).step_by(batch_size) {
            let batch_end = std::cmp::min(block_num + batch_size as u64, max_block);
            let batch_cleaned = self.cleanup_block_batch(block_num, batch_end).await?;
            total_cleaned += batch_cleaned;
        }

        // Verify cleanup completion
        self.verify_cleanup_completion(min_block, max_block).await?;

        debug!(
            "🗑️ Cleaned {} expired blocks (indexes + data)",
            total_cleaned
        );
        Ok(total_cleaned)
    }

    /// Clean up a batch of blocks: get hashes from indexes, delete indexes, delete data
    async fn cleanup_block_batch(&self, start_block: u64, end_block: u64) -> Result<usize> {
        let mut cleaned_count = 0;
        let mut index_keys_to_delete = Vec::new();
        let mut data_keys_to_delete = Vec::new();
        let mut hashes_to_clean = Vec::new();

        // Step 1: Get hashes from active indexes for the block range
        for block_number in start_block..end_block {
            let active_key =
                keys::block_index_active_key(&self.scanner_cfg.chain_name, block_number);

            if let Some(index_json) = self.storage.read(&active_key)?
                && let Ok(index) = serde_json::from_str::<BlockIndex>(&index_json)
            {
                hashes_to_clean.push(index.block_hash.clone());
                index_keys_to_delete.push(active_key.into_bytes());
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
                index_keys_to_delete.push(key.into_bytes());
            }
        }

        // Step 3: Prepare data keys for deletion based on collected hashes
        for hash in &hashes_to_clean {
            let block_data_key = keys::block_data_key(&self.scanner_cfg.chain_name, hash);
            let block_receipts_key = keys::block_receipts_key(&self.scanner_cfg.chain_name, hash);
            let block_debug_trace_key =
                keys::block_debug_trace_key(&self.scanner_cfg.chain_name, hash);
            data_keys_to_delete.push(block_data_key.into_bytes());
            data_keys_to_delete.push(block_receipts_key.into_bytes());
            data_keys_to_delete.push(block_debug_trace_key.into_bytes());
        }

        // Step 4: Delete indexes first
        if !index_keys_to_delete.is_empty() {
            self.storage.delete_batch(&index_keys_to_delete)?;
            cleaned_count += index_keys_to_delete.len();
        }

        // Step 5: Delete block data, receipts, and trace logs
        if !data_keys_to_delete.is_empty() {
            self.storage.delete_batch(&data_keys_to_delete)?;
            cleaned_count += data_keys_to_delete.len(); // Count all data keys deleted
        }

        Ok(cleaned_count)
    }

    /// Clean up orphaned data (not referenced by any active index)
    async fn cleanup_orphaned_data(&self) -> Result<usize> {
        let mut cleaned_count = 0;

        // Build active hash set for quick lookup
        let active_hashes = self.build_active_hash_set().await?;

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

        if !keys_to_delete.is_empty() {
            self.storage.delete_batch(&keys_to_delete)?;
            cleaned_count = keys_to_delete.len(); // Count all keys deleted (data + receipts + trace_logs)
        }

        debug!("🗑️ Cleaned {} orphaned block data entries", cleaned_count);
        Ok(cleaned_count)
    }

    /// Verify that cleanup was completed successfully (optimized: only check start and end)
    async fn verify_cleanup_completion(&self, min_block: u64, max_block: u64) -> Result<()> {
        let mut remaining_blocks = Vec::new();

        // Only check the first and last blocks in the range for active indexes (optimized)
        let check_blocks = if max_block > min_block {
            vec![min_block, max_block - 1]
        } else {
            vec![min_block]
        };

        for block_number in check_blocks {
            let active_key =
                keys::block_index_active_key(&self.scanner_cfg.chain_name, block_number);
            if self.storage.exists(&active_key)? {
                remaining_blocks.push(format!("active_index:{}", block_number));
            }
        }

        // Check for remaining history indexes
        let history_prefix = format!(
            "{}:{}:",
            self.scanner_cfg.chain_name,
            keys::BLOCK_INDEX_HISTORY_PREFIX
        );
        let history_results = self.storage.scan_prefix(&history_prefix, None)?;

        for (key, _) in history_results {
            if let Some(block_number) = self.extract_block_number_from_history_key(&key)
                && block_number >= min_block
                && block_number < max_block
            {
                remaining_blocks.push(format!("history_index:{}", block_number));
            }
        }

        if !remaining_blocks.is_empty() {
            warn!(
                "⚠️ Cleanup verification failed: {} blocks still exist",
                remaining_blocks.len()
            );
            for block in &remaining_blocks {
                warn!("  - {}", block);
            }
        } else {
            debug!(
                "✅ Cleanup verification passed: blocks {}-{} cleaned (sampled start/end)",
                min_block,
                max_block - 1
            );
        }

        Ok(())
    }

    /// Build active hash set for quick lookup
    async fn build_active_hash_set(&self) -> Result<HashSet<String>> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        core::table::{BlockData, BlockIndex, ScannerProgress},
        storage::schema::keys,
    };
    use chrono::Utc;

    // Test constants
    const TEST_CHAIN_NAME: &str = "test_chain";
    const TEST_START_BLOCK: u64 = 100;
    const TEST_BATCH_SIZE: usize = 50;

    // Helper to create test config with cleanup enabled
    fn create_test_config(retention_blocks: Option<u64>, cleanup_enabled: bool) -> ScannerConfig {
        ScannerConfig {
            chain_type: "evm".to_string(),
            chain_name: TEST_CHAIN_NAME.to_string(),
            concurrency: 1,
            start_block: TEST_START_BLOCK,
            confirm_blocks: 1,
            realtime: true,
            timeout_secs: 15,
            cleanup_enabled,
            retention_blocks,
            cleanup_interval_secs: 3600,
            cleanup_batch_size: TEST_BATCH_SIZE,
            cleanup_orphaned_enabled: false, // Default disabled for tests
            synced_interval_secs: 3,
            catching_up_interval_millis: 10,
        }
    }

    // Helper to create test storage
    fn create_test_storage(test_name: &str) -> (RocksDBStorage, String) {
        let temp_dir = std::env::temp_dir();
        let path = temp_dir.join(format!(
            "rocksdb_cleaner_test_{}_{}",
            test_name,
            std::process::id()
        ));
        let path_str = path.to_str().unwrap().to_string();

        // Clean up if exists
        let _ = std::fs::remove_dir_all(&path_str);

        let storage = RocksDBStorage::new(&path_str).unwrap();
        (storage, path_str)
    }

    // Helper to create mock block data
    fn create_mock_block_data(block_num: u64) -> BlockData {
        BlockData {
            hash: format!("0xhash{}", block_num),
            block_data_json: format!(r#"{{"header":{{"number":"0x{:x}}}"}}"#, block_num),
            block_receipts_json: "[]".to_string(),
            debug_trace_block_json: "[]".to_string(),
        }
    }

    // Helper to store mock blocks in storage with active indexes
    fn store_mock_blocks(
        storage: &RocksDBStorage,
        chain_name: &str,
        start: u64,
        end: u64,
    ) -> Result<()> {
        for block_num in start..end {
            let block_data = create_mock_block_data(block_num);
            let block_data_key = keys::block_data_key(chain_name, &block_data.hash);
            let block_receipts_key = keys::block_receipts_key(chain_name, &block_data.hash);
            let block_debug_trace_key = keys::block_debug_trace_key(chain_name, &block_data.hash);
            // Store block data
            storage.write_json(&block_data_key, &block_data)?;
            storage.write(&block_receipts_key, &block_data.block_receipts_json)?;
            storage.write(&block_debug_trace_key, &block_data.debug_trace_block_json)?;
            // Create and store active index
            let active_index = BlockIndex {
                block_hash: block_data.hash.clone(),
                parent_hash: format!("0xparent{}", block_num - 1),
                created_at: Utc::now(),
            };
            let active_key = keys::block_index_active_key(chain_name, block_num);
            storage.write_json(&active_key, &active_index)?;
        }
        Ok(())
    }

    // Helper to create progress
    fn create_test_progress(current_block: u64, min_block: Option<u64>) -> ScannerProgress {
        ScannerProgress {
            chain: TEST_CHAIN_NAME.to_string(),
            current_block,
            target_block: current_block,
            network_latest_block: Some(current_block),
            status: "scanning".to_string(),
            updated_at: Utc::now(),
            reorg_block: None,
            finalized_block: None,
            min_block,
            version: 1,
        }
    }

    #[tokio::test]
    async fn test_cleanup_disabled() -> Result<()> {
        let (storage, _path) = create_test_storage("cleanup_disabled");
        let config = create_test_config(Some(10), false); // cleanup disabled

        let cleaner = EvmCleaner::new(config, storage);

        // Should return 0 without doing anything
        let result = cleaner.cleanup().await?;
        assert_eq!(result, 0, "Should not clean when disabled");

        println!("✅ Cleanup disabled test passed");
        Ok(())
    }

    #[tokio::test]
    async fn test_cleanup_no_retention_blocks() -> Result<()> {
        let (storage, _path) = create_test_storage("no_retention");
        let config = create_test_config(None, true); // no retention_blocks

        let cleaner = EvmCleaner::new(config, storage);

        // Should return 0 without doing anything
        let result = cleaner.cleanup().await?;
        assert_eq!(result, 0, "Should not clean without retention_blocks");

        println!("✅ No retention blocks test passed");
        Ok(())
    }

    #[tokio::test]
    async fn test_cleanup_no_progress() -> Result<()> {
        let (storage, _path) = create_test_storage("no_progress");
        let config = create_test_config(Some(10), true);

        let cleaner = EvmCleaner::new(config, storage);

        // Should return 0 when no progress exists
        let result = cleaner.cleanup().await?;
        assert_eq!(result, 0, "Should not clean when no progress");

        println!("✅ No progress test passed");
        Ok(())
    }

    #[tokio::test]
    async fn test_cleanup_not_enough_blocks() -> Result<()> {
        let (storage, _path) = create_test_storage("not_enough_blocks");
        let config = create_test_config(Some(100), true);

        // Create progress with only 50 blocks
        let progress = create_test_progress(50, Some(10));
        let progress_key = keys::progress_key(TEST_CHAIN_NAME);
        storage.write_json(&progress_key, &progress)?;

        let cleaner = EvmCleaner::new(config, storage);

        // Should not clean when current_block <= retention_blocks
        let result = cleaner.cleanup().await?;
        assert_eq!(result, 0, "Should not clean when not enough blocks");

        println!("✅ Not enough blocks test passed");
        Ok(())
    }

    #[tokio::test]
    async fn test_cleanup_basic() -> Result<()> {
        let (storage, _path) = create_test_storage("cleanup_basic");
        let config = create_test_config(Some(10), true);

        // Store blocks 100-120 (20 blocks)
        store_mock_blocks(&storage, TEST_CHAIN_NAME, 100, 120)?;

        // Create progress: current=120, min=100
        let progress = create_test_progress(120, Some(100));
        let progress_key = keys::progress_key(TEST_CHAIN_NAME);
        storage.write_json(&progress_key, &progress)?;

        let cleaner = EvmCleaner::new(config, storage.clone());

        // Cleanup should remove blocks 100-109 (keep latest 10)
        // New logic: expired blocks (10 active indexes + 30 data items: 10 block_data + 10 receipts + 10 trace_logs) + orphaned data (0) = 40 total
        let result = cleaner.cleanup().await?;
        assert_eq!(
            result, 40,
            "Should attempt to clean 40 items (10 expired blocks: 10 indexes + 30 data items)"
        );

        // Verify blocks 100-109 are deleted (check by hash)
        for block_num in 100..110 {
            let block_data = create_mock_block_data(block_num);
            let key = keys::block_data_key(TEST_CHAIN_NAME, &block_data.hash);
            assert!(
                !storage.exists(&key)?,
                "Block {} should be deleted",
                block_num
            );
        }

        // Verify blocks 110-119 still exist (check by hash)
        for block_num in 110..120 {
            let block_data = create_mock_block_data(block_num);
            let key = keys::block_data_key(TEST_CHAIN_NAME, &block_data.hash);
            assert!(
                storage.exists(&key)?,
                "Block {} should still exist",
                block_num
            );
        }

        // Verify min_block is updated
        let updated_progress: ScannerProgress = storage.read_json(&progress_key)?.unwrap();
        assert_eq!(
            updated_progress.min_block,
            Some(110),
            "min_block should be updated to 110"
        );

        println!("✅ Basic cleanup test passed");
        Ok(())
    }

    #[tokio::test]
    async fn test_cleanup_batch_delete() -> Result<()> {
        let (storage, _path) = create_test_storage("cleanup_batch");
        let config = create_test_config(Some(10), true);

        // Store 150 blocks (100-250)
        store_mock_blocks(&storage, TEST_CHAIN_NAME, 100, 250)?;

        // Create progress: current=250, min=100
        let progress = create_test_progress(250, Some(100));
        let progress_key = keys::progress_key(TEST_CHAIN_NAME);
        storage.write_json(&progress_key, &progress)?;

        let cleaner = EvmCleaner::new(config, storage.clone());

        // Cleanup should remove blocks 100-239 (keep latest 10: 240-249)
        // New logic: expired blocks (140 active indexes + 420 data items: 140 block_data + 140 receipts + 140 trace_logs) + orphaned data (0) = 560 total
        let result = cleaner.cleanup().await?;
        assert_eq!(
            result, 560,
            "Should attempt to clean 560 items (140 expired blocks: 140 indexes + 420 data items)"
        );

        // Verify old blocks are deleted (check by hash)
        for block_num in 100..240 {
            let block_data = create_mock_block_data(block_num);
            let key = keys::block_data_key(TEST_CHAIN_NAME, &block_data.hash);
            assert!(
                !storage.exists(&key)?,
                "Block {} should be deleted",
                block_num
            );
        }

        // Verify recent blocks still exist (check by hash)
        for block_num in 240..250 {
            let block_data = create_mock_block_data(block_num);
            let key = keys::block_data_key(TEST_CHAIN_NAME, &block_data.hash);
            assert!(
                storage.exists(&key)?,
                "Block {} should still exist",
                block_num
            );
        }

        // Verify min_block is updated to 240
        let updated_progress: ScannerProgress = storage.read_json(&progress_key)?.unwrap();
        assert_eq!(
            updated_progress.min_block,
            Some(240),
            "min_block should be updated to 240"
        );

        println!("✅ Batch delete test passed");
        Ok(())
    }

    #[tokio::test]
    async fn test_cleanup_no_existing_blocks() -> Result<()> {
        let (storage, _path) = create_test_storage("cleanup_no_blocks");
        let config = create_test_config(Some(10), true);

        // Create progress but don't store any blocks
        let progress = create_test_progress(120, Some(100));
        let progress_key = keys::progress_key(TEST_CHAIN_NAME);
        storage.write_json(&progress_key, &progress)?;

        let cleaner = EvmCleaner::new(config, storage.clone());

        // Cleanup should not fail, returns 0 when no blocks exist
        let result = cleaner.cleanup().await?;
        assert_eq!(result, 0, "Should return 0 when no blocks exist to clean");

        println!("✅ No existing blocks test passed");
        Ok(())
    }

    #[tokio::test]
    async fn test_cleanup_orphaned_disabled() -> Result<()> {
        let (storage, _path) = create_test_storage("orphaned_disabled");
        let config = create_test_config(Some(10), true);
        // cleanup_orphaned_enabled is false by default in create_test_config

        // Store some blocks
        store_mock_blocks(&storage, TEST_CHAIN_NAME, 100, 120)?;

        // Create progress: current=120, min=100
        let progress = create_test_progress(120, Some(100));
        let progress_key = keys::progress_key(TEST_CHAIN_NAME);
        storage.write_json(&progress_key, &progress)?;

        let cleaner = EvmCleaner::new(config, storage.clone());

        // Cleanup should only clean expired blocks, not orphaned data
        let result = cleaner.cleanup().await?;
        assert_eq!(
            result, 40,
            "Should clean 40 items (10 expired blocks: 10 indexes + 30 data items), but no orphaned data"
        );

        println!("✅ Orphaned cleanup disabled test passed");
        Ok(())
    }

    #[tokio::test]
    async fn test_cleanup_orphaned_enabled() -> Result<()> {
        let (storage, _path) = create_test_storage("orphaned_enabled");
        let mut config = create_test_config(Some(10), true);
        config.cleanup_orphaned_enabled = true; // Enable orphaned cleanup

        // Store some blocks
        store_mock_blocks(&storage, TEST_CHAIN_NAME, 100, 120)?;

        // Create progress: current=120, min=100
        let progress = create_test_progress(120, Some(100));
        let progress_key = keys::progress_key(TEST_CHAIN_NAME);
        storage.write_json(&progress_key, &progress)?;

        let cleaner = EvmCleaner::new(config, storage.clone());

        // Cleanup should clean both expired blocks and orphaned data
        let result = cleaner.cleanup().await?;
        assert_eq!(
            result, 40,
            "Should clean 40 items (10 expired blocks: 10 indexes + 30 data items + 0 orphaned)"
        );

        println!("✅ Orphaned cleanup enabled test passed");
        Ok(())
    }
}
