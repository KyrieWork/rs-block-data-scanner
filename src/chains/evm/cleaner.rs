use anyhow::Result;
use async_trait::async_trait;
use tracing::{debug, info};

use crate::{
    config::ScannerConfig,
    core::{cleaner::Cleaner, types::ScannerProgress},
    storage::{rocksdb::RocksDBStorage, schema::keys, traits::KVStorage},
};

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
            "🧹 Starting cleanup: current={}, retention={}, threshold={}, range=[{}, {})",
            current_block, retention_blocks, cleanup_threshold, min_stored_block, cleanup_threshold
        );

        // Batch delete for high performance
        let batch_size = self.scanner_cfg.cleanup_batch_size;
        let mut cleaned_count = 0;
        let total_to_clean = (cleanup_threshold - min_stored_block) as usize;

        let mut current_batch_start = min_stored_block;

        while current_batch_start < cleanup_threshold {
            let batch_end =
                std::cmp::min(current_batch_start + batch_size as u64, cleanup_threshold);

            // Collect keys to delete in this batch
            let mut keys_to_delete: Vec<Vec<u8>> = Vec::with_capacity(batch_size * 2);

            for block_num in current_batch_start..batch_end {
                let block_data_key = keys::block_data_key(&self.scanner_cfg.chain_name, block_num);
                let block_receipts_key =
                    keys::block_receipts_key(&self.scanner_cfg.chain_name, block_num);

                // Directly add to delete queue, RocksDB will ignore non-existent keys
                keys_to_delete.push(block_data_key.into_bytes());
                keys_to_delete.push(block_receipts_key.into_bytes());
            }

            // Batch delete
            if !keys_to_delete.is_empty() {
                self.storage.delete_batch(&keys_to_delete)?;
                // Calculate cleaned count from number of keys (2 keys per block)
                let batch_cleaned = keys_to_delete.len() / 2;
                cleaned_count += batch_cleaned;
            }

            // Log progress
            if cleaned_count > 0 {
                let progress_pct = (cleaned_count * 100) / total_to_clean.max(1);
                debug!(
                    "  └─ Cleaned {}/{} blocks ({}%)",
                    cleaned_count, total_to_clean, progress_pct
                );
            }

            current_batch_start = batch_end;
        }

        // Update min_block in progress
        if cleaned_count > 0 {
            let updated_progress = ScannerProgress {
                min_block: Some(cleanup_threshold),
                ..progress
            };
            self.storage.write_json(&progress_key, &updated_progress)?;

            info!(
                "✅ Cleanup completed: {} blocks removed, new min_block: {}",
                cleaned_count, cleanup_threshold
            );
        } else {
            debug!("No blocks found to clean in the specified range");
        }

        Ok(cleaned_count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        core::types::{BlockData, ScannerProgress},
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
            block_data_json: format!(r#"{{"number":"{}"}}"#, block_num),
            block_receipts_json: "[]".to_string(),
        }
    }

    // Helper to store mock blocks in storage
    fn store_mock_blocks(
        storage: &RocksDBStorage,
        chain_name: &str,
        start: u64,
        end: u64,
    ) -> Result<()> {
        for block_num in start..end {
            let block_data = create_mock_block_data(block_num);
            let block_data_key = keys::block_data_key(chain_name, block_num);
            let block_receipts_key = keys::block_receipts_key(chain_name, block_num);

            storage.write_json(&block_data_key, &block_data)?;
            storage.write(&block_receipts_key, &block_data.block_receipts_json)?;
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
        let result = cleaner.cleanup().await?;
        assert_eq!(result, 10, "Should attempt to clean 10 blocks");

        // Verify blocks 100-109 are deleted
        for block_num in 100..110 {
            let key = keys::block_data_key(TEST_CHAIN_NAME, block_num);
            assert!(
                !storage.exists(&key)?,
                "Block {} should be deleted",
                block_num
            );
        }

        // Verify blocks 110-119 still exist
        for block_num in 110..120 {
            let key = keys::block_data_key(TEST_CHAIN_NAME, block_num);
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
        let result = cleaner.cleanup().await?;
        assert_eq!(result, 140, "Should attempt to clean 140 blocks");

        // Verify old blocks are deleted
        for block_num in 100..240 {
            let key = keys::block_data_key(TEST_CHAIN_NAME, block_num);
            assert!(
                !storage.exists(&key)?,
                "Block {} should be deleted",
                block_num
            );
        }

        // Verify recent blocks still exist
        for block_num in 240..250 {
            let key = keys::block_data_key(TEST_CHAIN_NAME, block_num);
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

        // Cleanup should not fail, returns attempt count even if blocks don't exist
        let result = cleaner.cleanup().await?;
        assert_eq!(
            result, 10,
            "Should attempt to clean 10 blocks (RocksDB ignores non-existent keys)"
        );

        println!("✅ No existing blocks test passed");
        Ok(())
    }
}
