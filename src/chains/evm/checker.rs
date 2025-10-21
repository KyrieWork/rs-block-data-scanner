use crate::{
    config::ScannerConfig,
    core::table::{BlockData, BlockIndex, BlockIndexHistory},
    storage::manager::{ScannerBlockIndexStorage, ScannerProgressStorage},
    storage::{rocksdb::RocksDBStorage, schema::keys, traits::KVStorage},
};
use anyhow::Result;
use chrono::Utc;
use std::sync::Arc;
use tracing::{debug, warn};

/// Maximum number of blocks to rollback during reorg detection
const MAX_REORG_DEPTH: u64 = 100;

pub struct EvmChecker {
    pub scanner_cfg: ScannerConfig,
    pub progress: Arc<ScannerProgressStorage>,
    pub index: Arc<ScannerBlockIndexStorage>,
    pub storage: Arc<RocksDBStorage>,
}

impl EvmChecker {
    pub fn new(
        scanner_cfg: ScannerConfig,
        progress: Arc<ScannerProgressStorage>,
        index: Arc<ScannerBlockIndexStorage>,
        storage: Arc<RocksDBStorage>,
    ) -> Self {
        Self {
            scanner_cfg,
            progress,
            index,
            storage,
        }
    }

    /// Verify if a reorg has occurred by comparing parent hash
    /// Returns true if no reorg detected, false if reorg detected
    pub fn verify_reorg(&self, parent_hash: &str, block_number: u64) -> Result<bool> {
        // Skip reorg check for start_block or earlier
        if block_number <= self.scanner_cfg.start_block {
            debug!(
                "Skipping reorg check: block_number {} <= start_block {}",
                block_number, self.scanner_cfg.start_block
            );
            return Ok(true);
        }

        // Retrieve the hash of the previous block from the active index
        let prev_block_number = block_number - 1;
        let prev_block_index = self.index.get_active(prev_block_number).ok();

        if prev_block_index.is_none() {
            debug!(
                "Previous block {} not found in active index, allowing scan to continue",
                prev_block_number
            );
            return Ok(true);
        }

        let prev_block_hash = prev_block_index.unwrap().block_hash;
        let stored_hash = &prev_block_hash;

        debug!(
            "Comparing hashes - Stored: {}, Parent: {}",
            stored_hash, parent_hash
        );
        let matches = stored_hash == parent_hash;
        debug!("Hash match result: {}", matches);

        Ok(matches)
    }

    /// Update block index on reorg - move current index to history and update active index
    pub fn update_block_index_on_reorg(
        &self,
        block_number: u64,
        new_block_data: &BlockData,
    ) -> Result<()> {
        let timestamp = Utc::now().timestamp();

        // Parse new block data to get parent_hash
        let block: serde_json::Value = serde_json::from_str(&new_block_data.block_data_json)?;
        let new_parent_hash = if let Some(parent) = block["header"]["parent_hash"].as_str() {
            parent.to_string()
        } else if let Some(parent) = block["parentHash"].as_str() {
            parent.to_string()
        } else {
            return Err(anyhow::anyhow!(
                "Failed to parse parentHash from block data"
            ));
        };

        // Prepare batch operations
        let mut batch_operations = Vec::new();

        // 1. Get current active index and move to history
        let active_key = keys::block_index_active_key(&self.scanner_cfg.chain_name, block_number);
        if let Some(current_index) = self.storage.read_json::<BlockIndex>(&active_key)? {
            // 2. Move current index to history table
            let history_key = keys::block_index_history_key(
                &self.scanner_cfg.chain_name,
                block_number,
                timestamp,
            );
            let history_entry = BlockIndexHistory {
                block_hash: current_index.block_hash.clone(),
                parent_hash: current_index.parent_hash.clone(),
                created_at: current_index.created_at,
                is_active: false,
                version: timestamp as u64,
                replaced_at: Some(Utc::now()),
            };
            batch_operations.push((history_key, serde_json::to_string(&history_entry)?));
            debug!(
                "📝 Moved block {} index to history: {}",
                block_number, current_index.block_hash
            );
        }

        // 3. Update active index
        let new_index = BlockIndex {
            block_hash: new_block_data.hash.clone(),
            parent_hash: new_parent_hash,
            created_at: Utc::now(),
        };
        batch_operations.push((active_key, serde_json::to_string(&new_index)?));
        debug!(
            "✅ Updated active index for block {}: {}",
            block_number, new_block_data.hash
        );

        // Execute batch write
        if !batch_operations.is_empty() {
            self.storage.batch_write(batch_operations)?;
        }

        Ok(())
    }

    /// Handle reorg by updating block index and progress
    pub fn handle_reorg(&self, block_number: u64, new_block_data: &BlockData) -> Result<()> {
        let progress = self.progress.get()?;
        let rollback_block = block_number - 1;

        // Check rollback depth limit
        let rollback_depth = progress.current_block.saturating_sub(rollback_block);
        if rollback_depth >= MAX_REORG_DEPTH {
            return Err(anyhow::anyhow!(
                "Reorg depth exceeds maximum limit ({}). Rollback stopped at block {}",
                MAX_REORG_DEPTH,
                rollback_block
            ));
        }

        // Check if rollback would go before start_block
        if rollback_block <= self.scanner_cfg.start_block {
            return Err(anyhow::anyhow!(
                "Cannot rollback before start_block ({})",
                self.scanner_cfg.start_block
            ));
        }

        // Update index instead of deleting data
        self.update_block_index_on_reorg(rollback_block, new_block_data)?;

        // Update progress
        self.update_back_progress(rollback_block - 1, Some(rollback_block))?;

        warn!(
            "⚠️ Reorg detected at block {}! Updated index to new hash: {}",
            rollback_block, new_block_data.hash
        );

        Ok(())
    }

    /// Update progress to reflect rollback state
    pub fn update_back_progress(&self, current_block: u64, reorg_block: Option<u64>) -> Result<()> {
        let mut progress = self.progress.get()?;

        progress.current_block = current_block;
        progress.status = "reorg_detected".to_string();
        progress.reorg_block = reorg_block;
        progress.updated_at = Utc::now();

        self.progress.update(progress)?;

        debug!(
            "📝 Updated progress after reorg - current: {}, reorg_block: {:?}",
            current_block, reorg_block
        );

        Ok(())
    }
}
