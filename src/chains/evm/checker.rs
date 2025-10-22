use crate::{
    config::ScannerConfig,
    core::table::BlockData,
    storage::manager::{ScannerBlockIndexStorage, ScannerProgressStorage},
    storage::rocksdb::RocksDBStorage,
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

    /// Handle reorg by updating block index and progress
    pub fn handle_reorg(&self, block_number: u64, block_data: &BlockData) -> Result<()> {
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

        // Move active index to history
        self.index.active_move_to_history(block_number)?;

        // Update progress
        self.update_back_progress(rollback_block - 1, Some(rollback_block))?;

        warn!(
            "⚠️ Reorg detected at block {}! Updated index to new hash: {}",
            rollback_block, block_data.hash
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
