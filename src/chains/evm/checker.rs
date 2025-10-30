use crate::{
    chains::evm::context::EvmScannerContext,
    config::ScannerConfig,
    core::table::{BlockData, ScannerStatus},
    storage::manager::{ScannerBlockIndexStorage, ScannerProgressStorage},
};
use anyhow::Result;
use chrono::{DateTime, Utc};
use std::time::Duration;
use tracing::{debug, error, info, warn};

/// Batch verification result enumeration
#[derive(Debug, Clone)]
pub enum BatchVerificationResult {
    /// Verification passed, can save
    Valid,

    /// Reorg detected, need rollback
    ReorgDetected {
        /// Block number where reorg occurred
        reorg_block: u64,
        /// Type of reorg
        reorg_type: ReorgType,
        /// Suggested rollback depth
        suggested_rollback_depth: u64,
    },

    /// Batch internal continuity broken
    ContinuityBroken {
        /// Block number where continuity was broken
        broken_block: u64,
        /// Error type
        error_type: ContinuityError,
    },

    /// Other validation errors
    ValidationError {
        /// Error description
        message: String,
        /// Block number where error occurred
        block_number: Option<u64>,
    },
}

/// Reorg type enumeration
#[derive(Debug, Clone)]
pub enum ReorgType {
    /// Discontinuity with database stored blocks
    DatabaseDiscontinuity,
    /// Discontinuity within batch
    BatchInternalDiscontinuity,
}

/// Continuity error enumeration
#[derive(Debug, Clone)]
pub enum ContinuityError {
    /// Parent hash mismatch
    ParentHashMismatch,
    /// Block number gap
    BlockNumberGap,
    /// Duplicate block hash
    DuplicateBlockHash,
    /// Invalid block data format
    InvalidBlockData,
}

/// Validation error enumeration for error handling
#[derive(Debug, Clone)]
pub enum ValidationError {
    /// Network error, can retry
    NetworkError(String),
    /// Data format error, need to refetch
    DataFormatError(String),
    /// Reorg error, need rollback
    ReorgError {
        reorg_block: u64,
        rollback_depth: u64,
    },
    /// System error, need manual intervention
    SystemError(String),
}

impl ValidationError {
    pub fn should_retry(&self) -> bool {
        match self {
            ValidationError::NetworkError(_) => true,
            ValidationError::DataFormatError(_) => true,
            ValidationError::ReorgError { .. } => false,
            ValidationError::SystemError(_) => false,
        }
    }

    pub fn retry_delay(&self) -> Duration {
        match self {
            ValidationError::NetworkError(_) => Duration::from_secs(5),
            ValidationError::DataFormatError(_) => Duration::from_secs(1),
            _ => Duration::from_secs(0),
        }
    }
}

pub struct EvmChecker {
    context: EvmScannerContext,
}

impl EvmChecker {
    pub fn new(context: EvmScannerContext) -> Self {
        Self { context }
    }

    fn cfg(&self) -> &ScannerConfig {
        self.context.config.as_ref()
    }

    fn progress_store(&self) -> &ScannerProgressStorage {
        self.context.storage_manager.progress.as_ref()
    }

    fn index_store(&self) -> &ScannerBlockIndexStorage {
        self.context.storage_manager.block_index.as_ref()
    }

    /// Verify if a reorg has occurred by comparing parent hash
    /// Returns true if no reorg detected, false if reorg detected
    pub fn verify_reorg(&self, parent_hash: &str, block_number: u64) -> Result<bool> {
        // Skip reorg check for start_block or earlier
        if block_number <= self.cfg().start_block {
            debug!(
                "Skipping reorg check: block_number {} <= start_block {}",
                block_number,
                self.cfg().start_block
            );
            return Ok(true);
        }

        // Retrieve the hash of the previous block from the active index
        let prev_block_number = block_number - 1;
        let prev_block_index = self.index_store().get_active(prev_block_number).ok();

        let prev_block_index = match prev_block_index {
            Some(index) => index,
            None => {
                debug!(
                    "Previous block {} not found in active index, allowing scan to continue",
                    prev_block_number
                );
                return Ok(true);
            }
        };

        let prev_block_hash = prev_block_index.block_hash;
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
    pub fn handle_reorg(&self, block_number: u64) -> Result<()> {
        let progress = self.progress_store().get()?;
        let rollback_block = block_number - 1;

        // Check rollback depth limit
        let rollback_depth = progress.current_block.saturating_sub(rollback_block);
        let max_depth = self.cfg().max_rollback_depth;
        if rollback_depth >= max_depth {
            return Err(anyhow::anyhow!(
                "Reorg depth {} exceeds configured max rollback depth {} (rollback target block {})",
                rollback_depth,
                max_depth,
                rollback_block
            ));
        }

        // Check if rollback would go before start_block
        if rollback_block <= self.cfg().start_block {
            return Err(anyhow::anyhow!(
                "Cannot rollback before start_block ({})",
                self.cfg().start_block
            ));
        }

        // Move active index to history
        self.index_store().active_move_to_history(rollback_block)?;

        // Update progress
        let current_block = progress.current_block;
        self.update_back_progress(current_block.saturating_sub(1), Some(rollback_block))?;

        warn!(
            "⚠️ Reorg detected at block {}! Current block: {}, Updated progress to {}",
            block_number,
            current_block,
            rollback_block - 1
        );

        Ok(())
    }

    /// Update progress to reflect rollback state
    pub fn update_back_progress(&self, current_block: u64, reorg_block: Option<u64>) -> Result<()> {
        let mut progress = self.progress_store().get()?;

        progress.current_block = current_block;
        progress.status = ScannerStatus::ReorgDetected;
        progress.reorg_block = reorg_block;
        progress.updated_at = Utc::now();

        self.progress_store().update(progress)?;

        debug!(
            "📝 Updated progress after reorg - current: {}, reorg_block: {:?}",
            current_block, reorg_block
        );

        Ok(())
    }

    /// Verify batch continuity - main entry point for batch verification
    pub fn verify_batch_continuity(
        &self,
        blocks: &[BlockData],
        start_block: u64,
    ) -> Result<BatchVerificationResult> {
        if blocks.is_empty() {
            return Ok(BatchVerificationResult::Valid);
        }

        // 1. Verify first block continuity with database
        let first_block_verification =
            self.verify_first_block_continuity(&blocks[0], start_block)?;
        if !matches!(first_block_verification, BatchVerificationResult::Valid) {
            return Ok(first_block_verification);
        }

        // 2. Verify internal continuity within batch
        let internal_continuity = self.verify_internal_continuity(blocks)?;
        if !matches!(internal_continuity, BatchVerificationResult::Valid) {
            return Ok(internal_continuity);
        }

        Ok(BatchVerificationResult::Valid)
    }

    /// Verify first block continuity with database
    fn verify_first_block_continuity(
        &self,
        first_block: &BlockData,
        start_block: u64,
    ) -> Result<BatchVerificationResult> {
        if start_block <= self.cfg().start_block {
            return Ok(BatchVerificationResult::Valid);
        }

        if first_block.block_number != start_block {
            return Ok(BatchVerificationResult::ContinuityBroken {
                broken_block: first_block.block_number,
                error_type: ContinuityError::BlockNumberGap,
            });
        }

        let parent_hash = first_block.parent_hash.clone();
        let prev_block_number = start_block - 1;

        // Get previous block from database
        let prev_block_index = self.index_store().get_active(prev_block_number).ok();
        let prev_block_index = match prev_block_index {
            Some(index) => index,
            None => {
                debug!(
                    "Previous block {} not found in active index, allowing scan to continue",
                    prev_block_number
                );
                return Ok(BatchVerificationResult::Valid);
            }
        };

        let stored_hash = &prev_block_index.block_hash;

        debug!(
            "Comparing hashes - Stored: {}, Parent: {}",
            stored_hash, parent_hash
        );

        if stored_hash != &parent_hash {
            let rollback_depth = 1; // First block reorg is always depth 1
            return Ok(BatchVerificationResult::ReorgDetected {
                reorg_block: start_block,
                reorg_type: ReorgType::DatabaseDiscontinuity,
                suggested_rollback_depth: rollback_depth,
            });
        }

        Ok(BatchVerificationResult::Valid)
    }

    /// Verify internal continuity within batch
    fn verify_internal_continuity(&self, blocks: &[BlockData]) -> Result<BatchVerificationResult> {
        if blocks.len() <= 1 {
            return Ok(BatchVerificationResult::Valid);
        }

        // Check for duplicate block hashes
        let mut seen_hashes = std::collections::HashSet::new();
        for block in blocks {
            if !seen_hashes.insert(&block.hash) {
                return Ok(BatchVerificationResult::ContinuityBroken {
                    broken_block: block.block_number,
                    error_type: ContinuityError::DuplicateBlockHash,
                });
            }
        }

        // Verify parent-child relationships
        for i in 1..blocks.len() {
            let current_block = &blocks[i];
            let prev_block = &blocks[i - 1];
            let current_block_number = current_block.block_number;

            if current_block_number != prev_block.block_number + 1 {
                return Ok(BatchVerificationResult::ContinuityBroken {
                    broken_block: current_block_number,
                    error_type: ContinuityError::BlockNumberGap,
                });
            }

            if current_block.parent_hash != prev_block.hash {
                return Ok(BatchVerificationResult::ContinuityBroken {
                    broken_block: current_block_number,
                    error_type: ContinuityError::ParentHashMismatch,
                });
            }
        }

        Ok(BatchVerificationResult::Valid)
    }

    /// Enter reorg mode
    pub fn enter_reorg_mode(&self, reorg_block: u64) -> Result<()> {
        let mut progress = self.progress_store().get()?;

        progress.status = ScannerStatus::ReorgDetected;
        progress.reorg_count += 1;
        progress.reorg_block = Some(reorg_block);
        progress.reorg_start_time = Some(Utc::now());
        progress.consecutive_success_count = 0;
        progress.updated_at = Utc::now();

        let reorg_count = progress.reorg_count;
        self.progress_store().update(progress)?;

        warn!(
            "🔄 Entering reorg mode: block {}, reorg count: {}",
            reorg_block, reorg_count
        );

        Ok(())
    }

    /// Exit reorg mode
    pub fn exit_reorg_mode(&self) -> Result<()> {
        let mut progress = self.progress_store().get()?;

        progress.status = ScannerStatus::Scanning;
        progress.reorg_block = None;
        progress.reorg_start_time = None;
        progress.updated_at = Utc::now();

        self.progress_store().update(progress)?;

        info!("✅ Exited reorg mode, resuming normal operation");

        Ok(())
    }

    /// Check if should exit reorg mode based on provided parameters
    pub fn should_exit_reorg_mode(
        &self,
        consecutive_success_count: u64,
        reorg_start_time: Option<DateTime<Utc>>,
    ) -> bool {
        // Check consecutive success count
        if consecutive_success_count >= self.cfg().reorg_mode_exit_threshold {
            return true;
        }

        // Check reorg mode timeout
        if let Some(start_time) = reorg_start_time {
            let duration = Utc::now().signed_duration_since(start_time);
            if duration.num_seconds() > self.cfg().reorg_mode_exit_timeout as i64 {
                warn!("Reorg mode timeout, forcing exit");
                return true;
            }
        }

        false
    }

    /// Handle deep reorg detection
    pub fn handle_deep_reorg(&self, rollback_depth: u64, reorg_block: u64) -> Result<()> {
        if rollback_depth >= self.cfg().max_rollback_depth {
            let message = format!(
                "Reorg depth {} exceeds maximum rollback depth {} at block {}",
                rollback_depth,
                self.cfg().max_rollback_depth,
                reorg_block
            );
            error!("🚨 CRITICAL: {message}");
            self.record_shutdown_reason("Deep reorg detected", reorg_block)?;
            return Err(anyhow::anyhow!(message));
        }

        Ok(())
    }

    /// Record shutdown reason
    fn record_shutdown_reason(&self, reason: &str, block_number: u64) -> Result<()> {
        error!(
            "🛑 Critical scanner fault: {} at block {}",
            reason, block_number
        );
        Ok(())
    }
}
