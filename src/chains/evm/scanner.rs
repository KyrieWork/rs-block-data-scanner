use std::{sync::Arc, time::Duration};

use crate::{
    chains::evm::{
        checker::{BatchVerificationResult, EvmChecker},
        cleaner::EvmCleaner,
        client::EvmClient,
    },
    config::ScannerConfig,
    core::table::{BlockData, ScannerProgress, ScannerStatus},
    storage::manager::ScannerStorageManager,
};
use anyhow::Result;
use chrono::Utc;
use tokio::sync::broadcast;
use tokio::time::timeout;
use tracing::{debug, error, info};

pub struct EvmScanner {
    pub scanner_cfg: ScannerConfig,
    pub client: Arc<EvmClient>,
    pub storage_manager: Arc<ScannerStorageManager>,
    pub checker: Arc<EvmChecker>,
    pub cleaner: Arc<EvmCleaner>,
}

impl EvmScanner {
    pub fn new(
        scanner_cfg: ScannerConfig,
        client: Arc<EvmClient>,
        storage_manager: Arc<ScannerStorageManager>,
        checker: Arc<EvmChecker>,
        cleaner: Arc<EvmCleaner>,
    ) -> Self {
        Self {
            scanner_cfg,
            client,
            storage_manager,
            checker,
            cleaner,
        }
    }

    /// Initialize scanner progress
    pub async fn init(&self) -> Result<()> {
        // Check if progress already exists
        match self.storage_manager.progress.get() {
            Ok(existing_progress) => {
                info!(
                    "📊 Found existing progress, continuing from block: {}",
                    existing_progress.current_block
                );
                info!("✅ Scanner initialized with existing progress");
                Ok(())
            }
            Err(_) => {
                // No existing progress, initialize with start_block
                let start_block = if self.scanner_cfg.start_block == 0 {
                    // If start_block is 0, start from the latest block
                    let latest_block = self.client.get_latest_block_number().await?;
                    info!("🚀 Start block is 0, using latest block: {}", latest_block);
                    latest_block
                } else {
                    self.scanner_cfg.start_block
                };

                let progress = self
                    .storage_manager
                    .progress
                    .get_initial_progress(start_block);
                self.storage_manager.progress.update(progress)?;
                info!("✅ Scanner initialized with start_block: {}", start_block);
                Ok(())
            }
        }
    }

    /// Get target block and network latest block
    async fn get_target_block(&self) -> Result<(u64, u64)> {
        let progress = self.storage_manager.progress.get()?;
        let latest_block = self.client.get_latest_block_number().await?;

        // Calculate safe target block: latest_block - confirm_blocks
        let safe_target = latest_block.saturating_sub(self.scanner_cfg.confirm_blocks);

        // Target block should be at least current_block (never go backward)
        let target_block = safe_target.max(progress.current_block);

        Ok((target_block, latest_block))
    }

    /// Check if scanner is synced with the network
    fn is_synced(&self, current_block: u64, network_latest_block: u64) -> bool {
        let gap = network_latest_block.saturating_sub(current_block);
        gap <= self.scanner_cfg.confirm_blocks
    }

    /// Get dynamic scan interval based on sync status
    fn get_scan_interval(&self, current_block: u64, network_latest_block: u64) -> Duration {
        if self.is_synced(current_block, network_latest_block) {
            Duration::from_secs(self.scanner_cfg.synced_interval_secs)
        } else {
            Duration::from_millis(self.scanner_cfg.catching_up_interval_millis)
        }
    }

    /// Scan multiple blocks concurrently (for catching_up state)
    async fn scan_blocks_concurrent(
        &self,
        start_block: u64,
        count: usize,
    ) -> Result<Vec<BlockData>> {
        let mut handles = Vec::new();
        let timeout_secs = self.scanner_cfg.timeout_secs;

        for i in 0..count {
            let block_number = start_block + i as u64;
            let client = self.client.clone();

            let handle = tokio::spawn(async move {
                timeout(
                    Duration::from_secs(timeout_secs),
                    client.fetch_block_data_by_number(block_number),
                )
                .await
            });

            handles.push(handle);
        }

        let mut results = Vec::with_capacity(count);
        let mut failed_count = 0;

        // Process results in order to maintain block sequence
        for (i, handle) in handles.into_iter().enumerate() {
            let block_number = start_block + i as u64;
            match handle.await? {
                Ok(Ok(block_data)) => {
                    if block_data.hash.is_empty() {
                        error!("Empty block hash for block {}", block_number);
                        failed_count += 1;
                    } else {
                        results.push(block_data);
                    }
                }
                Ok(Err(e)) => {
                    error!("Failed to fetch block {}: {}", block_number, e);
                    failed_count += 1;
                }
                Err(e) => {
                    error!("Timeout fetching block {}: {}", block_number, e);
                    failed_count += 1;
                }
            }
        }

        // Allow some failures but not too many
        if failed_count > count / 2 {
            return Err(anyhow::anyhow!(
                "Too many blocks failed to fetch: {}/{}",
                failed_count,
                count
            ));
        }

        if results.is_empty() {
            return Err(anyhow::anyhow!("No blocks were successfully fetched"));
        }

        Ok(results)
    }

    /// Batch save blocks and handle cleanup
    async fn batch_save_blocks(&self, blocks: Vec<BlockData>) -> Result<()> {
        if blocks.is_empty() {
            return Ok(());
        }

        let mut cleanup_keys = Vec::new();

        // Check if we need cleanup based on data span
        let progress = self.storage_manager.progress.get()?;
        if let Some(min_block) = progress.min_block
            && progress.current_block - min_block >= self.scanner_cfg.cleanup_interval_blocks
        {
            let cleanup_result = self.cleaner.get_cleanup_keys()?;
            if !cleanup_result.is_empty() {
                let total_keys = cleanup_result.total_keys();
                cleanup_keys.extend(cleanup_result.keys_to_delete);
                info!("🧹 Cleanup: {} keys to delete", total_keys);
            }
        }

        // Use StorageManager to batch save blocks with indexes
        self.storage_manager
            .batch_save_blocks(blocks.clone(), cleanup_keys)?;

        // Log detailed block information for debugging
        if !blocks.is_empty() {
            let first_block = blocks[0].hash.clone();
            let last_block = blocks[blocks.len() - 1].hash.clone();
            if blocks.len() == 1 {
                debug!("✅ Batch saved 1 block (hash: {})", first_block);
            } else {
                debug!(
                    "✅ Batch saved {} blocks (first: {}, last: {})",
                    blocks.len(),
                    first_block,
                    last_block
                );
            }
        }
        Ok(())
    }

    /// Scan next block(s) based on current state
    async fn scan_next_blocks(&self) -> Result<ScannerProgress> {
        let mut progress = self.storage_manager.progress.get()?;
        let (target_block, network_latest_block) = self.get_target_block().await?;

        // Update network info
        progress.network_latest_block = Some(network_latest_block);
        progress.target_block = target_block;

        // Check if there are new blocks to scan
        if target_block > progress.current_block {
            let blocks_to_scan = (target_block - progress.current_block) as usize;

            // Get scan concurrency based on current status
            let concurrency = self.get_scan_concurrency()?;

            let scan_count = if self.is_synced(progress.current_block, network_latest_block) {
                // Synced: scan one block at a time
                1
            } else {
                // Catching up: scan multiple blocks concurrently based on status
                if concurrency > 1 {
                    let max_concurrent = std::cmp::min(blocks_to_scan, concurrency);
                    let max_batch = self.scanner_cfg.batch_save_size;
                    std::cmp::min(max_concurrent, max_batch)
                } else {
                    1
                }
            };

            // Ensure scan_count is valid
            if scan_count == 0 {
                return Err(anyhow::anyhow!("Invalid scan count: {}", scan_count));
            }

            let start_block = progress.current_block + 1;
            let blocks = self.scan_blocks_concurrent(start_block, scan_count).await?;

            // Verify batch continuity if enabled
            if self.scanner_cfg.reorg_check_enabled {
                let verification_result =
                    self.checker.verify_batch_continuity(&blocks, start_block)?;

                match verification_result {
                    BatchVerificationResult::Valid => {
                        // Verification passed, continue with saving
                    }
                    BatchVerificationResult::ReorgDetected {
                        reorg_block,
                        reorg_type: _,
                        suggested_rollback_depth,
                    } => {
                        // Handle reorg detection
                        self.handle_reorg_detection(reorg_block, suggested_rollback_depth)
                            .await?;
                        return self.storage_manager.progress.get();
                    }
                    BatchVerificationResult::ContinuityBroken {
                        broken_block,
                        error_type,
                    } => {
                        // Handle continuity break - reset success count
                        self.handle_continuity_break(broken_block, error_type)
                            .await?;
                        self.reset_success_count()?;
                        return self.storage_manager.progress.get();
                    }
                    BatchVerificationResult::ValidationError {
                        message,
                        block_number,
                    } => {
                        // Handle validation error - reset success count
                        self.handle_validation_error(message, block_number).await?;
                        self.reset_success_count()?;
                        return self.storage_manager.progress.get();
                    }
                }
            }

            // Save all blocks first
            self.batch_save_blocks(blocks).await?;

            // Update progress only after successful save
            let previous_block = progress.current_block;
            progress.current_block += scan_count as u64;
            progress.consecutive_success_count += 1;

            // Check if should exit reorg mode
            if progress.status == ScannerStatus::ReorgDetected
                && self.checker.should_exit_reorg_mode(
                    progress.consecutive_success_count,
                    progress.reorg_start_time,
                )
            {
                self.checker.exit_reorg_mode()?;
            }

            // Set status based on sync state
            let blocks_behind = target_block.saturating_sub(progress.current_block);
            progress.status = if blocks_behind > 10 {
                ScannerStatus::CatchingUp
            } else if blocks_behind > 0 {
                ScannerStatus::Scanning
            } else {
                ScannerStatus::Synced
            };

            progress.updated_at = Utc::now();
            self.storage_manager.progress.update(progress.clone())?;

            // Log detailed block information
            let block_range = if scan_count == 1 {
                format!("block {}", previous_block + 1)
            } else {
                format!("blocks {}-{}", previous_block + 1, progress.current_block)
            };

            info!(
                "✅ Scanned {} blocks ({}) - Status: {:?} - Current: {}",
                scan_count, block_range, progress.status, progress.current_block
            );
        } else {
            // Already caught up
            progress.status = ScannerStatus::Idle;
            progress.updated_at = Utc::now();
            self.storage_manager.progress.update(progress.clone())?;
            info!("🔄 Already caught up - Status: {:?}", progress.status);
        }

        Ok(progress)
    }

    /// Print final scanner status
    fn print_final_status(&self) -> Result<()> {
        info!("📊 Final scanner status:");
        let final_progress = self.storage_manager.progress.get()?;
        info!("  └─ Chain: {}", final_progress.chain);
        info!("  └─ Current block: {}", final_progress.current_block);
        info!("  └─ Target block: {}", final_progress.target_block);
        if let Some(network_latest) = final_progress.network_latest_block {
            info!("  └─ Network latest: {}", network_latest);
        }
        info!("  └─ Status: {:?}", final_progress.status);
        Ok(())
    }

    /// Run scanner with graceful shutdown support
    pub async fn run(&self, mut shutdown: broadcast::Receiver<()>) -> Result<()> {
        info!("🔄 Scanner loop started");

        loop {
            tokio::select! {
                // Branch 1: Wait for shutdown signal
                _ = shutdown.recv() => {
                    info!("🛑 Shutdown signal received, stopping scanner gracefully...");
                    break;
                }

                // Branch 2: Execute scanning logic
                result = self.scan_next_blocks() => {
                    match result {
                        Ok(progress) => {
                            // Calculate dynamic interval based on sync status
                            let network_latest = progress.network_latest_block.unwrap_or(progress.current_block);
                            let interval = self.get_scan_interval(progress.current_block, network_latest);

                            // Log sync status
                            let gap = network_latest.saturating_sub(progress.current_block);
                            if self.is_synced(progress.current_block, network_latest) {
                                debug!("⏱️  Synced with network (gap: {}), waiting {:?}", gap, interval);
                            } else {
                                debug!("⚡ Catching up (gap: {}), waiting {:?}", gap, interval);
                            }

                            tokio::time::sleep(interval).await;
                        }
                        Err(e) => {
                            error!("❌ Scan error: {}", e);
                            // Use configured interval on error
                            tokio::time::sleep(Duration::from_secs(self.scanner_cfg.error_interval_secs)).await;
                        }
                    }
                }
            }
        }

        // Print final status after loop exits
        self.print_final_status()?;
        info!("👋 Scanner stopped gracefully");
        Ok(())
    }

    /// Handle reorg detection
    async fn handle_reorg_detection(&self, reorg_block: u64, rollback_depth: u64) -> Result<()> {
        // Check for deep reorg
        self.checker
            .handle_deep_reorg(rollback_depth, reorg_block)?;

        // Execute rollback
        self.checker.handle_reorg(reorg_block)?;

        // Enter reorg mode
        self.checker.enter_reorg_mode(reorg_block)?;

        info!(
            "🔄 Reorg detected at block {}, entering reorg mode",
            reorg_block
        );

        Ok(())
    }

    /// Handle continuity break
    async fn handle_continuity_break(
        &self,
        broken_block: u64,
        error_type: crate::chains::evm::checker::ContinuityError,
    ) -> Result<()> {
        error!(
            "❌ Batch continuity broken at block {}: {:?}",
            broken_block, error_type
        );

        // Don't save any data, wait for next scan
        Ok(())
    }

    /// Handle validation error
    async fn handle_validation_error(
        &self,
        message: String,
        block_number: Option<u64>,
    ) -> Result<()> {
        error!(
            "❌ Validation error{}: {}",
            if let Some(block) = block_number {
                format!(" at block {}", block)
            } else {
                String::new()
            },
            message
        );

        // Don't save any data, wait for next scan
        Ok(())
    }

    /// Get scan concurrency based on current status
    fn get_scan_concurrency(&self) -> Result<usize> {
        let progress = self.storage_manager.progress.get()?;

        match progress.status {
            ScannerStatus::ReorgDetected => {
                // Reorg mode: only single block scanning
                debug!("Reorg detected mode: limiting concurrency to 1");
                Ok(1)
            }
            ScannerStatus::Error => {
                // Error state: no scanning allowed
                Err(anyhow::anyhow!("Scanner is in error state"))
            }
            _ => {
                // Normal mode: use configured concurrency
                Ok(self.scanner_cfg.concurrency)
            }
        }
    }

    /// Check if service is running
    #[allow(dead_code)]
    fn is_service_running(&self) -> Result<bool> {
        let progress = self.storage_manager.progress.get()?;

        match progress.status {
            ScannerStatus::Error => Ok(false),
            _ => Ok(true),
        }
    }

    /// Reset consecutive success count
    fn reset_success_count(&self) -> Result<()> {
        let mut progress = self.storage_manager.progress.get()?;
        progress.consecutive_success_count = 0;
        progress.updated_at = Utc::now();
        self.storage_manager.progress.update(progress)?;
        Ok(())
    }
}
