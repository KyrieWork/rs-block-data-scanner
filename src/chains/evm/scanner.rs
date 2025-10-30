use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use crate::{
    chains::evm::{
        checker::{BatchVerificationResult, EvmChecker},
        cleaner::{CleanupResult, EvmCleaner},
        client::EvmClient,
        context::EvmScannerContext,
    },
    config::ScannerConfig,
    core::table::{BlockData, ScannerProgress, ScannerStatus},
    storage::manager::ScannerStorageManager,
    utils::metrics::{BlockFetchFailureReason, ScannerMetrics},
};
use anyhow::{Context, Result};
use chrono::Utc;
use tokio::sync::broadcast;
use tokio::task;
use tokio::time::timeout;
use tracing::{debug, error, info};

pub struct EvmScanner {
    context: EvmScannerContext,
    pub client: Arc<EvmClient>,
    pub checker: Arc<EvmChecker>,
    pub cleaner: Arc<EvmCleaner>,
}

impl EvmScanner {
    pub fn new(
        context: EvmScannerContext,
        client: Arc<EvmClient>,
        checker: Arc<EvmChecker>,
        cleaner: Arc<EvmCleaner>,
    ) -> Self {
        Self {
            context,
            client,
            checker,
            cleaner,
        }
    }

    fn cfg(&self) -> &ScannerConfig {
        self.context.config.as_ref()
    }

    fn storage_manager(&self) -> &ScannerStorageManager {
        self.context.storage_manager.as_ref()
    }

    fn metrics(&self) -> &dyn ScannerMetrics {
        self.context.metrics.as_ref()
    }

    /// Initialize scanner progress
    pub async fn init(&self) -> Result<()> {
        // Check if progress already exists
        match self.storage_manager().progress.get() {
            Ok(existing_progress) => {
                info!(
                    "📊 Found existing progress, continuing from block: {}",
                    existing_progress.current_block
                );
                info!("✅ Scanner initialized with existing progress");
                self.storage_manager().progress.log_current_progress()?;
                Ok(())
            }
            Err(_) => {
                // No existing progress, initialize with start_block
                let start_block = if self.cfg().start_block == 0 {
                    // If start_block is 0, start from the latest block
                    let latest_block = self.client.get_latest_block_number().await?;
                    info!("🚀 Start block is 0, using latest block: {}", latest_block);
                    latest_block
                } else {
                    self.cfg().start_block
                };

                let progress = self
                    .storage_manager()
                    .progress
                    .get_initial_progress(start_block);
                self.storage_manager().progress.update(progress)?;
                info!("✅ Scanner initialized with start_block: {}", start_block);
                self.storage_manager().progress.log_current_progress()?;
                Ok(())
            }
        }
    }

    /// Get target block and network latest block
    async fn get_target_block(&self) -> Result<(u64, u64)> {
        let progress = self.storage_manager().progress.get()?;
        let latest_block = self.client.get_latest_block_number().await?;

        // Calculate safe target block: latest_block - confirm_blocks
        let safe_target = latest_block.saturating_sub(self.cfg().confirm_blocks);

        // Target block should be at least current_block (never go backward)
        let target_block = safe_target.max(progress.current_block);

        Ok((target_block, latest_block))
    }

    /// Check if scanner is synced with the network
    fn is_synced(&self, current_block: u64, network_latest_block: u64) -> bool {
        let gap = network_latest_block.saturating_sub(current_block);
        gap <= self.cfg().confirm_blocks
    }

    /// Get dynamic scan interval based on sync status
    fn get_scan_interval(&self, current_block: u64, network_latest_block: u64) -> Duration {
        if self.is_synced(current_block, network_latest_block) {
            Duration::from_secs(self.cfg().synced_interval_secs)
        } else {
            Duration::from_millis(self.cfg().catching_up_interval_millis)
        }
    }

    /// Scan multiple blocks concurrently (for catching_up state)
    async fn scan_blocks_concurrent(
        &self,
        start_block: u64,
        count: usize,
    ) -> Result<Vec<BlockData>> {
        let mut handles = Vec::new();
        let timeout_secs = self.cfg().timeout_secs;
        let semaphore = self.context.rpc_semaphore.clone();

        for i in 0..count {
            let block_number = start_block + i as u64;
            let client = self.client.clone();
            let permit = semaphore
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| anyhow::anyhow!("RPC concurrency semaphore closed"))?;

            let handle = tokio::spawn(async move {
                let _permit = permit;
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
                        self.metrics()
                            .record_block_fetch_failure(BlockFetchFailureReason::Empty);
                    } else {
                        self.metrics().record_block_fetch_success();
                        results.push(block_data);
                    }
                }
                Ok(Err(e)) => {
                    error!("Failed to fetch block {}: {}", block_number, e);
                    failed_count += 1;
                    self.metrics()
                        .record_block_fetch_failure(BlockFetchFailureReason::Rpc);
                }
                Err(e) => {
                    error!("Timeout fetching block {}: {}", block_number, e);
                    failed_count += 1;
                    self.metrics()
                        .record_block_fetch_failure(BlockFetchFailureReason::Timeout);
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
    async fn batch_save_blocks(&self, blocks: Vec<BlockData>) -> Result<Option<CleanupResult>> {
        if blocks.is_empty() {
            return Ok(None);
        }

        let mut cleanup_keys = Vec::new();
        let mut cleanup_result = None;

        // Check if we need cleanup based on data span
        let progress = self.storage_manager().progress.get()?;
        if let (Some(min_block), Some(retention_blocks)) =
            (progress.min_block, self.cfg().retention_blocks)
        {
            let span_distance = progress
                .current_block
                .saturating_sub(min_block)
                .saturating_sub(retention_blocks);

            if span_distance >= self.cfg().cleanup_interval_blocks {
                let get_cleanup_result = self.cleaner.get_cleanup_keys()?;
                if !get_cleanup_result.is_empty() {
                    let total_keys = get_cleanup_result.total_keys();
                    cleanup_keys.extend(get_cleanup_result.keys_to_delete.clone());
                    info!("🧹 Cleanup: {} keys to delete", total_keys);
                    cleanup_result = Some(get_cleanup_result);
                }
            }
        }

        let batch_size = blocks.len();
        let log_summary = blocks
            .first()
            .zip(blocks.last())
            .map(|(first, last)| (first.hash.clone(), last.hash.clone()));

        let storage_manager = self.context.storage_manager.clone();
        let cleanup_key_count = cleanup_keys.len();
        let storage_start = Instant::now();
        task::spawn_blocking(move || storage_manager.batch_save_blocks(blocks, cleanup_keys))
            .await
            .context("Blocking storage task failed")??;

        let storage_duration = storage_start.elapsed();
        self.metrics()
            .record_storage_batch(storage_duration, batch_size, cleanup_key_count);

        if let Some((first_hash, last_hash)) = log_summary {
            if batch_size == 1 {
                debug!("✅ Batch saved 1 block (hash: {})", first_hash);
            } else {
                debug!(
                    "✅ Batch saved {} blocks (first: {}, last: {})",
                    batch_size, first_hash, last_hash
                );
            }
        }
        Ok(cleanup_result)
    }

    /// Scan next block(s) based on current state
    async fn scan_next_blocks(&self) -> Result<ScannerProgress> {
        let mut progress = self.storage_manager().progress.get()?;
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
                    let max_batch = self.cfg().batch_save_size;
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
            if self.cfg().reorg_check_enabled {
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
                        return self.storage_manager().progress.get();
                    }
                    BatchVerificationResult::ContinuityBroken {
                        broken_block,
                        error_type,
                    } => {
                        // Handle continuity break - reset success count
                        self.handle_continuity_break(broken_block, error_type)
                            .await?;
                        self.reset_success_count()?;
                        return self.storage_manager().progress.get();
                    }
                    BatchVerificationResult::ValidationError {
                        message,
                        block_number,
                    } => {
                        // Handle validation error - reset success count
                        self.handle_validation_error(message, block_number).await?;
                        self.reset_success_count()?;
                        return self.storage_manager().progress.get();
                    }
                }
            }

            // Save all blocks first
            let last_block_number = blocks.last().map(|block| block.block_number);
            let cleanup_result = self.batch_save_blocks(blocks).await?;

            // Update progress only after successful save
            let previous_block = progress.current_block;
            if let Some(last_block_number) = last_block_number {
                progress.current_block = last_block_number;
            } else {
                progress.current_block += scan_count as u64;
            }
            progress.consecutive_success_count += 1;

            // Update min_block if cleanup was performed
            if let Some(cleanup) = cleanup_result
                && let Some(min_block) = cleanup.new_min_block
            {
                progress.min_block = Some(min_block);
            }

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
            self.storage_manager().progress.update(progress.clone())?;

            self.metrics()
                .record_blocks_processed(scan_count as u64, progress.current_block);

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
            self.storage_manager().progress.update(progress.clone())?;
            info!("🔄 Already caught up - Status: {:?}", progress.status);
            self.metrics()
                .record_blocks_processed(0, progress.current_block);
        }

        Ok(progress)
    }

    /// Print final scanner status
    fn print_final_status(&self) -> Result<()> {
        info!("📊 Final scanner status:");
        let final_progress = self.storage_manager().progress.get()?;
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
                            self.metrics().record_sync_gap(gap);
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
                            tokio::time::sleep(Duration::from_secs(self.cfg().error_interval_secs)).await;
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
        let progress = self.storage_manager().progress.get()?;

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
                Ok(self.cfg().concurrency)
            }
        }
    }

    /// Check if service is running
    #[allow(dead_code)]
    fn is_service_running(&self) -> Result<bool> {
        let progress = self.storage_manager().progress.get()?;

        match progress.status {
            ScannerStatus::Error => Ok(false),
            _ => Ok(true),
        }
    }

    /// Reset consecutive success count
    fn reset_success_count(&self) -> Result<()> {
        let mut progress = self.storage_manager().progress.get()?;
        progress.consecutive_success_count = 0;
        progress.updated_at = Utc::now();
        self.storage_manager().progress.update(progress)?;
        Ok(())
    }
}
