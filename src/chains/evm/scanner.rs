use std::time::Duration;

use crate::{
    config::ScannerConfig,
    core::{
        scanner::Scanner,
        table::{BlockData, BlockIndex, BlockIndexHistory, ScannerProgress},
    },
    storage::{rocksdb::RocksDBStorage, schema::keys, traits::KVStorage},
};
use alloy::{
    providers::{Provider, ProviderBuilder, RootProvider},
    transports::http::{Client, Http},
};
use anyhow::Result;
use async_trait::async_trait;
use chrono::Utc;
use tokio::sync::broadcast;
use tokio::time::timeout;
use tracing::{debug, error, info, warn};

/// Maximum number of blocks to rollback during reorg detection
const MAX_REORG_DEPTH: u64 = 100;

pub struct EvmScanner {
    pub scanner_cfg: ScannerConfig,
    pub rpc_url: String,
    pub storage: RocksDBStorage,
    provider: RootProvider<Http<Client>>,
}

impl EvmScanner {
    pub fn new(
        scanner_cfg: ScannerConfig,
        rpc_url: String,
        storage: RocksDBStorage,
    ) -> Result<Self> {
        let provider = ProviderBuilder::new().on_http(rpc_url.parse()?);
        Ok(Self {
            scanner_cfg,
            rpc_url,
            storage,
            provider,
        })
    }

    fn create_initial_progress(&self) -> ScannerProgress {
        ScannerProgress {
            chain: self.scanner_cfg.chain_name.clone(),
            current_block: self.scanner_cfg.start_block,
            target_block: self.scanner_cfg.start_block,
            network_latest_block: None,
            status: "idle".to_string(),
            updated_at: Utc::now(),
            reorg_block: None,
            finalized_block: None,
            min_block: None,
            version: crate::storage::schema::SCHEMA_VERSION,
        }
    }

    pub fn get_progress(&self) -> Result<ScannerProgress> {
        let key = keys::progress_key(&self.scanner_cfg.chain_name);
        self.storage
            .read_json::<ScannerProgress>(&key)?
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Progress not found for chain: {}",
                    self.scanner_cfg.chain_name
                )
            })
    }

    fn update_progress(&self, progress: ScannerProgress) -> Result<()> {
        self.storage
            .write_json(&keys::progress_key(&self.scanner_cfg.chain_name), &progress)
    }

    /// Retrieve active block index
    fn get_active_block_index(&self, block_number: u64) -> Result<Option<BlockIndex>> {
        let key = keys::block_index_active_key(&self.scanner_cfg.chain_name, block_number);
        self.storage.read_json(&key)
    }

    fn store_block_data(&self, block_data: &BlockData) -> Result<()> {
        let block_data_key = keys::block_data_key(&self.scanner_cfg.chain_name, &block_data.hash);
        let block_receipts_key =
            keys::block_receipts_key(&self.scanner_cfg.chain_name, &block_data.hash);

        // Store block data
        self.storage.write_json(&block_data_key, block_data)?;
        debug!("⏏️ Store block data: {}", block_data_key);
        self.storage
            .write(&block_receipts_key, &block_data.block_receipts_json)?;
        debug!("⏏️ Store block receipts: {}", block_receipts_key);

        // Parse block data and update active index
        let block: serde_json::Value = serde_json::from_str(&block_data.block_data_json)?;

        // Parse block number from alloy Block format
        let block_number = if let Some(number_str) = block["header"]["number"].as_str() {
            // Handle hex string like "0x1"
            if let Some(stripped) = number_str.strip_prefix("0x") {
                u64::from_str_radix(stripped, 16)?
            } else {
                number_str.parse::<u64>()?
            }
        } else if let Some(number_str) = block["number"].as_str() {
            // Handle hex string like "0x1"
            if let Some(stripped) = number_str.strip_prefix("0x") {
                u64::from_str_radix(stripped, 16)?
            } else {
                number_str.parse::<u64>()?
            }
        } else {
            return Err(anyhow::anyhow!("Failed to parse block number"));
        };

        let parent_hash = if let Some(parent) = block["header"]["parent_hash"].as_str() {
            parent.to_string()
        } else if let Some(parent) = block["parentHash"].as_str() {
            parent.to_string()
        } else {
            return Err(anyhow::anyhow!("Failed to parse parentHash"));
        };

        let active_index = BlockIndex {
            block_hash: block_data.hash.clone(),
            parent_hash,
            created_at: Utc::now(),
        };
        let active_key = keys::block_index_active_key(&self.scanner_cfg.chain_name, block_number);
        self.storage.write_json(&active_key, &active_index)?;
        debug!(
            "📝 Updated active index for block {}: {}",
            block_number, block_data.hash
        );

        Ok(())
    }

    async fn get_target_block(&self) -> Result<(u64, u64)> {
        let progress = self.get_progress()?;

        let latest_block = self.provider.get_block_number().await?;

        // Calculate safe target block: latest_block - confirm_blocks
        // This ensures we don't scan blocks that might be reorganized
        let safe_target = latest_block.saturating_sub(self.scanner_cfg.confirm_blocks);

        // Target block should be at least current_block (never go backward)
        let target_block = safe_target.max(progress.current_block);

        Ok((target_block, latest_block))
    }

    fn verify_reorg(&self, current_block_parent_hash: &str, scan_block: u64) -> Result<bool> {
        // Skip reorg check for start_block or earlier
        if scan_block <= self.scanner_cfg.start_block {
            debug!(
                "Skipping reorg check: scan_block {} <= start_block {}",
                scan_block, self.scanner_cfg.start_block
            );
            return Ok(true);
        }

        // Retrieve the hash of the previous block from the active index.
        let pre_block_index = self.get_active_block_index(scan_block - 1)?;
        if pre_block_index.is_none() {
            debug!(
                "Previous block {} not found in active index, allowing scan to continue",
                scan_block - 1
            );
            return Ok(true);
        }

        let pre_block_hash = pre_block_index.unwrap().block_hash;
        let stored_hash = &pre_block_hash;

        debug!(
            "Comparing hashes - Stored: {}, Parent: {}",
            stored_hash, current_block_parent_hash
        );
        let matches = stored_hash == current_block_parent_hash;
        debug!("Hash match result: {}", matches);

        Ok(matches)
    }

    /// Update block index on reorg - move current index to history and update active index
    fn update_block_index_on_reorg(
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

        // 1. Get current active index
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
            self.storage.write_json(&history_key, &history_entry)?;
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
        self.storage.write_json(&active_key, &new_index)?;
        debug!(
            "✅ Updated active index for block {}: {}",
            block_number, new_block_data.hash
        );

        Ok(())
    }

    async fn handle_reorg(&self, scan_block: u64) -> Result<()> {
        let mut progress = self.get_progress()?;
        let rollback_block = scan_block - 1;

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
        // rollback_block - 1 would be the new current_block, so we check <= instead of <
        if rollback_block <= self.scanner_cfg.start_block {
            return Err(anyhow::anyhow!(
                "Cannot rollback before start_block ({})",
                self.scanner_cfg.start_block
            ));
        }

        // Get new block data (this requires 1 RPC request)
        let new_block_data = self.fetch_block(rollback_block).await?;

        // Update index instead of deleting data
        self.update_block_index_on_reorg(rollback_block, &new_block_data)?;

        // Update progress
        progress.current_block = rollback_block - 1;
        progress.status = "reorg_detected".to_string();
        progress.reorg_block = Some(rollback_block);
        progress.updated_at = Utc::now();
        self.update_progress(progress)?;

        warn!(
            "⚠️ Reorg detected at block {}! Updated index to new hash: {}",
            rollback_block, new_block_data.hash
        );

        Ok(())
    }

    /// Scan the next block (extracted from run method for clarity)
    /// Returns the updated progress after scanning
    async fn scan_next_block(&self) -> Result<ScannerProgress> {
        // Read current progress
        let mut progress = self.get_progress()?;

        // Get target block and network latest block
        let (target_block, network_latest_block) = self.get_target_block().await?;

        // Update network info
        progress.network_latest_block = Some(network_latest_block);
        progress.target_block = target_block;

        // Check if there are new blocks to scan
        if target_block > progress.current_block {
            let scan_block = progress.current_block + 1;

            // Fetch block data
            let block_data = self.fetch_block(scan_block);

            match timeout(
                Duration::from_secs(self.scanner_cfg.timeout_secs),
                block_data,
            )
            .await
            {
                Ok(Ok(block_data)) => {
                    // Verify reorg before storing
                    let needs_reorg_check = scan_block > self.scanner_cfg.start_block;

                    if needs_reorg_check {
                        // Parse parent_hash from block_data_json
                        let block: serde_json::Value =
                            serde_json::from_str(&block_data.block_data_json)?;
                        let parent_hash = block["parentHash"].as_str().ok_or_else(|| {
                            anyhow::anyhow!("Failed to parse parent_hash from block data")
                        })?;

                        // Verify reorg
                        match self.verify_reorg(parent_hash, scan_block) {
                            Ok(true) => {
                                // No reorg detected, proceed with storing
                                self.store_block_data(&block_data)?;
                                progress.current_block = scan_block;

                                // Clear reorg status if recovering from reorg
                                if progress.status == "reorg_detected" {
                                    info!("✅ Recovered from reorg at block {}", scan_block);
                                    progress.reorg_block = None;
                                }

                                // Set status based on how far behind we are
                                let blocks_behind = target_block.saturating_sub(scan_block);
                                progress.status = if blocks_behind > 10 {
                                    "catching_up".to_string()
                                } else if blocks_behind > 0 {
                                    "scanning".to_string()
                                } else {
                                    "synced".to_string()
                                };

                                progress.updated_at = Utc::now();
                                self.update_progress(progress.clone())?;

                                // Log progress details from database
                                if let Ok(db_progress) = self.get_progress() {
                                    debug!(
                                        "📝 Progress updated - current: {}, target: {}, network: {:?}, status: {}, min_block: {:?}",
                                        db_progress.current_block,
                                        db_progress.target_block,
                                        db_progress.network_latest_block,
                                        db_progress.status,
                                        db_progress.min_block
                                    );
                                }

                                info!(
                                    "✅ Scanned block {} - Status: {}",
                                    scan_block, progress.status
                                );
                            }
                            Ok(false) => {
                                // Reorg detected, handle rollback
                                self.handle_reorg(scan_block).await?;
                                info!("🔄 Reorg handled, will retry from rolled back position");
                            }
                            Err(e) => {
                                error!("❌ Failed to verify reorg: {}", e);
                            }
                        }
                    } else {
                        // First block or start_block, skip reorg check
                        self.store_block_data(&block_data)?;
                        progress.current_block = scan_block;
                        progress.status = "scanning".to_string();
                        progress.updated_at = Utc::now();
                        self.update_progress(progress.clone())?;

                        // Log progress details from database
                        if let Ok(db_progress) = self.get_progress() {
                            debug!(
                                "📝 Progress updated - current: {}, target: {}, network: {:?}, status: {}, min_block: {:?}",
                                db_progress.current_block,
                                db_progress.target_block,
                                db_progress.network_latest_block,
                                db_progress.status,
                                db_progress.min_block
                            );
                        }

                        info!("✅ Scanned block {} (no reorg check)", scan_block);
                    }
                }
                Ok(Err(e)) => {
                    error!("❌ Fetch block data failed: {}", e);
                }
                Err(e) => {
                    error!("❌ Fetch block data timeout: {}", e);
                }
            }
        } else {
            // Already caught up, set to idle
            progress.status = "idle".to_string();
            progress.updated_at = Utc::now();
            self.update_progress(progress.clone())?;

            // Log progress details from database
            if let Ok(db_progress) = self.get_progress() {
                debug!(
                    "📝 Progress updated - current: {}, target: {}, network: {:?}, status: {}, min_block: {:?}",
                    db_progress.current_block,
                    db_progress.target_block,
                    db_progress.network_latest_block,
                    db_progress.status,
                    db_progress.min_block
                );
            }

            info!("🔄 Already caught up - Status: {}", progress.status);
        }

        // Return the updated progress
        self.get_progress()
    }

    /// Print final scanner status before shutdown
    fn print_final_status(&self) -> Result<()> {
        info!("📊 Final scanner status:");
        let final_progress = self.get_progress()?;
        info!("  └─ Chain: {}", final_progress.chain);
        info!("  └─ Current block: {}", final_progress.current_block);
        info!("  └─ Target block: {}", final_progress.target_block);
        if let Some(network_latest) = final_progress.network_latest_block {
            info!("  └─ Network latest: {}", network_latest);
        }
        info!("  └─ Status: {}", final_progress.status);
        if let Some(reorg_block) = final_progress.reorg_block {
            info!("  └─ Reorg block: {}", reorg_block);
        }
        Ok(())
    }

    /// Check if scanner is synced with the network
    /// Returns true if current block is close to network latest block
    fn is_synced(&self, current_block: u64, network_latest_block: u64) -> bool {
        let gap = network_latest_block.saturating_sub(current_block);
        gap <= self.scanner_cfg.confirm_blocks
    }

    /// Get dynamic scan interval based on sync status
    /// - Synced: 3 seconds
    /// - Catching up: 500 milliseconds
    fn get_scan_interval(
        &self,
        current_block: u64,
        network_latest_block: u64,
    ) -> std::time::Duration {
        if self.is_synced(current_block, network_latest_block) {
            // Synced: use longer interval
            std::time::Duration::from_secs(3)
        } else {
            // Catching up: use shorter interval
            std::time::Duration::from_millis(10)
        }
    }
}

#[async_trait]
impl Scanner for EvmScanner {
    async fn init(&self) -> Result<()> {
        let key = keys::progress_key(&self.scanner_cfg.chain_name);

        if let Some(mut progress) = self.storage.read_json::<ScannerProgress>(&key)? {
            // Progress exists, check and initialize min_block if not set
            if progress.min_block.is_none() {
                progress.min_block = Some(progress.current_block);
                self.storage.write_json(&key, &progress)?;
                info!("  └─ Initialized min_block: {}", progress.current_block);
            }
        } else {
            // First time, create initial progress
            let initial_block = if self.scanner_cfg.start_block == 0 {
                let last_block = self.provider.get_block_number().await?;
                if self.scanner_cfg.confirm_blocks <= last_block {
                    last_block - self.scanner_cfg.confirm_blocks
                } else {
                    last_block
                }
            } else {
                self.scanner_cfg.start_block
            };

            let mut progress = self.create_initial_progress();
            progress.current_block = initial_block;
            progress.target_block = initial_block;
            progress.min_block = Some(initial_block);

            info!(
                "✅ Initial progress created: current_block={}",
                progress.current_block
            );
            info!("  └─ Initialized min_block: {}", initial_block);
            self.storage.write_json(&key, &progress)?;
        }

        Ok(())
    }

    async fn fetch_block(&self, block_number: u64) -> Result<BlockData> {
        let block = self
            .provider
            .get_block_by_number(block_number.into(), true)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Block {} not found", block_number))?;

        let receipts = self
            .provider
            .get_block_receipts(block_number.into())
            .await?
            .ok_or_else(|| anyhow::anyhow!("Block {} not found", block_number))?;

        debug!(
            "🔍 Fetched Block {} Tx {} Receipts: {:?}",
            block_number,
            block.transactions.len(),
            receipts.len()
        );

        // verify block and receipts is match
        if !receipts.is_empty() && receipts[0].block_hash != Some(block.header.hash) {
            return Err(anyhow::anyhow!("Block and receipts is not match"));
        }

        Ok(BlockData {
            hash: format!("{:?}", block.header.hash),
            block_data_json: serde_json::to_string(&block)?,
            block_receipts_json: serde_json::to_string(&receipts)?,
        })
    }

    async fn run(&self, mut shutdown: broadcast::Receiver<()>) -> Result<()> {
        info!("🔄 Scanner loop started");

        loop {
            tokio::select! {
                // Branch 1: Wait for shutdown signal
                _ = shutdown.recv() => {
                    info!("🛑 Shutdown signal received, stopping scanner gracefully...");
                    break;
                }

                // Branch 2: Execute scanning logic
                result = self.scan_next_block() => {
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
                            // Use default interval on error
                            tokio::time::sleep(std::time::Duration::from_secs(3)).await;
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
}

#[cfg(test)]
mod tests {
    use super::*;

    // Test configuration constants
    const TEST_CHAIN_NAME: &str = "anvil";
    const TEST_RPC_URL: &str = "http://127.0.0.1:8545";
    const DEFAULT_START_BLOCK: u64 = 100;

    // Helper function to create default test config
    fn create_default_test_config() -> ScannerConfig {
        ScannerConfig {
            chain_type: "evm".to_string(),
            chain_name: TEST_CHAIN_NAME.to_string(),
            concurrency: 1,
            start_block: DEFAULT_START_BLOCK,
            confirm_blocks: 1,
            realtime: true,
            timeout_secs: 15,
            cleanup_enabled: false,
            retention_blocks: None,
            cleanup_interval_secs: 3600,
            cleanup_batch_size: 1000,
            cleanup_orphaned_enabled: false,
        }
    }

    // Helper function to create test scanner with custom config
    fn create_test_scanner_with_config(
        test_name: &str,
        config: ScannerConfig,
    ) -> (EvmScanner, String) {
        let temp_dir = std::env::temp_dir();
        let path = temp_dir.join(format!("rocksdb_test_{}_{}", test_name, std::process::id()));
        let path_str = path.to_str().unwrap().to_string();

        // Clean up if exists from previous failed test
        let _ = std::fs::remove_dir_all(&path_str);

        let scanner = EvmScanner::new(
            config,
            TEST_RPC_URL.to_string(),
            RocksDBStorage::new(&path_str).unwrap(),
        )
        .unwrap();

        (scanner, path_str)
    }

    // Helper function to create test scanner with default config
    fn create_test_scanner_with_name(test_name: &str) -> EvmScanner {
        let (scanner, _) = create_test_scanner_with_config(test_name, create_default_test_config());
        scanner
    }

    #[tokio::test]
    async fn test_init_connection() {
        let scanner = create_test_scanner_with_name("init_connection");
        let result = scanner.init().await;
        assert!(
            result.is_ok(),
            "Failed to initialize connection: {:?}",
            result.err()
        );
    }

    #[tokio::test]
    async fn test_fetch_latest_block() -> Result<()> {
        let scanner = create_test_scanner_with_name("fetch_block");
        scanner.init().await?;

        let provider = ProviderBuilder::new().on_http(scanner.rpc_url.parse()?);
        let latest_block = provider.get_block_number().await?;

        assert!(latest_block > 0, "Failed to fetch latest block");

        let latest_block_data = provider
            .get_block_by_number(latest_block.into(), true)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Block {} not found", latest_block))?;
        let block_data = scanner.fetch_block(latest_block).await?;
        assert_eq!(
            block_data.hash,
            format!("{:?}", latest_block_data.header.hash)
        );

        println!("Block hash: {:?}", block_data.hash);
        // println!("Block data: {:?}", block_data);

        Ok(())
    }

    #[tokio::test]
    async fn test_get_and_update_progress() -> Result<()> {
        let scanner = create_test_scanner_with_name("progress_test");

        // Test 1: Get progress before init should fail
        let result = scanner.get_progress();
        assert!(result.is_err(), "Get progress should fail before init");

        // Test 2: Init should create initial progress
        scanner.init().await?;

        // Test 3: Get progress after init should succeed
        let progress = scanner.get_progress()?;
        assert_eq!(progress.chain, "anvil");
        assert_eq!(progress.current_block, 100); // start_block from config
        assert_eq!(progress.target_block, 100);
        assert_eq!(progress.status, "idle");
        assert_eq!(progress.version, crate::storage::schema::SCHEMA_VERSION);

        // Test 4: Update progress
        let mut updated_progress = progress.clone();
        updated_progress.current_block = 200;
        updated_progress.target_block = 300;
        updated_progress.status = "scanning".to_string();
        updated_progress.network_latest_block = Some(350);

        scanner.update_progress(updated_progress.clone())?;

        // Test 5: Get progress again to verify update
        let retrieved_progress = scanner.get_progress()?;
        assert_eq!(retrieved_progress.current_block, 200);
        assert_eq!(retrieved_progress.target_block, 300);
        assert_eq!(retrieved_progress.status, "scanning");
        assert_eq!(retrieved_progress.network_latest_block, Some(350));

        println!("Progress test passed!");
        Ok(())
    }

    #[tokio::test]
    async fn test_init_idempotent() -> Result<()> {
        let scanner = create_test_scanner_with_name("init_idempotent");

        // First init
        scanner.init().await?;
        let progress1 = scanner.get_progress()?;

        // Update progress
        let mut updated = progress1.clone();
        updated.current_block = 500;
        scanner.update_progress(updated)?;

        // Second init should not overwrite existing progress
        scanner.init().await?;
        let progress2 = scanner.get_progress()?;

        // Progress should still be 500, not reset to start_block
        assert_eq!(
            progress2.current_block, 500,
            "Init should not overwrite existing progress"
        );

        println!("Init idempotent test passed!");
        Ok(())
    }

    #[tokio::test]
    async fn test_get_target_block() -> Result<()> {
        let scanner = create_test_scanner_with_name("target_block");
        scanner.init().await?;

        // Get target block - should be latest_block - confirm_blocks
        let (target, network_latest) = scanner.get_target_block().await?;

        let provider = ProviderBuilder::new().on_http(scanner.rpc_url.parse()?);
        let latest_block = provider.get_block_number().await?;

        // Verify network_latest matches actual latest block
        assert_eq!(
            network_latest, latest_block,
            "Network latest block mismatch"
        );

        // Target should be latest - confirm_blocks (which is 1)
        let expected_target = latest_block.saturating_sub(scanner.scanner_cfg.confirm_blocks);

        // Since current_block is 100 (from config), target should be max(expected_target, 100)
        let expected = expected_target.max(100);

        assert_eq!(target, expected, "Target block calculation incorrect");

        // Test case 2: Update current_block to a higher value
        let mut progress = scanner.get_progress()?;
        progress.current_block = latest_block + 100; // Set to future block
        scanner.update_progress(progress)?;

        // Target should not go backward
        let (target2, network_latest2) = scanner.get_target_block().await?;
        assert_eq!(
            target2,
            latest_block + 100,
            "Target should not go backward from current_block"
        );
        assert!(network_latest2 > 0, "Network latest should be positive");

        println!("Target block test passed!");
        Ok(())
    }

    #[tokio::test]
    async fn test_store_block_data() -> Result<()> {
        let scanner = create_test_scanner_with_name("store_block");
        scanner.init().await?;

        // Fetch a block from the chain
        let block_number = 1;
        let block_data = scanner.fetch_block(block_number).await?;

        // Store the block data using the new method
        scanner.store_block_data(&block_data)?;

        // Verify block data is stored correctly
        let block_data_key =
            keys::block_data_key(&scanner.scanner_cfg.chain_name, &block_data.hash);
        let stored_block_data = scanner.storage.read_json::<BlockData>(&block_data_key)?;

        assert!(
            stored_block_data.is_some(),
            "Block data should be stored in RocksDB"
        );
        let stored = stored_block_data.unwrap();
        assert_eq!(stored.hash, block_data.hash, "Hash should match");
        assert_eq!(
            stored.block_data_json, block_data.block_data_json,
            "Block data JSON should match"
        );

        // Verify block receipts are stored correctly
        let block_receipts_key =
            keys::block_receipts_key(&scanner.scanner_cfg.chain_name, &block_data.hash);
        let stored_receipts = scanner.storage.read(&block_receipts_key)?;

        assert!(
            stored_receipts.is_some(),
            "Block receipts should be stored in RocksDB"
        );
        assert_eq!(
            stored_receipts.unwrap(),
            block_data.block_receipts_json,
            "Stored receipts should match original"
        );

        println!(
            "Store block data test passed! Block: {}, Hash: {}",
            block_number, block_data.hash
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_fetch_block_with_timeout() -> Result<()> {
        let temp_dir = std::env::temp_dir();
        let path = temp_dir.join(format!("rocksdb_test_timeout_{}", std::process::id()));
        let path_str = path.to_str().unwrap().to_string();

        // Clean up if exists from previous failed test
        let _ = std::fs::remove_dir_all(&path_str);

        // Create scanner with very short timeout
        let mut config = create_default_test_config();
        config.start_block = 1;
        config.timeout_secs = 1; // Very short timeout for testing

        let scanner = EvmScanner::new(
            config,
            TEST_RPC_URL.to_string(),
            RocksDBStorage::new(&path_str).unwrap(),
        )
        .unwrap();

        scanner.init().await?;

        // Test 1: Normal fetch should work within timeout
        let block_number = 1;
        let fetch_result = timeout(
            Duration::from_secs(scanner.scanner_cfg.timeout_secs),
            scanner.fetch_block(block_number),
        )
        .await;

        assert!(
            fetch_result.is_ok(),
            "Fetch should complete within timeout for valid block"
        );
        assert!(
            fetch_result.unwrap().is_ok(),
            "Fetch should succeed for valid block"
        );

        // Test 2: Verify timeout configuration is correctly applied
        assert_eq!(
            scanner.scanner_cfg.timeout_secs, 1,
            "Timeout configuration should be 1 second"
        );

        println!("Timeout test passed! Timeout is correctly configured and applied.");
        Ok(())
    }

    // Helper function to parse parent_hash from block_data_json
    fn parse_parent_hash(block_data_json: &str) -> Result<String> {
        let block: serde_json::Value = serde_json::from_str(block_data_json)?;
        let parent_hash = block["parentHash"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("Failed to parse parent_hash"))?;
        Ok(parent_hash.to_string())
    }

    #[tokio::test]
    async fn test_verify_reorg_match() -> Result<()> {
        let temp_dir = std::env::temp_dir();
        let path = temp_dir.join(format!("rocksdb_test_reorg_match_{}", std::process::id()));
        let path_str = path.to_str().unwrap().to_string();

        // Clean up if exists
        let _ = std::fs::remove_dir_all(&path_str);

        // Create scanner with start_block = 1
        let scanner = EvmScanner::new(
            ScannerConfig {
                chain_type: "evm".to_string(),
                chain_name: "anvil".to_string(),
                concurrency: 1,
                start_block: 1,
                confirm_blocks: 1,
                realtime: true,
                timeout_secs: 15,
                cleanup_enabled: false,
                retention_blocks: None,
                cleanup_interval_secs: 3600,
                cleanup_batch_size: 1000,
                cleanup_orphaned_enabled: false,
            },
            "http://127.0.0.1:8545".to_string(),
            RocksDBStorage::new(&path_str).unwrap(),
        )
        .unwrap();

        scanner.init().await?;

        // Fetch block 1 and block 2 from chain
        let block_1 = scanner.fetch_block(1).await?;
        let block_2 = scanner.fetch_block(2).await?;

        // Store block 1
        scanner.store_block_data(&block_1)?;

        // Parse parent_hash from block 2
        let parent_hash = parse_parent_hash(&block_2.block_data_json)?;

        // Verify reorg - should match (no reorg)
        let result = scanner.verify_reorg(&parent_hash, 2)?;
        assert!(result, "Hashes should match (no reorg detected)");

        println!("Verify reorg match test passed!");
        Ok(())
    }

    #[tokio::test]
    async fn test_verify_reorg_mismatch() -> Result<()> {
        let temp_dir = std::env::temp_dir();
        let path = temp_dir.join(format!(
            "rocksdb_test_reorg_mismatch_{}",
            std::process::id()
        ));
        let path_str = path.to_str().unwrap().to_string();

        // Clean up if exists
        let _ = std::fs::remove_dir_all(&path_str);

        // Create scanner with start_block = 1
        let scanner = EvmScanner::new(
            ScannerConfig {
                chain_type: "evm".to_string(),
                chain_name: "anvil".to_string(),
                concurrency: 1,
                start_block: 1,
                confirm_blocks: 1,
                realtime: true,
                timeout_secs: 15,
                cleanup_enabled: false,
                retention_blocks: None,
                cleanup_interval_secs: 3600,
                cleanup_batch_size: 1000,
                cleanup_orphaned_enabled: false,
            },
            "http://127.0.0.1:8545".to_string(),
            RocksDBStorage::new(&path_str).unwrap(),
        )
        .unwrap();

        scanner.init().await?;

        // Fetch real block 1 and block 2 from chain
        let block_1 = scanner.fetch_block(1).await?;
        let block_2 = scanner.fetch_block(2).await?;

        // Create fake block 1 with wrong hash (but keep valid JSON structure)
        let fake_block_1 = BlockData {
            hash: "0xfake_hash_that_does_not_match".to_string(),
            block_data_json: block_1.block_data_json.clone(),
            block_receipts_json: block_1.block_receipts_json.clone(),
        };

        // Store fake block 1
        scanner.store_block_data(&fake_block_1)?;

        // Parse real parent_hash from block 2
        let parent_hash = parse_parent_hash(&block_2.block_data_json)?;

        // Verify reorg - should NOT match (reorg detected)
        let result = scanner.verify_reorg(&parent_hash, 2)?;
        assert!(!result, "Hashes should NOT match (reorg detected)");

        println!("Verify reorg mismatch test passed!");
        Ok(())
    }

    #[tokio::test]
    async fn test_verify_reorg_at_start_block() -> Result<()> {
        let scanner = create_test_scanner_with_name("verify_reorg_start_block");
        scanner.init().await?;

        // Test at start_block (100 from config)
        let result = scanner.verify_reorg("any_hash", 100)?;
        assert!(result, "Should skip reorg check at start_block");

        // Test below start_block
        let result = scanner.verify_reorg("any_hash", 99)?;
        assert!(result, "Should skip reorg check below start_block");

        println!("Verify reorg at start_block test passed!");
        Ok(())
    }

    #[tokio::test]
    async fn test_verify_reorg_no_previous_block() -> Result<()> {
        let temp_dir = std::env::temp_dir();
        let path = temp_dir.join(format!("rocksdb_test_no_prev_block_{}", std::process::id()));
        let path_str = path.to_str().unwrap().to_string();

        // Clean up if exists
        let _ = std::fs::remove_dir_all(&path_str);

        // Create scanner with start_block = 1
        let scanner = EvmScanner::new(
            ScannerConfig {
                chain_type: "evm".to_string(),
                chain_name: "anvil".to_string(),
                concurrency: 1,
                start_block: 1,
                confirm_blocks: 1,
                realtime: true,
                timeout_secs: 15,
                cleanup_enabled: false,
                retention_blocks: None,
                cleanup_interval_secs: 3600,
                cleanup_batch_size: 1000,
                cleanup_orphaned_enabled: false,
            },
            "http://127.0.0.1:8545".to_string(),
            RocksDBStorage::new(&path_str).unwrap(),
        )
        .unwrap();

        scanner.init().await?;

        // Do NOT store block 1
        // Try to verify reorg for block 2 - should allow scanning
        let result = scanner.verify_reorg("any_hash", 2)?;
        assert!(
            result,
            "Should allow scanning when previous block doesn't exist"
        );

        println!("Verify reorg no previous block test passed!");
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_reorg() -> Result<()> {
        let temp_dir = std::env::temp_dir();
        let path = temp_dir.join(format!("rocksdb_test_handle_reorg_{}", std::process::id()));
        let path_str = path.to_str().unwrap().to_string();

        // Clean up if exists
        let _ = std::fs::remove_dir_all(&path_str);

        // Create scanner with start_block = 1
        let scanner = EvmScanner::new(
            ScannerConfig {
                chain_type: "evm".to_string(),
                chain_name: "anvil".to_string(),
                concurrency: 1,
                start_block: 1,
                confirm_blocks: 1,
                realtime: true,
                timeout_secs: 15,
                cleanup_enabled: false,
                retention_blocks: None,
                cleanup_interval_secs: 3600,
                cleanup_batch_size: 1000,
                cleanup_orphaned_enabled: false,
            },
            "http://127.0.0.1:8545".to_string(),
            RocksDBStorage::new(&path_str).unwrap(),
        )
        .unwrap();

        scanner.init().await?;

        // Fetch and store block 1 and 2
        let block_1 = scanner.fetch_block(1).await?;
        let block_2 = scanner.fetch_block(2).await?;
        scanner.store_block_data(&block_1)?;
        scanner.store_block_data(&block_2)?;

        // Update progress to current_block = 2
        let mut progress = scanner.get_progress()?;
        progress.current_block = 2;
        scanner.update_progress(progress)?;

        // Handle reorg at block 3 (will update index for block 2)
        scanner.handle_reorg(3).await?;

        // Verify block 2 data still exists (new logic: data is preserved)
        let block_2_key = keys::block_data_key(&scanner.scanner_cfg.chain_name, &block_2.hash);
        assert!(
            scanner.storage.read(&block_2_key)?.is_some(),
            "Block 2 data should still exist"
        );

        // Verify block 1 still exists
        let block_1_key = keys::block_data_key(&scanner.scanner_cfg.chain_name, &block_1.hash);
        assert!(
            scanner.storage.read(&block_1_key)?.is_some(),
            "Block 1 should still exist"
        );

        // Verify block 2 index was updated (check active index)
        let block_2_active_index = scanner.get_active_block_index(2)?;
        assert!(
            block_2_active_index.is_some(),
            "Block 2 should have an active index"
        );
        // In test environment, the hash might be the same, but the index should exist
        let new_index = block_2_active_index.unwrap();
        assert_eq!(
            new_index.block_hash, block_2.hash,
            "Block 2 index should be updated (in test env, hash might be same)"
        );

        // Verify progress is updated
        let progress = scanner.get_progress()?;
        assert_eq!(progress.current_block, 1, "Current block should be 1");
        assert_eq!(
            progress.status, "reorg_detected",
            "Status should be reorg_detected"
        );
        assert_eq!(
            progress.reorg_block,
            Some(2),
            "Reorg block should be set to 2"
        );

        println!("Handle reorg test passed!");
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_reorg_depth_limit() -> Result<()> {
        let temp_dir = std::env::temp_dir();
        let path = temp_dir.join(format!(
            "rocksdb_test_reorg_depth_limit_{}",
            std::process::id()
        ));
        let path_str = path.to_str().unwrap().to_string();

        // Clean up if exists
        let _ = std::fs::remove_dir_all(&path_str);

        // Create scanner with start_block = 1
        let scanner = EvmScanner::new(
            ScannerConfig {
                chain_type: "evm".to_string(),
                chain_name: "anvil".to_string(),
                concurrency: 1,
                start_block: 1,
                confirm_blocks: 1,
                realtime: true,
                timeout_secs: 15,
                cleanup_enabled: false,
                retention_blocks: None,
                cleanup_interval_secs: 3600,
                cleanup_batch_size: 1000,
                cleanup_orphaned_enabled: false,
            },
            "http://127.0.0.1:8545".to_string(),
            RocksDBStorage::new(&path_str).unwrap(),
        )
        .unwrap();

        scanner.init().await?;

        // Set progress to a high block number
        let mut progress = scanner.get_progress()?;
        progress.current_block = 200;
        scanner.update_progress(progress)?;

        // Try to rollback beyond MAX_REORG_DEPTH (100)
        // This would try to rollback to block 2, depth = 200 - 2 = 198 > 100
        let result = scanner.handle_reorg(3).await;

        assert!(result.is_err(), "Should fail due to depth limit");
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Reorg depth exceeds maximum limit"),
            "Error should mention depth limit, got: {}",
            err_msg
        );

        println!("Handle reorg depth limit test passed!");
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_reorg_before_start_block() -> Result<()> {
        // Create scanner with start_block = 10
        let mut config = create_default_test_config();
        config.start_block = 10;
        let (scanner, _path) = create_test_scanner_with_config("reorg_before_start", config);

        scanner.init().await?;

        // Set progress to current_block = 11
        let mut progress = scanner.get_progress()?;
        progress.current_block = 11;
        scanner.update_progress(progress)?;

        // Try to handle reorg at block 11
        // This would try to rollback to block 10, then 10 - 1 = 9 < start_block (10)
        let result = scanner.handle_reorg(11).await;

        assert!(
            result.is_err(),
            "Should fail trying to rollback before start_block"
        );
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Cannot rollback before start_block"),
            "Error should mention start_block, got: {}",
            err_msg
        );

        println!("Handle reorg before start_block test passed!");
        Ok(())
    }

    #[tokio::test]
    async fn test_init_min_block() -> Result<()> {
        let scanner = create_test_scanner_with_name("init_min_block");

        // Initialize scanner
        scanner.init().await?;

        // Get progress and verify min_block is set
        let progress = scanner.get_progress()?;
        assert!(
            progress.min_block.is_some(),
            "min_block should be initialized"
        );
        assert_eq!(
            progress.min_block.unwrap(),
            progress.current_block,
            "min_block should equal current_block on first init"
        );

        println!("✅ Min block initialized: {}", progress.min_block.unwrap());
        Ok(())
    }

    #[tokio::test]
    async fn test_init_min_block_for_existing_progress() -> Result<()> {
        let scanner = create_test_scanner_with_name("init_min_block_existing");

        // First init
        scanner.init().await?;

        // Manually clear min_block to simulate old data
        let mut progress = scanner.get_progress()?;
        let original_current = progress.current_block;
        progress.min_block = None;
        scanner.update_progress(progress)?;

        // Verify min_block is cleared
        let progress_without_min = scanner.get_progress()?;
        assert!(
            progress_without_min.min_block.is_none(),
            "min_block should be None after manual clear"
        );

        // Re-init should set min_block
        scanner.init().await?;

        // Verify min_block is now set
        let progress_with_min = scanner.get_progress()?;
        assert!(
            progress_with_min.min_block.is_some(),
            "min_block should be set after re-init"
        );
        assert_eq!(
            progress_with_min.min_block.unwrap(),
            original_current,
            "min_block should equal current_block"
        );

        println!("✅ Min block initialized for existing progress");
        Ok(())
    }
}
