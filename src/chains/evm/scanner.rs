use crate::{
    config::ScannerConfig,
    core::{
        scanner::Scanner,
        types::{BlockData, ScannerProgress},
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
use tracing::info;

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

    fn store_block_data(&self, block_number: u64, block_data: &BlockData) -> Result<()> {
        let block_data_key =
            keys::block_data_key(&self.scanner_cfg.chain_name, block_number, &block_data.hash);
        let block_receipts_key =
            keys::block_receipts_key(&self.scanner_cfg.chain_name, block_number, &block_data.hash);

        self.storage
            .write(&block_data_key, &block_data.block_data_json)?;
        self.storage
            .write(&block_receipts_key, &block_data.block_receipts_json)?;

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
}

#[async_trait]
impl Scanner for EvmScanner {
    async fn init(&self) -> Result<()> {
        let key = keys::progress_key(&self.scanner_cfg.chain_name);
        if self.storage.read_json::<ScannerProgress>(&key)?.is_none() {
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

            info!(
                "✅ Initial progress created: current_block={}",
                progress.current_block
            );
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

    async fn run(&self) -> Result<()> {
        loop {
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
                let block_data = self.fetch_block(scan_block).await?;

                // Store block data and receipts to RocksDB
                self.store_block_data(scan_block, &block_data)?;

                // Update progress
                progress.current_block = scan_block;
                progress.updated_at = Utc::now();

                // Set status based on how far behind we are
                let blocks_behind = target_block.saturating_sub(scan_block);
                progress.status = if blocks_behind > 10 {
                    "catching_up".to_string()
                } else if blocks_behind > 0 {
                    "scanning".to_string()
                } else {
                    "synced".to_string()
                };

                self.update_progress(progress.clone())?;

                info!(
                    "✅ Scanned block {} - Status: {}",
                    scan_block, progress.status
                );
            } else {
                // Already caught up, set to idle
                progress.status = "idle".to_string();
                progress.updated_at = Utc::now();
                self.update_progress(progress.clone())?;

                info!("🔄 Already caught up - Status: {}", progress.status);
            }

            // Wait for next scan cycle
            tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_scanner_with_name(test_name: &str) -> EvmScanner {
        let temp_dir = std::env::temp_dir();
        let path = temp_dir.join(format!("rocksdb_test_{}_{}", test_name, std::process::id()));
        let path_str = path.to_str().unwrap().to_string();

        // Clean up if exists from previous failed test
        let _ = std::fs::remove_dir_all(&path_str);

        EvmScanner::new(
            ScannerConfig {
                chain_type: "evm".to_string(),
                chain_name: "anvil".to_string(),
                concurrency: 1,
                start_block: 100,
                confirm_blocks: 1,
                realtime: true,
            },
            "http://127.0.0.1:8545".to_string(),
            RocksDBStorage::new(&path_str).unwrap(),
        )
        .unwrap()
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
        scanner.store_block_data(block_number, &block_data)?;

        // Verify block data is stored correctly
        let block_data_key = keys::block_data_key(
            &scanner.scanner_cfg.chain_name,
            block_number,
            &block_data.hash,
        );
        let stored_block_data = scanner.storage.read(&block_data_key)?;

        assert!(
            stored_block_data.is_some(),
            "Block data should be stored in RocksDB"
        );
        assert_eq!(
            stored_block_data.unwrap(),
            block_data.block_data_json,
            "Stored block data should match original"
        );

        // Verify block receipts are stored correctly
        let block_receipts_key = keys::block_receipts_key(
            &scanner.scanner_cfg.chain_name,
            block_number,
            &block_data.hash,
        );
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
}
