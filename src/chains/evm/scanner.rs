use crate::core::scanner::Scanner;
use crate::core::types::BlockData;
use alloy::providers::{Provider, ProviderBuilder};
use anyhow::Result;
use async_trait::async_trait;

pub struct EvmScanner {
    pub rpc_url: String,
}

impl EvmScanner {
    pub fn new(rpc_url: String) -> Self {
        Self { rpc_url }
    }
}

#[async_trait]
impl Scanner for EvmScanner {
    async fn init(&self) -> Result<()> {
        Ok(())
    }

    async fn fetch_block(&self, block_number: u64) -> Result<BlockData> {
        let provider = ProviderBuilder::new().on_http(self.rpc_url.parse()?);

        let block = provider
            .get_block_by_number(block_number.into(), true)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Block {} not found", block_number))?;

        let receipts = provider
            .get_block_receipts(block_number.into())
            .await?
            .ok_or_else(|| anyhow::anyhow!("Block {} not found", block_number))?;

        // verify block and receipts is match
        if receipts.len() > 0 && receipts[0].block_hash != Some(block.header.hash) {
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
            let provider = ProviderBuilder::new().on_http(self.rpc_url.parse()?);
            let latest_block = provider.get_block_number().await?;
            let _ = self.fetch_block(latest_block).await?;

            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_scanner() -> EvmScanner {
        EvmScanner::new("http://127.0.0.1:8545".to_string())
    }

    #[tokio::test]
    async fn test_init_connection() {
        let scanner = create_test_scanner();
        let result = scanner.init().await;
        assert!(
            result.is_ok(),
            "Failed to initialize connection: {:?}",
            result.err()
        );
    }

    #[tokio::test]
    async fn test_fetch_latest_block() -> Result<()> {
        let scanner = create_test_scanner();
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
}
