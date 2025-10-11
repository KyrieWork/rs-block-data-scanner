use crate::core::types::BlockData;
use anyhow::Result;
use async_trait::async_trait;

#[async_trait]
pub trait Scanner: Send + Sync {
    async fn init(&self) -> Result<()>;
    async fn fetch_block(&self, block_number: u64) -> Result<BlockData>;
    async fn run(&self) -> Result<()>;
}
