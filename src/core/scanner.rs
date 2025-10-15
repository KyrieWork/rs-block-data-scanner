use crate::core::table::BlockData;
use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::broadcast;

#[async_trait]
pub trait Scanner: Send + Sync {
    async fn init(&self) -> Result<()>;
    async fn fetch_block(&self, block_number: u64) -> Result<BlockData>;

    /// Run scanner with graceful shutdown support
    async fn run(&self, shutdown: broadcast::Receiver<()>) -> Result<()>;
}
