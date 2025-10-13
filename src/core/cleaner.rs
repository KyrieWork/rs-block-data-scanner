use anyhow::Result;
use async_trait::async_trait;

#[async_trait]
pub trait Cleaner: Send + Sync {
    /// Execute cleanup once
    /// Returns the number of blocks cleaned up
    async fn cleanup(&self) -> Result<usize>;
}
