use std::sync::Arc;

use tokio::sync::Semaphore;

use crate::{
    config::ScannerConfig, storage::manager::ScannerStorageManager, utils::metrics::ScannerMetrics,
};

#[derive(Clone)]
pub struct EvmScannerContext {
    pub config: Arc<ScannerConfig>,
    pub storage_manager: Arc<ScannerStorageManager>,
    pub metrics: Arc<dyn ScannerMetrics>,
    pub rpc_semaphore: Arc<Semaphore>,
}

impl EvmScannerContext {
    pub fn new(
        config: Arc<ScannerConfig>,
        storage_manager: Arc<ScannerStorageManager>,
        metrics: Arc<dyn ScannerMetrics>,
        rpc_semaphore: Arc<Semaphore>,
    ) -> Self {
        Self {
            config,
            storage_manager,
            metrics,
            rpc_semaphore,
        }
    }

    pub fn chain_name(&self) -> &str {
        &self.config.chain_name
    }
}
