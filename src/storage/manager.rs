use crate::core::table::{BlockData, BlockIndex, BlockIndexHistory, ScannerProgress};
use crate::core::types::{EvmBlockData, EvmBlockReceipts};
use crate::storage::schema::keys;
use crate::storage::traits::KVStorage;
use anyhow::Result;
use chrono::Utc;
use std::sync::Arc;

use super::rocksdb::RocksDBStorage;

pub struct ScannerProgressStorage {
    pub storage: Arc<RocksDBStorage>,
    pub chain: String,
}

impl ScannerProgressStorage {
    pub fn get_initial_progress(&self, start_block: u64) -> ScannerProgress {
        ScannerProgress {
            chain: self.chain.clone(),
            current_block: start_block,
            target_block: start_block,
            network_latest_block: None,
            status: "idle".to_string(),
            updated_at: Utc::now(),
            reorg_block: None,
            finalized_block: None,
            min_block: None,
            version: crate::storage::schema::SCHEMA_VERSION,
        }
    }

    pub fn get(&self) -> Result<ScannerProgress> {
        let key = keys::progress_key(&self.chain);
        self.storage
            .read_json::<ScannerProgress>(&key)?
            .ok_or_else(|| anyhow::anyhow!("Progress not found for chain: {}", self.chain))
    }

    pub fn update(&self, progress: ScannerProgress) -> Result<()> {
        let key = keys::progress_key(&self.chain);
        self.storage.write_json(&key, &progress)
    }

    pub fn delete(&self) -> Result<()> {
        let key = keys::progress_key(&self.chain);
        self.storage.delete(&key)
    }
}

pub struct ScannerBlockIndexStorage {
    pub storage: Arc<RocksDBStorage>,
    pub chain: String,
}

impl ScannerBlockIndexStorage {
    pub fn get_active(&self, block_number: u64) -> Result<BlockIndex> {
        let key = keys::block_index_active_key(&self.chain, block_number);
        self.storage.read_json(&key)?.ok_or_else(|| {
            anyhow::anyhow!(
                "Block index not found for chain: {}, block number: {}",
                self.chain,
                block_number
            )
        })
    }

    pub fn get_history_list(&self, block_number: u64) -> Result<Vec<BlockIndexHistory>> {
        let prefix = keys::block_index_history_prefix(&self.chain, block_number);
        self.storage.scan_prefix(&prefix, None).map(|results| {
            results
                .into_iter()
                .map(|(_, value)| serde_json::from_str::<BlockIndexHistory>(&value).unwrap())
                .collect()
        })
    }

    /// Batch save multiple block indexes
    pub fn batch_save_indexes(&self, indexes: Vec<(u64, BlockIndex)>) -> Result<()> {
        if indexes.is_empty() {
            return Ok(());
        }

        let mut writes = Vec::new();
        for (block_number, index) in indexes {
            let key = keys::block_index_active_key(&self.chain, block_number);
            let value = serde_json::to_string(&index)?;
            writes.push((key, value));
        }

        self.storage.batch_write(writes)
    }
}

pub struct ScannerBlockDataStorage {
    pub storage: Arc<RocksDBStorage>,
    pub chain: String,
}

impl ScannerBlockDataStorage {
    pub fn get_data(&self, block_hash: &str) -> Result<EvmBlockData> {
        let key = keys::block_data_key(&self.chain, block_hash);
        self.storage
            .read_json::<EvmBlockData>(&key)?
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Block data not found for chain: {}, block hash: {}",
                    self.chain,
                    block_hash
                )
            })
    }

    pub fn get_receipts(&self, block_hash: &str) -> Result<EvmBlockReceipts> {
        let key = keys::block_receipts_key(&self.chain, block_hash);
        self.storage
            .read_json::<EvmBlockReceipts>(&key)?
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Block receipts not found for chain: {}, block hash: {}",
                    self.chain,
                    block_hash
                )
            })
    }

    pub fn get_debug_trace(&self, block_hash: &str) -> Result<String> {
        let key = keys::block_debug_trace_key(&self.chain, block_hash);
        self.storage.read(&key)?.ok_or_else(|| {
            anyhow::anyhow!(
                "Block debug trace not found for chain: {}, block hash: {}",
                self.chain,
                block_hash
            )
        })
    }

    /// Batch save multiple block data
    pub fn batch_save_data(&self, blocks: Vec<BlockData>) -> Result<()> {
        if blocks.is_empty() {
            return Ok(());
        }

        let mut writes = Vec::new();
        for block_data in blocks {
            // Block data
            let block_data_key = keys::block_data_key(&self.chain, &block_data.hash);
            writes.push((block_data_key, block_data.block_data_json));

            // Block receipts
            let block_receipts_key = keys::block_receipts_key(&self.chain, &block_data.hash);
            writes.push((block_receipts_key, block_data.block_receipts_json));

            // Block debug trace
            let block_debug_trace_key = keys::block_debug_trace_key(&self.chain, &block_data.hash);
            writes.push((block_debug_trace_key, block_data.debug_trace_block_json));
        }

        self.storage.batch_write(writes)
    }
}

pub struct ScannerStorageManager {
    pub storage: Arc<RocksDBStorage>,
    pub progress: Arc<ScannerProgressStorage>,
    pub block_index: Arc<ScannerBlockIndexStorage>,
    pub block_data: Arc<ScannerBlockDataStorage>,
}

impl ScannerStorageManager {
    pub fn new(storage: Arc<RocksDBStorage>, chain: String) -> Self {
        Self {
            progress: Arc::new(ScannerProgressStorage {
                storage: storage.clone(),
                chain: chain.clone(),
            }),
            block_index: Arc::new(ScannerBlockIndexStorage {
                storage: storage.clone(),
                chain: chain.clone(),
            }),
            block_data: Arc::new(ScannerBlockDataStorage {
                storage: storage.clone(),
                chain: chain.clone(),
            }),
            storage,
        }
    }

    /// Batch save blocks with indexes and handle cleanup
    pub fn batch_save_blocks(
        &self,
        blocks: Vec<BlockData>,
        cleanup_keys: Vec<Vec<u8>>,
    ) -> Result<()> {
        if blocks.is_empty() {
            return Ok(());
        }

        // Prepare block indexes
        let mut indexes = Vec::new();
        for block_data in &blocks {
            // Parse block data to get block number and parent hash
            let block: serde_json::Value = serde_json::from_str(&block_data.block_data_json)?;
            let block_number = if let Some(number_str) = block["header"]["number"].as_str() {
                if let Some(stripped) = number_str.strip_prefix("0x") {
                    u64::from_str_radix(stripped, 16)?
                } else {
                    number_str.parse::<u64>()?
                }
            } else if let Some(number_str) = block["number"].as_str() {
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

            let index = BlockIndex {
                block_hash: block_data.hash.clone(),
                parent_hash,
                created_at: Utc::now(),
            };
            indexes.push((block_number, index));
        }

        // Get chain name from progress storage
        let chain_name = &self.progress.chain;

        // Prepare writes for block data
        let mut writes = Vec::new();
        for block_data in blocks {
            // Block data
            let block_data_key = keys::block_data_key(chain_name, &block_data.hash);
            writes.push((block_data_key, block_data.block_data_json));

            // Block receipts
            let block_receipts_key = keys::block_receipts_key(chain_name, &block_data.hash);
            writes.push((block_receipts_key, block_data.block_receipts_json));

            // Block debug trace
            let block_debug_trace_key = keys::block_debug_trace_key(chain_name, &block_data.hash);
            writes.push((block_debug_trace_key, block_data.debug_trace_block_json));
        }

        // Add block indexes to writes
        for (block_number, index) in indexes {
            let index_key = keys::block_index_active_key(chain_name, block_number);
            let index_value = serde_json::to_string(&index)?;
            writes.push((index_key, index_value));
        }

        // Execute batch write and delete
        self.storage.batch_write_delete(writes, cleanup_keys)
    }
}
