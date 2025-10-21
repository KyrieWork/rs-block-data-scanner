use std::sync::Arc;

use anyhow::{Context, Result};
use rocksdb::{DB, DBCompactionStyle, Direction, IteratorMode, Options, WriteBatch};
use serde::{Serialize, de::DeserializeOwned};

use crate::storage::traits::KVStorage;
use crate::utils::format::format_size_bytes;

#[derive(Clone, Debug)]
pub struct DatabaseHealth {
    pub status: String,
    pub num_keys: u64,
    pub sst_size: u64,
    pub l0_files: u64,
    pub is_healthy: bool,
}

#[derive(Clone)]
pub struct RocksDBStorage {
    db: Arc<DB>,
}

impl RocksDBStorage {
    pub fn new(path: &str) -> Result<Self> {
        let mut opts = Options::default();

        // ============================
        // Production-grade RocksDB configuration optimization
        // ============================

        // 1. Write buffer configuration - reduce frequent flush
        opts.create_if_missing(true);
        opts.set_write_buffer_size(256 * 1024 * 1024); // 256MB (default 64MB too small)
        opts.set_max_write_buffer_number(4); // increase to 4 buffers (default 3)
        opts.set_min_write_buffer_number_to_merge(2); // merge at least 2 buffers before flush

        // 2. SST file size configuration - reduce number of small files
        opts.set_target_file_size_base(512 * 1024 * 1024); // 512MB (default 64MB too small)
        opts.set_max_bytes_for_level_base(2 * 1024 * 1024 * 1024); // 2GB (default 256MB too small)
        opts.set_max_bytes_for_level_multiplier(10.0); // size multiplier per level

        // 3. Compression configuration - improve compression efficiency
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4); // use LZ4 compression
        opts.set_compaction_style(DBCompactionStyle::Universal); // universal compaction strategy
        opts.set_max_background_jobs(8); // increase background compression threads (default 2)
        opts.set_max_subcompactions(4); // parallel compaction sub-tasks

        // 4. WAL configuration - prevent disk space exhaustion
        opts.set_max_total_wal_size(1024 * 1024 * 1024); // 1GB WAL size limit
        opts.set_wal_bytes_per_sync(16 * 1024 * 1024); // sync every 16MB
        opts.set_bytes_per_sync(16 * 1024 * 1024); // sync every 16MB

        // 5. Error recovery configuration - improve data safety
        opts.set_paranoid_checks(true); // enable strict checks
        opts.set_advise_random_on_open(true); // random access optimization

        // 6. Level configuration - optimize level structure
        opts.set_level_zero_file_num_compaction_trigger(8); // Level 0 compaction trigger file count
        opts.set_level_zero_slowdown_writes_trigger(20); // Level 0 write slowdown threshold
        opts.set_level_zero_stop_writes_trigger(36); // Level 0 stop writes threshold

        let db = DB::open(&opts, path)
            .with_context(|| format!("Failed to open RocksDB at path: {}", path))?;
        Ok(Self { db: Arc::new(db) })
    }

    /// Delete multiple keys in a batch (atomic operation for better performance)
    pub fn delete_batch(&self, keys: &[Vec<u8>]) -> Result<()> {
        let mut batch = WriteBatch::default();
        for key in keys {
            batch.delete(key);
        }
        self.db
            .write(batch)
            .with_context(|| "Failed to execute batch delete")?;
        Ok(())
    }

    /// Batch write and delete operations in a single transaction
    /// First deletes the specified keys, then writes the new key-value pairs
    pub fn batch_write_delete(
        &self,
        writes: Vec<(String, String)>,
        deletes: Vec<Vec<u8>>,
    ) -> Result<()> {
        let mut batch = WriteBatch::default();

        // First, delete the specified keys
        for key in deletes {
            batch.delete(key);
        }

        // Then, write the new key-value pairs
        for (key, value) in writes {
            batch.put(key.as_bytes(), value.as_bytes());
        }

        self.db
            .write(batch)
            .with_context(|| "Failed to execute batch write and delete")?;
        Ok(())
    }

    /// Database health check
    pub fn health_check(&self) -> Result<DatabaseHealth> {
        let status = self.db.property_value("rocksdb.dbstats")?;
        let num_keys = self
            .db
            .property_value("rocksdb.estimate-num-keys")?
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);
        let sst_size = self
            .db
            .property_value("rocksdb.total-sst-files-size")?
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);

        // Check Level 0 file count (too many may cause write blocking)
        let l0_files = self
            .db
            .property_value("rocksdb.num-files-at-level0")?
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);

        let is_healthy = l0_files < 20; // consider healthy if Level 0 files less than 20

        Ok(DatabaseHealth {
            status: status.unwrap_or_default(),
            num_keys,
            sst_size,
            l0_files,
            is_healthy,
        })
    }

    /// Flush database to ensure all data is written to disk
    pub fn flush(&self) -> Result<()> {
        self.db
            .flush()
            .with_context(|| "Failed to flush database to disk")
    }

    /// Force cleanup of memory buffers and prepare for shutdown
    /// This method should be called before program exit to ensure proper resource cleanup
    pub fn force_cleanup(&self) -> Result<()> {
        // Flush all pending writes
        self.db.flush()?;

        // Force compaction to reduce memory usage
        // This will trigger immediate compaction of memtables
        self.db.compact_range::<&[u8], &[u8]>(None, None);

        // Note: RocksDB doesn't provide a direct way to clear memtables
        // The above operations should help reduce memory usage before exit

        Ok(())
    }

    /// Get database size using RocksDB internal properties
    /// Returns size in human-readable format (e.g., "1.23 GB")
    pub fn get_db_size(&self) -> Result<String> {
        // Get total SST files size (most accurate for actual data)
        let total_sst = self
            .db
            .property_value("rocksdb.total-sst-files-size")?
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);

        // Get live data size estimate
        let live_data = self
            .db
            .property_value("rocksdb.estimate-live-data-size")?
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);

        // Use the larger of the two for more accurate reporting
        let size_bytes = total_sst.max(live_data);

        Ok(format_size_bytes(size_bytes))
    }

    /// Get detailed database statistics
    pub fn get_db_stats(&self) -> Result<(String, u64)> {
        let total_sst = self
            .db
            .property_value("rocksdb.total-sst-files-size")?
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);

        let num_keys = self
            .db
            .property_value("rocksdb.estimate-num-keys")?
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);

        Ok((format_size_bytes(total_sst), num_keys))
    }
}

impl KVStorage for RocksDBStorage {
    fn init(&self) -> Result<()> {
        Ok(())
    }

    fn write(&self, key: &str, value: &str) -> Result<()> {
        self.db
            .put(key.as_bytes(), value.as_bytes())
            .with_context(|| format!("Failed to write key: {}", key))
    }

    fn read(&self, key: &str) -> Result<Option<String>> {
        match self.db.get(key.as_bytes())? {
            Some(value) => {
                let s = String::from_utf8(value)
                    .with_context(|| format!("Failed to parse value for key: {}", key))?;
                Ok(Some(s))
            }
            None => Ok(None),
        }
    }

    fn delete(&self, key: &str) -> Result<()> {
        self.db
            .delete(key.as_bytes())
            .with_context(|| format!("Failed to delete key: {}", key))
    }

    fn exists(&self, key: &str) -> Result<bool> {
        Ok(self.db.get(key.as_bytes())?.is_some())
    }

    fn write_json<T: Serialize>(&self, key: &str, value: &T) -> Result<()> {
        let json = serde_json::to_string(value)
            .with_context(|| format!("Failed to serialize value for key: {}", key))?;
        self.write(key, &json)
    }

    fn read_json<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>> {
        match self.read(key)? {
            Some(json) => {
                let value = serde_json::from_str(&json)
                    .with_context(|| format!("Failed to deserialize value for key: {}", key))?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    fn batch_write(&self, pairs: Vec<(String, String)>) -> Result<()> {
        let mut batch = WriteBatch::default();
        for (key, value) in pairs {
            batch.put(key.as_bytes(), value.as_bytes());
        }
        self.db
            .write(batch)
            .context("Failed to execute batch write")
    }

    fn batch_write_json<T: Serialize>(&self, pairs: Vec<(String, T)>) -> Result<()> {
        let mut batch = WriteBatch::default();
        for (key, value) in pairs {
            let json = serde_json::to_string(&value)
                .with_context(|| format!("Failed to serialize value for key: {}", key))?;
            batch.put(key.as_bytes(), json.as_bytes());
        }
        self.db
            .write(batch)
            .context("Failed to execute batch write")
    }

    fn scan_prefix(&self, prefix: &str, limit: Option<usize>) -> Result<Vec<(String, String)>> {
        let mut results = Vec::new();
        let prefix_bytes = prefix.as_bytes();

        let iter = self
            .db
            .iterator(IteratorMode::From(prefix_bytes, Direction::Forward));

        for item in iter {
            let (key, value) = item.context("Failed to read from iterator")?;

            // Check if key starts with prefix
            if !key.starts_with(prefix_bytes) {
                break;
            }

            let key_str =
                String::from_utf8(key.to_vec()).context("Failed to parse key as UTF-8")?;
            let value_str =
                String::from_utf8(value.to_vec()).context("Failed to parse value as UTF-8")?;

            results.push((key_str, value_str));

            if let Some(limit) = limit
                && results.len() >= limit
            {
                break;
            }
        }

        Ok(results)
    }
}
