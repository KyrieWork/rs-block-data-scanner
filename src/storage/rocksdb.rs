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
        opts.set_max_write_buffer_number(6); // increase to 6 buffers (default 3)
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
    struct TestData {
        id: u64,
        name: String,
        value: f64,
    }

    fn create_test_storage(test_name: &str) -> (RocksDBStorage, String) {
        let temp_dir = std::env::temp_dir();
        let path = temp_dir.join(format!("rocksdb_test_{}_{}", test_name, std::process::id()));
        let path_str = path.to_str().unwrap().to_string();

        // Clean up if exists from previous failed test
        let _ = std::fs::remove_dir_all(&path_str);

        let storage = RocksDBStorage::new(&path_str).expect("Failed to create test storage");
        (storage, path_str)
    }

    fn cleanup_test_storage(path: &str) {
        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn test_new_and_init() {
        let (storage, path) = create_test_storage("new_and_init");

        let result = storage.init();
        assert!(result.is_ok(), "Failed to initialize storage");

        cleanup_test_storage(&path);
    }

    #[test]
    fn test_write_and_read() {
        let (storage, path) = create_test_storage("write_and_read");

        let key = "test_key";
        let value = "test_value";

        storage.write(key, value).expect("Failed to write");
        let result = storage.read(key).expect("Failed to read");

        assert_eq!(result, Some(value.to_string()));

        cleanup_test_storage(&path);
    }

    #[test]
    fn test_read_nonexistent_key() {
        let (storage, path) = create_test_storage("read_nonexistent");

        let result = storage.read("nonexistent_key").expect("Failed to read");

        assert_eq!(result, None);

        cleanup_test_storage(&path);
    }

    #[test]
    fn test_delete() {
        let (storage, path) = create_test_storage("delete");

        let key = "delete_key";
        let value = "delete_value";

        storage.write(key, value).expect("Failed to write");
        assert!(storage.read(key).expect("Failed to read").is_some());

        storage.delete(key).expect("Failed to delete");
        let result = storage.read(key).expect("Failed to read after delete");

        assert_eq!(result, None);

        cleanup_test_storage(&path);
    }

    #[test]
    fn test_exists() {
        let (storage, path) = create_test_storage("exists");

        let key = "exists_key";
        let value = "exists_value";

        assert!(!storage.exists(key).expect("Failed to check existence"));

        storage.write(key, value).expect("Failed to write");
        assert!(storage.exists(key).expect("Failed to check existence"));

        storage.delete(key).expect("Failed to delete");
        assert!(!storage.exists(key).expect("Failed to check existence"));

        cleanup_test_storage(&path);
    }

    #[test]
    fn test_write_json_and_read_json() {
        let (storage, path) = create_test_storage("write_read_json");

        let key = "json_key";
        let data = TestData {
            id: 42,
            name: "test_name".to_string(),
            value: 42.5,
        };

        storage
            .write_json(key, &data)
            .expect("Failed to write JSON");
        let result: Option<TestData> = storage.read_json(key).expect("Failed to read JSON");

        assert_eq!(result, Some(data));

        cleanup_test_storage(&path);
    }

    #[test]
    fn test_read_json_nonexistent() {
        let (storage, path) = create_test_storage("read_json_nonexistent");

        let result: Option<TestData> = storage
            .read_json("nonexistent_json_key")
            .expect("Failed to read JSON");

        assert_eq!(result, None);

        cleanup_test_storage(&path);
    }

    #[test]
    fn test_batch_write() {
        let (storage, path) = create_test_storage("batch_write");

        let pairs = vec![
            ("key1".to_string(), "value1".to_string()),
            ("key2".to_string(), "value2".to_string()),
            ("key3".to_string(), "value3".to_string()),
        ];

        storage
            .batch_write(pairs.clone())
            .expect("Failed to batch write");

        for (key, expected_value) in pairs {
            let result = storage.read(&key).expect("Failed to read");
            assert_eq!(result, Some(expected_value));
        }

        cleanup_test_storage(&path);
    }

    #[test]
    fn test_batch_write_json() {
        let (storage, path) = create_test_storage("batch_write_json");

        let data1 = TestData {
            id: 1,
            name: "first".to_string(),
            value: 1.1,
        };
        let data2 = TestData {
            id: 2,
            name: "second".to_string(),
            value: 2.2,
        };
        let data3 = TestData {
            id: 3,
            name: "third".to_string(),
            value: 3.3,
        };

        let pairs = vec![
            ("data1".to_string(), data1.clone()),
            ("data2".to_string(), data2.clone()),
            ("data3".to_string(), data3.clone()),
        ];

        storage
            .batch_write_json(pairs)
            .expect("Failed to batch write JSON");

        let result1: TestData = storage.read_json("data1").expect("Failed to read").unwrap();
        let result2: TestData = storage.read_json("data2").expect("Failed to read").unwrap();
        let result3: TestData = storage.read_json("data3").expect("Failed to read").unwrap();

        assert_eq!(result1, data1);
        assert_eq!(result2, data2);
        assert_eq!(result3, data3);

        cleanup_test_storage(&path);
    }

    #[test]
    fn test_scan_prefix() {
        let (storage, path) = create_test_storage("scan_prefix");

        storage.write("block:1", "data1").expect("Failed to write");
        storage.write("block:2", "data2").expect("Failed to write");
        storage.write("block:3", "data3").expect("Failed to write");
        storage.write("tx:1", "tx_data").expect("Failed to write");

        let results = storage.scan_prefix("block:", None).expect("Failed to scan");

        assert_eq!(results.len(), 3);
        assert!(results.iter().all(|(k, _)| k.starts_with("block:")));

        cleanup_test_storage(&path);
    }

    #[test]
    fn test_scan_prefix_with_limit() {
        let (storage, path) = create_test_storage("scan_prefix_limit");

        for i in 1..=10 {
            let key = format!("item:{}", i);
            let value = format!("value{}", i);
            storage.write(&key, &value).expect("Failed to write");
        }

        let results = storage
            .scan_prefix("item:", Some(5))
            .expect("Failed to scan");

        assert_eq!(results.len(), 5);

        cleanup_test_storage(&path);
    }

    #[test]
    fn test_scan_prefix_no_match() {
        let (storage, path) = create_test_storage("scan_prefix_no_match");

        storage.write("key1", "value1").expect("Failed to write");
        storage.write("key2", "value2").expect("Failed to write");

        let results = storage
            .scan_prefix("nonexistent:", None)
            .expect("Failed to scan");

        assert_eq!(results.len(), 0);

        cleanup_test_storage(&path);
    }
}
