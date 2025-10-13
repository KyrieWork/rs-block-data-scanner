use std::sync::Arc;

use anyhow::{Context, Result};
use rocksdb::{DB, Direction, IteratorMode, WriteBatch};
use serde::{Serialize, de::DeserializeOwned};

use crate::storage::traits::KVStorage;

#[derive(Clone)]
pub struct RocksDBStorage {
    db: Arc<DB>,
}

impl RocksDBStorage {
    pub fn new(path: &str) -> Result<Self> {
        let db = DB::open_default(path)
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
