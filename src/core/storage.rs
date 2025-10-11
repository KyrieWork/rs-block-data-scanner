use anyhow::Result;
use serde::{Serialize, de::DeserializeOwned};

pub trait KVStorage: Send + Sync {
    fn init(&self) -> Result<()>;

    // Basic string operations
    fn write(&self, key: &str, value: &str) -> Result<()>;
    fn read(&self, key: &str) -> Result<Option<String>>;
    fn delete(&self, key: &str) -> Result<()>;
    fn exists(&self, key: &str) -> Result<bool>;

    // JSON serialization/deserialization
    fn write_json<T: Serialize>(&self, key: &str, value: &T) -> Result<()>;
    fn read_json<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>>;

    // Batch operations
    fn batch_write(&self, pairs: Vec<(String, String)>) -> Result<()>;
    fn batch_write_json<T: Serialize>(&self, pairs: Vec<(String, T)>) -> Result<()>;

    // Range query
    fn scan_prefix(
        &self,
        prefix: &str,
        limit: Option<usize>,
    ) -> Result<Vec<(String, String)>>;
}
