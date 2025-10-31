use crate::storage::rocksdb::RocksDBStorage;
use crate::storage::traits::KVStorage;
use anyhow::{Context, Result};
use std::sync::Arc;

pub struct ApiStorage {
    db: Arc<RocksDBStorage>,
    chain: String,
}

impl ApiStorage {
    pub fn open_readonly(path: &str, chain: impl Into<String>) -> Result<Self> {
        let db = RocksDBStorage::open_read_only(path)
            .with_context(|| format!("Failed to open RocksDB at {path}"))?;
        Ok(Self {
            db: Arc::new(db),
            chain: chain.into(),
        })
    }

    pub fn read_raw(&self, key: &str) -> Result<Option<String>> {
        self.db.read(key)
    }

    pub fn read_progress(&self) -> Result<Option<String>> {
        let key = format!("{}:progress", self.chain);
        self.db.read(&key)
    }

    pub fn chain(&self) -> &str {
        &self.chain
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{rocksdb::RocksDBStorage, traits::KVStorage};
    use std::path::Path;
    use tempfile::TempDir;

    fn prepare_db(path: &Path) -> Result<()> {
        let storage = RocksDBStorage::new(path.to_str().unwrap())?;
        storage.init()?;
        storage.write("test:key", "value")?;
        storage.write("test:progress", "{\"current_block\":1}")?;
        Ok(())
    }

    #[test]
    fn read_existing_key_returns_value() -> Result<()> {
        let temp_dir = TempDir::new()?;
        prepare_db(temp_dir.path())?;
        let api_storage = ApiStorage::open_readonly(temp_dir.path().to_str().unwrap(), "test")?;
        let value = api_storage.read_raw("test:key")?;
        assert_eq!(value.as_deref(), Some("value"));
        Ok(())
    }

    #[test]
    fn read_missing_key_returns_none() -> Result<()> {
        let temp_dir = TempDir::new()?;
        prepare_db(temp_dir.path())?;
        let api_storage = ApiStorage::open_readonly(temp_dir.path().to_str().unwrap(), "test")?;
        let value = api_storage.read_raw("missing")?;
        assert!(value.is_none());
        Ok(())
    }

    #[test]
    fn read_progress_returns_value() -> Result<()> {
        let temp_dir = TempDir::new()?;
        prepare_db(temp_dir.path())?;
        let api_storage = ApiStorage::open_readonly(temp_dir.path().to_str().unwrap(), "test")?;
        let value = api_storage.read_progress()?;
        assert!(value.is_some());
        Ok(())
    }
}
