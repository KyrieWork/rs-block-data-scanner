pub mod handlers;
pub mod storage;

use crate::config::AppConfig;
use crate::utils::logger::init_logger;
use handlers::{ApiHttpResponse, get_key_value, get_progress};
use storage::ApiStorage;

pub fn handle_request(path: &str, storage: &ApiStorage) -> ApiHttpResponse {
    if let Some(key) = path.strip_prefix("/kv/") {
        if key.is_empty() {
            return ApiHttpResponse {
                status: 400,
                body: "{\"error\":\"Key cannot be empty\"}".to_string(),
                content_type: "application/json",
            };
        }
        return get_key_value(key, storage);
    }

    if path == "/progress" {
        return get_progress(storage);
    }

    ApiHttpResponse {
        status: 404,
        body: "{\"error\":\"Unknown endpoint\"}".to_string(),
        content_type: "application/json",
    }
}

pub fn init_logging(cfg: &AppConfig) {
    let log_path = format!("{}/api.log", cfg.logging.path.trim_end_matches('/'));
    init_logger(
        &cfg.logging.level,
        cfg.logging.to_file,
        &log_path,
        &cfg.logging.timezone,
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{rocksdb::RocksDBStorage, traits::KVStorage};
    use tempfile::TempDir;

    fn prepare_storage(temp: &TempDir) -> ApiStorage {
        let path = temp.path().join("rocksdb");
        let storage = RocksDBStorage::new(path.to_str().unwrap()).unwrap();
        storage.init().unwrap();
        storage.write("alpha", "beta").unwrap();
        storage.write("json", "{\"hello\":\"world\"}").unwrap();
        storage
            .write("test:progress", "{\"chain\":\"test\",\"current_block\":1}")
            .unwrap();
        drop(storage);
        ApiStorage::open_readonly(path.to_str().unwrap(), "test").unwrap()
    }

    #[test]
    fn handle_request_returns_value_for_known_key() {
        let temp_dir = TempDir::new().unwrap();
        let storage = prepare_storage(&temp_dir);
        let response = handle_request("/kv/alpha", &storage);
        assert_eq!(response.status, 200);
        let json: serde_json::Value = serde_json::from_str(&response.body).unwrap();
        assert_eq!(json["value"], "beta");
    }

    #[test]
    fn handle_request_returns_not_found_for_unknown_key() {
        let temp_dir = TempDir::new().unwrap();
        let storage = prepare_storage(&temp_dir);
        let response = handle_request("/kv/missing", &storage);
        assert_eq!(response.status, 404);
    }

    #[test]
    fn handle_request_parses_json_value() {
        let temp_dir = TempDir::new().unwrap();
        let storage = prepare_storage(&temp_dir);
        let response = handle_request("/kv/json", &storage);
        assert_eq!(response.status, 200);
        let json: serde_json::Value = serde_json::from_str(&response.body).unwrap();
        assert_eq!(json["value"]["hello"], "world");
    }

    #[test]
    fn handle_request_returns_progress() {
        let temp_dir = TempDir::new().unwrap();
        let storage = prepare_storage(&temp_dir);
        let response = handle_request("/progress", &storage);
        assert_eq!(response.status, 200);
        let json: serde_json::Value = serde_json::from_str(&response.body).unwrap();
        assert_eq!(json["chain"], "test");
    }
}
