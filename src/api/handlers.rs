use crate::api::storage::ApiStorage;
use axum::http::StatusCode;
use serde::Serialize;
use serde_json::Value;

#[derive(Serialize, Debug, PartialEq, Eq)]
pub struct ErrorResponse {
    pub error: String,
}

#[derive(Debug, PartialEq, Eq)]
pub struct ApiHttpResponse {
    pub status: u16,
    pub body: String,
    pub content_type: &'static str,
}

pub fn get_key_value(key: &str, storage: &ApiStorage) -> ApiHttpResponse {
    match storage.read_raw(key) {
        Ok(Some(stored)) => {
            let parsed_value = match serde_json::from_str::<Value>(&stored) {
                Ok(json) => json,
                Err(_) => Value::String(stored),
            };
            let payload = serde_json::json!({
                "key": key,
                "value": parsed_value,
            });

            match serde_json::to_string(&payload) {
                Ok(body) => ApiHttpResponse {
                    status: StatusCode::OK.as_u16(),
                    body,
                    content_type: "application/json",
                },
                Err(err) => ApiHttpResponse {
                    status: StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
                    body: format!("{{\"error\":\"failed to serialize response: {}\"}}", err),
                    content_type: "application/json",
                },
            }
        }
        Ok(None) => {
            let error = ErrorResponse {
                error: format!("Key '{key}' not found"),
            };
            let body = serde_json::to_string(&error)
                .unwrap_or_else(|_| "{\"error\":\"Key not found\"}".to_string());
            ApiHttpResponse {
                status: StatusCode::NOT_FOUND.as_u16(),
                body,
                content_type: "application/json",
            }
        }
        Err(err) => {
            let error = ErrorResponse {
                error: format!("Failed to read key '{key}': {err}"),
            };
            let body = serde_json::to_string(&error)
                .unwrap_or_else(|_| "{\"error\":\"Internal server error\"}".to_string());
            ApiHttpResponse {
                status: StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
                body,
                content_type: "application/json",
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{rocksdb::RocksDBStorage, traits::KVStorage};
    use axum::http::StatusCode;
    use tempfile::TempDir;

    fn prepare_storage(temp: &TempDir) -> ApiStorage {
        let path = temp.path().join("rocksdb");
        let storage = RocksDBStorage::new(path.to_str().unwrap()).unwrap();
        storage.init().unwrap();
        storage.write("existing", "value").unwrap();
        storage.write("json", "{\"foo\":\"bar\"}").unwrap();
        drop(storage);
        ApiStorage::open_readonly(path.to_str().unwrap()).unwrap()
    }

    #[test]
    fn handler_returns_value_for_existing_key() {
        let temp_dir = TempDir::new().unwrap();
        let storage = prepare_storage(&temp_dir);
        let response = get_key_value("existing", &storage);
        assert_eq!(response.status, StatusCode::OK.as_u16());
        let json: serde_json::Value = serde_json::from_str(&response.body).unwrap();
        assert_eq!(json["key"], "existing");
        assert_eq!(json["value"], "value");
    }

    #[test]
    fn handler_returns_not_found_for_missing_key() {
        let temp_dir = TempDir::new().unwrap();
        let storage = prepare_storage(&temp_dir);
        let response = get_key_value("missing", &storage);
        assert_eq!(response.status, StatusCode::NOT_FOUND.as_u16());
    }

    #[test]
    fn handler_parses_json_value_when_possible() {
        let temp_dir = TempDir::new().unwrap();
        let storage = prepare_storage(&temp_dir);
        let response = get_key_value("json", &storage);
        assert_eq!(response.status, StatusCode::OK.as_u16());
        let json: serde_json::Value = serde_json::from_str(&response.body).unwrap();
        assert_eq!(json["value"]["foo"], "bar");
    }
}
