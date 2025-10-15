use anyhow::Result;
use config as config_loader;
use dotenvy::dotenv;
use serde::Deserialize;
use std::path::Path;

/// Global config structure
#[derive(Debug, Deserialize, Clone)]
pub struct AppConfig {
    pub rpc: RpcConfig,
    pub storage: StorageConfig,
    pub scanner: ScannerConfig,
    pub logging: LoggingConfig,
    pub metrics: MetricsConfig,
}

/// RPC related config
#[derive(Debug, Deserialize, Clone)]
pub struct RpcConfig {
    pub url: String,

    #[serde(default)]
    pub backups: Option<Vec<String>>,
}

/// RocksDB storage config
#[derive(Debug, Deserialize, Clone)]
pub struct StorageConfig {
    pub path: String,
    #[serde(default = "StorageConfig::default_compression")]
    pub compression: bool,
    #[serde(default)]
    pub column_families: Option<Vec<String>>,
}

impl StorageConfig {
    fn default_compression() -> bool {
        true
    }
}

/// Scanner task config
#[derive(Debug, Deserialize, Clone)]
pub struct ScannerConfig {
    pub chain_type: String,
    pub chain_name: String,
    #[serde(default = "ScannerConfig::default_concurrency")]
    pub concurrency: usize,
    #[serde(default = "ScannerConfig::default_start_block")]
    pub start_block: u64,
    #[serde(default = "ScannerConfig::default_confirm_blocks")]
    pub confirm_blocks: u64,
    #[serde(default = "ScannerConfig::default_realtime")]
    pub realtime: bool,
    #[serde(default = "ScannerConfig::default_timeout_secs")]
    pub timeout_secs: u64,

    // Cleanup configuration
    #[serde(default = "ScannerConfig::default_cleanup_enabled")]
    pub cleanup_enabled: bool,
    #[serde(default)]
    pub retention_blocks: Option<u64>,
    #[serde(default = "ScannerConfig::default_cleanup_interval_secs")]
    pub cleanup_interval_secs: u64,
    #[serde(default = "ScannerConfig::default_cleanup_batch_size")]
    pub cleanup_batch_size: usize,
    #[serde(default = "ScannerConfig::default_cleanup_orphaned_enabled")]
    pub cleanup_orphaned_enabled: bool,
}

impl ScannerConfig {
    fn default_concurrency() -> usize {
        4
    }
    fn default_start_block() -> u64 {
        0
    }
    fn default_confirm_blocks() -> u64 {
        10
    }
    fn default_realtime() -> bool {
        true
    }
    fn default_timeout_secs() -> u64 {
        15
    }
    fn default_cleanup_enabled() -> bool {
        false
    }
    fn default_cleanup_interval_secs() -> u64 {
        3600 // 1 hour
    }
    fn default_cleanup_batch_size() -> usize {
        1000
    }
    fn default_cleanup_orphaned_enabled() -> bool {
        false
    }
}

/// Logging config
#[derive(Debug, Deserialize, Clone)]
pub struct LoggingConfig {
    #[serde(default = "LoggingConfig::default_level")]
    pub level: String,
    #[serde(default = "LoggingConfig::default_to_file")]
    pub to_file: bool,
    #[serde(default = "LoggingConfig::default_path")]
    pub path: String,
}

impl LoggingConfig {
    fn default_level() -> String {
        "info".to_string()
    }
    fn default_to_file() -> bool {
        true
    }
    fn default_path() -> String {
        "./logs".to_string()
    }
}

/// Metrics config
#[derive(Debug, Deserialize, Clone)]
pub struct MetricsConfig {
    #[serde(default = "MetricsConfig::default_enable")]
    pub enable: bool,
    #[serde(default = "MetricsConfig::default_prometheus_exporter_port")]
    pub prometheus_exporter_port: u16,
}

impl MetricsConfig {
    fn default_enable() -> bool {
        true
    }
    fn default_prometheus_exporter_port() -> u16 {
        9100
    }
}

impl AppConfig {
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self> {
        dotenv().ok(); // Load the .env file

        if !path.as_ref().exists() {
            anyhow::bail!("Config file not found: {:?}", path.as_ref());
        }

        // Use config crate to parse the config file
        let builder = config_loader::Config::builder()
            .add_source(config_loader::File::from(path.as_ref().to_path_buf()))
            .add_source(config_loader::Environment::with_prefix("SCANNER").separator("__"))
            .build()?;

        Ok(builder.try_deserialize::<AppConfig>()?)
    }
}
