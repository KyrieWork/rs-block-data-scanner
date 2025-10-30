use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use metrics_exporter_prometheus::PrometheusBuilder;
use rs_block_data_scanner::{
    chains::evm::{
        checker::EvmChecker, cleaner::EvmCleaner, client::EvmClient, context::EvmScannerContext,
        scanner::EvmScanner,
    },
    cli::Cli,
    config::AppConfig,
    storage::{manager::ScannerStorageManager, rocksdb::RocksDBStorage, traits::KVStorage},
    utils::{
        format::format_size_bytes,
        logger::init_logger,
        metrics::{NoopScannerMetrics, PrometheusScannerMetrics, ScannerMetrics},
    },
};
use tokio::sync::{Semaphore, broadcast};
use tracing::{error, info, warn};

#[tokio::main]
async fn main() -> Result<()> {
    let args = Cli::parse();
    let cfg = AppConfig::load(&args.config)?;

    // Initialize log path
    let log_path = format!(
        "{}/{}.scanner.log",
        cfg.logging.path.trim_end_matches('/'),
        cfg.scanner.chain_name
    );

    // Initialize logger system
    init_logger(
        &cfg.logging.level,
        cfg.logging.to_file,
        &log_path,
        &cfg.logging.timezone,
    );

    if cfg.metrics.enable {
        let metrics_port = cfg.metrics.prometheus_exporter_port;
        PrometheusBuilder::new()
            .with_http_listener(([0, 0, 0, 0], metrics_port))
            .install()
            .with_context(|| {
                format!(
                    "Failed to start Prometheus exporter on 0.0.0.0:{}",
                    metrics_port
                )
            })?;
        info!(
            port = metrics_port,
            "📈 Prometheus metrics exporter listening"
        );
    }

    info!("✅ Configuration load successful");
    info!(chain = %cfg.scanner.chain_type, "Chain type configuration");
    info!(chain_name = %cfg.scanner.chain_name, "Chain name");
    info!(start_block = cfg.scanner.start_block, "Start block number");
    info!(rpc_url = %cfg.rpc.url, "RPC node");

    // Validate scanner configuration
    cfg.scanner.validate()?;

    // Create shutdown channel
    let (shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);

    // Spawn signal handler task for Ctrl+C
    let shutdown_tx_sigint = shutdown_tx.clone();
    tokio::spawn(async move {
        if let Err(e) = tokio::signal::ctrl_c().await {
            error!("Failed to listen for Ctrl+C: {}", e);
            return;
        }

        info!("📡 Received shutdown signal (Ctrl+C)");
        // Send shutdown signal (ignore error if receiver is dropped)
        let _ = shutdown_tx_sigint.send(());
    });

    // SIGTERM handler (Unix only)
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let shutdown_tx_sigterm = shutdown_tx.clone();
        tokio::spawn(async move {
            if let Ok(mut sigterm) = signal(SignalKind::terminate()) {
                sigterm.recv().await;
                info!("📡 Received SIGTERM signal");
                let _ = shutdown_tx_sigterm.send(());
            }
        });
    }

    // Start scanner based on chain type
    match cfg.scanner.chain_type.as_str() {
        "evm" => {
            info!("🚀 Starting EVM blockchain scanner...");

            let scanner_cfg = Arc::new(cfg.scanner.clone());

            // Initialize storage with chain-specific path
            let storage_path = format!(
                "{}/{}",
                cfg.storage.path.trim_end_matches('/'),
                scanner_cfg.chain_name
            );

            // Create directory if not exists
            std::fs::create_dir_all(&storage_path)
                .with_context(|| format!("Failed to create storage directory: {}", storage_path))?;

            let storage = Arc::new(RocksDBStorage::new(&storage_path)?);
            storage.init()?;

            // Perform database health check
            match storage.health_check() {
                Ok(health) => {
                    if health.is_healthy {
                        info!("✅ Storage initialized and healthy");
                    } else {
                        warn!("⚠️ Storage initialized but health check failed");
                        warn!("  └─ Level 0 files: {}", health.l0_files);
                    }
                    info!("  └─ Total keys: {}", health.num_keys);
                    info!("  └─ SST size: {}", format_size_bytes(health.sst_size));
                }
                Err(e) => {
                    warn!("⚠️ Storage health check failed: {}", e);
                }
            }

            info!("✅ Storage initialized");
            info!("  └─ Path: {}", storage_path);
            info!("  └─ Base: {}", cfg.storage.path);
            info!("  └─ Chain: {}", scanner_cfg.chain_name);

            // Create storage manager
            let storage_manager = Arc::new(ScannerStorageManager::new(
                storage.clone(),
                scanner_cfg.chain_name.clone(),
            ));

            let metrics_handle: Arc<dyn ScannerMetrics> = if cfg.metrics.enable {
                Arc::new(PrometheusScannerMetrics::new(
                    scanner_cfg.chain_name.clone(),
                ))
            } else {
                Arc::new(NoopScannerMetrics::new())
            };
            let rpc_semaphore = Arc::new(Semaphore::new(scanner_cfg.concurrency));

            let scanner_context = EvmScannerContext::new(
                Arc::clone(&scanner_cfg),
                Arc::clone(&storage_manager),
                metrics_handle,
                rpc_semaphore,
            );

            // Create EVM client
            let client = Arc::new(EvmClient::new(
                cfg.rpc.url.clone(),
                cfg.rpc.backups.clone().unwrap_or_default(),
            )?);

            // Create EVM checker
            let checker = Arc::new(EvmChecker::new(scanner_context.clone()));

            // Create EVM cleaner
            let cleaner = Arc::new(EvmCleaner::new(scanner_context.clone()));

            // Create EVM scanner
            let scanner = EvmScanner::new(
                scanner_context,
                client,
                Arc::clone(&checker),
                Arc::clone(&cleaner),
            );

            // Initialize scanner
            scanner.init().await?;

            // Cleanup is integrated into scanner, no separate task needed
            if scanner_cfg.cleanup_enabled {
                info!("🧹 Cleanup enabled - integrated into scanner");
                info!("  └─ Retention blocks: {:?}", scanner_cfg.retention_blocks);
                info!(
                    "  └─ Cleanup interval: {} blocks",
                    scanner_cfg.cleanup_interval_blocks
                );
                info!("  └─ Batch size: {}", scanner_cfg.cleanup_batch_size);
            }

            // Start scanning with shutdown support
            info!("🔄 Starting block scanner...");
            info!("💡 Press Ctrl+C to stop gracefully");

            scanner.run(shutdown_rx).await?;

            // Force cleanup of memory buffers and prepare for shutdown
            info!("🧹 Starting resource cleanup...");
            match storage.force_cleanup() {
                Ok(_) => info!("✅ Database cleanup completed successfully"),
                Err(e) => warn!("⚠️ Failed to cleanup database: {}", e),
            }

            info!("✨ Scanner exited successfully");
        }
        "solana" => {
            info!("🚀 Start Solana blockchain data scanning service...");
            warn!("⚠️ Not supported yet");
        }
        other => {
            warn!("⚠️ Unknown chain type: {}", other);
        }
    }

    Ok(())
}
