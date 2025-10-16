use anyhow::{Context, Result};
use clap::Parser;
use rs_block_data_scanner::{
    chains::evm::{cleaner::EvmCleaner, scanner::EvmScanner},
    cli::Cli,
    config::AppConfig,
    core::{cleaner::Cleaner, scanner::Scanner},
    storage::{rocksdb::RocksDBStorage, traits::KVStorage},
    utils::{format::format_size_bytes, logger::init_logger},
};
use tokio::sync::broadcast;
use tracing::{debug, error, info, warn};

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
    init_logger(&cfg.logging.level, cfg.logging.to_file, &log_path);

    info!("✅ Configuration load successful");
    info!(chain = %cfg.scanner.chain_type, "Chain type configuration");
    info!(chain_name = %cfg.scanner.chain_name, "Chain name");
    info!(start_block = cfg.scanner.start_block, "Start block number");
    info!(rpc_url = %cfg.rpc.url, "RPC node");

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

            // Initialize storage with chain-specific path
            let storage_path = format!(
                "{}/{}",
                cfg.storage.path.trim_end_matches('/'),
                cfg.scanner.chain_name
            );

            // Create directory if not exists
            std::fs::create_dir_all(&storage_path)
                .with_context(|| format!("Failed to create storage directory: {}", storage_path))?;

            let storage = RocksDBStorage::new(&storage_path)?;
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
            info!("  └─ Chain: {}", cfg.scanner.chain_name);

            // Create EVM scanner
            let scanner =
                EvmScanner::new(cfg.scanner.clone(), cfg.rpc.url.clone(), storage.clone())?;

            // Initialize scanner
            scanner.init().await?;

            // Spawn cleanup task if enabled
            if cfg.scanner.cleanup_enabled {
                info!("🧹 Cleanup task enabled");
                info!("  └─ Retention blocks: {:?}", cfg.scanner.retention_blocks);
                info!(
                    "  └─ Cleanup interval: {}s",
                    cfg.scanner.cleanup_interval_secs
                );
                info!("  └─ Batch size: {}", cfg.scanner.cleanup_batch_size);

                let cleaner = EvmCleaner::new(cfg.scanner.clone(), storage.clone());
                let cleanup_interval = cfg.scanner.cleanup_interval_secs;
                let mut shutdown_rx_cleanup = shutdown_tx.subscribe();

                tokio::spawn(async move {
                    loop {
                        tokio::select! {
                            _ = shutdown_rx_cleanup.recv() => {
                                info!("🧹 Cleanup task received shutdown signal");
                                break;
                            }
                            _ = tokio::time::sleep(std::time::Duration::from_secs(cleanup_interval)) => {
                                match cleaner.cleanup().await {
                                    Ok(count) if count > 0 => {
                                        info!("🧹 Cleanup cycle completed: {} blocks removed", count);
                                    }
                                    Ok(_) => {
                                        debug!("🧹 Cleanup cycle completed: no blocks to remove");
                                    }
                                    Err(e) => {
                                        warn!("⚠️ Cleanup failed: {}", e);
                                    }
                                }

                                // Print database size after cleanup (skip for now due to private field)
                                // TODO: Add public method to get database size from cleaner
                            }
                        }
                    }
                    info!("🧹 Cleanup task stopped");
                });
            }

            // Start scanning with shutdown support
            info!("🔄 Starting block scanner...");
            info!("💡 Press Ctrl+C to stop gracefully");

            scanner.run(shutdown_rx).await?;

            // Flush database before exit to ensure data consistency
            match storage.flush() {
                Ok(_) => info!("✅ Database flushed successfully"),
                Err(e) => warn!("⚠️ Failed to flush database: {}", e),
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
