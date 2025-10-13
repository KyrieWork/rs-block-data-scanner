use anyhow::Result;
use clap::Parser;
use rs_block_data_scanner::{
    chains::evm::scanner::EvmScanner,
    cli::Cli,
    config::AppConfig,
    core::scanner::Scanner,
    storage::{rocksdb::RocksDBStorage, traits::KVStorage},
    utils::logger::init_logger,
};
use tokio::sync::broadcast;
use tracing::{error, info, warn};

#[tokio::main]
async fn main() -> Result<()> {
    let args = Cli::parse();
    let cfg = AppConfig::load(&args.config)?;

    // Initialize logger system
    init_logger(
        &cfg.logging.level,
        cfg.logging.to_file,
        &cfg.logging.file_path,
    );

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

            // Initialize storage
            let storage = RocksDBStorage::new(&cfg.storage.path)?;
            storage.init()?;
            info!("✅ Storage initialized at: {}", cfg.storage.path);

            // Create EVM scanner
            let scanner = EvmScanner::new(cfg.scanner.clone(), cfg.rpc.url.clone(), storage)?;

            // Initialize scanner
            scanner.init().await?;

            // Start scanning with shutdown support
            info!("🔄 Starting block scanner...");
            info!("💡 Press Ctrl+C to stop gracefully");

            scanner.run(shutdown_rx).await?;

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
