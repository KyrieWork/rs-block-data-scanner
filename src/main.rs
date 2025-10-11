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
use tracing::{info, warn};

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
    info!(start_block = cfg.scanner.start_block, "Start block number");
    info!(rpc_url = %cfg.rpc.url, "RPC node");

    // Simulate service startup (here can be expanded to start different modules according to chain_type)
    match cfg.scanner.chain_type.as_str() {
        "evm" => {
            info!("🚀 Start EVM blockchain data scanning service...");

            // Initialize storage
            let storage = RocksDBStorage::new(&cfg.storage.path)?;
            storage.init()?;
            info!("✅ Storage initialized at: {}", cfg.storage.path);

            // Create EVM scanner
            let scanner = EvmScanner::new(cfg.scanner.clone(), cfg.rpc.url.clone(), storage);

            // Initialize scanner (creates initial progress if not exists)
            scanner.init().await?;
            info!(
                "✅ Scanner initialized - Chain: {}, Start block: {}",
                cfg.scanner.chain_name, cfg.scanner.start_block
            );

            // Start scanning loop
            info!("🔄 Starting block scanner...");
            scanner.run().await?;
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
