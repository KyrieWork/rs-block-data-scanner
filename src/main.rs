mod cli;
mod config;
mod utils;

use anyhow::Result;
use clap::Parser;
use cli::Cli;
use config::AppConfig;
use tracing::{info, warn};
use utils::logger::init_logger;

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
        }
        "solana" => {
            info!("🚀 Start Solana blockchain data scanning service...");
        }
        other => {
            warn!("⚠️ Unknown chain type: {}", other);
        }
    }

    Ok(())
}
