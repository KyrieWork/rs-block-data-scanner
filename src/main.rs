mod cli;
mod config;

use anyhow::Result;
use clap::Parser;
use cli::Cli;
use config::AppConfig;

#[tokio::main]
async fn main() -> Result<()> {
    let args = Cli::parse();

    let cfg = AppConfig::load(&args.config)?;

    println!("✅ Config file loaded successfully");
    println!("{:#?}", cfg);

    // Simulate service startup (here can be expanded to start different modules according to chain_type)
    match cfg.scanner.chain_type.as_str() {
        "evm" => {
            println!("🚀 Start EVM blockchain data scanning service...");
        }
        "solana" => {
            println!("🚀 Start Solana blockchain data scanning service...");
        }
        other => {
            println!("⚠️ Unknown chain type: {}", other);
        }
    }

    Ok(())
}
