use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(
    name = "scanner",
    author = "K",
    version,
    about = "Multi-chain block data scanner (EVM / Solana)"
)]
pub struct Cli {
    /// Specify the config file path (default: ./config.yaml)
    #[arg(long, default_value = "config.yaml")]
    pub config: PathBuf,

    /// Start subcommand (future extensible)
    #[command(subcommand)]
    pub command: Option<Commands>,
}

/// Extensible commands (future support export analysis)
#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Start scanning service
    Run,
}
