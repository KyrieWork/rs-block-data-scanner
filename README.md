# RS Block Data Scanner

A high-performance blockchain data scanner written in Rust, supporting real-time block data collection and storage for EVM-compatible chains.

## Project Overview

RS Block Data Scanner is a professional blockchain data collection tool designed for efficient and reliable scanning and storage of blockchain data. Built with Rust, it leverages Rust's memory safety and high-performance characteristics to handle large-scale real-time blockchain data collection requirements.

### Key Features

- **Multi-chain Support**: Currently supports EVM-compatible chains (Ethereum, BSC, etc.) with architecture designed for easy extension to other chains
- **High-Performance Storage**: RocksDB-based high-performance key-value storage optimized for large block data
- **Real-time Scanning**: Supports real-time block scanning and historical data backfill
- **Reorganization Detection**: Intelligent detection and handling of blockchain reorganization events
- **Data Cleanup**: Configurable automatic data cleanup mechanisms
- **Monitoring Metrics**: Integrated Prometheus metrics export
- **Graceful Shutdown**: Signal handling and graceful shutdown support

## Project Structure

```
rs-block-data-scanner/
├── Cargo.toml                      # Project configuration and dependencies
├── config_example.yaml             # Configuration file example
├── config.bsc.yaml                 # BSC chain configuration
├── config.bsc.prod.yaml            # BSC production configuration
├── start.sh                        # Startup script
├── stop.sh                         # Stop script
├── rebuild.sh                      # Rebuild script
├── Makefile                        # Build tools
├── data/                           # Data storage directory
│   └── rocksdb/                    # RocksDB database files
├── logs/                           # Log files directory
└── src/
    ├── main.rs                     # Main program entry point
    ├── cli.rs                      # Command-line argument parsing
    ├── config.rs                   # Configuration management
    ├── lib.rs                      # Library entry point
    │
    ├── core/                       # Core modules
    │   ├── mod.rs
    │   ├── types.rs                # Common data structures
    │   └── table.rs                # Data table structures
    │
    ├── chains/                     # Blockchain support modules
    │   ├── mod.rs
    │   └── evm/                    # EVM-compatible chain support
    │       ├── mod.rs
    │       ├── client.rs           # RPC client
    │       ├── scanner.rs          # EVM scanner implementation
    │       ├── checker.rs          # Data validator
    │       └── cleaner.rs          # EVM data cleaner
    │
    ├── storage/                    # Storage modules
    │   ├── mod.rs
    │   ├── rocksdb.rs              # RocksDB storage implementation
    │   ├── manager.rs              # Storage manager
    │   ├── schema.rs               # Data schema definitions
    │   └── traits.rs               # Storage trait definitions
    │
    └── utils/                      # Utility modules
        ├── mod.rs
        ├── logger.rs               # Logging utilities
        └── format.rs               # Formatting utilities
```

## Technology Stack

| Module | Library | Description |
|--------|---------|-------------|
| **Async Runtime** | `tokio` | Task scheduling and concurrent block fetching |
| **RPC Client** | `alloy` | Interaction with EVM nodes |
| **Storage Engine** | `rocksdb` | High-performance key-value storage for raw JSON data |
| **Logging System** | `tracing` + `tracing-subscriber` | Structured logging and span-based tracing |
| **Config Management** | `config` | Reading RPC URL, concurrency, start block, etc. from config.yaml |
| **Metrics Monitoring** | `metrics` + `prometheus` | Capturing fetch rate, error rate, latency, etc. |
| **Error Handling** | `anyhow` | Unified error stack |
| **Serialization** | `serde` + `serde_json` | JSON data serialization and deserialization |
| **Command Line** | `clap` | Command-line argument parsing |
| **Time Processing** | `chrono` | Timestamp handling |

## Quick Start

### 1. Prerequisites

- Rust 1.75+ (for edition 2024 support) 
- Sufficient disk space (recommended at least 100GB for storing block data)
- Stable network connection and RPC node access

### 2. Build the Project

```bash
# Clone the project
git clone <repository-url>
cd rs-block-data-scanner

# Build the project
cargo build --release

# Or use the provided script
./rebuild.sh
```

### 3. Configuration

Copy the configuration file and modify the relevant settings:

```bash
cp config_example.yaml config.yaml
```

Main configuration items:
- `scanner.chain_name`: Chain name (e.g., "ethereum", "bsc")
- `scanner.start_block`: Starting block number (0 means start from the latest block)
- `rpc.url`: RPC node URL
- `storage.path`: Data storage path
- `logging.level`: Log level

### 4. Run

```bash
# Run directly
./target/release/rs-block-data-scanner --config config.yaml

# Or use the startup script
./start.sh
```

### 5. Monitoring

- View logs: `tail -f logs/<chain_name>.scanner.log`
- Stop service: `./stop.sh`
- Monitor metrics: Visit `http://localhost:9100/metrics`

## Configuration

The project supports rich configuration options, including:

- **Scanning Behavior**: Concurrency, timeout, confirmation blocks
- **Performance Optimization**: Batch size, reorganization detection, retry mechanism
- **Data Cleanup**: Automatic cleanup, retention policy, cleanup interval
- **Storage Configuration**: Compression, column families, WAL settings
- **Logging Configuration**: Level, file output, timezone settings
- **Monitoring Configuration**: Prometheus export port

For detailed configuration instructions, please refer to the `config_example.yaml` file.

## Data Storage

The project uses RocksDB as the storage engine, optimized for large block data:

- **Compression**: Uses LZ4 compression algorithm to reduce storage space
- **Batch Processing**: Batch writes to improve performance
- **Health Check**: Regular database health status checks
- **Data Cleanup**: Supports automatic cleanup of expired data

Stored data includes:
- Block data (complete block information)
- Transaction receipts
- Transaction trace logs
- Scanning progress information

## Performance Features

- **High Concurrency**: Supports multi-threaded concurrent scanning
- **Memory Optimization**: Memory usage optimization for large block data
- **Network Optimization**: Intelligent retry and error handling
- **Storage Optimization**: RocksDB parameter tuning for large-scale data storage

## Development Guide

### Code Quality

```bash
# Format code
make fmt

# Run tests
make test

# Code linting
make clippy

# Complete verification
make verify
```

### Extending New Chains

1. Create a new chain module under `src/chains/`
2. Implement the `Scanner` trait
3. Add corresponding configuration support
4. Update the main program to support the new chain type

## License

[Please add license information]

## Contributing

Issues and Pull Requests are welcome to improve the project.

