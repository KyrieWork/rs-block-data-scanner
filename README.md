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
- **Smart RPC Failover**: Primary endpoint first, automatic fallback to backups with lightweight health probes
- **Dynamic Concurrency Control**: Tokio semaphore limits concurrent RPC calls to the configured concurrency
- **Monitoring Metrics**: Integrated Prometheus metrics export controllable via configuration
- **Graceful Shutdown**: Signal handling and graceful shutdown support

## Project Structure

```
rs-block-data-scanner/
├── Cargo.toml                      # Project configuration and dependencies
├── Makefile                        # Build and verification commands
├── rebuild.sh                      # Convenience rebuild script
├── config_example.yaml             # Example configuration template
├── config.yaml                     # Default (editable) configuration used by scripts
├── config.bsc.prod.yaml            # Production-oriented configuration example
├── scripts/                        # Deployment helpers
│   ├── start_scanner.sh            # Start the scanner service (background)
│   ├── stop_scanner.sh             # Stop the scanner service
│   ├── start_api.sh                # Start the read-only API service
│   ├── stop_api.sh                 # Stop the API service
│   └── help.sh                     # Quick reference for script usage
├── data/                           # Runtime data (RocksDB, etc.)
│   └── rocksdb/
├── logs/                           # Runtime logs (scanner/api)
├── src/
│   ├── bin/
│   │   └── api_main.rs             # API service entrypoint
│   ├── api/                        # API modules (request handlers and storage adapters)
│   ├── chains/                     # Blockchain-specific logic (EVM, etc.)
│   ├── core/                       # Shared data structures
│   ├── storage/                    # RocksDB abstractions and manager
│   ├── utils/                      # Logger, metrics, helpers
│   ├── cli.rs                      # Command-line parsing
│   ├── config.rs                   # Config loading/validation
│   ├── main.rs                     # Scanner entrypoint
│   └── lib.rs                      # Library exports
└── tests/                          # Integration tests (e.g., reorg scenarios)
```

## Technology Stack

| Module                 | Library                          | Description                                                      |
| ---------------------- | -------------------------------- | ---------------------------------------------------------------- |
| **Async Runtime**      | `tokio`                          | Task scheduling and concurrent block fetching                    |
| **RPC Client**         | `alloy`                          | Interaction with EVM nodes                                       |
| **Storage Engine**     | `rocksdb`                        | High-performance key-value storage for raw JSON data             |
| **Logging System**     | `tracing` + `tracing-subscriber` | Structured logging and span-based tracing                        |
| **Config Management**  | `config`                         | Reading RPC URL, concurrency, start block, etc. from config.yaml |
| **Metrics Monitoring** | `metrics` + `prometheus`         | Capturing fetch rate, error rate, latency, etc.                  |
| **Error Handling**     | `anyhow`                         | Unified error stack                                              |
| **Serialization**      | `serde` + `serde_json`           | JSON data serialization and deserialization                      |
| **Command Line**       | `clap`                           | Command-line argument parsing                                    |
| **Time Processing**    | `chrono`                         | Timestamp handling                                               |

## Quick Start

### 1. Prerequisites

- Rust 1.75+ (for edition 2024 support)
- Sufficient disk space (Recommend at least 30GB for storing block data, depending on your configuration)
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

The scanner expects a `config.yaml` file at the repository root. You can use the provided template as a starting point:

```bash
cp config_example.yaml config.yaml
```

Update the file before running the service (especially `rpc.url`, `storage.path`, logging settings, and cleanup policy). Key configuration groups:

- `scanner.*`: Chain metadata, concurrency, clean-up behaviour and reorg thresholds
- `rpc.url`: Primary RPC endpoint (required)
- `rpc.backups`: Optional list of backup endpoints; each request starts from the primary and fails over only when necessary
- `storage.path`: Data storage path
- `logging.*`: Log level, log directory and timezone
- `metrics.enable`: Enable/disable Prometheus exporter
- `metrics.prometheus_exporter_port`: Exporter listen port (default `9100`)

### 4. Run

```bash
# Run directly
./target/release/rs-block-data-scanner --config config.yaml

# Or start via script (runs the release binary in the background)
./scripts/start_scanner.sh
```

### 5. Monitoring

- View logs: `tail -f logs/<chain_name>.scanner.log`
- Stop service: `./scripts/stop_scanner.sh`
- Monitor metrics: Visit `http://localhost:9100/metrics` (if `metrics.enable = true`)
- Optional: start the read-only API service with `./scripts/start_api.sh`

### API Service

The API binary (`api_main`) shares the same configuration file as the scanner and reads RocksDB in read-only mode, so both services can run simultaneously.

```bash
# Start API service
./scripts/start_api.sh

# Stop API service
./scripts/stop_api.sh

# Query kv endpoint
curl "http://localhost:9001/kv/bsc:block_data:<block_hash>"

# Query progress endpoint
curl "http://localhost:9001/progress"
```

Available endpoints:

- `GET /kv/{key}` returns `{ "key": string, "value": any }`. If the stored value is valid JSON (e.g. block data, receipts, traces) it is parsed and returned as JSON; otherwise the raw string is returned.
- `GET /progress` returns the latest scanner progress JSON for the configured `scanner.chain_name`, which is also a convenient health check of the API ↔ RocksDB connection.

Common keys:

- `bsc:block_data:<block_hash>` – block payload.
- `bsc:block_receipts:<block_hash>` – receipts payload.
- `bsc:block_debug_trace:<block_hash>` – debug trace payload.
- `bsc:progress` – scanner progress record (consumed by `/progress`).

## Configuration Details

The project supports rich configuration options, including:

- **Scanning Behavior**: Concurrency, timeout, confirmation blocks
- **Performance Optimization**: Batch size, reorganization detection, retry mechanism
- **Data Cleanup**: Automatic cleanup, retention policy, cleanup interval
- **Storage Configuration**: Compression, column families, WAL settings
- **Logging Configuration**: Level, file output, timezone settings
- **Monitoring Configuration**: Enable switch, Prometheus export port

See `config_example.yaml` for inline documentation of all available fields.

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

- **High Concurrency**: Supports multi-threaded concurrent scanning with semaphore-based rate limiting
- **Memory Optimization**: Memory usage optimization for large block data
- **Network Optimization**: Intelligent retry, RPC health probes and automatic failover
- **Storage Optimization**: RocksDB parameter tuning for large-scale data storage

## Testing

Integration and unit tests can be executed with:

```bash
make verify
```

This includes the reorg integration test located at `tests/evm_reorg.rs`, which exercises rollback safety guards.

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
