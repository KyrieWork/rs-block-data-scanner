# Blockchain Data Scanner

## Project Structure

```
scanner/
├── Cargo.toml
└── src/
    ├── main.rs
    ├── cli.rs                      # Command-line parsing
    ├── config.rs                   # Global configuration
    │
    ├── core/
    │   ├── mod.rs
    │   ├── scanner.rs              # Defines the Scanner trait
    │   ├── types.rs                # Common structures (BlockData, TxData)
    │   └── error.rs                # Common error types
    │
    ├── chains/
    │   ├── mod.rs
    │   ├── evm/
    │   │   ├── mod.rs
    │   │   ├── rpc_client.rs
    │   │   ├── parser.rs
    │   │   └── scanner.rs          # Implements Scanner for EVM
    │   ├── solana/
    │   │   ├── mod.rs
    │   │   ├── rpc_client.rs
    │   │   ├── parser.rs
    │   │   └── scanner.rs          # Implements Scanner for Solana
    │   └── ... (other chains)
    │
    ├── storage/
    │   ├── mod.rs
    │   ├── rocksdb.rs
    │   ├── schema.rs
    │   └── traits.rs
    │
    └── utils/
        ├── logger.rs
        └── retry.rs

```

## Dependencies

| Module             | Library                                | Description                                              |
|--------------------|----------------------------------------|----------------------------------------------------------|
| **Async Runtime**  | `tokio`                                | Task scheduling and concurrent block fetching            |
| **RPC Client**     | `alloy`                                | Interaction with EVM nodes                               |
| **Storage**        | `rocksdb`                              | Storing raw JSON data                                    |
| **Logging System** | `tracing` + `tracing-subscriber`       | Structured logging and span-based tracing                |
| **Config Mgmt**    | `config`                               | Read RPC url, concurrency, start block, etc. from config.yaml |
| **Metrics**        | `metrics` + `prometheus`               | Capture fetch rate, error rate, latency, etc.            |
| **Error Handling** | `anyhow`                               | Unified error stack                                      |

