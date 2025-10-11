# 区块链数据扫描器

## 项目结构

```
scanner/
├── Cargo.toml
└── src/
    ├── main.rs
    ├── cli.rs                      # 命令行解析
    ├── config.rs                   # 全局配置
    │
    ├── core/
    │   ├── mod.rs
    │   ├── scanner.rs              # 定义 Scanner trait
    │   ├── types.rs                # 通用结构（BlockData, TxData）
    │   └── error.rs                # 通用错误类型
    │
    ├── chains/
    │   ├── mod.rs
    │   ├── evm/
    │   │   ├── mod.rs
    │   │   ├── rpc_client.rs
    │   │   ├── parser.rs
    │   │   └── scanner.rs          # 实现 Scanner for EVM
    │   ├── solana/
    │   │   ├── mod.rs
    │   │   ├── rpc_client.rs
    │   │   ├── parser.rs
    │   │   └── scanner.rs          # 实现 Scanner for Solana
    │   └── ... (其他链)
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

## 依赖库

| 模块          | 库                                | 说明                                  |
| ----------- | -------------------------------- | ----------------------------------- |
| **异步调度**    | `tokio`                          | 任务调度、并发获取区块                         |
| **RPC 客户端** | `alloy`                          | 与 EVM 节点交互                          |
| **存储**      | `rocksdb`                        | 原始 JSON 数据存储                        |
| **日志系统**    | `tracing` + `tracing-subscriber` | 结构化日志、span 链路追踪                     |
| **配置管理**    | `config`                         | 从 `config.yaml` 读取 RPC 地址、并发度、起始区块等 |
| **监控指标**    | `metrics` + `prometheus`         | 记录抓取速率、错误率、延迟等                      |
| **错误处理**    | `anyhow`                         | 统一错误堆栈                              |
