use std::sync::Arc;

use anyhow::Result;
use chrono::Utc;
use rs_block_data_scanner::{
    chains::evm::{checker::EvmChecker, context::EvmScannerContext},
    config::ScannerConfig,
    core::table::BlockIndex,
    storage::{manager::ScannerStorageManager, rocksdb::RocksDBStorage, traits::KVStorage},
    utils::metrics::{NoopScannerMetrics, ScannerMetrics},
};
use tempfile::TempDir;
use tokio::sync::Semaphore;

fn test_scanner_config() -> ScannerConfig {
    ScannerConfig {
        chain_type: "evm".to_string(),
        chain_name: "test-chain".to_string(),
        concurrency: 4,
        start_block: 0,
        confirm_blocks: 12,
        timeout_secs: 10,
        cleanup_enabled: true,
        retention_blocks: Some(50),
        cleanup_interval_blocks: 10,
        cleanup_batch_size: 25,
        cleanup_orphaned_enabled: false,
        batch_save_size: 50,
        reorg_check_enabled: true,
        synced_interval_secs: 3,
        catching_up_interval_millis: 10,
        error_interval_secs: 1,
        max_rollback_depth: 64,
        deep_reorg_confirmation_count: 3,
        reorg_mode_exit_threshold: 5,
        reorg_mode_exit_timeout: 300,
        max_retry_count: 3,
    }
}

fn build_context(
    config: ScannerConfig,
) -> Result<(
    TempDir,
    Arc<RocksDBStorage>,
    Arc<ScannerStorageManager>,
    EvmScannerContext,
)> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("rocksdb");
    let db_path_str = db_path.to_string_lossy().to_string();
    let storage = Arc::new(RocksDBStorage::new(&db_path_str)?);
    storage.init()?;

    let storage_manager = Arc::new(ScannerStorageManager::new(
        storage.clone(),
        config.chain_name.clone(),
    ));

    let metrics: Arc<dyn ScannerMetrics> = Arc::new(NoopScannerMetrics::new());
    let semaphore = Arc::new(Semaphore::new(config.concurrency));

    let context = EvmScannerContext::new(
        Arc::new(config),
        Arc::clone(&storage_manager),
        metrics,
        semaphore,
    );

    Ok((temp_dir, storage, storage_manager, context))
}

#[test]
fn handle_reorg_within_limits_updates_progress() -> Result<()> {
    let mut config = test_scanner_config();
    config.max_rollback_depth = 10;

    let (_temp_dir, storage, storage_manager, context) = build_context(config.clone())?;

    let progress_store = storage_manager.progress.clone();
    let mut progress = progress_store.get_initial_progress(20);
    progress.current_block = 20;
    progress_store.update(progress)?;

    let index = BlockIndex {
        block_hash: "hash-19".to_string(),
        parent_hash: "hash-18".to_string(),
        created_at: Utc::now(),
    };
    storage_manager
        .block_index
        .batch_save_indexes(vec![(19, index)])?;

    let checker = EvmChecker::new(context.clone());
    checker.handle_reorg(20)?;

    let updated = progress_store.get()?;
    assert_eq!(updated.current_block, 19);
    assert!(matches!(updated.reorg_block, Some(19)));
    assert!(matches!(
        updated.status,
        rs_block_data_scanner::core::table::ScannerStatus::ReorgDetected
    ));

    assert!(storage_manager.block_index.get_active(19).is_err());

    drop(checker);
    drop(storage_manager);
    drop(storage);

    Ok(())
}

#[test]
fn handle_reorg_exceeding_depth_returns_error() -> Result<()> {
    let mut config = test_scanner_config();
    config.max_rollback_depth = 5;

    let (_temp_dir, storage, storage_manager, context) = build_context(config.clone())?;

    let progress_store = storage_manager.progress.clone();
    let mut progress = progress_store.get_initial_progress(50);
    progress.current_block = 50;
    progress_store.update(progress)?;

    let index = BlockIndex {
        block_hash: "hash-29".to_string(),
        parent_hash: "hash-28".to_string(),
        created_at: Utc::now(),
    };
    storage_manager
        .block_index
        .batch_save_indexes(vec![(29, index)])?;

    let checker = EvmChecker::new(context);
    let err = checker
        .handle_reorg(30)
        .expect_err("expected rollback depth error");
    assert!(
        err.to_string()
            .contains("exceeds configured max rollback depth")
    );

    drop(storage_manager);
    drop(storage);

    Ok(())
}
