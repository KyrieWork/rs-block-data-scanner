use std::time::Duration;

/// Categorizes block fetch failures for metrics reporting.
#[derive(Clone, Copy)]
pub enum BlockFetchFailureReason {
    Empty,
    Rpc,
    Timeout,
}

/// Abstraction over scanner runtime metrics, allowing a no-op implementation when disabled.
pub trait ScannerMetrics: Send + Sync {
    fn record_block_fetch_success(&self);
    fn record_block_fetch_failure(&self, reason: BlockFetchFailureReason);
    fn record_storage_batch(&self, duration: Duration, block_count: usize, cleanup_keys: usize);
    fn record_blocks_processed(&self, processed: u64, current_block: u64);
    fn record_sync_gap(&self, gap: u64);
}

#[derive(Default)]
pub struct NoopScannerMetrics;

impl NoopScannerMetrics {
    pub fn new() -> Self {
        Self
    }
}

impl ScannerMetrics for NoopScannerMetrics {
    fn record_block_fetch_success(&self) {}

    fn record_block_fetch_failure(&self, _reason: BlockFetchFailureReason) {}

    fn record_storage_batch(&self, _duration: Duration, _block_count: usize, _cleanup_keys: usize) {
    }

    fn record_blocks_processed(&self, _processed: u64, _current_block: u64) {}

    fn record_sync_gap(&self, _gap: u64) {}
}

pub struct PrometheusScannerMetrics {
    chain: String,
}

impl PrometheusScannerMetrics {
    pub fn new(chain: impl Into<String>) -> Self {
        Self {
            chain: chain.into(),
        }
    }
}

impl ScannerMetrics for PrometheusScannerMetrics {
    fn record_block_fetch_success(&self) {
        let chain = self.chain.clone();
        metrics::counter!(
            "scanner_block_fetch_success_total",
            1,
            "chain" => chain
        );
    }

    fn record_block_fetch_failure(&self, reason: BlockFetchFailureReason) {
        let chain = self.chain.clone();
        let reason_label = match reason {
            BlockFetchFailureReason::Empty => "empty",
            BlockFetchFailureReason::Rpc => "rpc_error",
            BlockFetchFailureReason::Timeout => "timeout",
        };
        metrics::counter!(
            "scanner_block_fetch_failure_total",
            1,
            "chain" => chain,
            "reason" => reason_label
        );
    }

    fn record_storage_batch(&self, duration: Duration, block_count: usize, cleanup_keys: usize) {
        let chain = self.chain.clone();
        metrics::histogram!(
            "scanner_storage_batch_seconds",
            duration.as_secs_f64(),
            "chain" => chain.clone()
        );
        metrics::counter!(
            "scanner_storage_batch_total",
            1,
            "chain" => chain.clone()
        );
        metrics::gauge!(
            "scanner_storage_batch_blocks",
            block_count as f64,
            "chain" => chain.clone()
        );
        if cleanup_keys > 0 {
            metrics::counter!(
                "scanner_cleanup_keys_total",
                cleanup_keys as u64,
                "chain" => chain
            );
        }
    }

    fn record_blocks_processed(&self, processed: u64, current_block: u64) {
        let chain = self.chain.clone();
        if processed > 0 {
            metrics::counter!(
                "scanner_blocks_processed_total",
                processed,
                "chain" => chain.clone()
            );
        }
        metrics::gauge!(
            "scanner_current_block",
            current_block as f64,
            "chain" => chain
        );
    }

    fn record_sync_gap(&self, gap: u64) {
        let chain = self.chain.clone();
        metrics::gauge!(
            "scanner_sync_gap_blocks",
            gap as f64,
            "chain" => chain
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
    use std::sync::Once;

    fn install_debug_recorder() {
        static INIT: Once = Once::new();
        INIT.call_once(|| {
            let recorder = DebuggingRecorder::per_thread();
            let _ = recorder.install();
        });
    }

    #[test]
    fn noop_metrics_do_not_panic() {
        let metrics = NoopScannerMetrics::new();
        metrics.record_block_fetch_success();
        metrics.record_block_fetch_failure(BlockFetchFailureReason::Rpc);
        metrics.record_storage_batch(Duration::from_secs(1), 5, 3);
        metrics.record_blocks_processed(10, 20);
        metrics.record_sync_gap(0);
    }

    #[test]
    fn prometheus_metrics_emit_values() {
        install_debug_recorder();

        let metrics = PrometheusScannerMetrics::new("test-chain");
        metrics.record_block_fetch_success();
        metrics.record_block_fetch_failure(BlockFetchFailureReason::Timeout);
        metrics.record_storage_batch(Duration::from_millis(500), 4, 2);
        metrics.record_blocks_processed(7, 15);
        metrics.record_sync_gap(6);

        let snapshot = Snapshotter::current_thread_snapshot().expect("snapshot");
        let entries = snapshot.into_vec();

        let mut seen_success = false;
        let mut seen_failure = false;
        let mut seen_blocks = false;
        let mut seen_current = false;
        let mut seen_gap = false;

        for (key, _, _, value) in entries {
            let name = key.key().name().to_string();
            match (name.as_str(), value) {
                ("scanner_block_fetch_success_total", DebugValue::Counter(1)) => {
                    seen_success = true;
                }
                ("scanner_block_fetch_failure_total", DebugValue::Counter(1)) => {
                    seen_failure = true;
                }
                ("scanner_storage_batch_blocks", DebugValue::Gauge(v)) => {
                    assert_eq!(v.into_inner(), 4.0);
                    seen_blocks = true;
                }
                ("scanner_current_block", DebugValue::Gauge(v)) => {
                    assert_eq!(v.into_inner(), 15.0);
                    seen_current = true;
                }
                ("scanner_sync_gap_blocks", DebugValue::Gauge(v)) => {
                    assert_eq!(v.into_inner(), 6.0);
                    seen_gap = true;
                }
                _ => {}
            }
        }

        assert!(seen_success, "success counter missing");
        assert!(seen_failure, "failure counter missing");
        assert!(seen_blocks, "storage gauge missing");
        assert!(seen_current, "current block gauge missing");
        assert!(seen_gap, "sync gap gauge missing");
    }
}
