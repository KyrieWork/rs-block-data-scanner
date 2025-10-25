use alloy::{
    providers::{Provider, ProviderBuilder, RootProvider},
    transports::http::{Client, Http},
};
use anyhow::Result;
use std::sync::Arc;
use tracing::{debug, warn};

use crate::core::table::BlockData;

pub struct EvmClient {
    pub provider: Arc<RootProvider<Http<Client>>>,
}

impl EvmClient {
    pub fn new(rpc_url: &str) -> Result<Self> {
        let provider = Arc::new(ProviderBuilder::new().on_http(rpc_url.parse()?));
        Ok(Self { provider })
    }

    pub async fn get_debug_trace_by_hash(&self, block_hash: &str) -> Result<String> {
        debug!("🖨️ Fetching debug trace block for block {}", block_hash);
        // debug_traceBlockByHash requires tracer options to specify the tracer type
        // Using "callTracer" to get the call tree format that matches our structure
        let tracer_options = serde_json::json!({
            "tracer": "callTracer"
        });
        // Try to get debug trace data, but handle cases where the method is not supported
        // Use raw_request_dyn to get raw JSON response without deserialization to avoid recursion limit
        let params = serde_json::value::to_raw_value(&(block_hash, tracer_options))
            .map_err(|e| anyhow::anyhow!("Failed to serialize request parameters: {}", e))?;

        match self
            .provider
            .raw_request_dyn("debug_traceBlockByHash".into(), &params)
            .await
        {
            Ok(raw_response) => {
                // Get the raw JSON string directly without any deserialization
                let debug_trace_json = raw_response.get();

                debug!(
                    "🖨️ Debug trace block JSON length: {} characters",
                    debug_trace_json.len()
                );

                Ok(debug_trace_json.to_string())
            }
            Err(e) => {
                // Check for various error conditions and handle gracefully
                let error_msg = e.to_string();
                if error_msg.contains("-32601") || error_msg.contains("Method not found") {
                    warn!(
                        "⚠️ debug_traceBlockByHash not supported by this node, using empty trace data"
                    );
                    Ok("[]".to_string()) // Return empty array as fallback
                } else {
                    Err(anyhow::anyhow!("debug_traceBlockByHash failed: {}", e))
                }
            }
        }
    }

    pub async fn get_latest_block_number(&self) -> Result<u64> {
        let latest_block_number = self.provider.get_block_number().await?;
        Ok(latest_block_number)
    }

    pub async fn fetch_block_data_by_number(&self, block_number: u64) -> Result<BlockData> {
        let block = self
            .provider
            .get_block_by_number(block_number.into(), true)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Block {} not found", block_number))?;

        let receipts = self
            .provider
            .get_block_receipts(block_number.into())
            .await?
            .ok_or_else(|| anyhow::anyhow!("Block {} not found", block_number))?;

        let debug_trace_str = self
            .get_debug_trace_by_hash(&block.header.hash.to_string())
            .await?;

        Ok(BlockData {
            hash: format!("{:?}", block.header.hash),
            block_data_json: serde_json::to_string(&block)?,
            block_receipts_json: serde_json::to_string(&receipts)?,
            debug_trace_block_json: debug_trace_str,
        })
    }
}
