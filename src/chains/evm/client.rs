use alloy::{
    providers::{Provider, ProviderBuilder, RootProvider},
    transports::http::{Client, Http},
};
use anyhow::{Context, Result, anyhow};
use std::{future::Future, sync::Arc};
use tokio::sync::Mutex;
use tracing::{debug, warn};

use crate::core::table::BlockData;

#[derive(Clone)]
struct RpcEndpoint {
    url: String,
    provider: Option<Arc<RootProvider<Http<Client>>>>,
}

impl RpcEndpoint {
    fn new(url: String) -> Self {
        Self {
            url,
            provider: None,
        }
    }
}

struct RpcEndpointPool {
    endpoints: Vec<RpcEndpoint>,
}

impl RpcEndpointPool {
    fn new(primary: String, backups: Vec<String>) -> Self {
        let mut endpoints = Vec::new();
        endpoints.push(RpcEndpoint::new(primary));
        for backup in backups {
            if backup.trim().is_empty() {
                continue;
            }
            endpoints.push(RpcEndpoint::new(backup));
        }
        Self { endpoints }
    }

    fn len(&self) -> usize {
        self.endpoints.len()
    }

    fn endpoint_url(&self, idx: usize) -> Option<&str> {
        self.endpoints
            .get(idx)
            .map(|endpoint| endpoint.url.as_str())
    }

    fn ensure_provider(&mut self, idx: usize) -> Result<Arc<RootProvider<Http<Client>>>> {
        let endpoint = self
            .endpoints
            .get_mut(idx)
            .ok_or_else(|| anyhow!("RPC endpoint index {} out of bounds", idx))?;

        if let Some(provider) = endpoint.provider.clone() {
            return Ok(provider);
        }

        let provider = Arc::new(
            ProviderBuilder::new()
                .on_http(endpoint.url.parse().context("Invalid RPC endpoint URL")?),
        );
        endpoint.provider = Some(provider.clone());
        Ok(provider)
    }

    fn invalidate(&mut self, idx: usize) {
        if let Some(endpoint) = self.endpoints.get_mut(idx) {
            endpoint.provider = None;
        }
    }
}

pub struct EvmClient {
    pool: Arc<Mutex<RpcEndpointPool>>,
}

impl EvmClient {
    pub fn new(primary: String, backups: Vec<String>) -> Result<Self> {
        if primary.trim().is_empty() {
            return Err(anyhow!("Primary RPC endpoint cannot be empty"));
        }

        let pool = RpcEndpointPool::new(primary, backups);
        Ok(Self {
            pool: Arc::new(Mutex::new(pool)),
        })
    }

    async fn probe_provider(provider: Arc<RootProvider<Http<Client>>>) -> Result<()> {
        provider.get_block_number().await?;
        Ok(())
    }

    async fn provider_for_index(
        &self,
        idx: usize,
    ) -> Result<(Arc<RootProvider<Http<Client>>>, String)> {
        let (provider, url) = {
            let mut pool = self.pool.lock().await;
            if idx >= pool.len() {
                return Err(anyhow!("RPC endpoint index {} out of bounds", idx));
            }
            let url = pool
                .endpoint_url(idx)
                .map(|url| url.to_string())
                .unwrap_or_default();
            let provider = pool.ensure_provider(idx)?;
            (provider, url)
        };
        Ok((provider, url))
    }

    async fn execute_with_failover_internal<T, FO, FP, FutO, FutP>(
        &self,
        mut operation: FO,
        mut probe: FP,
    ) -> Result<T>
    where
        T: Send,
        FO: FnMut(usize, Arc<RootProvider<Http<Client>>>, String) -> FutO,
        FP: FnMut(usize, Arc<RootProvider<Http<Client>>>, String) -> FutP,
        FutO: Future<Output = Result<T>> + Send,
        FutP: Future<Output = Result<()>> + Send,
    {
        let mut attempt_index = 0usize;
        let mut last_err: Option<anyhow::Error> = None;

        loop {
            let endpoint_count = {
                let pool = self.pool.lock().await;
                pool.len()
            };

            if attempt_index >= endpoint_count {
                return match last_err {
                    Some(err) => Err(err),
                    None => Err(anyhow!("No RPC endpoints available")),
                };
            }

            let (provider, url) = self.provider_for_index(attempt_index).await?;

            match operation(attempt_index, provider.clone(), url.clone()).await {
                Ok(result) => return Ok(result),
                Err(err) => {
                    if probe(attempt_index, provider.clone(), url.clone())
                        .await
                        .is_ok()
                    {
                        return Err(err);
                    }

                    let mut pool = self.pool.lock().await;
                    if let Some(endpoint_url) = pool.endpoint_url(attempt_index) {
                        warn!(
                            url = %endpoint_url,
                            "RPC endpoint unhealthy, attempting failover"
                        );
                    }
                    pool.invalidate(attempt_index);
                    last_err = Some(err);
                    attempt_index += 1;
                    continue;
                }
            }
        }
    }

    async fn execute_with_failover<T, F, Fut>(&self, operation: F) -> Result<T>
    where
        T: Send,
        F: Fn(Arc<RootProvider<Http<Client>>>) -> Fut + Send + Sync,
        Fut: Future<Output = Result<T>> + Send,
    {
        self.execute_with_failover_internal(
            |_, provider, _| operation(provider),
            |_, provider, _| async move { Self::probe_provider(provider).await },
        )
        .await
    }

    async fn fetch_debug_trace_with_provider(
        provider: Arc<RootProvider<Http<Client>>>,
        block_hash: &str,
    ) -> Result<String> {
        debug!("🖨️ Fetching debug trace block for block {}", block_hash);
        let tracer_options = serde_json::json!({
            "tracer": "callTracer"
        });

        let params = serde_json::value::to_raw_value(&(block_hash, tracer_options))
            .map_err(|e| anyhow!("Failed to serialize request parameters: {}", e))?;

        match provider
            .raw_request_dyn("debug_traceBlockByHash".into(), &params)
            .await
        {
            Ok(raw_response) => {
                let debug_trace_json = raw_response.get();
                debug!(
                    "🖨️ Debug trace block JSON length: {} characters",
                    debug_trace_json.len()
                );
                Ok(debug_trace_json.to_string())
            }
            Err(e) => {
                let error_msg = e.to_string();
                if error_msg.contains("-32601") || error_msg.contains("Method not found") {
                    warn!(
                        "⚠️ debug_traceBlockByHash not supported by this node, using empty trace data"
                    );
                    Ok("[]".to_string())
                } else {
                    Err(anyhow!("debug_traceBlockByHash failed: {}", e))
                }
            }
        }
    }

    pub async fn get_debug_trace_by_hash(&self, block_hash: &str) -> Result<String> {
        let hash = block_hash.to_string();
        self.execute_with_failover(move |provider| {
            let hash = hash.clone();
            async move { Self::fetch_debug_trace_with_provider(provider, &hash).await }
        })
        .await
    }

    pub async fn get_latest_block_number(&self) -> Result<u64> {
        self.execute_with_failover(|provider| async move {
            let latest_block_number = provider.get_block_number().await?;
            Ok(latest_block_number)
        })
        .await
    }

    pub async fn fetch_block_data_by_number(&self, block_number: u64) -> Result<BlockData> {
        self.execute_with_failover(move |provider| async move {
            let block = provider
                .get_block_by_number(block_number.into(), true)
                .await?
                .ok_or_else(|| anyhow!("Block {} not found", block_number))?;

            let receipts = provider
                .get_block_receipts(block_number.into())
                .await?
                .ok_or_else(|| anyhow!("Block {} not found", block_number))?;

            let debug_trace_str = Self::fetch_debug_trace_with_provider(
                provider.clone(),
                &block.header.hash.to_string(),
            )
            .await?;

            let number = block.header.number;
            let parent_hash = format!("{:?}", block.header.parent_hash);
            let block_data_json = serde_json::to_string(&block)?;

            Ok(BlockData {
                hash: format!("{:?}", block.header.hash),
                block_number: number,
                parent_hash,
                block_data_json,
                block_receipts_json: serde_json::to_string(&receipts)?,
                debug_trace_block_json: debug_trace_str,
            })
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::{Result, anyhow};
    use std::{collections::VecDeque, sync::Arc};
    use tokio::sync::Mutex as AsyncMutex;

    #[tokio::test]
    async fn primary_endpoint_succeeds() -> Result<()> {
        let client = EvmClient::new("http://localhost:1".to_string(), Vec::new())?;
        let value = client
            .execute_with_failover_internal(
                |idx, provider, url| async move {
                    assert_eq!(idx, 0);
                    assert!(!url.is_empty());
                    let _ = provider;
                    Ok::<_, anyhow::Error>(123u64)
                },
                |_, provider, _| async move {
                    let _ = provider;
                    Ok::<_, anyhow::Error>(())
                },
            )
            .await?;
        assert_eq!(value, 123);
        Ok(())
    }

    #[tokio::test]
    async fn failover_to_backup_and_restore_primary() -> Result<()> {
        let client = EvmClient::new(
            "http://primary.endpoint".to_string(),
            vec!["http://backup.endpoint".to_string()],
        )?;

        struct EndpointBehavior {
            operation: VecDeque<Result<u64>>,
            probe: VecDeque<Result<()>>,
        }

        let state = Arc::new(AsyncMutex::new(vec![
            EndpointBehavior {
                operation: VecDeque::from([Err(anyhow!("primary failure")), Ok(5)]),
                probe: VecDeque::from([Err(anyhow!("primary unhealthy")), Ok(())]),
            },
            EndpointBehavior {
                operation: VecDeque::from([Ok(2)]),
                probe: VecDeque::from([Ok(())]),
            },
        ]));

        let result = client
            .execute_with_failover_internal(
                {
                    let state = state.clone();
                    move |idx, provider, _| {
                        let state = state.clone();
                        let _ = provider;
                        async move {
                            let mut guard = state.lock().await;
                            guard[idx].operation.pop_front().expect("operation entry")
                        }
                    }
                },
                {
                    let state = state.clone();
                    move |idx, provider, _| {
                        let state = state.clone();
                        let _ = provider;
                        async move {
                            let mut guard = state.lock().await;
                            guard[idx].probe.pop_front().unwrap_or(Ok(()))
                        }
                    }
                },
            )
            .await?;
        assert_eq!(result, 2);

        let result_primary = client
            .execute_with_failover_internal(
                {
                    let state = state.clone();
                    move |idx, provider, _| {
                        let state = state.clone();
                        let _ = provider;
                        async move {
                            let mut guard = state.lock().await;
                            guard[idx]
                                .operation
                                .pop_front()
                                .unwrap_or_else(|| Err(anyhow!("unexpected operation entry")))
                        }
                    }
                },
                {
                    let state = state.clone();
                    move |idx, provider, _| {
                        let state = state.clone();
                        let _ = provider;
                        async move {
                            let mut guard = state.lock().await;
                            guard[idx].probe.pop_front().unwrap_or(Ok(()))
                        }
                    }
                },
            )
            .await?;
        assert_eq!(result_primary, 5);

        let remaining = state.lock().await;
        assert!(remaining[0].operation.is_empty());
        assert!(remaining[1].operation.is_empty());
        Ok(())
    }
}
