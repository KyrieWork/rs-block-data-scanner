use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, info};

/// Retry configuration
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Maximum number of retries; if None, indicates infinite retries
    pub max_retries: Option<u32>,
    /// Delay for each retry (base value)
    pub base_delay: Duration,
    /// Whether to enable exponential backoff (delay * 2)
    pub exponential_backoff: bool,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: Some(3),
            base_delay: Duration::from_secs(2),
            exponential_backoff: true,
        }
    }
}

/// Generic asynchronous retry function
///
/// # Parameters
/// - `operation`: An asynchronous closure that returns `Result<T, E>`
/// - `config`: RetryConfig
///
/// # Returns
/// - Success returns `Ok(T)`
/// - If the maximum number of retries is reached, the last error is returned
pub async fn retry_async<F, Fut, T, E>(mut operation: F, config: RetryConfig) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
    let mut attempt = 0;
    let mut delay = config.base_delay;

    loop {
        attempt += 1;
        match operation().await {
            Ok(result) => {
                if attempt > 1 {
                    info!("✅ Success after {attempt} retries");
                }
                return Ok(result);
            }
            Err(e) => {
                // Print error log
                error!("❌ Attempt {attempt} failed: {e}");

                // If the maximum number of retries is reached, return the last error
                if let Some(max) = config.max_retries
                    && attempt >= max
                {
                    error!("🚫 Maximum retries ({max}) reached, stopping retries.");
                    return Err(e);
                }

                // Calculate backoff time
                info!("⏳ Waiting {:?} before retrying...", delay);
                sleep(delay).await;

                if config.exponential_backoff {
                    delay = std::cmp::min(delay * 2, Duration::from_secs(60)); // Maximum 60s
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    };
    use tokio::time::Instant;

    #[tokio::test]
    async fn test_retry_immediate_success() {
        let config = RetryConfig {
            max_retries: Some(3),
            base_delay: Duration::from_millis(10),
            exponential_backoff: true,
        };

        let result = retry_async(|| async { Ok::<_, String>("success") }, config).await;

        assert!(result.is_ok(), "Should succeed immediately");
        assert_eq!(result.unwrap(), "success");
        println!("✅ Immediate success test passed!");
    }

    #[tokio::test]
    async fn test_retry_success_after_failures() {
        let attempt_count = Arc::new(AtomicU32::new(0));
        let attempt_count_clone = attempt_count.clone();

        let config = RetryConfig {
            max_retries: Some(5),
            base_delay: Duration::from_millis(10),
            exponential_backoff: false,
        };

        let result = retry_async(
            move || {
                let count = attempt_count_clone.fetch_add(1, Ordering::SeqCst) + 1;
                async move {
                    if count < 3 {
                        Err(format!("Attempt {} failed", count))
                    } else {
                        Ok("success after retries")
                    }
                }
            },
            config,
        )
        .await;

        assert!(result.is_ok(), "Should succeed after retries");
        assert_eq!(result.unwrap(), "success after retries");
        assert_eq!(
            attempt_count.load(Ordering::SeqCst),
            3,
            "Should have attempted 3 times"
        );
        println!("✅ Success after failures test passed!");
    }

    #[tokio::test]
    async fn test_retry_max_retries_reached() {
        let attempt_count = Arc::new(AtomicU32::new(0));
        let attempt_count_clone = attempt_count.clone();

        let config = RetryConfig {
            max_retries: Some(3),
            base_delay: Duration::from_millis(10),
            exponential_backoff: false,
        };

        let result = retry_async(
            move || {
                let count = attempt_count_clone.fetch_add(1, Ordering::SeqCst) + 1;
                async move { Err::<(), _>(format!("Attempt {} failed", count)) }
            },
            config,
        )
        .await;

        assert!(result.is_err(), "Should fail after max retries");
        assert_eq!(
            attempt_count.load(Ordering::SeqCst),
            3,
            "Should have attempted exactly 3 times"
        );
        println!("✅ Max retries reached test passed!");
    }

    #[tokio::test]
    async fn test_retry_exponential_backoff() {
        let attempt_count = Arc::new(AtomicU32::new(0));
        let attempt_count_clone = attempt_count.clone();

        let config = RetryConfig {
            max_retries: Some(4),
            base_delay: Duration::from_millis(50),
            exponential_backoff: true,
        };

        let start = Instant::now();

        let result = retry_async(
            move || {
                let count = attempt_count_clone.fetch_add(1, Ordering::SeqCst) + 1;
                async move {
                    if count < 4 {
                        Err(format!("Attempt {} failed", count))
                    } else {
                        Ok("success")
                    }
                }
            },
            config,
        )
        .await;

        let elapsed = start.elapsed();

        assert!(result.is_ok(), "Should succeed after retries");
        assert_eq!(attempt_count.load(Ordering::SeqCst), 4);

        // Expected delays: 50ms + 100ms + 200ms = 350ms
        // Allow some tolerance for execution time
        assert!(
            elapsed >= Duration::from_millis(300),
            "Should have exponential backoff delays, elapsed: {:?}",
            elapsed
        );
        assert!(
            elapsed < Duration::from_millis(500),
            "Delays should not be too long, elapsed: {:?}",
            elapsed
        );

        println!("✅ Exponential backoff test passed! Elapsed: {:?}", elapsed);
    }

    #[tokio::test]
    async fn test_retry_without_exponential_backoff() {
        let attempt_count = Arc::new(AtomicU32::new(0));
        let attempt_count_clone = attempt_count.clone();

        let config = RetryConfig {
            max_retries: Some(4),
            base_delay: Duration::from_millis(50),
            exponential_backoff: false,
        };

        let start = Instant::now();

        let result = retry_async(
            move || {
                let count = attempt_count_clone.fetch_add(1, Ordering::SeqCst) + 1;
                async move {
                    if count < 4 {
                        Err(format!("Attempt {} failed", count))
                    } else {
                        Ok("success")
                    }
                }
            },
            config,
        )
        .await;

        let elapsed = start.elapsed();

        assert!(result.is_ok(), "Should succeed after retries");
        assert_eq!(attempt_count.load(Ordering::SeqCst), 4);

        // Expected delays: 50ms + 50ms + 50ms = 150ms (fixed delay)
        assert!(
            elapsed >= Duration::from_millis(100),
            "Should have fixed delays, elapsed: {:?}",
            elapsed
        );
        assert!(
            elapsed < Duration::from_millis(250),
            "Fixed delays should not vary much, elapsed: {:?}",
            elapsed
        );

        println!(
            "✅ Without exponential backoff test passed! Elapsed: {:?}",
            elapsed
        );
    }

    #[tokio::test]
    async fn test_retry_custom_config() {
        let attempt_count = Arc::new(AtomicU32::new(0));
        let attempt_count_clone = attempt_count.clone();

        // Custom config with 2 retries and 20ms delay
        let config = RetryConfig {
            max_retries: Some(2),
            base_delay: Duration::from_millis(20),
            exponential_backoff: true,
        };

        let result = retry_async(
            move || {
                let count = attempt_count_clone.fetch_add(1, Ordering::SeqCst) + 1;
                async move { Err::<(), _>(format!("Attempt {} failed", count)) }
            },
            config,
        )
        .await;

        assert!(result.is_err(), "Should fail with custom max retries");
        assert_eq!(
            attempt_count.load(Ordering::SeqCst),
            2,
            "Should respect custom max_retries"
        );

        // Test default config
        let default_config = RetryConfig::default();
        assert_eq!(
            default_config.max_retries,
            Some(3),
            "Default max_retries should be 3"
        );
        assert_eq!(
            default_config.base_delay,
            Duration::from_secs(2),
            "Default base_delay should be 2s"
        );
        assert!(
            default_config.exponential_backoff,
            "Default should enable exponential backoff"
        );

        println!("✅ Custom config test passed!");
    }
}
