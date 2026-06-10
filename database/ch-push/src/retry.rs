use std::future::Future;
use std::sync::atomic::Ordering;
use std::time::Duration;

use backon::{BackoffBuilder, ExponentialBuilder};

use crate::stats::DeadLetterStats;

#[derive(Clone, Copy, Debug)]
pub struct RetryConfig {
    pub max_retries: usize,
    pub initial_backoff: Duration,
}

fn retry_backoff_sequence(config: RetryConfig) -> impl Iterator<Item = Duration> {
    ExponentialBuilder::default()
        .with_min_delay(config.initial_backoff)
        .with_factor(2.0)
        .without_max_delay()
        .with_max_times(config.max_retries)
        .build()
}

pub async fn with_retry<F, Fut, T, E>(config: RetryConfig, stats: Option<&DeadLetterStats>, mut op: F) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    // First attempt (no backoff)
    match op().await {
        Ok(value) => return Ok(value),
        Err(err) if config.max_retries == 0 => return Err(err),
        Err(_) => {}
    }

    // Retry attempts
    for (index, backoff) in retry_backoff_sequence(config).enumerate() {
        let attempt = index + 1;
        if let Some(stats) = stats {
            stats.retry_attempts.fetch_add(1, Ordering::Relaxed);
        }
        tokio::time::sleep(backoff).await;

        match op().await {
            Ok(value) => {
                if let Some(stats) = stats {
                    stats.successful_retries.fetch_add(1, Ordering::Relaxed);
                }
                return Ok(value);
            }
            Err(err) if attempt == config.max_retries => return Err(err),
            Err(_) => {}
        }
    }

    // Only reachable if max_retries is 0 and first attempt didn't return
    // (which can't happen due to the guard above)
    op().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

    fn assert_duration_close(actual: Duration, expected: Duration) {
        assert!(actual.abs_diff(expected) <= Duration::from_micros(1), "expected {expected:?}, got {actual:?}");
    }

    #[tokio::test]
    async fn with_retry_first_attempt_is_immediate() {
        let config = RetryConfig { max_retries: 4, initial_backoff: Duration::from_millis(1) };
        let stats = DeadLetterStats::default();
        let attempts = Arc::new(AtomicUsize::new(0));

        let result = with_retry(config, Some(&stats), {
            let attempts = attempts.clone();
            move || {
                let attempts = attempts.clone();
                async move {
                    attempts.fetch_add(1, AtomicOrdering::SeqCst);
                    Ok::<usize, &'static str>(7)
                }
            }
        })
        .await;

        assert_eq!(result, Ok(7));
        assert_eq!(attempts.load(AtomicOrdering::SeqCst), 1);
        assert_eq!(stats.retry_attempts.load(AtomicOrdering::Relaxed), 0);
        assert_eq!(stats.successful_retries.load(AtomicOrdering::Relaxed), 0);
    }

    #[tokio::test]
    async fn with_retry_uses_one_plus_max_retries_on_persistent_failure() {
        let config = RetryConfig { max_retries: 3, initial_backoff: Duration::from_millis(1) };
        let stats = DeadLetterStats::default();
        let attempts = Arc::new(AtomicUsize::new(0));

        let result = with_retry(config, Some(&stats), {
            let attempts = attempts.clone();
            move || {
                let attempts = attempts.clone();
                async move {
                    attempts.fetch_add(1, AtomicOrdering::SeqCst);
                    Err::<(), &'static str>("failed")
                }
            }
        })
        .await;

        assert_eq!(result, Err("failed"));
        assert_eq!(attempts.load(AtomicOrdering::SeqCst), 4);
        assert_eq!(stats.retry_attempts.load(AtomicOrdering::Relaxed), 3);
        assert_eq!(stats.successful_retries.load(AtomicOrdering::Relaxed), 0);
    }

    #[tokio::test]
    async fn with_retry_success_on_nth_retry_increments_successful_retries_once() {
        let config = RetryConfig { max_retries: 5, initial_backoff: Duration::from_millis(1) };
        let stats = DeadLetterStats::default();
        let attempts = Arc::new(AtomicUsize::new(0));

        let result = with_retry(config, Some(&stats), {
            let attempts = attempts.clone();
            move || {
                let attempts = attempts.clone();
                async move {
                    let attempt = attempts.fetch_add(1, AtomicOrdering::SeqCst) + 1;
                    if attempt >= 3 {
                        Ok::<usize, &'static str>(attempt)
                    } else {
                        Err::<usize, &'static str>("retry")
                    }
                }
            }
        })
        .await;

        assert_eq!(result, Ok(3));
        assert_eq!(attempts.load(AtomicOrdering::SeqCst), 3);
        assert_eq!(stats.retry_attempts.load(AtomicOrdering::Relaxed), 2);
        assert_eq!(stats.successful_retries.load(AtomicOrdering::Relaxed), 1);
    }

    #[test]
    fn retry_backoff_sequence_doubles_each_retry_and_respects_retry_count() {
        let config = RetryConfig { max_retries: 5, initial_backoff: Duration::from_millis(5) };

        let delays: Vec<_> = retry_backoff_sequence(config).collect();

        assert_eq!(delays.len(), 5);
        let expected = [
            Duration::from_millis(5),
            Duration::from_millis(10),
            Duration::from_millis(20),
            Duration::from_millis(40),
            Duration::from_millis(80),
        ];
        for (actual, expected) in delays.into_iter().zip(expected) {
            assert_duration_close(actual, expected);
        }
    }

    #[test]
    fn retry_backoff_sequence_saturates_at_duration_max() {
        let config = RetryConfig {
            max_retries: 3,
            initial_backoff: Duration::from_secs(u64::MAX),
        };
        let delays: Vec<_> = retry_backoff_sequence(config).collect();

        assert_eq!(delays.len(), 3);
        assert_eq!(delays[1], Duration::MAX);
        assert_eq!(delays[2], Duration::MAX);
    }
}
