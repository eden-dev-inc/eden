//! Async batching for telemetry metrics
//! Currently unused because of performance overhead but can be integrated with current metrics

use opentelemetry::KeyValue;
use tokio::sync::mpsc;
use tokio::time::{Duration, interval, timeout};

/// A single metric record to be batched
#[derive(Debug, Clone)]
pub enum MetricRecord {
    /// Counter increment: (metric_name, value, labels)
    CounterAdd { name: String, value: u64, labels: Vec<KeyValue> },
    /// UpDownCounter change: (metric_name, delta, labels)
    UpDownCounterAdd { name: String, delta: i64, labels: Vec<KeyValue> },
    /// Histogram recording: (metric_name, value, labels)
    HistogramRecordU64 { name: String, value: u64, labels: Vec<KeyValue> },
    /// Histogram recording f64: (metric_name, value, labels)
    HistogramRecordF64 { name: String, value: f64, labels: Vec<KeyValue> },
}

/// Configuration for async batching
#[derive(Debug, Clone)]
pub struct AsyncBatchConfig {
    /// Maximum batch size before auto-flush
    pub max_batch_size: usize,
    /// Maximum wait time before auto-flush
    pub max_flush_interval_ms: u64,
    /// Channel buffer size
    pub channel_buffer_size: usize,
}

impl Default for AsyncBatchConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 512,
            max_flush_interval_ms: 5000,
            channel_buffer_size: 1024,
        }
    }
}

#[derive(Clone)]
pub struct AsyncBatchSender {
    tx: mpsc::Sender<MetricRecord>,
}

impl AsyncBatchSender {
    pub fn add_counter(&self, name: impl Into<String>, value: u64, labels: Vec<KeyValue>) {
        let _ = self.tx.try_send(MetricRecord::CounterAdd { name: name.into(), value, labels });
    }

    pub fn add_up_down_counter(&self, name: impl Into<String>, delta: i64, labels: Vec<KeyValue>) {
        let _ = self.tx.try_send(MetricRecord::UpDownCounterAdd { name: name.into(), delta, labels });
    }

    pub fn record_histogram_u64(&self, name: impl Into<String>, value: u64, labels: Vec<KeyValue>) {
        let _ = self.tx.try_send(MetricRecord::HistogramRecordU64 { name: name.into(), value, labels });
    }

    pub fn record_histogram_f64(&self, name: impl Into<String>, value: f64, labels: Vec<KeyValue>) {
        let _ = self.tx.try_send(MetricRecord::HistogramRecordF64 { name: name.into(), value, labels });
    }
}

/// Async batch processor (runs in background task)
pub struct AsyncBatchProcessor {
    rx: mpsc::Receiver<MetricRecord>,
    config: AsyncBatchConfig,
}

impl AsyncBatchProcessor {
    /// let (sender, processor) = AsyncBatchProcessor::new(AsyncBatchConfig::default());
    /// tokio::spawn(async move {
    ///     processor.run(|batch| {
    ///         // Flush batch to OpenTelemetry
    ///         for record in batch {
    ///             match record {
    ///                 MetricRecord::CounterAdd { name, value, labels } => {
    ///                     metrics[name].add(value, &labels);
    ///                 }
    ///             }
    ///         }
    ///     }).await;
    /// });
    ///
    /// sender.add_counter("requests", 1, labels).await;
    pub fn new(config: AsyncBatchConfig) -> (AsyncBatchSender, Self) {
        let (tx, rx) = mpsc::channel(config.channel_buffer_size);

        let sender = AsyncBatchSender { tx };
        let processor = Self { rx, config };

        (sender, processor)
    }

    pub async fn run<F>(mut self, flush_fn: F)
    where
        F: Fn(Vec<MetricRecord>) + Send + 'static,
    {
        let flush_interval = Duration::from_millis(self.config.max_flush_interval_ms);

        loop {
            let mut batch = Vec::with_capacity(self.config.max_batch_size);

            // Collect records until batch is full or timeout
            while batch.len() < self.config.max_batch_size {
                match timeout(flush_interval, self.rx.recv()).await {
                    Ok(Some(record)) => {
                        batch.push(record);
                    }
                    Ok(None) => {
                        // Channel closed, flush remaining and exit
                        if !batch.is_empty() {
                            flush_fn(batch);
                        }
                        log::info!("AsyncBatchProcessor shutting down");
                        return;
                    }
                    Err(_) => {
                        // Timeout - flush what we have
                        break;
                    }
                }
            }

            // Flush batch if not empty
            if !batch.is_empty() {
                log::trace!("Flushing metrics batch: {} records", batch.len());
                flush_fn(batch);
            }
        }
    }

    pub async fn run_with_ticker<F>(mut self, flush_fn: F)
    where
        F: Fn(Vec<MetricRecord>) + Send + 'static,
    {
        let mut ticker = interval(Duration::from_millis(self.config.max_flush_interval_ms));
        let mut batch = Vec::with_capacity(self.config.max_batch_size);

        loop {
            tokio::select! {
                // Receive records
                Some(record) = self.rx.recv() => {
                    batch.push(record);

                    // Flush if batch is full
                    if batch.len() >= self.config.max_batch_size {
                        log::trace!("Flushing metrics batch (size): {} records", batch.len());
                        flush_fn(std::mem::replace(&mut batch, Vec::with_capacity(self.config.max_batch_size)));
                    }
                }

                // Periodic flush
                _ = ticker.tick() => {
                    if !batch.is_empty() {
                        log::trace!("Flushing metrics batch (time): {} records", batch.len());
                        flush_fn(std::mem::replace(&mut batch, Vec::with_capacity(self.config.max_batch_size)));
                    }
                }
            }
        }
    }
}

/// Statistics about the batching system
#[derive(Debug, Clone)]
pub struct BatchStats {
    pub total_batches_flushed: u64,
    pub total_records_flushed: u64,
    pub avg_batch_size: f64,
    pub max_batch_size: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use tokio::time::sleep;

    #[tokio::test]
    async fn test_batch_on_size() {
        let config = AsyncBatchConfig {
            max_batch_size: 3,
            max_flush_interval_ms: 10000, // Long timeout
            channel_buffer_size: 10,
        };

        let flushed = Arc::new(AtomicU64::new(0));
        let flushed_clone = flushed.clone();

        let (sender, processor) = AsyncBatchProcessor::new(config);

        // Spawn processor
        tokio::spawn(async move {
            processor
                .run(move |batch| {
                    flushed_clone.fetch_add(batch.len() as u64, Ordering::SeqCst);
                })
                .await;
        });

        // Send 3 records (should trigger flush)
        sender.add_counter("test", 1, vec![]);
        sender.add_counter("test", 1, vec![]);
        sender.add_counter("test", 1, vec![]);

        // Give time for flush
        sleep(Duration::from_millis(50)).await;

        assert_eq!(flushed.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_batch_on_timeout() {
        let config = AsyncBatchConfig {
            max_batch_size: 100,       // Large batch
            max_flush_interval_ms: 50, // Short timeout
            channel_buffer_size: 100,
        };

        let flushed = Arc::new(AtomicU64::new(0));
        let flushed_clone = flushed.clone();

        let (sender, processor) = AsyncBatchProcessor::new(config);

        tokio::spawn(async move {
            processor
                .run(move |batch| {
                    flushed_clone.fetch_add(batch.len() as u64, Ordering::SeqCst);
                })
                .await;
        });

        // Send only 2 records (won't hit size limit)
        sender.add_counter("test", 1, vec![]);
        sender.add_counter("test", 1, vec![]);

        // Wait for timeout flush
        sleep(Duration::from_millis(100)).await;

        assert_eq!(flushed.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_high_throughput() {
        let config = AsyncBatchConfig::default();
        let flushed = Arc::new(AtomicU64::new(0));
        let flushed_clone = flushed.clone();

        let (sender, processor) = AsyncBatchProcessor::new(config);

        tokio::spawn(async move {
            processor
                .run(move |batch| {
                    flushed_clone.fetch_add(batch.len() as u64, Ordering::SeqCst);
                })
                .await;
        });

        // Send 1000 records rapidly
        for i in 0..1000 {
            sender.add_counter("test", i, vec![]);
        }

        // Wait for all to flush
        sleep(Duration::from_millis(200)).await;

        assert_eq!(flushed.load(Ordering::SeqCst), 512);
    }
}
