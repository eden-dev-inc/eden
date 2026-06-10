use crate::duration::DurationNanos;
use crate::metrics::endpoint::EndpointMetrics;
use std::time::Instant;

#[must_use = "EndpointGuard must be held for duration of operation - dropping immediately defeats the purpose"]
pub struct EndpointGuard<'a> {
    start: Instant,
    metrics: &'a EndpointMetrics,
    labels: Vec<(String, String)>,
}

impl<'a> EndpointGuard<'a> {
    pub fn new(metrics: &'a EndpointMetrics, labels: &[(&str, &str)]) -> Self {
        // Increment both total_requests and active_requests
        metrics.start_endpoint_request(labels);

        // Store owned copies for use in Drop
        let labels_owned: Vec<(String, String)> = labels.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();

        Self { start: Instant::now(), metrics, labels: labels_owned }
    }

    /// Get elapsed time as typed duration
    pub fn elapsed(&self) -> DurationNanos {
        DurationNanos::from(self.start.elapsed())
    }

    /// Get elapsed time in nanoseconds (raw value)
    pub fn elapsed_nanos(&self) -> u64 {
        self.elapsed().as_nanos()
    }
}

impl<'a> Drop for EndpointGuard<'a> {
    fn drop(&mut self) {
        let duration = self.elapsed();

        // Record duration and decrement active_requests
        let label_refs: Vec<(&str, &str)> = self.labels.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        self.metrics.finish_endpoint_request(duration, &label_refs);

        log::trace!("EndpointGuard recorded: {} ms", duration.as_millis());
    }
}
