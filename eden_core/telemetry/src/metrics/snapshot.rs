//! Snapshot metrics for data snapshot operations (fan-out copies).

use fast_telemetry::{DynamicCounter, DynamicDistribution, DynamicGauge, ExportMetrics};

/// Default shard count for thread-sharded counters.
const SHARD_COUNT: usize = 16;
const SNAPSHOT_DIST_MAX_SERIES: usize = 512;

/// Metrics for snapshot operations.
#[derive(ExportMetrics)]
#[metric_prefix = "snapshot"]
#[otlp]
#[clickhouse]
pub struct SnapshotMetrics {
    /// Snapshots started (with snapshot_id, source_endpoint_uuid, target_count labels)
    #[help = "Total snapshots started"]
    started_total: DynamicCounter,

    /// Snapshots completed successfully (with snapshot_id, target_count labels)
    #[help = "Total snapshots completed successfully"]
    completed_total: DynamicCounter,

    /// Snapshots failed (with snapshot_id, error_type labels)
    #[help = "Total snapshots failed"]
    failed_total: DynamicCounter,

    /// Snapshot duration in seconds (with snapshot_id labels)
    #[help = "Snapshot duration in seconds"]
    duration_seconds: DynamicGauge,

    /// Total batches processed in a snapshot (with snapshot_id labels)
    #[help = "Total batches processed in snapshot"]
    batches_total: DynamicGauge,

    /// Per-target write operations (with snapshot_id, target_endpoint_uuid, result labels)
    #[help = "Total per-target write operations"]
    target_writes_total: DynamicCounter,

    /// Per-target write duration in milliseconds (with snapshot_id, target_endpoint_uuid labels)
    #[help = "Per-target write latency (ms)"]
    target_write_duration_milliseconds: DynamicDistribution,

    /// Bytes read from source during snapshot (with snapshot_id labels)
    #[help = "Total bytes read from source"]
    bytes_read_total: DynamicCounter,

    /// Bytes written to targets during snapshot (with snapshot_id, target_endpoint_uuid labels)
    #[help = "Total bytes written to targets"]
    bytes_written_total: DynamicCounter,

    /// Scheduler poll cycles (no labels)
    #[help = "Snapshot scheduler poll cycles"]
    scheduler_polls_total: DynamicCounter,

    /// Snapshots due at last poll (no labels)
    #[help = "Snapshots due at last scheduler poll"]
    scheduler_snapshots_due: DynamicGauge,

    // === CDC Metrics ===
    /// CDC WAL rows consumed (with snapshot_id labels)
    #[help = "Total CDC WAL rows consumed"]
    cdc_rows_consumed_total: DynamicCounter,

    /// CDC rows written to destination (with snapshot_id labels)
    #[help = "Total CDC rows written to destination"]
    cdc_rows_written_total: DynamicCounter,

    /// CDC flush duration in seconds (with snapshot_id labels)
    #[help = "CDC flush duration in seconds"]
    cdc_flush_duration_seconds: DynamicDistribution,

    /// CDC backfill rows (with snapshot_id labels)
    #[help = "Total CDC backfill rows loaded"]
    cdc_backfill_rows_total: DynamicCounter,

    /// CDC active workers gauge
    #[help = "Number of active CDC workers"]
    cdc_active_workers: DynamicGauge,
}

impl SnapshotMetrics {
    pub fn new() -> Self {
        Self {
            started_total: DynamicCounter::new(SHARD_COUNT),
            completed_total: DynamicCounter::new(SHARD_COUNT),
            failed_total: DynamicCounter::new(SHARD_COUNT),
            duration_seconds: DynamicGauge::new(SHARD_COUNT),
            batches_total: DynamicGauge::new(SHARD_COUNT),
            target_writes_total: DynamicCounter::new(SHARD_COUNT),
            target_write_duration_milliseconds: DynamicDistribution::with_max_series(SHARD_COUNT, SNAPSHOT_DIST_MAX_SERIES),
            bytes_read_total: DynamicCounter::new(SHARD_COUNT),
            bytes_written_total: DynamicCounter::new(SHARD_COUNT),
            scheduler_polls_total: DynamicCounter::new(SHARD_COUNT),
            scheduler_snapshots_due: DynamicGauge::new(SHARD_COUNT),
            cdc_rows_consumed_total: DynamicCounter::new(SHARD_COUNT),
            cdc_rows_written_total: DynamicCounter::new(SHARD_COUNT),
            cdc_flush_duration_seconds: DynamicDistribution::with_max_series(SHARD_COUNT, SNAPSHOT_DIST_MAX_SERIES),
            cdc_backfill_rows_total: DynamicCounter::new(SHARD_COUNT),
            cdc_active_workers: DynamicGauge::new(SHARD_COUNT),
        }
    }

    #[inline]
    pub fn record_started(&self, labels: &[(&str, &str)]) {
        log::debug!("[METRIC_RECORDED] snapshot.started_total += 1 (DynamicCounter)");
        self.started_total.inc(labels);
    }

    #[inline]
    pub fn record_completed(&self, labels: &[(&str, &str)]) {
        log::debug!("[METRIC_RECORDED] snapshot.completed_total += 1 (DynamicCounter)");
        self.completed_total.inc(labels);
    }

    #[inline]
    pub fn record_failed(&self, labels: &[(&str, &str)]) {
        log::debug!("[METRIC_RECORDED] snapshot.failed_total += 1 (DynamicCounter)");
        self.failed_total.inc(labels);
    }

    #[inline]
    pub fn record_duration(&self, duration_secs: f64, labels: &[(&str, &str)]) {
        log::debug!("[METRIC_RECORDED] snapshot.duration_seconds = {:.2}s (DynamicGauge)", duration_secs);
        self.duration_seconds.set(labels, duration_secs);
    }

    #[inline]
    pub fn record_batches_total(&self, count: u64, labels: &[(&str, &str)]) {
        log::debug!("[METRIC_RECORDED] snapshot.batches_total = {} (DynamicGauge)", count);
        self.batches_total.set(labels, count as f64);
    }

    #[inline]
    pub fn record_target_write(&self, labels: &[(&str, &str)]) {
        log::debug!("[METRIC_RECORDED] snapshot.target_writes_total += 1 (DynamicCounter)");
        self.target_writes_total.inc(labels);
    }

    #[inline]
    pub fn record_target_write_duration(&self, duration_secs: f64, labels: &[(&str, &str)]) {
        let duration_ms = duration_secs * 1_000.0;
        log::debug!(
            "[METRIC_RECORDED] snapshot.target_write_duration_milliseconds = {:.2}ms (DynamicDistribution)",
            duration_ms
        );
        self.target_write_duration_milliseconds.record(labels, duration_ms as u64);
    }

    #[inline]
    pub fn record_bytes_read(&self, bytes: u64, labels: &[(&str, &str)]) {
        log::debug!("[METRIC_RECORDED] snapshot.bytes_read_total += {} (DynamicCounter)", bytes);
        self.bytes_read_total.add(labels, bytes as isize);
    }

    #[inline]
    pub fn record_bytes_written(&self, bytes: u64, labels: &[(&str, &str)]) {
        log::debug!("[METRIC_RECORDED] snapshot.bytes_written_total += {} (DynamicCounter)", bytes);
        self.bytes_written_total.add(labels, bytes as isize);
    }

    #[inline]
    pub fn record_scheduler_poll(&self, snapshots_due: usize, labels: &[(&str, &str)]) {
        log::debug!("[METRIC_RECORDED] snapshot.scheduler_polls_total += 1 (DynamicCounter)");
        self.scheduler_polls_total.inc(labels);
        self.scheduler_snapshots_due.set(labels, snapshots_due as f64);
    }

    // === CDC metric recorders ===

    #[inline]
    pub fn record_cdc_rows_consumed(&self, count: u64, labels: &[(&str, &str)]) {
        log::debug!("[METRIC_RECORDED] snapshot.cdc_rows_consumed_total += {} (DynamicCounter)", count);
        self.cdc_rows_consumed_total.add(labels, count as isize);
    }

    #[inline]
    pub fn record_cdc_rows_written(&self, count: u64, labels: &[(&str, &str)]) {
        log::debug!("[METRIC_RECORDED] snapshot.cdc_rows_written_total += {} (DynamicCounter)", count);
        self.cdc_rows_written_total.add(labels, count as isize);
    }

    #[inline]
    pub fn record_cdc_flush_duration(&self, duration_secs: f64, labels: &[(&str, &str)]) {
        let duration_ms = duration_secs * 1_000.0;
        log::debug!("[METRIC_RECORDED] snapshot.cdc_flush_duration_seconds = {:.2}ms (DynamicDistribution)", duration_ms);
        self.cdc_flush_duration_seconds.record(labels, duration_ms as u64);
    }

    #[inline]
    pub fn record_cdc_backfill_rows(&self, count: u64, labels: &[(&str, &str)]) {
        log::debug!("[METRIC_RECORDED] snapshot.cdc_backfill_rows_total += {} (DynamicCounter)", count);
        self.cdc_backfill_rows_total.add(labels, count as isize);
    }

    #[inline]
    pub fn record_cdc_active_workers(&self, count: usize, labels: &[(&str, &str)]) {
        log::debug!("[METRIC_RECORDED] snapshot.cdc_active_workers = {} (DynamicGauge)", count);
        self.cdc_active_workers.set(labels, count as f64);
    }

    /// Get total series cardinality across all dynamic snapshot metrics.
    pub fn cardinality(&self) -> usize {
        self.started_total.cardinality()
            + self.completed_total.cardinality()
            + self.failed_total.cardinality()
            + self.duration_seconds.cardinality()
            + self.batches_total.cardinality()
            + self.target_writes_total.cardinality()
            + self.target_write_duration_milliseconds.cardinality()
            + self.bytes_read_total.cardinality()
            + self.bytes_written_total.cardinality()
            + self.scheduler_polls_total.cardinality()
            + self.scheduler_snapshots_due.cardinality()
            + self.cdc_rows_consumed_total.cardinality()
            + self.cdc_rows_written_total.cardinality()
            + self.cdc_flush_duration_seconds.cardinality()
            + self.cdc_backfill_rows_total.cardinality()
            + self.cdc_active_workers.cardinality()
    }

    /// Evict stale series from all snapshot metrics.
    pub fn evict_stale(&self, max_staleness: u32) -> usize {
        let mut evicted = 0;
        evicted += self.started_total.evict_stale(max_staleness);
        evicted += self.completed_total.evict_stale(max_staleness);
        evicted += self.failed_total.evict_stale(max_staleness);
        evicted += self.duration_seconds.evict_stale(max_staleness);
        evicted += self.batches_total.evict_stale(max_staleness);
        evicted += self.target_writes_total.evict_stale(max_staleness);
        evicted += self.target_write_duration_milliseconds.evict_stale(max_staleness);
        evicted += self.bytes_read_total.evict_stale(max_staleness);
        evicted += self.bytes_written_total.evict_stale(max_staleness);
        evicted += self.scheduler_polls_total.evict_stale(max_staleness);
        evicted += self.scheduler_snapshots_due.evict_stale(max_staleness);
        evicted += self.cdc_rows_consumed_total.evict_stale(max_staleness);
        evicted += self.cdc_rows_written_total.evict_stale(max_staleness);
        evicted += self.cdc_flush_duration_seconds.evict_stale(max_staleness);
        evicted += self.cdc_backfill_rows_total.evict_stale(max_staleness);
        evicted += self.cdc_active_workers.evict_stale(max_staleness);
        evicted
    }
}

impl Default for SnapshotMetrics {
    fn default() -> Self {
        Self::new()
    }
}
