-- Infrastructure snapshot metrics
-- Periodic snapshots of data snapshot (fan-out copy) operation metrics

CREATE TABLE IF NOT EXISTS analytics.infrastructure_snapshots
(
    -- Timing & Identity
    snapshot_time DateTime64(3, 'UTC'),
    organization_uuid String CODEC(ZSTD(3)),
    snapshot_uuid String CODEC(ZSTD(3)),
    source_endpoint_uuid String CODEC(ZSTD(3)),

    -- Status
    status LowCardinality(String),  -- 'started', 'completed', 'failed', 'scheduler_poll'
    error_type Nullable(String) CODEC(ZSTD(3)),

    -- Counts
    target_count UInt32 DEFAULT 0,
    batches_total UInt64 DEFAULT 0,

    -- Duration
    duration_secs Float64 DEFAULT 0,

    -- Throughput
    bytes_written_total UInt64 DEFAULT 0,

    -- Per-target aggregates
    target_writes_success UInt64 DEFAULT 0,
    target_writes_failure UInt64 DEFAULT 0,

    -- Scheduler metrics (only populated for scheduler poll events)
    is_scheduler_poll UInt8 DEFAULT 0,
    scheduler_snapshots_due UInt32 DEFAULT 0
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(snapshot_time)
ORDER BY (organization_uuid, snapshot_uuid, snapshot_time)
TTL toDateTime(snapshot_time) + INTERVAL 90 DAY;
