-- Oracle poll metrics from metadata collection
-- One row per endpoint per frequency tier per collection cycle

CREATE TABLE IF NOT EXISTS analytics.oracle_poll_metrics
(
    -- Common
    snapshot_time DateTime64(3, 'UTC'),
    organization_uuid String CODEC(ZSTD(3)),
    endpoint_uuid String CODEC(ZSTD(3)),
    frequency LowCardinality(String),
    collection_ms UInt32,
    had_fatal UInt8 DEFAULT 0,

    -- Activity / Connection / Session (high)
    active_sessions Nullable(UInt64),
    total_sessions Nullable(UInt64),
    max_sessions Nullable(UInt64),
    session_utilization_pct Nullable(Float64),
    waiting_sessions_count Nullable(UInt64),
    blocking_sessions_count Nullable(UInt64),
    current_processes Nullable(UInt64),
    max_processes Nullable(UInt64),
    process_utilization_pct Nullable(Float64),
    sga_size Nullable(UInt64),
    current_pga_used Nullable(UInt64),

    -- Locks (high)
    total_active_locks Nullable(UInt64),
    blocking_locks Nullable(UInt64),
    blocked_sessions Nullable(UInt64),
    total_deadlocks Nullable(UInt64),
    max_lock_wait_time Nullable(Float64),

    -- Performance (high)
    health_score Nullable(Float64),

    -- Transactions (high)
    active_transactions Nullable(UInt64),
    user_commits Nullable(UInt64),
    user_rollbacks Nullable(UInt64),
    rollback_ratio Nullable(Float64),
    transaction_health_score Nullable(Float64),

    -- Wait events (high)
    cpu_time_percent Nullable(Float64),
    wait_time_percent Nullable(Float64),
    wait_health_score Nullable(Float64),

    -- JSON blobs (high)
    activity_info_json String DEFAULT '{}' CODEC(ZSTD(3)),
    connection_info_json String DEFAULT '{}' CODEC(ZSTD(3)),
    lock_info_json String DEFAULT '{}' CODEC(ZSTD(3)),
    performance_stats_json String DEFAULT '{}' CODEC(ZSTD(3)),
    session_info_json String DEFAULT '{}' CODEC(ZSTD(3)),
    transaction_info_json String DEFAULT '{}' CODEC(ZSTD(3)),
    wait_events_json String DEFAULT '{}' CODEC(ZSTD(3)),

    -- Database stats scalars (medium, first element)
    buffer_cache_hit_ratio Nullable(Float64),
    transactions_per_sec Nullable(Float64),
    physical_reads_per_sec Nullable(Float64),
    database_size Nullable(UInt64),
    used_space Nullable(UInt64),
    uptime_seconds Nullable(Float64),

    -- JSON blobs (medium)
    database_stats_json String DEFAULT '[]' CODEC(ZSTD(3)),
    index_info_json String DEFAULT '[]' CODEC(ZSTD(3)),
    redolog_info_json String DEFAULT '{}' CODEC(ZSTD(3)),
    segment_info_json String DEFAULT '[]' CODEC(ZSTD(3)),
    storage_info_json String DEFAULT '{}' CODEC(ZSTD(3)),
    table_info_json String DEFAULT '[]' CODEC(ZSTD(3)),
    tablespace_info_json String DEFAULT '[]' CODEC(ZSTD(3)),

    -- JSON blobs (low)
    parameter_info_json String DEFAULT '{}' CODEC(ZSTD(3))
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(snapshot_time)
ORDER BY (organization_uuid, endpoint_uuid, frequency, snapshot_time)
TTL toDateTime(snapshot_time) + INTERVAL 90 DAY;
