-- MongoDB poll metrics from metadata collection
-- One row per endpoint per frequency tier per collection cycle

CREATE TABLE IF NOT EXISTS analytics.mongo_poll_metrics
(
    -- Common
    snapshot_time DateTime64(3, 'UTC'),
    organization_uuid String CODEC(ZSTD(3)),
    endpoint_uuid String CODEC(ZSTD(3)),
    frequency LowCardinality(String),
    collection_ms UInt32,
    had_fatal UInt8 DEFAULT 0,

    -- Connections (high)
    current_connections Nullable(UInt64),
    available_connections Nullable(UInt64),
    total_created Nullable(UInt64),

    -- Locks (high)
    current_queue_total Nullable(UInt64),
    current_queue_readers Nullable(UInt64),
    current_queue_writers Nullable(UInt64),
    deadlocks_detected Nullable(UInt64),
    lock_contention_ratio Nullable(Float64),

    -- Network (high)
    bytes_in Nullable(UInt64),
    bytes_out Nullable(UInt64),
    num_requests Nullable(Float64),

    -- Performance (high)
    overall_performance_score Nullable(Float64),

    -- WiredTiger (high)
    cache_bytes_currently_in_cache Nullable(UInt64),
    cache_maximum_bytes_configured Nullable(UInt64),
    cache_evictions Nullable(UInt64),
    pages_read_into_cache Nullable(UInt64),
    pages_written_from_cache Nullable(UInt64),
    cache_hit_ratio Nullable(Float64),

    -- Replication (high)
    is_primary Nullable(UInt8),
    replication_lag_ms Nullable(Float64),
    member_count Nullable(UInt32),

    -- Transactions (high)
    total_started Nullable(UInt64),
    total_committed Nullable(UInt64),
    total_aborted Nullable(UInt64),

    -- JSON blobs (high)
    server_info_json String DEFAULT '{}' CODEC(ZSTD(3)),
    replication_info_json String DEFAULT '{}' CODEC(ZSTD(3)),
    wiredtiger_info_json String DEFAULT '{}' CODEC(ZSTD(3)),

    -- Oplog (medium)
    oplog_size_mb Nullable(Float64),
    oplog_used_mb Nullable(Float64),

    -- JSON (medium)
    aggregation_stats_json String DEFAULT '{}' CODEC(ZSTD(3)),
    collection_info_json String DEFAULT '[]' CODEC(ZSTD(3)),
    database_stats_json String DEFAULT '[]' CODEC(ZSTD(3)),
    index_info_json String DEFAULT '[]' CODEC(ZSTD(3)),
    profiler_info_json String DEFAULT '{}' CODEC(ZSTD(3)),
    sharding_info_json String DEFAULT '{}' CODEC(ZSTD(3)),

    -- JSON (low)
    balancer_info_json String DEFAULT '{}' CODEC(ZSTD(3)),
    memory_info_json String DEFAULT '{}' CODEC(ZSTD(3)),
    security_info_json String DEFAULT '{}' CODEC(ZSTD(3)),
    user_info_json String DEFAULT '[]' CODEC(ZSTD(3))
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(snapshot_time)
ORDER BY (organization_uuid, endpoint_uuid, frequency, snapshot_time)
TTL toDateTime(snapshot_time) + INTERVAL 90 DAY;
