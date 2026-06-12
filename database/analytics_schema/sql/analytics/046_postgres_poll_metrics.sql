-- PostgreSQL poll metrics from metadata collection
-- One row per endpoint per frequency tier per collection cycle

CREATE TABLE IF NOT EXISTS analytics.postgres_poll_metrics
(
    -- Common
    snapshot_time DateTime64(3, 'UTC'),
    organization_uuid String CODEC(ZSTD(3)),
    endpoint_uuid String CODEC(ZSTD(3)),
    frequency LowCardinality(String),
    collection_ms UInt32,
    had_fatal UInt8 DEFAULT 0,

    -- Activity (high)
    active_connections Nullable(UInt64),
    idle_connections Nullable(UInt64),
    idle_in_transaction Nullable(UInt64),
    total_connections Nullable(UInt64),
    max_connections Nullable(UInt64),
    connection_utilization_pct Nullable(Float64),
    waiting_queries_count Nullable(UInt64),
    blocking_queries_count Nullable(UInt64),

    -- Locks (high)
    total_locks Nullable(UInt64),
    granted_locks Nullable(UInt64),
    waiting_locks Nullable(UInt64),
    deadlock_count Nullable(UInt64),
    max_lock_wait_time Nullable(Float64),

    -- Performance (high)
    buffer_cache_hit_ratio Nullable(Float64),
    index_hit_ratio Nullable(Float64),
    total_operations Nullable(UInt64),
    total_transactions Nullable(UInt64),
    total_blocks_read Nullable(UInt64),
    total_blocks_hit Nullable(UInt64),
    total_temp_files Nullable(UInt64),
    total_temp_bytes Nullable(UInt64),

    -- Replication (high)
    is_primary Nullable(UInt8),
    is_in_recovery Nullable(UInt8),
    active_replicas Nullable(UInt64),
    max_replica_lag_seconds Nullable(Float64),
    synchronous_replicas Nullable(UInt64),

    -- Transactions (high)
    xact_committed Nullable(UInt64),
    xact_rolled_back Nullable(UInt64),
    commit_ratio Nullable(Float64),
    deadlocks_total Nullable(UInt64),

    -- WAL (high)
    wal_bytes Nullable(UInt64),
    wal_records Nullable(UInt64),
    wal_fpi Nullable(UInt64),

    -- JSON blobs (high)
    activity_info_json String DEFAULT '{}' CODEC(ZSTD(3)),
    lock_info_json String DEFAULT '{}' CODEC(ZSTD(3)),

    -- BGWriter (medium)
    buffers_checkpoint Nullable(UInt64),
    buffers_clean Nullable(UInt64),
    buffers_backend Nullable(UInt64),

    -- JSON (medium)
    database_stats_json String DEFAULT '[]' CODEC(ZSTD(3)),
    table_info_json String DEFAULT '{}' CODEC(ZSTD(3)),
    index_info_json String DEFAULT '{}' CODEC(ZSTD(3)),
    vacuum_info_json String DEFAULT '{}' CODEC(ZSTD(3)),

    -- JSON (low)
    extensions_json String DEFAULT '[]' CODEC(ZSTD(3)),
    settings_json String DEFAULT '{}' CODEC(ZSTD(3))
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(snapshot_time)
ORDER BY (organization_uuid, endpoint_uuid, frequency, snapshot_time)
TTL toDateTime(snapshot_time) + INTERVAL 90 DAY;
