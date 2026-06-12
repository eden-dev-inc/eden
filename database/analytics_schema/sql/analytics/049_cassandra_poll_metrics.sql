-- Cassandra poll metrics from metadata collection
-- One row per endpoint per frequency tier per collection cycle

CREATE TABLE IF NOT EXISTS analytics.cassandra_poll_metrics
(
    -- Common
    snapshot_time DateTime64(3, 'UTC'),
    organization_uuid String CODEC(ZSTD(3)),
    endpoint_uuid String CODEC(ZSTD(3)),
    frequency LowCardinality(String),
    collection_ms UInt32,
    had_fatal UInt8 DEFAULT 0,

    -- Cluster (high)
    total_nodes Nullable(UInt64),
    up_nodes Nullable(UInt64),
    down_nodes Nullable(UInt64),
    cluster_health_pct Nullable(Float64),
    schema_agreement Nullable(UInt8),
    total_client_connections Nullable(UInt64),
    pending_compactions Nullable(UInt64),
    active_repairs Nullable(UInt64),

    -- Node resource metrics (high, first node)
    heap_memory_used_mb Nullable(Float64),
    heap_memory_max_mb Nullable(Float64),
    heap_memory_utilization_pct Nullable(Float64),
    cpu_utilization_pct Nullable(Float64),
    disk_used_gb Nullable(Float64),
    disk_utilization_pct Nullable(Float64),

    -- Node performance metrics (high, first node)
    read_requests_per_sec Nullable(Float64),
    write_requests_per_sec Nullable(Float64),
    avg_read_latency_ms Nullable(Float64),
    avg_write_latency_ms Nullable(Float64),
    cache_hit_ratio_pct Nullable(Float64),

    -- Thread pools (high)
    threadpool_active_threads Nullable(UInt64),
    threadpool_pending_tasks Nullable(UInt64),
    threadpool_dropped_tasks Nullable(UInt64),
    threadpool_health_score Nullable(Float64),

    -- JSON blobs (high)
    cluster_info_json String DEFAULT '{}' CODEC(ZSTD(3)),
    node_info_json String DEFAULT '[]' CODEC(ZSTD(3)),
    threadpool_info_json String DEFAULT '{}' CODEC(ZSTD(3)),

    -- Compaction (medium)
    compaction_pending Nullable(UInt64),
    compaction_active Nullable(UInt64),
    compaction_rate_mb_per_sec Nullable(Float64),

    -- Repair (medium)
    repair_success_rate_pct Nullable(Float64),
    keyspaces_needing_repair Nullable(UInt64),

    -- Tombstone (medium)
    tombstone_health_score Nullable(Float64),
    high_tombstone_ratio_tables Nullable(UInt64),

    -- JSON blobs (medium)
    compaction_info_json String DEFAULT '{}' CODEC(ZSTD(3)),
    repair_info_json String DEFAULT '{}' CODEC(ZSTD(3)),
    tombstone_info_json String DEFAULT '{}' CODEC(ZSTD(3)),
    keyspace_info_json String DEFAULT '[]' CODEC(ZSTD(3)),
    table_info_json String DEFAULT '[]' CODEC(ZSTD(3)),
    snapshot_info_json String DEFAULT '{}' CODEC(ZSTD(3)),

    -- JSON blobs (low)
    schema_info_json String DEFAULT '{}' CODEC(ZSTD(3))
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(snapshot_time)
ORDER BY (organization_uuid, endpoint_uuid, frequency, snapshot_time)
TTL toDateTime(snapshot_time) + INTERVAL 90 DAY;
