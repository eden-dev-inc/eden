-- ClickHouse poll metrics from metadata collection
-- One row per endpoint per frequency tier per collection cycle

CREATE TABLE IF NOT EXISTS analytics.clickhouse_poll_metrics
(
    -- Common
    snapshot_time DateTime64(3, 'UTC'),
    organization_uuid String CODEC(ZSTD(3)),
    endpoint_uuid String CODEC(ZSTD(3)),
    frequency LowCardinality(String),
    collection_ms UInt32,
    had_fatal UInt8 DEFAULT 0,

    -- Activity (high)
    running_queries Nullable(UInt64),
    queued_queries Nullable(UInt64),
    longest_query_duration Nullable(Float64),
    queries_per_second Nullable(Float64),
    query_memory_usage Nullable(UInt64),

    -- Connections (high)
    total_connections Nullable(UInt64),
    max_connections Nullable(UInt64),
    connection_utilization_pct Nullable(Float64),
    active_users_count Nullable(UInt64),

    -- Queries (high)
    slow_queries Nullable(UInt64),
    high_memory_queries Nullable(UInt64),
    avg_query_execution_time Nullable(Float64),
    total_bytes_read Nullable(UInt64),
    total_rows_processed Nullable(UInt64),

    -- Cluster (high)
    cluster_health_pct Nullable(Float64),
    total_shards Nullable(UInt64),
    total_replicas Nullable(UInt64),

    -- Replication (high)
    avg_replication_lag Nullable(Float64),
    max_replication_lag Nullable(Float64),
    lagging_tables Nullable(UInt64),
    readonly_tables Nullable(UInt64),
    total_queue_size Nullable(UInt64),

    -- Storage (high)
    total_disk_usage Nullable(UInt64),
    total_rows_stored Nullable(UInt64),
    avg_compression_ratio Nullable(Float64),
    fragmented_tables Nullable(UInt64),
    reclaimable_space Nullable(UInt64),

    -- ZooKeeper (high)
    zk_active_connections Nullable(UInt64),
    zk_max_replication_lag_seconds Nullable(Float64),
    zk_detached_replicas Nullable(UInt64),
    zk_readonly_replicas Nullable(UInt64),

    -- JSON blobs (high)
    activity_info_json String DEFAULT '{}' CODEC(ZSTD(3)),
    connection_info_json String DEFAULT '{}' CODEC(ZSTD(3)),
    query_info_json String DEFAULT '{}' CODEC(ZSTD(3)),
    cluster_info_json String DEFAULT '{}' CODEC(ZSTD(3)),
    replication_info_json String DEFAULT '{}' CODEC(ZSTD(3)),
    storage_info_json String DEFAULT '{}' CODEC(ZSTD(3)),
    zookeeper_info_json String DEFAULT '{}' CODEC(ZSTD(3)),

    -- Merges (medium)
    running_merges Nullable(UInt64),
    queued_merges Nullable(UInt64),
    avg_merge_throughput Nullable(Float64),

    -- Mutations (medium)
    active_mutations Nullable(UInt64),
    failed_mutations Nullable(UInt64),
    stuck_mutations Nullable(UInt64),

    -- JSON blobs (medium)
    merge_info_json String DEFAULT '{}' CODEC(ZSTD(3)),
    mutation_info_json String DEFAULT '{}' CODEC(ZSTD(3)),
    part_info_json String DEFAULT '[]' CODEC(ZSTD(3)),
    database_stats_json String DEFAULT '[]' CODEC(ZSTD(3)),
    table_info_json String DEFAULT '[]' CODEC(ZSTD(3)),

    -- JSON blobs (low)
    dictionary_info_json String DEFAULT '[]' CODEC(ZSTD(3)),
    settings_info_json String DEFAULT '{}' CODEC(ZSTD(3))
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(snapshot_time)
ORDER BY (organization_uuid, endpoint_uuid, frequency, snapshot_time)
TTL toDateTime(snapshot_time) + INTERVAL 90 DAY;
