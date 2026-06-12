-- Protocol-neutral endpoint metrics snapshots
-- Periodic snapshots of per-endpoint aggregated metrics

CREATE TABLE IF NOT EXISTS analytics.endpoint_metrics
(
    -- Timing & Identity
    snapshot_time DateTime64(3, 'UTC'),
    organization_uuid String CODEC(ZSTD(3)),
    endpoint_uuid String CODEC(ZSTD(3)),

    -- Protocol info
    protocol LowCardinality(String),
    source LowCardinality(String),

    -- Throughput metrics
    ops_per_sec Nullable(Float64),
    total_commands Nullable(UInt64),
    total_errors Nullable(UInt64),
    slow_query_count Nullable(UInt64),

    -- Latency percentiles
    latency_p50_us Nullable(UInt64),
    latency_p95_us Nullable(UInt64),
    latency_p99_us Nullable(UInt64),
    latency_p999_us Nullable(UInt64),

    -- Rates
    error_rate Nullable(Float64),
    cache_hit_rate Nullable(Float64),

    -- Distribution JSON fields
    command_distribution String CODEC(ZSTD(3)),
    hot_keys String CODEC(ZSTD(3)),
    top_slow_commands String CODEC(ZSTD(3)),

    -- Size distributions
    request_size_distribution String CODEC(ZSTD(3)),
    response_size_distribution String CODEC(ZSTD(3)),

    -- Pipeline metrics
    pipeline_depth_distribution String CODEC(ZSTD(3)),
    avg_pipeline_depth Nullable(Float64),

    -- Transaction metrics
    transactions_committed Nullable(UInt64),
    transactions_aborted Nullable(UInt64),
    avg_transaction_size Nullable(Float64),
    transaction_size_distribution String CODEC(ZSTD(3)),

    -- TTL metrics
    ttl_distribution String CODEC(ZSTD(3)),
    keys_with_ttl_pct Nullable(Float64),

    -- Connection churn
    connections_opened Nullable(UInt64),
    connections_closed Nullable(UInt64),

    -- Protocol-specific extra metrics as JSON
    extra_metrics String CODEC(ZSTD(3)),

    -- Redis INFO / poll metrics
    used_memory_bytes Nullable(UInt64),
    peak_memory_bytes Nullable(UInt64),
    mem_fragmentation_ratio Nullable(Float32),
    connected_clients Nullable(UInt32),
    blocked_clients Nullable(UInt32),
    replication_role Nullable(String),
    used_cpu_sys Nullable(Float64),
    used_cpu_user Nullable(Float64),

    -- Typed columns (dual-write alongside JSON strings above)
    command_distribution_map Map(String, UInt64),
    pipeline_depth_samples UInt64 DEFAULT 0,
    pipeline_depth_sum_typed UInt64 DEFAULT 0,
    ttl_bucket_counts Array(UInt64),
    extra_metrics_map Map(String, Float64)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(snapshot_time)
ORDER BY (organization_uuid, endpoint_uuid, snapshot_time)
TTL toDateTime(snapshot_time) + INTERVAL 90 DAY;
