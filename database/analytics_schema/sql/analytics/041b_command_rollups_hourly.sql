-- Hourly rollup of command_rollups for long-term trend analysis.
-- Populated automatically by ClickHouse materialized view.
-- Raw 60-second data has 7-day TTL; hourly data lives 90 days.

CREATE TABLE IF NOT EXISTS analytics.command_rollups_hourly
(
    window_start DateTime,
    window_secs SimpleAggregateFunction(any, UInt16) DEFAULT 3600,

    organization_uuid String CODEC(ZSTD(3)),
    endpoint_uuid String CODEC(ZSTD(3)),

    protocol LowCardinality(String),
    service String DEFAULT '' CODEC(ZSTD(3)),
    command_id UInt16,
    command LowCardinality(String),
    category LowCardinality(String),

    request_count UInt64,
    success_count UInt64,
    error_count UInt64,
    slow_count UInt64,
    dangerous_count UInt64,
    write_command_count UInt64,

    latency_sum UInt64,
    latency_sample_count UInt64,
    latency_sample_sum_us Float64,
    latency_sample_sumsq_us2 Float64,
    latency_min SimpleAggregateFunction(min, UInt64),
    latency_max SimpleAggregateFunction(max, UInt64),
    latency_histogram Array(UInt64) DEFAULT CAST([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], 'Array(UInt64)'),
    request_bytes_sum UInt64,
    response_bytes_sum UInt64,
    request_size_histogram Array(UInt64) DEFAULT CAST([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], 'Array(UInt64)'),
    response_size_histogram Array(UInt64) DEFAULT CAST([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], 'Array(UInt64)'),

    target_count_sum UInt64 DEFAULT 0,

    cost_sum UInt64 DEFAULT 0,

    cache_hit_count UInt64 DEFAULT 0,
    cache_miss_count UInt64 DEFAULT 0,
    redirect_count UInt64 DEFAULT 0,
    server_error_count UInt64 DEFAULT 0,
    client_error_count UInt64 DEFAULT 0,

    bandwidth_cost UInt64 DEFAULT 0
)
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(window_start)
ORDER BY (organization_uuid, endpoint_uuid, window_start, protocol, service, command_id, command, category)
TTL window_start + INTERVAL 90 DAY;
