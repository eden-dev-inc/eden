-- Protocol-neutral command rollups table
-- Aggregated metrics for dashboards and trending analysis
-- Populated by snapshot-based flush from EndpointAggregator

CREATE TABLE IF NOT EXISTS analytics.command_rollups
(
    -- Time window
    window_start DateTime,
    window_secs UInt16,

    -- Identity
    organization_uuid String CODEC(ZSTD(3)),
    endpoint_uuid String CODEC(ZSTD(3)),

    -- Protocol & command
    protocol LowCardinality(String),
    service String DEFAULT '' CODEC(ZSTD(3)),
    command_id UInt16,
    command LowCardinality(String),
    category LowCardinality(String),

    -- Counters (SimpleAggregateFunction for correct SummingMergeTree merges)
    request_count SimpleAggregateFunction(sum, UInt64),
    success_count SimpleAggregateFunction(sum, UInt64),
    error_count SimpleAggregateFunction(sum, UInt64),
    slow_count SimpleAggregateFunction(sum, UInt64),
    dangerous_count SimpleAggregateFunction(sum, UInt64),
    write_command_count SimpleAggregateFunction(sum, UInt64),

    -- Latency aggregates
    latency_sum SimpleAggregateFunction(sum, UInt64),
    latency_sample_count SimpleAggregateFunction(sum, UInt64),
    latency_sample_sum_us SimpleAggregateFunction(sum, Float64),
    latency_sample_sumsq_us2 SimpleAggregateFunction(sum, Float64),
    latency_min SimpleAggregateFunction(min, UInt64),
    latency_max SimpleAggregateFunction(max, UInt64),
    latency_histogram Array(UInt64) DEFAULT CAST([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], 'Array(UInt64)'),

    -- Size aggregates
    request_bytes_sum SimpleAggregateFunction(sum, UInt64),
    response_bytes_sum SimpleAggregateFunction(sum, UInt64),
    request_size_histogram Array(UInt64) DEFAULT CAST([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], 'Array(UInt64)'),
    response_size_histogram Array(UInt64) DEFAULT CAST([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], 'Array(UInt64)'),

    -- Target count (number of keys/targets per command, e.g., MGET key count)
    target_count_sum SimpleAggregateFunction(sum, UInt64) DEFAULT 0,

    -- Cost estimation (request_count * cost_weight per command)
    cost_sum SimpleAggregateFunction(sum, UInt64) DEFAULT 0,

    -- Cache and error-category splits
    cache_hit_count SimpleAggregateFunction(sum, UInt64) DEFAULT 0,
    cache_miss_count SimpleAggregateFunction(sum, UInt64) DEFAULT 0,
    redirect_count SimpleAggregateFunction(sum, UInt64) DEFAULT 0,
    server_error_count SimpleAggregateFunction(sum, UInt64) DEFAULT 0,
    client_error_count SimpleAggregateFunction(sum, UInt64) DEFAULT 0,

    -- Bandwidth cost: (request_bytes + response_bytes) / 1024
    bandwidth_cost SimpleAggregateFunction(sum, UInt64) DEFAULT 0
)
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(window_start)
ORDER BY (organization_uuid, endpoint_uuid, window_start, window_secs, protocol, service, command_id, command, category)
TTL window_start + INTERVAL 7 DAY;
