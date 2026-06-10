-- Per-user command rollups table
-- Aggregated metrics broken down by user for dashboard "I/O by user" views
-- Populated by snapshot-based flush from UserRollupAccumulator

CREATE TABLE IF NOT EXISTS analytics.user_rollups
(
    -- Time window
    window_start DateTime,
    window_secs UInt16,

    -- Identity
    organization_uuid String CODEC(ZSTD(3)),
    endpoint_uuid String CODEC(ZSTD(3)),
    user_uuid String CODEC(ZSTD(3)),

    -- Protocol & command
    protocol LowCardinality(String),
    command LowCardinality(String),

    -- Counters
    request_count SimpleAggregateFunction(sum, UInt64),
    error_count SimpleAggregateFunction(sum, UInt64),

    -- Size aggregates (I/O by user)
    request_bytes_sum SimpleAggregateFunction(sum, UInt64),
    response_bytes_sum SimpleAggregateFunction(sum, UInt64),

    -- Latency aggregate
    latency_sum SimpleAggregateFunction(sum, UInt64)
)
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(window_start)
ORDER BY (organization_uuid, endpoint_uuid, user_uuid, window_start, window_secs, protocol, command)
TTL window_start + INTERVAL 7 DAY;
