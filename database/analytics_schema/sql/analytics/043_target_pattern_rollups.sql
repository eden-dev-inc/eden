-- Per-target-pattern cost rollups from sampled events
-- Approximate cost attribution by namespace or object family (sampled-only data)

CREATE TABLE IF NOT EXISTS analytics.target_pattern_rollups
(
    -- Time window
    window_start DateTime,
    window_secs UInt16,

    -- Identity
    organization_uuid String CODEC(ZSTD(3)),
    endpoint_uuid String CODEC(ZSTD(3)),

    -- Protocol & service
    protocol LowCardinality(String),
    service String DEFAULT '' CODEC(ZSTD(3)),

    -- Target pattern & command
    target_pattern String CODEC(ZSTD(3)),
    command LowCardinality(String),

    -- Counters (SimpleAggregateFunction for correct SummingMergeTree merges)
    request_count SimpleAggregateFunction(sum, UInt64),
    error_count SimpleAggregateFunction(sum, UInt64),
    cost_sum SimpleAggregateFunction(sum, UInt64),
    bandwidth_cost SimpleAggregateFunction(sum, UInt64),
    latency_sum SimpleAggregateFunction(sum, UInt64),
    read_count SimpleAggregateFunction(sum, UInt64),
    write_count SimpleAggregateFunction(sum, UInt64),
    ttl_present_count SimpleAggregateFunction(sum, UInt64),
    value_bytes_sum SimpleAggregateFunction(sum, UInt64)
)
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(window_start)
ORDER BY (organization_uuid, endpoint_uuid, window_start, window_secs, protocol, target_pattern, command)
TTL window_start + INTERVAL 30 DAY;
