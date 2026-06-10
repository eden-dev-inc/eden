-- Daily anti-pattern summary table

CREATE TABLE IF NOT EXISTS analytics.antipattern_daily
(
    day Date,
    organization_uuid String CODEC(ZSTD(3)),
    endpoint_uuid String CODEC(ZSTD(3)),
    pattern_type LowCardinality(String),

    occurrence_count SimpleAggregateFunction(sum, UInt64),
    unique_connections AggregateFunction(uniqExact, UInt64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(day)
ORDER BY (organization_uuid, endpoint_uuid, day, pattern_type)
TTL day + INTERVAL 180 DAY
