CREATE TABLE IF NOT EXISTS analytics.anti_patterns
(
    detected_at DateTime64(6, 'UTC'),
    organization_uuid String,
    endpoint_uuid String,
    protocol LowCardinality(String),
    pattern_type LowCardinality(String),
    details String CODEC(ZSTD(3)),
    connection_id UInt64,
    occurrence_count UInt32
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(detected_at)
ORDER BY (organization_uuid, endpoint_uuid, protocol, detected_at, pattern_type)
TTL toDateTime(detected_at) + INTERVAL 30 DAY
