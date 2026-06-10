CREATE TABLE IF NOT EXISTS analytics.anomaly_transitions
(
    transition_time DateTime64(3, 'UTC'),
    organization_uuid String CODEC(ZSTD(3)),
    endpoint_uuid String CODEC(ZSTD(3)),
    detector LowCardinality(String),
    from_level LowCardinality(String),
    to_level LowCardinality(String)
)
ENGINE = MergeTree
PARTITION BY toDate(transition_time)
ORDER BY (organization_uuid, endpoint_uuid, transition_time, detector)
TTL toDateTime(transition_time) + INTERVAL 90 DAY;
