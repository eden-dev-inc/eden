CREATE TABLE IF NOT EXISTS analytics.blocked_commands
(
    event_time DateTime64(6, 'UTC'),
    organization_uuid String CODEC(ZSTD(3)),
    endpoint_uuid String CODEC(ZSTD(3)),
    command LowCardinality(String),
    reason String CODEC(ZSTD(3)),
    severity UInt8,
    service String CODEC(ZSTD(3)),
    client_ip Nullable(String) CODEC(ZSTD(3))
)
ENGINE = MergeTree
PARTITION BY toDate(event_time)
ORDER BY (organization_uuid, endpoint_uuid, event_time)
TTL toDateTime(event_time) + INTERVAL 90 DAY
