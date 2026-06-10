CREATE TABLE IF NOT EXISTS analytics.audit_trail
(
    event_time DateTime64(6, 'UTC'),
    organization_uuid String CODEC(ZSTD(3)),
    endpoint_uuid String CODEC(ZSTD(3)),
    service String CODEC(ZSTD(3)),
    command LowCardinality(String),
    key Nullable(String) CODEC(ZSTD(3)),
    args_hash UInt64,
    latency_us UInt64,
    success UInt8,
    client_ip Nullable(String) CODEC(ZSTD(3)),
    connection_id UInt64 DEFAULT 0
)
ENGINE = MergeTree
PARTITION BY toDate(event_time)
ORDER BY (organization_uuid, endpoint_uuid, event_time)
TTL toDateTime(event_time) + INTERVAL 365 DAY  -- 1 year retention for compliance
