-- API usage history table
-- Tracks all API requests per user for usage monitoring and billing
-- Provides detailed request logs for debugging and auditing

CREATE TABLE IF NOT EXISTS analytics.api_usage_history
(
    -- Time
    request_time DateTime64(6, 'UTC'),

    -- Identity
    organization_uuid String CODEC(ZSTD(3)),
    user_uuid String CODEC(ZSTD(3)),
    user_id String CODEC(ZSTD(3)),
    session_uuid Nullable(String) CODEC(ZSTD(3)),

    -- Request details
    request_id String CODEC(ZSTD(3)),
    http_method LowCardinality(String),
    http_path String CODEC(ZSTD(3)),
    http_status UInt16,

    -- Target resource (if applicable)
    endpoint_uuid Nullable(String) CODEC(ZSTD(3)),
    endpoint_id Nullable(String) CODEC(ZSTD(3)),

    -- Performance
    latency_us UInt64,

    -- Size
    request_bytes UInt64 DEFAULT 0,
    response_bytes UInt64 DEFAULT 0,

    -- Client info
    client_ip String CODEC(ZSTD(3)),
    user_agent String CODEC(ZSTD(3)),

    -- Error info (if any)
    error_code Nullable(String) CODEC(ZSTD(3)),
    error_message Nullable(String) CODEC(ZSTD(3))
)
ENGINE = MergeTree
PARTITION BY toDate(request_time)
ORDER BY (organization_uuid, user_uuid, request_time)
TTL toDateTime(request_time) + INTERVAL 90 DAY;  -- 90 day retention for API logs
