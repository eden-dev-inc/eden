CREATE TABLE IF NOT EXISTS analytics.llm_operation_events
(
    organization_uuid String,
    endpoint_uuid String,
    provider LowCardinality(String),
    model LowCardinality(String),
    operation LowCardinality(String),
    traffic_source LowCardinality(String),
    consumer_id String DEFAULT '',
    credential_id String DEFAULT '',
    timestamp DateTime64(3),
    prompt_tokens UInt32,
    completion_tokens UInt32,
    total_tokens UInt32,
    request_bytes UInt32 DEFAULT 0,
    response_bytes UInt32 DEFAULT 0,
    estimated_cost_micros UInt64,
    latency_ms UInt64,
    success UInt8,
    error_message String DEFAULT '',
    streaming UInt8,
    tool_used UInt8,
    tool_call_count UInt32,
    message_count UInt32,
    policy_action LowCardinality(String),
    pii_detected UInt8,
    pii_types Array(String),
    prompt_fingerprint String DEFAULT '',
    agent_uuid String DEFAULT ''
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (organization_uuid, endpoint_uuid, timestamp)
TTL timestamp + INTERVAL 30 DAY
