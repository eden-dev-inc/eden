CREATE TABLE IF NOT EXISTS analytics.llm_operation_rollups
(
    organization_uuid String,
    endpoint_uuid String,
    provider LowCardinality(String),
    model LowCardinality(String),
    operation LowCardinality(String),
    traffic_source LowCardinality(String),
    consumer_id String DEFAULT '',
    credential_id String DEFAULT '',
    timestamp DateTime,
    request_count SimpleAggregateFunction(sum, UInt64),
    prompt_tokens_sum SimpleAggregateFunction(sum, UInt64),
    completion_tokens_sum SimpleAggregateFunction(sum, UInt64),
    total_tokens_sum SimpleAggregateFunction(sum, UInt64),
    estimated_cost_micros_sum SimpleAggregateFunction(sum, UInt64),
    latency_sum_ms SimpleAggregateFunction(sum, UInt64),
    error_count SimpleAggregateFunction(sum, UInt64),
    tool_use_count SimpleAggregateFunction(sum, UInt64),
    pii_detected_count SimpleAggregateFunction(sum, UInt64),
    streaming_count SimpleAggregateFunction(sum, UInt64)
)
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (organization_uuid, endpoint_uuid, provider, model, operation, traffic_source, consumer_id, credential_id, timestamp)
TTL timestamp + INTERVAL 90 DAY
