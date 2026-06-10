-- Exemplar table: representative samples of slow queries, errors, and dangerous commands.
-- One row per (endpoint, exemplar_type, command_id) per flush window.

CREATE TABLE IF NOT EXISTS analytics.exemplars
(
    -- Time window
    window_start DateTime,
    window_secs UInt16,

    -- Identity
    organization_uuid String CODEC(ZSTD(3)),
    endpoint_uuid String CODEC(ZSTD(3)),

    -- Exemplar classification
    exemplar_type LowCardinality(String),
    command_id UInt16,
    command_name LowCardinality(String),

    -- Representative sample
    latency_us UInt64,
    key_pattern String CODEC(ZSTD(3)),
    redacted_args String CODEC(ZSTD(3)),
    sample_timestamp DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(sample_timestamp)
PARTITION BY toYYYYMM(window_start)
ORDER BY (organization_uuid, endpoint_uuid, window_start, exemplar_type, command_id)
TTL window_start + INTERVAL 30 DAY;
