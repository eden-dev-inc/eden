-- PII aggregate table: time-bucketed PII detection counts per endpoint.
-- Replaces individual pii_detections rows with aggregate summaries.
-- One row per (endpoint, pii_type) per 60-second window.

CREATE TABLE IF NOT EXISTS analytics.pii_aggregate
(
    -- Time window
    window_start DateTime,
    window_secs UInt16,

    -- Identity
    organization_uuid String CODEC(ZSTD(3)),
    endpoint_uuid String CODEC(ZSTD(3)),

    -- PII classification
    pii_type LowCardinality(String),

    -- Aggregate counts
    detection_count UInt64,

    -- Representative samples (one per window, redacted)
    representative_key_pattern String CODEC(ZSTD(3)),
    representative_redacted_sample String CODEC(ZSTD(3))
)
ENGINE = SummingMergeTree((detection_count))
PARTITION BY toYYYYMM(window_start)
ORDER BY (organization_uuid, endpoint_uuid, window_start, pii_type)
TTL window_start + INTERVAL 30 DAY;
