-- Per-shape rollups for MongoDB query patterns (sampled-only data)
-- Top-N shapes per endpoint by frequency, flushed every rollup interval

CREATE TABLE IF NOT EXISTS analytics.mongo_shape_rollups
(
    -- Time window
    window_start DateTime,
    window_secs UInt16,

    -- Identity
    organization_uuid String CODEC(ZSTD(3)),
    endpoint_uuid String CODEC(ZSTD(3)),

    -- Shape key
    command LowCardinality(String),
    namespace String CODEC(ZSTD(3)),
    pipeline_stages String CODEC(ZSTD(3)),
    filter_shape String CODEC(ZSTD(3)),
    sort_fields String CODEC(ZSTD(3)),
    projection_fields String CODEC(ZSTD(3)),
    hint String CODEC(ZSTD(3)),
    skip_value SimpleAggregateFunction(max, UInt64),
    max_time_ms SimpleAggregateFunction(max, Nullable(UInt64)),
    has_javascript SimpleAggregateFunction(max, UInt8),
    max_in_array_len SimpleAggregateFunction(max, Nullable(UInt64)),
    read_concern LowCardinality(String),
    write_concern LowCardinality(String),
    latency_max SimpleAggregateFunction(max, UInt64),

    -- Counters (SimpleAggregateFunction for correct SummingMergeTree merges)
    count SimpleAggregateFunction(sum, UInt64),
    error_count SimpleAggregateFunction(sum, UInt64),
    total_latency_us SimpleAggregateFunction(sum, UInt64)
)
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(window_start)
ORDER BY (organization_uuid, endpoint_uuid, window_start, window_secs, command, namespace, pipeline_stages, filter_shape, sort_fields, projection_fields, hint)
TTL window_start + INTERVAL 30 DAY;
