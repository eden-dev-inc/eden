CREATE TABLE IF NOT EXISTS analytics.llm_price_snapshots
(
    fetched_at DateTime,
    provider LowCardinality(String),
    model String,
    source LowCardinality(String),
    input_micros_per_million UInt64,
    output_micros_per_million UInt64
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(fetched_at)
ORDER BY (provider, model, source, fetched_at)
TTL fetched_at + INTERVAL 30 DAY
