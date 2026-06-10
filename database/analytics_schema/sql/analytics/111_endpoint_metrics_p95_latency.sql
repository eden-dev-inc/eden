ALTER TABLE analytics.endpoint_metrics
    ADD COLUMN IF NOT EXISTS latency_p95_us Nullable(UInt64) AFTER latency_p50_us;
