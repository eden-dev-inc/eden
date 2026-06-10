CREATE TABLE IF NOT EXISTS agent_metrics_hourly (
    agent_id UUID NOT NULL,
    metric_hour TIMESTAMPTZ NOT NULL,
    success_count INTEGER NOT NULL DEFAULT 0,
    failure_count INTEGER NOT NULL DEFAULT 0,
    avg_duration_ms BIGINT,
    p95_duration_ms BIGINT,
    total_tokens INTEGER DEFAULT 0,
    total_cost_usd NUMERIC(12, 6) DEFAULT 0,
    PRIMARY KEY (agent_id, metric_hour)
);
