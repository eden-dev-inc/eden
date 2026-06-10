CREATE TABLE IF NOT EXISTS agent_metrics_hourly (
    agent_id TEXT NOT NULL,
    metric_hour TEXT NOT NULL,
    success_count INTEGER NOT NULL DEFAULT 0,
    failure_count INTEGER NOT NULL DEFAULT 0,
    avg_duration_ms INTEGER,
    p95_duration_ms INTEGER,
    total_tokens INTEGER DEFAULT 0,
    total_cost_usd REAL DEFAULT 0,
    PRIMARY KEY (agent_id, metric_hour)
);
