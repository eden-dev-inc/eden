CREATE TABLE IF NOT EXISTS llm_gateway_usage_rollups (
    organization_uuid TEXT NOT NULL REFERENCES organizations(uuid) ON DELETE CASCADE,
    consumer_kind TEXT NOT NULL,
    consumer_id TEXT NOT NULL,
    month_bucket INTEGER NOT NULL,
    endpoint_uuid TEXT,
    request_count INTEGER NOT NULL DEFAULT 0,
    prompt_tokens INTEGER NOT NULL DEFAULT 0,
    completion_tokens INTEGER NOT NULL DEFAULT 0,
    total_tokens INTEGER NOT NULL DEFAULT 0,
    estimated_cost_micros INTEGER NOT NULL DEFAULT 0,
    cache_hit_count INTEGER NOT NULL DEFAULT 0,
    kv_cache_hit_count INTEGER NOT NULL DEFAULT 0,
    rate_limited_count INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (organization_uuid, consumer_kind, consumer_id, month_bucket)
);
