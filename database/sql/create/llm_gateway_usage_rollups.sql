CREATE TABLE IF NOT EXISTS llm_gateway_usage_rollups (
    organization_uuid UUID NOT NULL REFERENCES organizations(uuid) ON DELETE CASCADE,
    consumer_kind TEXT NOT NULL,
    consumer_id TEXT NOT NULL,
    month_bucket INTEGER NOT NULL,
    endpoint_uuid UUID,
    request_count BIGINT NOT NULL DEFAULT 0,
    prompt_tokens BIGINT NOT NULL DEFAULT 0,
    completion_tokens BIGINT NOT NULL DEFAULT 0,
    total_tokens BIGINT NOT NULL DEFAULT 0,
    estimated_cost_micros BIGINT NOT NULL DEFAULT 0,
    cache_hit_count BIGINT NOT NULL DEFAULT 0,
    kv_cache_hit_count BIGINT NOT NULL DEFAULT 0,
    rate_limited_count BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    PRIMARY KEY (organization_uuid, consumer_kind, consumer_id, month_bucket)
);
