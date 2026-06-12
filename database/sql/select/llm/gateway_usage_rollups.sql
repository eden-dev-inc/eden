SELECT
    organization_uuid,
    consumer_kind,
    consumer_id,
    month_bucket,
    endpoint_uuid,
    request_count,
    prompt_tokens,
    completion_tokens,
    total_tokens,
    estimated_cost_micros,
    cache_hit_count,
    kv_cache_hit_count,
    rate_limited_count,
    updated_at
FROM llm_gateway_usage_rollups
WHERE organization_uuid = $1
  AND month_bucket = $2
ORDER BY estimated_cost_micros DESC, total_tokens DESC
LIMIT $3;
