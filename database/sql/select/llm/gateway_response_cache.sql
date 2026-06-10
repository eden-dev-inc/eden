SELECT
    cache_key,
    organization_uuid,
    endpoint_uuid,
    key_id,
    provider,
    model,
    request_hash,
    prompt_fingerprint,
    response_json,
    prompt_tokens,
    completion_tokens,
    total_tokens,
    estimated_cost_micros,
    hit_count,
    created_at,
    updated_at,
    expires_at,
    last_hit_at
FROM llm_gateway_response_cache
WHERE organization_uuid = $1
  AND cache_key = $2
  AND expires_at > $3;
