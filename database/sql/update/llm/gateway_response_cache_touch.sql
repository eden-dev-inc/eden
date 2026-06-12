UPDATE llm_gateway_response_cache
SET
    hit_count = hit_count + 1,
    last_hit_at = $3,
    updated_at = $3
WHERE organization_uuid = $1
  AND cache_key = $2
  AND expires_at > $3;
