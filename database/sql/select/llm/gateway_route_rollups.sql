SELECT
    organization_uuid,
    endpoint_uuid,
    provider,
    model,
    route_class,
    success_count,
    error_count,
    total_latency_ms,
    min_latency_ms,
    max_latency_ms,
    total_output_tokens,
    total_duration_ms,
    first_observed_at,
    last_observed_at,
    updated_at
FROM llm_gateway_route_rollups
WHERE organization_uuid = $1
ORDER BY last_observed_at DESC
LIMIT $2;
