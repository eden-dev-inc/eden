CREATE TABLE IF NOT EXISTS llm_gateway_route_rollups (
    organization_uuid UUID NOT NULL REFERENCES organizations(uuid) ON DELETE CASCADE,
    endpoint_uuid UUID NOT NULL REFERENCES endpoints(uuid) ON DELETE CASCADE,
    provider TEXT NOT NULL,
    model TEXT NOT NULL,
    route_class TEXT NOT NULL DEFAULT 'default',
    success_count BIGINT NOT NULL DEFAULT 0,
    error_count BIGINT NOT NULL DEFAULT 0,
    total_latency_ms BIGINT NOT NULL DEFAULT 0,
    min_latency_ms BIGINT NOT NULL DEFAULT 0,
    max_latency_ms BIGINT NOT NULL DEFAULT 0,
    total_output_tokens BIGINT NOT NULL DEFAULT 0,
    total_duration_ms BIGINT NOT NULL DEFAULT 0,
    first_observed_at TIMESTAMP WITH TIME ZONE NOT NULL,
    last_observed_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    PRIMARY KEY (organization_uuid, endpoint_uuid, provider, model, route_class)
);
