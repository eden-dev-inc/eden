CREATE TABLE IF NOT EXISTS llm_gateway_route_rollups (
    organization_uuid TEXT NOT NULL REFERENCES organizations(uuid) ON DELETE CASCADE,
    endpoint_uuid TEXT NOT NULL REFERENCES endpoints(uuid) ON DELETE CASCADE,
    provider TEXT NOT NULL,
    model TEXT NOT NULL,
    route_class TEXT NOT NULL DEFAULT 'default',
    success_count INTEGER NOT NULL DEFAULT 0,
    error_count INTEGER NOT NULL DEFAULT 0,
    total_latency_ms INTEGER NOT NULL DEFAULT 0,
    min_latency_ms INTEGER NOT NULL DEFAULT 0,
    max_latency_ms INTEGER NOT NULL DEFAULT 0,
    total_output_tokens INTEGER NOT NULL DEFAULT 0,
    total_duration_ms INTEGER NOT NULL DEFAULT 0,
    first_observed_at TEXT NOT NULL,
    last_observed_at TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (organization_uuid, endpoint_uuid, provider, model, route_class)
);
