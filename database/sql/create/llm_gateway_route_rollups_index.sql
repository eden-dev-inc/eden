CREATE INDEX IF NOT EXISTS idx_llm_gateway_route_rollups_org_last_seen
    ON llm_gateway_route_rollups (organization_uuid, last_observed_at DESC);
