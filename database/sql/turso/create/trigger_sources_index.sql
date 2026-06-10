CREATE INDEX IF NOT EXISTS trigger_sources_org_idx
    ON trigger_sources (organization_uuid, is_active, created_at DESC);
