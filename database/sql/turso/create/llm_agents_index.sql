CREATE INDEX IF NOT EXISTS idx_llm_agents_org_status
    ON llm_agents (organization_uuid, status)
