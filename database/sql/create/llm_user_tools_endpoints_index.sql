CREATE INDEX IF NOT EXISTS idx_llm_user_tools_endpoints_org
    ON llm_user_tools_endpoints (organization_uuid);
