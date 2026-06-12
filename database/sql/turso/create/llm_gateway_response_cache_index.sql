CREATE INDEX IF NOT EXISTS idx_llm_gateway_response_cache_org_expires
    ON llm_gateway_response_cache (organization_uuid, expires_at);

CREATE INDEX IF NOT EXISTS idx_llm_gateway_response_cache_prompt
    ON llm_gateway_response_cache (organization_uuid, endpoint_uuid, prompt_fingerprint);
