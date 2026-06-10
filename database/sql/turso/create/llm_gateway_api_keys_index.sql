CREATE INDEX IF NOT EXISTS idx_llm_gateway_api_keys_org
    ON llm_gateway_api_keys(organization_uuid);

CREATE INDEX IF NOT EXISTS idx_llm_gateway_api_keys_endpoint
    ON llm_gateway_api_keys(endpoint_uuid);
