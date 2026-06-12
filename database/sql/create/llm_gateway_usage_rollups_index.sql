CREATE INDEX IF NOT EXISTS idx_llm_gateway_usage_rollups_org_month
    ON llm_gateway_usage_rollups (organization_uuid, month_bucket, consumer_kind);
