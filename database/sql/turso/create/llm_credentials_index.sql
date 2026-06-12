CREATE INDEX IF NOT EXISTS llm_credentials_org_idx
    ON llm_credentials (organization_uuid)
    WHERE deleted_at IS NULL;
