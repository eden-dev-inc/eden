CREATE UNIQUE INDEX IF NOT EXISTS llm_credentials_org_label_idx
    ON llm_credentials (organization_uuid, LOWER(label))
    WHERE deleted_at IS NULL AND label IS NOT NULL;
