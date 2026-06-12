ALTER TABLE analytics.llm_operation_rollups
    ADD COLUMN IF NOT EXISTS organization_uuid String AFTER timestamp
