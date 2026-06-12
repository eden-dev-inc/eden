ALTER TABLE analytics.llm_operation_events
    ADD COLUMN IF NOT EXISTS organization_uuid String AFTER timestamp
