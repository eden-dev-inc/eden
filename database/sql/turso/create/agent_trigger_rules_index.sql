CREATE INDEX IF NOT EXISTS agent_trigger_rules_source_idx
    ON agent_trigger_rules (source_id, is_active, created_at DESC);
