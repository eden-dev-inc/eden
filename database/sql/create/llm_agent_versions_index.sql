CREATE INDEX IF NOT EXISTS llm_agent_versions_agent_version_idx
    ON llm_agent_versions (agent_id, version DESC);
