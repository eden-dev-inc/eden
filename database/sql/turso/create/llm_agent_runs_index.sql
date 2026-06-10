CREATE INDEX IF NOT EXISTS idx_llm_agent_runs_agent
    ON llm_agent_runs (agent_id, created_at DESC)
