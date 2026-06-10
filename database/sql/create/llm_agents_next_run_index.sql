CREATE INDEX IF NOT EXISTS idx_llm_agents_next_run
    ON llm_agents (next_run_at)
    WHERE status = 'active'
