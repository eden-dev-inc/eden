CREATE UNIQUE INDEX IF NOT EXISTS idx_llm_agent_runs_unique_running
    ON llm_agent_runs (agent_id)
    WHERE run_status = 'running'
