CREATE INDEX IF NOT EXISTS idx_llm_agent_runs_status
    ON llm_agent_runs (agent_id, run_status)
    WHERE run_status = 'running'
