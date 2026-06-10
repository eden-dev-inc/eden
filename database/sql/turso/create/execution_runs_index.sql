CREATE INDEX IF NOT EXISTS idx_execution_runs_org_created
    ON execution_runs (organization_uuid, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_execution_runs_state_updated
    ON execution_runs (state, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_execution_runs_agent_created
    ON execution_runs (agent_id, created_at DESC);
